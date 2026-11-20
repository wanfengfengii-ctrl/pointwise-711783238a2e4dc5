from dataclasses import dataclass

from intake.validation import require_text, validate_quality_flag
from lake_db import initialize_schema, write_transaction
from publication.store import find_publication_for_request
from scan.requests import ScanRequest
from scan.tasks import ScanAssignment


@dataclass(frozen=True, slots=True)
class ReceivingRequest:
    request: ScanRequest
    active_task_id: str


def initialize_receiving_requests(db):
    initialize_schema(db, '''
        CREATE TABLE IF NOT EXISTS receiving_requests (
            request_id TEXT PRIMARY KEY,
            revision_id TEXT NOT NULL,
            start INTEGER NOT NULL,
            stop INTEGER NOT NULL,
            quality TEXT NOT NULL,
            active_task_id TEXT NOT NULL,
            CHECK (start >= 0 AND stop > start)
        );
        CREATE TABLE IF NOT EXISTS receiving_assignments (
            task_id TEXT PRIMARY KEY,
            request_id TEXT NOT NULL REFERENCES receiving_requests(request_id),
            revision_id TEXT NOT NULL,
            replica_id TEXT NOT NULL,
            predecessor_id TEXT REFERENCES receiving_assignments(task_id),
            seal_key BLOB NOT NULL
        );
    ''')


def _find_request(db, request_id):
    row = db.execute('''
        SELECT request_id, revision_id, start, stop, quality, active_task_id
        FROM receiving_requests WHERE request_id = ?
    ''', (request_id,)).fetchone()
    return None if row is None else ReceivingRequest(ScanRequest(*row[:-1]), row[-1])


def get_receiving_request(db, request_id):
    entry = _find_request(db, request_id)
    if entry is None:
        raise LookupError(f'unknown receiving request {request_id!r}')
    return entry


def find_assignment(db, task_id):
    row = db.execute('''
        SELECT task_id, request_id, revision_id, replica_id, predecessor_id, seal_key
        FROM receiving_assignments WHERE task_id = ?
    ''', (task_id,)).fetchone()
    return None if row is None else ScanAssignment(*row)


def _check_assignment(obligation, assignment):
    if (assignment.request_id, assignment.revision_id) != (obligation.request_id, obligation.revision_id):
        raise ValueError('assignment belongs to a different obligation')
    require_text(assignment.task_id, 'task ID')
    require_text(assignment.replica_id, 'replica ID')
    if type(assignment.seal_key) is not bytes or len(assignment.seal_key) != 32:
        raise ValueError('invalid assignment seal key')


def _insert_assignment(db, assignment):
    db.execute('INSERT INTO receiving_assignments VALUES (?, ?, ?, ?, ?, ?)',
               (*assignment.identity(), assignment.seal_key))


def register_request(db, obligation, primary):
    require_text(obligation.request_id, 'request ID')
    require_text(obligation.revision_id, 'revision ID')
    validate_quality_flag(obligation.quality)
    if (type(obligation.start) is not int or type(obligation.stop) is not int
            or not 0 <= obligation.start < obligation.stop < 2**63):
        raise ValueError('invalid receiving range')
    _check_assignment(obligation, primary)
    if primary.predecessor_id is not None:
        raise ValueError('first assignment must be a primary')
    with write_transaction(db):
        current = _find_request(db, obligation.request_id)
        known = find_assignment(db, primary.task_id)
        if current is not None:
            if current.request != obligation or known != primary:
                raise ValueError('conflicting request registration')
            return current
        if known is not None:
            raise ValueError('primary task ID already registered')
        db.execute('INSERT INTO receiving_requests VALUES (?, ?, ?, ?, ?, ?)',
                   (obligation.request_id, obligation.revision_id, obligation.start,
                    obligation.stop, obligation.quality, primary.task_id))
        _insert_assignment(db, primary)
        return ReceivingRequest(obligation, primary.task_id)


def register_assignment(db, assignment):
    with write_transaction(db):
        current = get_receiving_request(db, assignment.request_id)
        _check_assignment(current.request, assignment)
        known = find_assignment(db, assignment.task_id)
        if known is not None:
            if known != assignment:
                raise ValueError('conflicting assignment registration')
            return current
        if assignment.predecessor_id != current.active_task_id:
            raise ValueError('successor must replace the active assignment')
        if find_publication_for_request(db, assignment.request_id) is not None:
            raise ValueError('request has already been signed')
        active = find_assignment(db, current.active_task_id)
        if assignment.replica_id == active.replica_id:
            raise ValueError('successor must use a different replica')
        _insert_assignment(db, assignment)
        db.execute('UPDATE receiving_requests SET active_task_id = ? WHERE request_id = ?',
                   (assignment.task_id, assignment.request_id))
        return ReceivingRequest(current.request, assignment.task_id)
