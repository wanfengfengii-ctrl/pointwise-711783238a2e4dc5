import secrets
from dataclasses import dataclass, field, replace

from catalog.replicas import get_replica
from intake.validation import require_text
from lake_db import initialize_schema, write_transaction
from scan.requests import get_request


@dataclass(frozen=True, slots=True)
class ScanAssignment:
    task_id: str
    request_id: str
    revision_id: str
    replica_id: str
    predecessor_id: str | None
    seal_key: bytes = field(repr=False)

    def identity(self):
        return (self.task_id, self.request_id, self.revision_id, self.replica_id, self.predecessor_id)


@dataclass(frozen=True, slots=True)
class ScanTask:
    assignment: ScanAssignment
    state: str


def initialize_scan_tasks(db):
    initialize_schema(db, '''
        CREATE TABLE IF NOT EXISTS scan_tasks (
            task_id TEXT PRIMARY KEY,
            request_id TEXT NOT NULL REFERENCES scan_requests(request_id),
            revision_id TEXT NOT NULL REFERENCES frozen_revisions(revision_id),
            replica_id TEXT NOT NULL REFERENCES available_replicas(replica_id),
            predecessor_id TEXT UNIQUE REFERENCES scan_tasks(task_id),
            seal_key BLOB NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('assigned', 'running', 'lost'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS one_primary_task ON scan_tasks(request_id)
        WHERE predecessor_id IS NULL;
    ''')


def _find_task(db, task_id):
    record = db.execute('''
        SELECT task_id, request_id, revision_id, replica_id, predecessor_id, seal_key, state
        FROM scan_tasks WHERE task_id = ?
    ''', (task_id,)).fetchone()
    return None if record is None else ScanTask(ScanAssignment(*record[:-1]), record[-1])


def get_task(db, task_id):
    task = _find_task(db, task_id)
    if task is None:
        raise LookupError(f'unknown scan task {task_id!r}')
    return task


def store_task_assignment(db, obligation, replica_id, task_id, predecessor_id):
    require_text(task_id, 'task ID')
    get_replica(db, obligation.revision_id, replica_id)
    identity = (task_id, obligation.request_id, obligation.revision_id, replica_id, predecessor_id)
    task = _find_task(db, task_id)
    if task is not None:
        if task.assignment.identity() != identity:
            raise ValueError('task ID already belongs to a different assignment')
        return task
    other = db.execute('''
        SELECT task_id FROM scan_tasks WHERE request_id = ? AND predecessor_id IS ?
    ''', (obligation.request_id, predecessor_id)).fetchone()
    if other is not None:
        raise ValueError('this dispatch already has a task')
    assignment = ScanAssignment(*identity, secrets.token_bytes(32))
    db.execute('INSERT INTO scan_tasks VALUES (?, ?, ?, ?, ?, ?, ?)',
               (*identity, assignment.seal_key, 'assigned'))
    return ScanTask(assignment, 'assigned')


def assign_task(db, request_id, replica_id, task_id):
    with write_transaction(db):
        return store_task_assignment(db, get_request(db, request_id), replica_id, task_id, None)


def record_task_state(db, task_id, state):
    if state not in ('running', 'lost'):
        raise ValueError('expected running or lost')
    with write_transaction(db):
        task = get_task(db, task_id)
        if task.state == state:
            return task
        if task.state == 'lost':
            raise ValueError('a lost task cannot start another read')
        db.execute('UPDATE scan_tasks SET state = ? WHERE task_id = ?', (state, task_id))
        return replace(task, state=state)
