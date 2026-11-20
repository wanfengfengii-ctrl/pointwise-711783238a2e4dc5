import json
from dataclasses import dataclass

from intake.validation import require_text, validate_quality_flag
from lake_db import canonical_json, initialize_schema, write_transaction
from publication.store import find_publication_for_request
from scan.requests import SOURCE_SENDER, ExclusiveDispatch, ScanRequest, UnsignedReceipt
from scan.tasks import ScanAssignment


@dataclass(frozen=True, slots=True)
class ReceivingRequest:
    request: ScanRequest
    active_task_id: str


@dataclass(frozen=True, slots=True)
class ExclusiveAuthorization:
    """The final authorization the dispatch ledger sends to one named library.

    ``request_id`` mirrors ``dispatch_id`` so the public scan entry points can use
    the authorization's ``request_id`` directly.
    """

    dispatch_id: str
    request_id: str
    revision_id: str
    destination_id: str
    start: int
    stop: int
    quality: str

    def binding(self):
        return ExclusiveDispatch(self.dispatch_id, self.request_id, self.revision_id,
                                 self.destination_id, self.start, self.stop, self.quality)

    def encode(self):
        return canonical_json([self.dispatch_id, self.revision_id, self.destination_id,
                               self.start, self.stop, self.quality])

    @classmethod
    def decode(cls, body):
        dispatch_id, revision_id, destination_id, start, stop, quality = json.loads(body)
        return cls(dispatch_id, dispatch_id, revision_id, destination_id, start, stop, quality)


def authorize_exclusive(dispatch):
    """Issue the final authorization for an exclusive dispatch binding."""
    if not isinstance(dispatch, ExclusiveDispatch):
        raise ValueError('expected an exclusive dispatch binding')
    return ExclusiveAuthorization(dispatch.dispatch_id, dispatch.request_id, dispatch.revision_id,
                                  dispatch.destination_id, dispatch.start, dispatch.stop,
                                  dispatch.quality)


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
        CREATE TABLE IF NOT EXISTS receiving_destination (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            destination_id TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS exclusive_authorizations (
            dispatch_id TEXT PRIMARY KEY,
            request_id TEXT NOT NULL UNIQUE REFERENCES receiving_requests(request_id),
            revision_id TEXT NOT NULL,
            destination_id TEXT NOT NULL,
            start INTEGER NOT NULL,
            stop INTEGER NOT NULL,
            quality TEXT NOT NULL,
            authorize_body TEXT NOT NULL,
            CHECK (start >= 0 AND stop > start)
        );
        CREATE TABLE IF NOT EXISTS exclusive_withdrawals (
            dispatch_id TEXT PRIMARY KEY,
            request_id TEXT NOT NULL,
            receipt_body TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS exclusive_notices (
            dispatch_id TEXT PRIMARY KEY,
            request_id TEXT NOT NULL UNIQUE,
            revision_id TEXT NOT NULL,
            destination_id TEXT NOT NULL,
            start INTEGER NOT NULL,
            stop INTEGER NOT NULL,
            quality TEXT NOT NULL,
            CHECK (start >= 0 AND stop > start)
        );
    ''')


def bind_destination(db, destination_id):
    """Persist this library's name; reopening with the same name is idempotent."""
    require_text(destination_id, 'destination ID')
    with write_transaction(db):
        row = db.execute('SELECT destination_id FROM receiving_destination WHERE singleton = 1').fetchone()
        if row is None:
            db.execute('INSERT INTO receiving_destination VALUES (1, ?)', (destination_id,))
            return destination_id
        if row[0] != destination_id:
            raise ValueError('receiving library is bound to a different destination')
        return row[0]


def get_destination(db):
    row = db.execute('SELECT destination_id FROM receiving_destination WHERE singleton = 1').fetchone()
    if row is None:
        raise LookupError('receiving library has no bound destination')
    return row[0]


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


def find_exclusive_authorization(db, dispatch_id):
    row = db.execute('''
        SELECT dispatch_id, request_id, revision_id, destination_id, start, stop, quality
        FROM exclusive_authorizations WHERE dispatch_id = ?
    ''', (dispatch_id,)).fetchone()
    return None if row is None else ExclusiveAuthorization(*row)


def find_exclusive_withdrawal(db, dispatch_id):
    row = db.execute('SELECT receipt_body FROM exclusive_withdrawals WHERE dispatch_id = ?',
                     (dispatch_id,)).fetchone()
    return None if row is None else UnsignedReceipt.decode(row[0])


def get_exclusive_withdrawal(db, dispatch_id):
    receipt = find_exclusive_withdrawal(db, dispatch_id)
    if receipt is None:
        raise LookupError(f'unknown exclusive withdrawal {dispatch_id!r}')
    return receipt


def is_exclusive(db, request_id):
    row = db.execute('''
        SELECT 1 FROM exclusive_authorizations WHERE dispatch_id = ?
        UNION ALL
        SELECT 1 FROM exclusive_withdrawals WHERE dispatch_id = ?
        UNION ALL
        SELECT 1 FROM exclusive_notices WHERE dispatch_id = ?
    ''', (request_id, request_id, request_id)).fetchone()
    return row is not None


def find_exclusive_notice(db, dispatch_id):
    row = db.execute('''
        SELECT dispatch_id, request_id, revision_id, destination_id, start, stop, quality
        FROM exclusive_notices WHERE dispatch_id = ?
    ''', (dispatch_id,)).fetchone()
    return None if row is None else ExclusiveAuthorization(*row)


def notice_exclusive(db, dispatch, *, sender_id):
    """Record an early dispatch notice addressed to this named library.

    This is the public entry point for the other handoff messages exchanged
    before the final authorization.  It pins the dispatch to the target so a
    foreign library can neither receive it nor sign it.
    """
    require_text(sender_id, 'sender ID')
    dispatch = _validate_dispatch(dispatch)
    with write_transaction(db):
        destination = get_destination(db)
        if sender_id != SOURCE_SENDER:
            raise ValueError('dispatch notice must come from the dispatch ledger')
        if dispatch.destination_id != destination:
            raise ValueError('dispatch is bound to a different target library')
        notice = find_exclusive_notice(db, dispatch.dispatch_id)
        if notice is not None:
            if notice.binding() != dispatch:
                raise ValueError('conflicting exclusive dispatch notice')
            return notice
        authorized = find_exclusive_authorization(db, dispatch.dispatch_id)
        if authorized is not None and authorized.binding() != dispatch:
            raise ValueError('notice differs from the accepted authorization')
        known = _find_request(db, dispatch.request_id)
        if known is not None and known.request != ScanRequest(
                dispatch.request_id, dispatch.revision_id, dispatch.start,
                dispatch.stop, dispatch.quality):
            raise ValueError('notice terms differ from the registered request')
        db.execute('''
            INSERT INTO exclusive_notices
            (dispatch_id, request_id, revision_id, destination_id, start, stop, quality)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (dispatch.dispatch_id, dispatch.request_id, dispatch.revision_id,
              dispatch.destination_id, dispatch.start, dispatch.stop, dispatch.quality))
        return authorize_exclusive(dispatch)


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


def _insert_request(db, obligation, primary):
    db.execute('INSERT INTO receiving_requests VALUES (?, ?, ?, ?, ?, ?)',
               (obligation.request_id, obligation.revision_id, obligation.start,
                obligation.stop, obligation.quality, primary.task_id))
    _insert_assignment(db, primary)


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
        if is_exclusive(db, obligation.request_id):
            # An exclusive dispatch can only enter through its final authorization.
            raise ValueError('exclusive dispatch requires its final authorization')
        current = _find_request(db, obligation.request_id)
        known = find_assignment(db, primary.task_id)
        if current is not None:
            if current.request != obligation or known != primary:
                raise ValueError('conflicting request registration')
            return current
        if known is not None:
            raise ValueError('primary task ID already registered')
        _insert_request(db, obligation, primary)
        return ReceivingRequest(obligation, primary.task_id)


def _validate_authorization(authorization):
    if isinstance(authorization, ExclusiveDispatch):
        authorization = authorize_exclusive(authorization)
    if not isinstance(authorization, ExclusiveAuthorization):
        raise ValueError('expected an exclusive final authorization')
    require_text(authorization.dispatch_id, 'dispatch ID')
    require_text(authorization.request_id, 'request ID')
    require_text(authorization.revision_id, 'revision ID')
    require_text(authorization.destination_id, 'destination ID')
    validate_quality_flag(authorization.quality)
    if authorization.request_id != authorization.dispatch_id:
        raise ValueError('authorization request ID does not match its dispatch')
    if (type(authorization.start) is not int or type(authorization.stop) is not int
            or not 0 <= authorization.start < authorization.stop < 2**63):
        raise ValueError('invalid authorization range')
    return ScanRequest(authorization.request_id, authorization.revision_id,
                       authorization.start, authorization.stop, authorization.quality)


def accept_exclusive(db, authorization, primary, *, sender_id):
    """Accept the final authorization and its primary task at the named library."""
    require_text(sender_id, 'sender ID')
    obligation = _validate_authorization(authorization)
    _check_assignment(obligation, primary)
    if primary.predecessor_id is not None:
        raise ValueError('first assignment must be a primary')
    with write_transaction(db):
        destination = get_destination(db)
        if sender_id != SOURCE_SENDER:
            raise ValueError('final authorization must come from the dispatch ledger')
        if authorization.destination_id != destination:
            raise ValueError('authorization names a different target library')
        existing_auth = find_exclusive_authorization(db, authorization.dispatch_id)
        known = find_assignment(db, primary.task_id)
        current = _find_request(db, obligation.request_id)
        if existing_auth is not None:
            if existing_auth != authorization or known != primary:
                raise ValueError('conflicting exclusive authorization')
            if find_exclusive_withdrawal(db, authorization.dispatch_id) is not None:
                raise ValueError('dispatch was withdrawn after this authorization was accepted')
            return current
        notice = find_exclusive_notice(db, authorization.dispatch_id)
        if notice is not None and notice.binding() != authorization.binding():
            raise ValueError('authorization differs from the dispatch notice')
        if find_exclusive_withdrawal(db, authorization.dispatch_id) is not None:
            raise ValueError('dispatch was withdrawn before the authorization arrived')
        if find_publication_for_request(db, obligation.request_id) is not None:
            raise ValueError('request has already been signed')
        if known is not None:
            raise ValueError('primary task ID already registered')
        if current is not None:
            raise ValueError('request ID is already registered')
        _insert_request(db, obligation, primary)
        db.execute('''
            INSERT INTO exclusive_authorizations
            (dispatch_id, request_id, revision_id, destination_id, start, stop, quality, authorize_body)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (authorization.dispatch_id, authorization.request_id, authorization.revision_id,
              authorization.destination_id, authorization.start, authorization.stop,
              authorization.quality, authorization.encode()))
        return ReceivingRequest(obligation, primary.task_id)


def _validate_dispatch(dispatch):
    if not isinstance(dispatch, ExclusiveDispatch):
        raise ValueError('expected an exclusive dispatch binding')
    require_text(dispatch.dispatch_id, 'dispatch ID')
    require_text(dispatch.request_id, 'request ID')
    require_text(dispatch.revision_id, 'revision ID')
    require_text(dispatch.destination_id, 'destination ID')
    validate_quality_flag(dispatch.quality)
    if dispatch.request_id != dispatch.dispatch_id:
        raise ValueError('dispatch binding has a mismatched request ID')
    if (type(dispatch.start) is not int or type(dispatch.stop) is not int
            or not 0 <= dispatch.start < dispatch.stop < 2**63):
        raise ValueError('invalid withdrawal range')
    return dispatch


def withdraw_exclusive(db, dispatch, *, sender_id):
    """Rule on an unsigned whole-dispatch withdrawal and return the target's receipt."""
    require_text(sender_id, 'sender ID')
    dispatch = _validate_dispatch(dispatch)
    with write_transaction(db):
        destination = get_destination(db)
        if sender_id != SOURCE_SENDER:
            raise ValueError('withdrawal instruction must come from the dispatch ledger')
        if dispatch.destination_id != destination:
            raise ValueError('dispatch is bound to a different target library')
        if dispatch.request_id != dispatch.dispatch_id:
            raise ValueError('dispatch binding has a mismatched request ID')
        existing = find_exclusive_withdrawal(db, dispatch.dispatch_id)
        if existing is not None:
            if existing.binding() != dispatch:
                raise ValueError('conflicting exclusive withdrawal')
            return existing
        auth = find_exclusive_authorization(db, dispatch.dispatch_id)
        if auth is not None and auth.binding() != dispatch:
            raise ValueError('withdrawal differs from the accepted authorization')
        notice = find_exclusive_notice(db, dispatch.dispatch_id)
        if notice is not None and notice.binding() != dispatch:
            raise ValueError('withdrawal differs from the dispatch notice')
        if find_publication_for_request(db, dispatch.request_id) is not None:
            raise ValueError('a signed dispatch cannot be withdrawn')
        # A target that never saw this dispatch can rule on nothing but its own.
        known = _find_request(db, dispatch.request_id)
        if known is not None and known.request != ScanRequest(
                dispatch.request_id, dispatch.revision_id, dispatch.start,
                dispatch.stop, dispatch.quality):
            raise ValueError('withdrawal terms differ from the registered request')
        receipt = UnsignedReceipt(f'unsigned:{dispatch.dispatch_id}', dispatch.dispatch_id,
                                  dispatch.request_id, dispatch.revision_id, dispatch.destination_id,
                                  dispatch.start, dispatch.stop, dispatch.quality)
        db.execute('INSERT INTO exclusive_withdrawals (dispatch_id, request_id, receipt_body) '
                   'VALUES (?, ?, ?)',
                   (dispatch.dispatch_id, dispatch.request_id, receipt.encode()))
        return receipt


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
