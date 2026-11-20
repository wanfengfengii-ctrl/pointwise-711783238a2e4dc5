import json
from dataclasses import dataclass

from catalog.revisions import get_revision
from intake.validation import require_text, validate_quality_flag
from lake_db import canonical_json, initialize_schema, write_transaction


# Trusted handoff channel endpoints: the dispatch ledger issues authorizations and
# withdrawals, while a named receiving library rules on a dispatch and issues an
# unsigned release receipt.
SOURCE_SENDER = 'source'


@dataclass(frozen=True, slots=True)
class ScanRequest:
    request_id: str
    revision_id: str
    start: int
    stop: int
    quality: str


@dataclass(frozen=True, slots=True)
class ExclusiveDispatch:
    """The immutable terms of an exclusive dispatch.

    The binding fixes the dispatch number, the frozen revision, the named target
    library, the quality flag and a nonempty half-open range.  ``request_id`` is
    public so the ordinary scan entry points can scan the corresponding task.
    """

    dispatch_id: str
    request_id: str
    revision_id: str
    destination_id: str
    start: int
    stop: int
    quality: str

    def terms(self):
        return (self.revision_id, self.destination_id, self.start, self.stop, self.quality)


@dataclass(frozen=True, slots=True)
class UnsignedReceipt:
    """An unsigned release receipt issued by the ruling target library."""

    receipt_id: str
    dispatch_id: str
    request_id: str
    revision_id: str
    destination_id: str
    start: int
    stop: int
    quality: str

    def terms(self):
        return (self.dispatch_id, self.request_id, self.revision_id, self.destination_id,
                self.start, self.stop, self.quality)

    def binding(self):
        return ExclusiveDispatch(self.dispatch_id, self.request_id, self.revision_id,
                                 self.destination_id, self.start, self.stop, self.quality)

    def encode(self):
        return canonical_json([self.receipt_id, *self.terms()])

    @classmethod
    def decode(cls, body):
        return cls(*json.loads(body))


def initialize_scan_requests(db):
    initialize_schema(db, '''
        CREATE TABLE IF NOT EXISTS scan_requests (
            request_id TEXT PRIMARY KEY,
            revision_id TEXT NOT NULL REFERENCES frozen_revisions(revision_id),
            start INTEGER NOT NULL,
            stop INTEGER NOT NULL,
            quality TEXT NOT NULL,
            CHECK (start >= 0 AND stop > start)
        );
        CREATE TABLE IF NOT EXISTS exclusive_dispatches (
            dispatch_id TEXT PRIMARY KEY,
            request_id TEXT NOT NULL UNIQUE REFERENCES scan_requests(request_id),
            revision_id TEXT NOT NULL REFERENCES frozen_revisions(revision_id),
            destination_id TEXT NOT NULL,
            start INTEGER NOT NULL,
            stop INTEGER NOT NULL,
            quality TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('reserved', 'released')),
            release_receipt TEXT,
            CHECK (start >= 0 AND stop > start),
            CHECK ((state = 'released') = (release_receipt IS NOT NULL))
        );
    ''')


def request(db, revision_id, request_id, start, stop, quality):
    require_text(request_id, 'request ID')
    validate_quality_flag(quality)
    revision = get_revision(db, revision_id)
    if type(start) is not int or type(stop) is not int or not 0 <= start < stop <= len(revision.rows):
        raise ValueError('request must be a nonempty half-open range within the frozen revision')
    entry = ScanRequest(request_id, revision_id, start, stop, quality)
    with write_transaction(db):
        old = db.execute('SELECT * FROM scan_requests WHERE request_id = ?', (request_id,)).fetchone()
        if old is None:
            db.execute('INSERT INTO scan_requests VALUES (?, ?, ?, ?, ?)',
                       (request_id, revision_id, start, stop, quality))
        elif ScanRequest(*old) != entry:
            raise ValueError('request ID already has different obligations')
    return entry


def get_request(db, request_id):
    row = db.execute('''
        SELECT request_id, revision_id, start, stop, quality FROM scan_requests WHERE request_id = ?
    ''', (request_id,)).fetchone()
    if row is None:
        raise LookupError(f'unknown scan request {request_id!r}')
    return ScanRequest(*row)


_DISPATCH_COLUMNS = '''
    dispatch_id, request_id, revision_id, destination_id, start, stop, quality,
    state, release_receipt
'''


def _find_dispatch(db, dispatch_id):
    row = db.execute(f'SELECT {_DISPATCH_COLUMNS} FROM exclusive_dispatches WHERE dispatch_id = ?',
                     (dispatch_id,)).fetchone()
    if row is None:
        return None
    binding = ExclusiveDispatch(*row[:7])
    receipt = None if row[8] is None else UnsignedReceipt.decode(row[8])
    return binding, row[7], receipt


def get_exclusive_dispatch(db, dispatch_id):
    require_text(dispatch_id, 'dispatch ID')
    found = _find_dispatch(db, dispatch_id)
    if found is None:
        raise LookupError(f'unknown exclusive dispatch {dispatch_id!r}')
    return found[0]


def _check_binding(dispatch):
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
        raise ValueError('dispatch binding must carry a nonempty half-open range')


def reserve_exclusive(db, revision_id, dispatch_id, destination_id, start, stop, quality):
    """Register the whole range for one named destination or reject the dispatch."""
    require_text(dispatch_id, 'dispatch ID')
    require_text(destination_id, 'destination ID')
    validate_quality_flag(quality)
    revision = get_revision(db, revision_id)
    if type(start) is not int or type(stop) is not int or not 0 <= start < stop <= len(revision.rows):
        raise ValueError('exclusive range must be a nonempty half-open range within the frozen revision')
    binding = ExclusiveDispatch(dispatch_id, dispatch_id, revision_id, destination_id,
                                start, stop, quality)
    with write_transaction(db):
        found = _find_dispatch(db, dispatch_id)
        if found is not None:
            if found[0] != binding:
                raise ValueError('dispatch ID already has different terms')
            return found[0]
        overlap = db.execute('''
            SELECT dispatch_id FROM exclusive_dispatches
            WHERE revision_id = ? AND state = 'reserved' AND start < ? AND stop > ?
            ORDER BY start LIMIT 1
        ''', (revision_id, stop, start)).fetchone()
        if overlap is not None:
            raise ValueError(f'frozen positions are already held by dispatch {overlap[0]!r}')
        current = db.execute('''
            SELECT revision_id, start, stop, quality FROM scan_requests WHERE request_id = ?
        ''', (dispatch_id,)).fetchone()
        if current is not None:
            raise ValueError('request ID already belongs to an ordinary scan request')
        db.execute('INSERT INTO scan_requests VALUES (?, ?, ?, ?, ?)',
                   (dispatch_id, revision_id, start, stop, quality))
        db.execute('''
            INSERT INTO exclusive_dispatches
            (dispatch_id, request_id, revision_id, destination_id, start, stop, quality,
             state, release_receipt)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'reserved', NULL)
        ''', (dispatch_id, dispatch_id, revision_id, destination_id, start, stop, quality))
        return binding


def release_exclusive(db, dispatch_id, receipt, *, sender_id):
    """Release a dispatch's range using the exact unsigned receipt from its target."""
    require_text(dispatch_id, 'dispatch ID')
    if not isinstance(receipt, UnsignedReceipt):
        raise ValueError('expected an unsigned exclusive receipt')
    require_text(sender_id, 'sender ID')
    with write_transaction(db):
        found = _find_dispatch(db, dispatch_id)
        if found is None:
            raise ValueError(f'unknown exclusive dispatch {dispatch_id!r}')
        dispatch, state, accepted = found
        if receipt.binding() != dispatch:
            raise ValueError('receipt does not match the original dispatch terms')
        if sender_id != dispatch.destination_id:
            raise ValueError('release receipt must come from the dispatch target library')
        if state == 'released':
            if accepted is not None and accepted == receipt:
                return accepted
            raise ValueError('dispatch was released by a different receipt')
        db.execute('''
            UPDATE exclusive_dispatches SET state = 'released', release_receipt = ?
            WHERE dispatch_id = ?
        ''', (receipt.encode(), dispatch.dispatch_id))
        return receipt
