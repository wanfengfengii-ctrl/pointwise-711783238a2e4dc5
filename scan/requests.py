from dataclasses import dataclass

from catalog.revisions import get_revision
from intake.validation import require_text, validate_quality_flag
from lake_db import initialize_schema, write_transaction


@dataclass(frozen=True, slots=True)
class ScanRequest:
    request_id: str
    revision_id: str
    start: int
    stop: int
    quality: str


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
