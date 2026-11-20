import json
from dataclasses import dataclass

from catalog.revisions import FrozenRow, decode_frozen_row, encode_frozen_row
from intake.rows import decode_liters, encode_liters
from lake_db import canonical_json, initialize_schema


@dataclass(frozen=True, slots=True)
class IssueReceipt:
    receipt_id: int
    result_id: int
    request_id: str


@dataclass(frozen=True, slots=True)
class PublishedResult:
    result_id: int
    request_id: str
    revision_id: str
    details: tuple[FrozenRow, ...]
    total_liters: int
    receipt: IssueReceipt


def initialize_publications(db):
    initialize_schema(db, '''
        CREATE TABLE IF NOT EXISTS publications (
            result_id INTEGER PRIMARY KEY,
            request_id TEXT NOT NULL UNIQUE REFERENCES receiving_requests(request_id),
            revision_id TEXT NOT NULL,
            details TEXT NOT NULL,
            total TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS issue_receipts (
            receipt_id INTEGER PRIMARY KEY,
            result_id INTEGER NOT NULL UNIQUE REFERENCES publications(result_id)
        );
    ''')


def save_publication(db, request_id, revision_id, details, total):
    result_id = db.execute('''
        INSERT INTO publications (request_id, revision_id, details, total) VALUES (?, ?, ?, ?)
    ''', (request_id, revision_id, canonical_json([encode_frozen_row(row) for row in details]),
          encode_liters(total))).lastrowid
    receipt_id = db.execute('INSERT INTO issue_receipts (result_id) VALUES (?)', (result_id,)).lastrowid
    return PublishedResult(result_id, request_id, revision_id, details, total,
                           IssueReceipt(receipt_id, result_id, request_id))


_PUBLICATION = '''
    SELECT p.result_id, p.request_id, p.revision_id, p.details, p.total, i.receipt_id
    FROM publications AS p JOIN issue_receipts AS i USING (result_id)
'''


def _restore(record):
    result_id, request_id, revision_id, details, total, receipt_id = record
    return PublishedResult(result_id, request_id, revision_id,
                           tuple(map(decode_frozen_row, json.loads(details))), decode_liters(total),
                           IssueReceipt(receipt_id, result_id, request_id))


def find_publication_for_request(db, request_id):
    row = db.execute(_PUBLICATION + ' WHERE p.request_id = ?', (request_id,)).fetchone()
    return None if row is None else _restore(row)


def get_publication(db, result_id):
    if type(result_id) is not int or not 0 < result_id < 2**63:
        raise LookupError('invalid result ID')
    row = db.execute(_PUBLICATION + ' WHERE p.result_id = ?', (result_id,)).fetchone()
    if row is None:
        raise LookupError(f'unknown result {result_id}')
    return _restore(row)
