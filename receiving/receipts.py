from dataclasses import dataclass

from lake_db import initialize_schema
from scan.segments import SegmentEvidence, restore_segment, segment_body


@dataclass(frozen=True, slots=True)
class SegmentReceipt:
    receipt_id: int
    request_id: str
    task_id: str
    group_number: int


@dataclass(frozen=True, slots=True)
class AcceptedSegment:
    evidence: SegmentEvidence
    receipt: SegmentReceipt


def initialize_segment_receipts(db):
    initialize_schema(db, '''
        CREATE TABLE IF NOT EXISTS accepted_segments (
            receipt_id INTEGER PRIMARY KEY,
            request_id TEXT NOT NULL REFERENCES receiving_requests(request_id),
            task_id TEXT NOT NULL REFERENCES receiving_assignments(task_id),
            group_number INTEGER NOT NULL,
            body TEXT NOT NULL,
            seal BLOB NOT NULL,
            UNIQUE (request_id, task_id, group_number)
        );
    ''')


def _restore(record):
    receipt_id, request_id, task_id, group_number, body, seal = record
    return AcceptedSegment(restore_segment(body, seal),
                           SegmentReceipt(receipt_id, request_id, task_id, group_number))


def find_accepted_segment(db, request_id, task_id, group_number):
    row = db.execute('''
        SELECT receipt_id, request_id, task_id, group_number, body, seal FROM accepted_segments
        WHERE request_id = ? AND task_id = ? AND group_number = ?
    ''', (request_id, task_id, group_number)).fetchone()
    return None if row is None else _restore(row)


def store_accepted_segment(db, evidence):
    cursor = db.execute('''
        INSERT INTO accepted_segments (request_id, task_id, group_number, body, seal)
        VALUES (?, ?, ?, ?, ?)
    ''', (evidence.request_id, evidence.task_id, evidence.group_number,
          segment_body(evidence), evidence.seal))
    return SegmentReceipt(cursor.lastrowid, evidence.request_id, evidence.task_id, evidence.group_number)


def list_accepted_segments(db, request_id):
    records = db.execute('''
        SELECT receipt_id, request_id, task_id, group_number, body, seal FROM accepted_segments
        WHERE request_id = ? ORDER BY receipt_id
    ''', (request_id,))
    return tuple(map(_restore, records))
