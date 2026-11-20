from dataclasses import dataclass

from intake.rows import decode_liters, encode_liters
from lake_db import initialize_schema
from publication.store import IssueReceipt


@dataclass(frozen=True, slots=True)
class AuditRecord:
    audit_id: int
    result_id: int
    receipt: IssueReceipt
    row_count: int
    total_liters: int


def initialize_audit_ledger(db):
    initialize_schema(db, '''
        CREATE TABLE IF NOT EXISTS audit_records (
            audit_id INTEGER PRIMARY KEY,
            result_id INTEGER NOT NULL REFERENCES publications(result_id),
            receipt_id INTEGER NOT NULL REFERENCES issue_receipts(receipt_id),
            row_count INTEGER NOT NULL,
            total TEXT NOT NULL
        );
    ''')


def save_audit_record(db, publication, row_count, total):
    audit_id = db.execute('''
        INSERT INTO audit_records (result_id, receipt_id, row_count, total) VALUES (?, ?, ?, ?)
    ''', (publication.result_id, publication.receipt.receipt_id, row_count, encode_liters(total))).lastrowid
    return AuditRecord(audit_id, publication.result_id, publication.receipt, row_count, total)


def get_audit_record(db, audit_id):
    if type(audit_id) is not int or not 0 < audit_id < 2**63:
        raise LookupError('invalid audit ID')
    row = db.execute('''
        SELECT a.result_id, p.request_id, a.receipt_id, a.row_count, a.total
        FROM audit_records AS a JOIN publications AS p USING (result_id)
        WHERE a.audit_id = ?
    ''', (audit_id,)).fetchone()
    if row is None:
        raise LookupError(f'unknown audit record {audit_id}')
    result_id, request_id, receipt_id, count, total = row
    return AuditRecord(audit_id, result_id, IssueReceipt(receipt_id, result_id, request_id),
                       count, decode_liters(total))
