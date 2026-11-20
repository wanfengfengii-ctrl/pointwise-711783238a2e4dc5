from dataclasses import dataclass

from intake.receipts import (ImportReceipt, create_import_receipt, get_import_receipt,
                             initialize_import_receipts, store_import_receipt)
from intake.rows import ObservationRow, decode_liters, encode_liters
from intake.validation import validate_batch
from lake_db import initialize_schema, write_transaction


@dataclass(frozen=True, slots=True)
class AcceptedBatch:
    batch_id: str
    rows: tuple[ObservationRow, ...]
    receipt: ImportReceipt


def initialize_batch_store(db):
    initialize_schema(db, '''
        CREATE TABLE IF NOT EXISTS batches (batch_id TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS observation_rows (
            batch_id TEXT NOT NULL REFERENCES batches(batch_id),
            ordinal INTEGER NOT NULL,
            original_position TEXT NOT NULL,
            origin TEXT NOT NULL,
            liters TEXT,
            quality TEXT NOT NULL,
            PRIMARY KEY (batch_id, ordinal),
            UNIQUE (batch_id, original_position)
        );
    ''')
    initialize_import_receipts(db)


def get_accepted_batch(db, batch_id):
    receipt = get_import_receipt(db, batch_id)
    records = db.execute('''
        SELECT original_position, origin, liters, quality FROM observation_rows
        WHERE batch_id = ? ORDER BY ordinal
    ''', (batch_id,))
    rows = tuple(ObservationRow(int(pos, 16), origin, decode_liters(liters), quality)
                 for pos, origin, liters, quality in records)
    return AcceptedBatch(batch_id, rows, receipt)


def ingest(db, batch_id, rows):
    ordered = validate_batch(batch_id, rows)
    with write_transaction(db):
        existing = db.execute('SELECT 1 FROM batches WHERE batch_id = ?', (batch_id,)).fetchone()
        if existing:
            accepted = get_accepted_batch(db, batch_id)
            if accepted.rows != ordered:
                raise ValueError('batch ID already has different observations')
            return accepted
        db.execute('INSERT INTO batches VALUES (?)', (batch_id,))
        db.executemany('INSERT INTO observation_rows VALUES (?, ?, ?, ?, ?, ?)', (
            (batch_id, ordinal, format(row.position, 'x'), row.origin,
             encode_liters(row.liters), row.quality)
            for ordinal, row in enumerate(ordered)
        ))
        receipt = create_import_receipt(batch_id, ordered)
        store_import_receipt(db, receipt)
        return AcceptedBatch(batch_id, ordered, receipt)
