from dataclasses import dataclass

from lake_db import initialize_schema


@dataclass(frozen=True, slots=True)
class ImportReceipt:
    receipt_id: str
    batch_id: str
    origins: tuple[tuple[int, str], ...]


def initialize_import_receipts(db):
    initialize_schema(db, '''
        CREATE TABLE IF NOT EXISTS import_receipts (
            batch_id TEXT PRIMARY KEY REFERENCES batches(batch_id),
            receipt_id TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS import_receipt_rows (
            batch_id TEXT NOT NULL REFERENCES import_receipts(batch_id),
            ordinal INTEGER NOT NULL,
            original_position TEXT NOT NULL,
            origin TEXT NOT NULL,
            PRIMARY KEY (batch_id, ordinal)
        );
    ''')


def create_import_receipt(batch_id, rows):
    return ImportReceipt(f'import:{batch_id}', batch_id,
                         tuple((row.position, row.origin) for row in rows))


def store_import_receipt(db, receipt):
    db.execute('INSERT INTO import_receipts VALUES (?, ?)',
               (receipt.batch_id, receipt.receipt_id))
    db.executemany('INSERT INTO import_receipt_rows VALUES (?, ?, ?, ?)', (
        (receipt.batch_id, ordinal, format(position, 'x'), origin)
        for ordinal, (position, origin) in enumerate(receipt.origins)
    ))


def get_import_receipt(db, batch_id):
    record = db.execute('SELECT receipt_id FROM import_receipts WHERE batch_id = ?',
                        (batch_id,)).fetchone()
    if record is None:
        raise LookupError(f'unknown import receipt for {batch_id!r}')
    origins = db.execute('''
        SELECT original_position, origin FROM import_receipt_rows
        WHERE batch_id = ? ORDER BY ordinal
    ''', (batch_id,))
    return ImportReceipt(record[0], batch_id, tuple((int(pos, 16), origin) for pos, origin in origins))
