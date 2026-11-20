from dataclasses import dataclass

from intake.rows import decode_liters, encode_liters
from lake_db import initialize_schema


@dataclass(frozen=True, slots=True)
class FrozenRow:
    position: int
    original_position: int
    origin: str
    liters: int | None
    quality: str


@dataclass(frozen=True, slots=True)
class FrozenRevision:
    revision_id: str
    batch_id: str
    rows: tuple[FrozenRow, ...]


def encode_frozen_row(row):
    return (row.position, format(row.original_position, 'x'), row.origin,
            encode_liters(row.liters), row.quality)


def decode_frozen_row(record):
    position, original_position, origin, liters, quality = record
    return FrozenRow(position, int(original_position, 16), origin, decode_liters(liters), quality)


def initialize_revisions(db):
    initialize_schema(db, '''
        CREATE TABLE IF NOT EXISTS frozen_revisions (
            revision_id TEXT PRIMARY KEY,
            batch_id TEXT NOT NULL UNIQUE REFERENCES batches(batch_id)
        );
        CREATE TABLE IF NOT EXISTS frozen_rows (
            revision_id TEXT NOT NULL REFERENCES frozen_revisions(revision_id),
            position INTEGER NOT NULL,
            original_position TEXT NOT NULL,
            origin TEXT NOT NULL,
            liters TEXT,
            quality TEXT NOT NULL,
            PRIMARY KEY (revision_id, position)
        );
    ''')


def save_revision(db, revision):
    db.execute('INSERT INTO frozen_revisions VALUES (?, ?)',
               (revision.revision_id, revision.batch_id))
    db.executemany('INSERT INTO frozen_rows VALUES (?, ?, ?, ?, ?, ?)', (
        (revision.revision_id, *encode_frozen_row(row)) for row in revision.rows
    ))


def get_revision(db, revision_id):
    record = db.execute('SELECT batch_id FROM frozen_revisions WHERE revision_id = ?',
                        (revision_id,)).fetchone()
    if record is None:
        raise LookupError(f'unknown frozen revision {revision_id!r}')
    rows = db.execute('''
        SELECT position, original_position, origin, liters, quality FROM frozen_rows
        WHERE revision_id = ? ORDER BY position
    ''', (revision_id,))
    return FrozenRevision(revision_id, record[0], tuple(map(decode_frozen_row, rows)))
