from catalog.revisions import FrozenRevision, FrozenRow, save_revision
from intake.batches import get_accepted_batch
from lake_db import write_transaction


def freeze(db, batch_id):
    """Freeze the immutable batch under its batch label in the revision namespace."""
    with write_transaction(db):
        accepted = get_accepted_batch(db, batch_id)
        existing = db.execute('SELECT revision_id FROM frozen_revisions WHERE batch_id = ?',
                              (batch_id,)).fetchone()
        if existing:
            return existing[0]
        rows = tuple(FrozenRow(position, row.position, row.origin, row.liters, row.quality)
                     for position, row in enumerate(accepted.rows))
        revision = FrozenRevision(batch_id, accepted.batch_id, rows)
        save_revision(db, revision)
        return revision.revision_id
