import hashlib
import json
from dataclasses import dataclass

from catalog.layouts import LayoutGroup, validate_layout
from catalog.revisions import decode_frozen_row, encode_frozen_row, get_revision
from intake.validation import require_text
from lake_db import canonical_json, initialize_schema, write_transaction


@dataclass(frozen=True, slots=True)
class Replica:
    replica_id: str
    revision_id: str
    manifest: tuple[tuple[int, str], ...]

    @property
    def group_numbers(self):
        return tuple(number for number, checksum in self.manifest)


def initialize_replica_catalog(db):
    initialize_schema(db, '''
        CREATE TABLE IF NOT EXISTS available_replicas (
            replica_id TEXT PRIMARY KEY,
            revision_id TEXT NOT NULL REFERENCES frozen_revisions(revision_id),
            manifest TEXT NOT NULL
        );
    ''')


def initialize_replica_store(db):
    initialize_schema(db, '''
        CREATE TABLE IF NOT EXISTS replica_identity (
            replica_id TEXT PRIMARY KEY,
            revision_id TEXT NOT NULL,
            manifest TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS replica_groups (
            replica_id TEXT NOT NULL REFERENCES replica_identity(replica_id),
            group_number INTEGER NOT NULL,
            body TEXT NOT NULL,
            PRIMARY KEY (replica_id, group_number)
        );
    ''')


def _digest(body):
    return hashlib.sha256(body.encode('utf-8')).hexdigest()


def get_replica(db, revision_id, replica_id):
    record = db.execute('''
        SELECT manifest FROM available_replicas WHERE replica_id = ? AND revision_id = ?
    ''', (replica_id, revision_id)).fetchone()
    if record is None:
        raise LookupError(f'replica {replica_id!r} unavailable for {revision_id!r}')
    return Replica(replica_id, revision_id, tuple((number, digest) for number, digest in json.loads(record[0])))


def replicate(db, replica_db, revision_id, replica_id, layout):
    require_text(replica_id, 'replica ID')
    if replica_db is db:
        raise ValueError('use an independent replica database')
    revision = get_revision(db, revision_id)
    validate_layout(revision, layout)
    groups = tuple((group.number, canonical_json([encode_frozen_row(row) for row in group.rows]))
                   for group in layout.groups)
    manifest = tuple((number, _digest(body)) for number, body in groups)
    header = (revision_id, canonical_json(manifest))
    existing = db.execute('SELECT revision_id, manifest FROM available_replicas WHERE replica_id = ?',
                          (replica_id,)).fetchone()
    if existing is not None and existing != header:
        raise ValueError('replica ID already has a different layout or revision')

    with write_transaction(replica_db):
        local = replica_db.execute('SELECT revision_id, manifest FROM replica_identity WHERE replica_id = ?',
                                   (replica_id,)).fetchone()
        if local is None:
            replica_db.execute('INSERT INTO replica_identity VALUES (?, ?, ?)', (replica_id, *header))
            replica_db.executemany('INSERT INTO replica_groups VALUES (?, ?, ?)',
                                   ((replica_id, number, body) for number, body in groups))
        else:
            if local != header:
                raise ValueError('replica store already has a different layout or revision')
            stored = replica_db.execute('SELECT group_number, body FROM replica_groups WHERE replica_id = ?',
                                        (replica_id,))
            if dict(stored) != dict(groups):
                raise ValueError('existing replica group contents differ')

    # A committed local copy can be retried if registering it in the catalog fails.
    with write_transaction(db):
        current = db.execute('SELECT revision_id, manifest FROM available_replicas WHERE replica_id = ?',
                             (replica_id,)).fetchone()
        if current is None:
            db.execute('INSERT INTO available_replicas VALUES (?, ?, ?)', (replica_id, *header))
        elif current != header:
            raise ValueError('conflicting replica registration')
    return Replica(replica_id, revision_id, manifest)


def read_replica_group(replica_db, replica, number):
    if type(number) is not int:
        raise ValueError('group number must be an integer')
    checksum = dict(replica.manifest).get(number)
    if checksum is None:
        raise LookupError(f'group {number} is absent from replica {replica.replica_id!r}')
    record = replica_db.execute('''
        SELECT g.body FROM replica_groups AS g JOIN replica_identity AS i USING (replica_id)
        WHERE g.replica_id = ? AND i.revision_id = ? AND g.group_number = ?
    ''', (replica.replica_id, replica.revision_id, number)).fetchone()
    if record is None or _digest(record[0]) != checksum:
        raise ValueError(f'unverified replica data for group {number}')
    return LayoutGroup(number, tuple(map(decode_frozen_row, json.loads(record[0]))))
