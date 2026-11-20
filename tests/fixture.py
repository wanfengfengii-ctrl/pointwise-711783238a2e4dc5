import unittest
from contextlib import ExitStack, closing
from pathlib import Path
from tempfile import TemporaryDirectory

from catalog.freeze import freeze
from catalog.layouts import ReplicaLayout
from catalog.replicas import replicate
from catalog.revisions import get_revision
from intake.batches import ingest
from intake.rows import ObservationRow
from lake_cli import initialize_directory
from lake_db import open_database
from receiving.intake import receive
from receiving.requests import register_request
from scan.requests import request
from scan.segments import scan
from scan.tasks import assign_task


def make_rows(batch, values, flags=None, positions=None):
    if flags is None:
        flags = ('可用',) * len(values)
    if positions is None:
        positions = range(len(values))
    return tuple(ObservationRow(pos, f'站点/{batch}/{index}', value, flag)
                 for index, (pos, value, flag) in enumerate(zip(positions, values, flags, strict=True)))


def details_view(rows):
    return tuple((row.position, row.original_position, row.origin, row.liters) for row in rows)


class LakeCase(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        folder = self.stack.enter_context(TemporaryDirectory(prefix='qingyuan-test-'))
        self.directory = Path(folder)
        initialize_directory(self.directory)
        self.source = self.open_store('source')
        self.primary = self.open_store('primary')
        self.standby = self.open_store('standby')
        self.inbox = self.open_store('receiving')

    def open_store(self, name):
        return self.stack.enter_context(closing(open_database(self.directory / f'{name}.sqlite')))

    def reopen_receiving(self):
        self.inbox.close()
        self.inbox = self.open_store('receiving')

    def close_sources(self):
        self.source.close()
        self.primary.close()
        self.standby.close()

    def frozen(self, batch, rows):
        ingest(self.source, batch, rows)
        return get_revision(self.source, freeze(self.source, batch))

    def load(self, revision, replica_id, handle, *groups):
        layout = ReplicaLayout.from_positions(revision, enumerate(groups))
        return replicate(self.source, handle, revision.revision_id, replica_id, layout)

    def dispatch(self, revision_id, request_id, replica_id, start, stop, flag='可用'):
        obligation = request(self.source, revision_id, request_id, start, stop, flag)
        ab = assign_task(self.source, request_id, replica_id, f'{request_id}-primary').assignment
        register_request(self.inbox, obligation, ab)
        return ab.task_id

    def all_received(self, request_id='night'):
        revision = self.frozen('F', make_rows('F', (7, 11, 7)))
        self.load(revision, 'main', self.primary, (0, 1), (2,))
        task = self.dispatch('F', request_id, 'main', 0, 3)
        segments = tuple(scan(self.source, self.primary, task, number) for number in (0, 1))
        receipts = tuple(receive(self.inbox, request_id, segment) for segment in segments)
        return segments, receipts

    def write_counts(self):
        return (self.inbox.execute('SELECT count(*) FROM publications').fetchone()[0],
                self.inbox.execute('SELECT count(*) FROM issue_receipts').fetchone()[0])
