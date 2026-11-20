from dataclasses import replace

from catalog.freeze import freeze
from catalog.layouts import LayoutGroup, ReplicaLayout
from catalog.replicas import get_replica, read_replica_group, replicate
from catalog.revisions import get_revision
from intake.batches import get_accepted_batch, ingest
from intake.receipts import get_import_receipt
from lake_cli import initialize_directory
from tests.fixture import LakeCase, details_view, make_rows


class CatalogTests(LakeCase):
    def test_v1_equivalent_replicas(self):
        revision = self.frozen('F', make_rows('F', (7, 11, 7)))
        primary = self.load(revision, 'main', self.primary, (0, 1), (2,))
        standby = self.load(revision, 'backup', self.standby, (0,), (1, 2))
        expected = ((0, 0, '站点/F/0', 7), (1, 1, '站点/F/1', 11), (2, 2, '站点/F/2', 7))
        for replica, handle, widths in ((primary, self.primary, (2, 1)), (standby, self.standby, (1, 2))):
            groups = tuple(read_replica_group(handle, replica, number) for number in replica.group_numbers)
            self.assertEqual(tuple(len(group.rows) for group in groups), widths)
            self.assertEqual(details_view(row for group in groups for row in group.rows), expected)
        self.assertEqual(get_import_receipt(self.source, 'F').origins,
                         ((0, '站点/F/0'), (1, '站点/F/1'), (2, '站点/F/2')))

    def test_v1_original_order(self):
        revision = self.frozen('ordered', make_rows('ordered', (7, 11, 7), positions=(42, 3, 19)))
        self.assertEqual(details_view(revision.rows),
                         ((0, 42, '站点/ordered/0', 7), (1, 3, '站点/ordered/1', 11),
                          (2, 19, '站点/ordered/2', 7)))
        self.assertEqual(freeze(self.source, 'ordered'), revision.revision_id)

    def test_v1_initialize_twice(self):
        rows = make_rows('operator', (5, None, 0), positions=(31, 4, 21))
        accepted = ingest(self.source, 'operator', rows)
        revision_id = freeze(self.source, 'operator')
        initialize_directory(self.directory)
        initialize_directory(self.directory)
        with self.subTest('batch and original receipt'):
            reopened = self.open_store('source')
            self.assertEqual(get_accepted_batch(reopened, 'operator'), accepted)
            self.assertEqual(get_import_receipt(reopened, 'operator'), accepted.receipt)
        self.assertEqual(details_view(get_revision(reopened, revision_id).rows),
                         ((0, 31, '站点/operator/0', 5), (1, 4, '站点/operator/1', None),
                          (2, 21, '站点/operator/2', 0)))
        self.assertEqual(ingest(reopened, 'operator', rows), accepted)
        with self.assertRaises(ValueError):
            ingest(reopened, 'operator', (replace(rows[0], liters=6), *rows[1:]))

    def test_v2_reject_duplicate_without_partial_batch(self):
        bad = make_rows('duplicate', (7, 11), positions=(4, 4))
        with self.assertRaisesRegex(ValueError, 'duplicate source position'):
            ingest(self.source, 'duplicate', bad)
        with self.assertRaises(LookupError):
            get_accepted_batch(self.source, 'duplicate')
        with self.assertRaises(LookupError):
            get_import_receipt(self.source, 'duplicate')
        with self.assertRaises(LookupError):
            freeze(self.source, 'duplicate')
        with self.assertRaises(LookupError):
            get_revision(self.source, 'duplicate')
        self.assertEqual(self.source.execute('SELECT count(*) FROM batches').fetchone()[0], 0)
        self.assertEqual(self.source.execute('SELECT count(*) FROM observation_rows').fetchone()[0], 0)

    def test_v2_invalid_values_and_origins(self):
        for changes in ({'liters': 7.0}, {'liters': True}, {'origin': ' '}, {'position': -1}, {'quality': '未知'}):
            with self.subTest(changes=changes):
                good = make_rows('bad', (7, 11))
                with self.assertRaises(ValueError):
                    ingest(self.source, 'bad', (good[0], replace(good[1], **changes)))
                with self.assertRaises(LookupError):
                    get_import_receipt(self.source, 'bad')

    def test_v3_reject_invalid_replicas_without_touching_primary(self):
        revision = self.frozen('F', make_rows('F', (7, 11, 7)))
        primary = self.load(revision, 'main', self.primary, (0,), (1, 2))
        invalid = {
            'missing': ReplicaLayout.from_positions(revision, ((0, (0, 1)),)),
            'duplicate': ReplicaLayout.from_positions(revision, ((0, (0, 1)), (1, (1, 2)))),
            'content': ReplicaLayout((LayoutGroup(0, (*revision.rows[:2], replace(revision.rows[2], liters=8))),))
        }
        for name, layout in invalid.items():
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    replicate(self.source, self.standby, 'F', name, layout)
                with self.assertRaises(LookupError):
                    get_replica(self.source, 'F', name)
                self.assertIsNone(self.standby.execute('SELECT 1 FROM replica_identity WHERE replica_id = ?',
                                                       (name,)).fetchone())
        actual = [row for number in (0, 1) for row in read_replica_group(self.primary, primary, number).rows]
        self.assertEqual(details_view(actual),
                         ((0, 0, '站点/F/0', 7), (1, 1, '站点/F/1', 11), (2, 2, '站点/F/2', 7)))
