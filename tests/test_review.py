from dataclasses import replace

from audit.checks import AuditMismatch, audit
from audit.ledger import get_audit_record
from catalog.revisions import FrozenRow
from lake_cli import initialize_directory
from publication.history import read
from publication.signing import sign
from tests.fixture import LakeCase, details_view


SHIFT_COPY = (FrozenRow(0, 0, '站点/F/0', 7, '可用'),
              FrozenRow(1, 1, '站点/F/1', 11, '可用'),
              FrozenRow(2, 2, '站点/F/2', 7, '可用'))


class ReviewTests(LakeCase):
    def test_v8_offline_retrieval_and_review(self):
        self.all_received()
        original = sign(self.inbox, 'night')
        self.close_sources()
        self.reopen_receiving()
        collected = read(self.inbox, original.result_id)
        self.assertEqual(collected, original)
        self.assertEqual(details_view(collected.details),
                         ((0, 0, '站点/F/0', 7), (1, 1, '站点/F/1', 11), (2, 2, '站点/F/2', 7)))
        self.assertEqual(collected.total_liters, 25)
        reviewed = audit(self.inbox, collected.result_id, SHIFT_COPY, 25)
        self.assertEqual((reviewed.result_id, reviewed.row_count, reviewed.total_liters),
                         (original.result_id, 3, 25))
        self.assertEqual(get_audit_record(self.inbox, reviewed.audit_id), reviewed)
        with self.assertRaises(LookupError):
            read(self.inbox, original.result_id + 1)

    def test_v8_original_issue_receipt(self):
        self.all_received()
        first = sign(self.inbox, 'night')
        self.close_sources()
        self.reopen_receiving()
        self.assertEqual(read(self.inbox, first.result_id).receipt, first.receipt)
        self.assertEqual(sign(self.inbox, 'night').receipt, first.receipt)
        record = audit(self.inbox, first.result_id, SHIFT_COPY, 25)
        self.reopen_receiving()
        self.assertEqual(get_audit_record(self.inbox, record.audit_id).receipt, first.receipt)
        self.assertEqual(read(self.inbox, first.result_id).receipt, first.receipt)

    def test_v8_initialize_twice(self):
        self.all_received('operator-request')
        result = sign(self.inbox, 'operator-request')
        recorded = audit(self.inbox, result.result_id, SHIFT_COPY, 25)
        self.close_sources()
        initialize_directory(self.directory)
        initialize_directory(self.directory)
        self.reopen_receiving()
        self.assertEqual(read(self.inbox, result.result_id), result)
        self.assertEqual(get_audit_record(self.inbox, recorded.audit_id), recorded)
        self.assertEqual(self.write_counts(), (1, 1))
        self.assertEqual(self.inbox.execute('SELECT count(*) FROM audit_records').fetchone()[0], 1)

    def test_v9_swapped_equal_value_origins_are_rejected(self):
        self.all_received()
        result = sign(self.inbox, 'night')
        first, middle, last = SHIFT_COPY
        swapped = (replace(first, origin=last.origin), middle, replace(last, origin=first.origin))
        with self.assertRaises(AuditMismatch) as caught:
            audit(self.inbox, result.result_id, swapped, 25)
        self.assertEqual(tuple((diff.field, diff.position, diff.expected, diff.actual)
                               for diff in caught.exception.differences),
                         (('出处', 0, '站点/F/0', '站点/F/2'), ('出处', 2, '站点/F/2', '站点/F/0')))
        self.assertEqual(self.inbox.execute('SELECT count(*) FROM audit_records').fetchone()[0], 0)
        self.assertEqual(read(self.inbox, result.result_id), result)

    def test_v9_wrong_order_or_total_is_rejected(self):
        self.all_received()
        original = sign(self.inbox, 'night')
        for submitted, total, field in ((SHIFT_COPY[::-1], 25, '逻辑位置顺序'),
                                         (SHIFT_COPY, 24, '上报合计')):
            with self.assertRaises(AuditMismatch) as caught:
                audit(self.inbox, original.result_id, submitted, total)
            self.assertIn(field, {diff.field for diff in caught.exception.differences})
        self.assertEqual(self.inbox.execute('SELECT count(*) FROM audit_records').fetchone()[0], 0)
