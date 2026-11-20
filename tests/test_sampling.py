from dataclasses import replace

from publication.signing import IncompleteCoverage, sign
from receiving.coverage import InvalidEvidence, get_confirmed_snapshot
from receiving.intake import receive
from receiving.receipts import list_accepted_segments
from scan.requests import get_request, request
from scan.segments import scan
from tests.fixture import LakeCase, details_view, make_rows


class SamplingTests(LakeCase):
    def test_v4_single_replica_reports_ordered_equal_values(self):
        revision = self.frozen('F', make_rows('F', (7, 9, 11, 7), ('可用', '待核', '可用', '可用')))
        self.load(revision, 'main', self.primary, (1, 0), (3, 2))
        task = self.dispatch('F', 'night', 'main', 0, 4)
        late_order = scan(self.source, self.primary, task, 1)
        original = receive(self.inbox, 'night', late_order)
        with self.assertRaises(IncompleteCoverage) as pending:
            sign(self.inbox, 'night')
        self.assertEqual(pending.exception.missing_positions, (0, 1))
        receive(self.inbox, 'night', scan(self.source, self.primary, task, 0))
        result = sign(self.inbox, 'night')
        self.assertEqual(details_view(result.details),
                         ((0, 0, '站点/F/0', 7), (2, 2, '站点/F/2', 11), (3, 3, '站点/F/3', 7)))
        self.assertEqual(result.total_liters, 25)
        self.assertEqual(receive(self.inbox, 'night', late_order), original)
        self.assertEqual(sign(self.inbox, 'night'), result)
        self.assertEqual(self.write_counts(), (1, 1))

    def test_v4_request_bounds(self):
        revision = self.frozen('F', make_rows('F', (7, 9, 11, 7)))
        self.load(revision, 'main', self.primary, (0, 1), (2,), (3,))
        for start, stop, flag in ((-1, 2, '可用'), (0, 5, '可用'), (1, 1, '可用'),
                                  (3, 1, '可用'), (False, 2, '可用'), (0, 2, '未知')):
            with self.subTest(start=start, stop=stop, flag=flag):
                with self.assertRaises(ValueError):
                    request(self.source, 'F', 'night', start, stop, flag)
                with self.assertRaises(LookupError):
                    get_request(self.source, 'night')
        task = self.dispatch('F', 'night', 'main', 1, 3)
        first = scan(self.source, self.primary, task, 0)
        self.assertEqual(first.checked_positions, (1,))
        self.assertIsNone(scan(self.source, self.primary, task, 2))
        receive(self.inbox, 'night', first)
        receive(self.inbox, 'night', scan(self.source, self.primary, task, 1))
        result = sign(self.inbox, 'night')
        self.assertEqual(details_view(result.details), ((1, 1, '站点/F/1', 9), (2, 2, '站点/F/2', 11)))
        self.assertEqual(result.total_liters, 20)

    def test_v4_missing_and_zero(self):
        revision = self.frozen('F', make_rows('F', (None, 0, -3)))
        self.load(revision, 'main', self.primary, (0, 1), (2,))
        task = self.dispatch('F', 'night', 'main', 0, 3)
        for number in (0, 1):
            receive(self.inbox, 'night', scan(self.source, self.primary, task, number))
        self.reopen_receiving()
        result = sign(self.inbox, 'night')
        self.assertEqual(details_view(result.details),
                         ((0, 0, '站点/F/0', None), (1, 1, '站点/F/1', 0), (2, 2, '站点/F/2', -3)))
        self.assertIs(type(result.details[1].liters), int)
        self.assertEqual(result.total_liters, -3)

    def test_v4_exact_liters(self):
        revision = self.frozen('F', make_rows('F', (7, 9007199254740993, 7)))
        self.load(revision, 'main', self.primary, (0,), (1, 2))
        task = self.dispatch('F', 'night', 'main', 0, 3)
        for number in (0, 1):
            receive(self.inbox, 'night', scan(self.source, self.primary, task, number))
        result = sign(self.inbox, 'night')
        self.assertEqual(details_view(result.details),
                         ((0, 0, '站点/F/0', 7), (1, 1, '站点/F/1', 9007199254740993),
                          (2, 2, '站点/F/2', 7)))
        self.assertEqual(result.total_liters, 9007199254741007)
        self.assertIs(type(result.total_liters), int)

    def test_v5_zero_hits_prove_coverage(self):
        revision = self.frozen('F', make_rows('F', (7, 9, 11, 7), ('待核',) * 4))
        self.load(revision, 'main', self.primary, (0, 1), (2, 3))
        task = self.dispatch('F', 'night', 'main', 0, 4)
        first, second = (scan(self.source, self.primary, task, n) for n in (0, 1))
        self.assertEqual((first.hits, second.hits), ((), ()))
        one = receive(self.inbox, 'night', first)
        with self.assertRaises(IncompleteCoverage) as pending:
            sign(self.inbox, 'night')
        self.assertEqual(pending.exception.missing_positions, (2, 3))
        self.assertEqual(self.write_counts(), (0, 0))
        two = receive(self.inbox, 'night', second)
        self.assertNotEqual(one, two)
        result = sign(self.inbox, 'night')
        self.assertEqual((result.details, result.total_liters), ((), 0))
        self.assertEqual(tuple(receive(self.inbox, 'night', seg) for seg in (first, second)), (one, two))

    def test_v6_reject_wrong_batch_and_changed_evidence(self):
        revision = self.frozen('F', make_rows('F', (7, 11, 7)))
        other = self.frozen('G', make_rows('G', (7, 11, 7)))
        self.load(revision, 'main', self.primary, (0,), (1, 2))
        self.load(other, 'other', self.standby, (0,), (1, 2))
        task = self.dispatch('F', 'night', 'main', 0, 3)
        alien = self.dispatch('G', 'other-night', 'other', 0, 3)
        first = scan(self.source, self.primary, task, 0)
        receipt = receive(self.inbox, 'night', first)
        foreign = scan(self.source, self.standby, alien, 1)
        valid_tail = scan(self.source, self.primary, task, 1)
        initial = get_confirmed_snapshot(self.inbox, 'night')
        for bad in (foreign, replace(foreign, request_id='night'), replace(valid_tail, revision_id='G'),
                    replace(valid_tail, hits=()), replace(first, checked_positions=(0, 1))):
            with self.assertRaises(InvalidEvidence):
                receive(self.inbox, 'night', bad)
        self.assertEqual(get_confirmed_snapshot(self.inbox, 'night'), initial)
        self.assertEqual(tuple(s.receipt for s in list_accepted_segments(self.inbox, 'night')), (receipt,))
        self.assertEqual(receive(self.inbox, 'night', first), receipt)
        with self.assertRaises(IncompleteCoverage):
            sign(self.inbox, 'night')
        self.assertEqual(self.write_counts(), (0, 0))
