from publication.history import read
from publication.signing import IncompleteCoverage, sign
from receiving.coverage import get_confirmed_snapshot
from receiving.intake import StaleAssignment, receive
from receiving.receipts import list_accepted_segments
from receiving.requests import get_receiving_request, register_assignment
from scan.handoff import begin_handoff
from scan.segments import scan
from scan.tasks import get_task, record_task_state
from tests.fixture import LakeCase, details_view, make_rows


class HandoffTests(LakeCase):
    def prepare_primary(self):
        revision = self.frozen('F', make_rows('F', (7, 9, 11, 7), ('可用', '待核', '可用', '可用'),
                                             (40, 12, 31, 88)))
        self.load(revision, 'main', self.primary, (0, 1), (2, 3))
        self.load(revision, 'backup', self.standby, (0,), (1, 2), (3,))
        task = self.dispatch('F', 'night', 'main', 0, 4)
        first = scan(self.source, self.primary, task, 0)
        receipt = receive(self.inbox, 'night', first)
        return task, first, receipt

    def handoff(self, task):
        self.primary.close()
        record_task_state(self.source, task, 'lost')
        standby = begin_handoff(self.source, 'night', task, 'backup', 'night-standby')
        register_assignment(self.inbox, standby.assignment)
        return standby.assignment.task_id

    def assert_pending_at_three(self):
        try:
            result = sign(self.inbox, 'night')
        except IncompleteCoverage as pending:
            self.assertEqual(pending.missing_positions, (3,))
        else:
            self.fail(f'抽查 F 尚未覆盖逻辑位置 3 已签发 '
                      f'{tuple(row.position for row in result.details)} 共 {result.total_liters} L')
        self.assertEqual(self.write_counts(), (0, 0))

    def assert_complete(self, result):
        self.assertEqual(result.revision_id, 'F')
        self.assertEqual(details_view(result.details),
                         ((0, 40, '站点/F/0', 7), (2, 31, '站点/F/2', 11), (3, 88, '站点/F/3', 7)))
        self.assertEqual(result.total_liters, 25)
        self.assertEqual(self.write_counts(), (1, 1))

    def test_v10_interleaved_delivery(self):
        task, first, receipt = self.prepare_primary()
        late = scan(self.source, self.primary, task, 1)
        standby = self.handoff(task)
        middle = scan(self.source, self.standby, standby, 1)
        middle_receipt = receive(self.inbox, 'night', middle)
        self.assert_pending_at_three()
        try:
            late_receipt = receive(self.inbox, 'night', late)
        except StaleAssignment:
            self.assert_pending_at_three()
        else:
            self.assertNotEqual(late_receipt, middle_receipt)
            self.assertEqual(receive(self.inbox, 'night', late), late_receipt)
            self.assert_complete(sign(self.inbox, 'night'))
        tail = scan(self.source, self.standby, standby, 2)
        receive(self.inbox, 'night', tail)
        self.assertEqual(receive(self.inbox, 'night', first), receipt)
        self.assertEqual(receive(self.inbox, 'night', middle), middle_receipt)
        result = sign(self.inbox, 'night')
        self.assert_complete(result)
        self.close_sources()
        self.reopen_receiving()
        self.assertEqual(read(self.inbox, result.result_id), result)

    def test_v10_logical_evidence(self):
        task, _, _ = self.prepare_primary()
        standby = self.handoff(task)
        middle = scan(self.source, self.standby, standby, 1)
        self.assertEqual((middle.request_id, middle.revision_id, middle.task_id, middle.replica_id),
                         ('night', 'F', standby, 'backup'))
        self.assertEqual(middle.group_number, 1)
        self.assertEqual(middle.checked_positions, (1, 2))
        self.assertEqual(details_view(middle.hits), ((2, 31, '站点/F/2', 11),))

    def test_v10_incomplete_after_overlap(self):
        task, _, _ = self.prepare_primary()
        standby = self.handoff(task)
        receive(self.inbox, 'night', scan(self.source, self.standby, standby, 1))
        self.reopen_receiving()
        self.assert_pending_at_three()
        snapshot = get_confirmed_snapshot(self.inbox, 'night')
        self.assertEqual(snapshot.checked_positions, frozenset((0, 1, 2)))
        self.assertEqual(details_view(snapshot.hits), ((0, 40, '站点/F/0', 7), (2, 31, '站点/F/2', 11)))

    def test_v10_backup_only(self):
        task, _, _ = self.prepare_primary()
        standby = self.handoff(task)
        with self.assertRaises(ValueError):
            scan(self.source, self.primary, task, 1)
        self.reopen_receiving()
        tail = scan(self.source, self.standby, standby, 2)
        receive(self.inbox, 'night', tail)
        with self.assertRaises(IncompleteCoverage) as pending:
            sign(self.inbox, 'night')
        self.assertEqual(pending.exception.missing_positions, (2,))
        receive(self.inbox, 'night', scan(self.source, self.standby, standby, 1))
        self.assert_complete(sign(self.inbox, 'night'))

    def test_v10_first_receipt_replay(self):
        task, first, receipt = self.prepare_primary()
        standby = self.handoff(task)
        self.reopen_receiving()
        self.assertEqual(get_receiving_request(self.inbox, 'night').active_task_id, standby)
        register_assignment(self.inbox, get_task(self.source, standby).assignment)
        self.assertEqual(receive(self.inbox, 'night', first), receipt)
        self.assertEqual(tuple(s.receipt for s in list_accepted_segments(self.inbox, 'night')), (receipt,))
        self.assertEqual(get_confirmed_snapshot(self.inbox, 'night').checked_positions, frozenset((0, 1)))
        self.assertEqual(self.write_counts(), (0, 0))

    def test_v10_failed_handoff_preserves_progress(self):
        task, first, receipt = self.prepare_primary()
        self.primary.close()
        record_task_state(self.source, task, 'lost')
        with self.assertRaises(LookupError):
            begin_handoff(self.source, 'night', task, 'absent', 'new-task')
        with self.assertRaises(LookupError):
            get_task(self.source, 'new-task')
        self.assertEqual(get_receiving_request(self.inbox, 'night').active_task_id, task)
        self.assertEqual(receive(self.inbox, 'night', first), receipt)
        self.assertEqual(get_confirmed_snapshot(self.inbox, 'night').checked_positions, frozenset((0, 1)))
        self.assertEqual(self.write_counts(), (0, 0))
