from dataclasses import replace

from lake_cli import initialize_receiving
from lake_db import open_database
from publication.history import read
from publication.signing import sign
from publication.store import find_publication_for_request
from receiving.intake import receive
from receiving.requests import (accept_exclusive, authorize_exclusive, bind_destination,
                                get_exclusive_withdrawal, get_receiving_request,
                                notice_exclusive, register_request, withdraw_exclusive)
from scan.requests import (get_exclusive_dispatch, release_exclusive, reserve_exclusive,
                           ScanRequest)
from scan.segments import scan
from scan.tasks import assign_task
from tests.fixture import LakeCase, details_view, make_rows


class ExclusiveTests(LakeCase):
    def setUp(self):
        super().setUp()
        bind_destination(self.inbox, 'alice')

    def second_inbox(self, name):
        path = self.directory / f'{name}.sqlite'
        db = self.open_store(name)
        initialize_receiving(db)
        bind_destination(db, name)
        return db

    def seed_four(self):
        revision = self.frozen('F', make_rows('F', (7, 7, 11, 7)))
        self.load(revision, 'main', self.primary, (0, 1), (2, 3))
        self.load(revision, 'backup', self.standby, (0,), (1, 2), (3,))
        return revision

    def exclusive(self, dispatch_id, destination, start, stop, flag='可用',
                  replica='main', task_id=None, inbox=None):
        binding = reserve_exclusive(self.source, 'F', dispatch_id, destination,
                                    start, stop, flag)
        task = assign_task(self.source, dispatch_id, replica, task_id or f'{dispatch_id}-scan')
        if inbox is not None:
            notice_exclusive(inbox, binding, sender_id='source')
        return binding, authorize_exclusive(binding), task.assignment

    def test_reserve_terms_and_occupancy(self):
        self.seed_four()
        binding, _, task = self.exclusive('A', 'alice', 0, 2)
        self.assertEqual((binding.dispatch_id, binding.request_id, binding.start, binding.stop),
                         ('A', 'A', 0, 2))
        self.assertEqual(reserve_exclusive(self.source, 'F', 'A', 'alice', 0, 2, '可用'), binding)
        for change in ({'destination_id': 'bob'}, {'start': 0, 'stop': 3}, {'quality': '待核'}):
            with self.subTest(change=change):
                with self.assertRaises(ValueError):
                    reserve_exclusive(self.source, 'F', 'A', change.get('destination_id', 'alice'),
                                      change.get('start', 0), change.get('stop', 2),
                                      change.get('quality', '可用'))
        with self.assertRaises(ValueError):
            reserve_exclusive(self.source, 'F', 'B', 'bob', 1, 3, '可用')
        edge = reserve_exclusive(self.source, 'F', 'B', 'bob', 2, 3, '可用')
        self.assertEqual((edge.request_id, edge.destination_id), ('B', 'bob'))
        zero = reserve_exclusive(self.source, 'F', 'Z', 'bob', 3, 4, '待核')
        self.assertEqual((zero.start, zero.stop), (3, 4))
        with self.assertRaises(ValueError):
            reserve_exclusive(self.source, 'F', 'Z2', 'alice', 3, 4, '待核')
        self.assertEqual(get_exclusive_dispatch(self.source, 'A'), binding)

    def test_bind_destination_is_persistent(self):
        self.assertEqual(bind_destination(self.inbox, 'alice'), 'alice')
        with self.assertRaises(ValueError):
            bind_destination(self.inbox, 'bob')
        self.reopen_receiving()
        self.assertEqual(bind_destination(self.inbox, 'alice'), 'alice')

    def test_accept_requires_named_library_and_binding(self):
        self.seed_four()
        binding, authorization, task = self.exclusive('A', 'alice', 0, 2, inbox=self.inbox)
        bob = self.second_inbox('bob-inbox')
        with self.assertRaises(ValueError):
            accept_exclusive(bob, authorization, task, sender_id='source')
        with self.assertRaises(ValueError):
            accept_exclusive(self.inbox, authorization, task, sender_id='bob')
        with self.assertRaises(ValueError):
            register_request(self.inbox, ScanRequest('A', 'F', 0, 2, '可用'), task)
        accepted = accept_exclusive(self.inbox, authorization, task, sender_id='source')
        self.assertEqual(accepted.active_task_id, task.task_id)
        self.assertEqual(accept_exclusive(self.inbox, authorization, task, sender_id='source'),
                         accepted)
        altered = replace(authorization, stop=3)
        with self.assertRaises(ValueError):
            accept_exclusive(self.inbox, altered, task, sender_id='source')
        self.assertEqual(get_receiving_request(self.inbox, 'A'), accepted)

    def test_sign_keeps_first_finality_and_blocks_withdrawal(self):
        self.seed_four()
        binding, authorization, task = self.exclusive('A', 'alice', 0, 2)
        accept_exclusive(self.inbox, authorization, task, sender_id='source')
        receive(self.inbox, 'A', scan(self.source, self.primary, task.task_id, 0))
        signed = sign(self.inbox, 'A')
        self.assertEqual((details_view(signed.details), signed.total_liters),
                         (((0, 0, '站点/F/0', 7), (1, 1, '站点/F/1', 7)), 14))
        self.assertEqual(sign(self.inbox, 'A'), signed)
        with self.assertRaises(ValueError):
            withdraw_exclusive(self.inbox, binding, sender_id='source')
        self.reopen_receiving()
        self.assertEqual(read(self.inbox, signed.result_id), signed)
        self.assertEqual(sign(self.inbox, 'A').receipt, signed.receipt)

    def test_withdraw_first_invalidates_late_authorization(self):
        self.seed_four()
        binding, authorization, task = self.exclusive('A', 'alice', 0, 2)
        receipt = withdraw_exclusive(self.inbox, binding, sender_id='source')
        self.assertEqual(withdraw_exclusive(self.inbox, binding, sender_id='source'), receipt)
        self.reopen_receiving()
        self.assertEqual(get_exclusive_withdrawal(self.inbox, 'A'), receipt)
        with self.assertRaises(ValueError):
            accept_exclusive(self.inbox, authorization, task, sender_id='source')
        accepted = release_exclusive(self.source, 'A', receipt, sender_id='alice')
        self.assertEqual(accepted, receipt)
        self.assertEqual(release_exclusive(self.source, 'A', receipt, sender_id='alice'), receipt)
        # The released number still resolves to its original binding, and its
        # range can now be dispatched to the other library.
        self.assertEqual(reserve_exclusive(self.source, 'F', 'A', 'alice', 0, 2, '可用'), binding)
        other = reserve_exclusive(self.source, 'F', 'B', 'bob', 0, 2, '可用')
        self.assertEqual(other.request_id, 'B')

    def test_withdraw_requires_signed_guard_and_target(self):
        self.seed_four()
        binding, authorization, task = self.exclusive('A', 'alice', 0, 2, inbox=self.inbox)
        bob = self.second_inbox('bob-inbox')
        with self.assertRaises(ValueError):
            withdraw_exclusive(bob, binding, sender_id='source')
        with self.assertRaises(ValueError):
            withdraw_exclusive(self.inbox, binding, sender_id='bob')
        # The early notice pinned the terms, so a same-number different-terms
        # withdrawal must be refused even before the final authorization arrives.
        with self.assertRaises(ValueError):
            withdraw_exclusive(self.inbox, replace(binding, stop=3), sender_id='source')
        self.assertEqual(withdraw_exclusive(self.inbox, binding, sender_id='source')
                         .dispatch_id, 'A')

    def test_foreign_notice_pins_the_target(self):
        self.seed_four()
        binding = reserve_exclusive(self.source, 'F', 'A', 'alice', 0, 2, '可用')
        bob = self.second_inbox('bob-inbox')
        with self.assertRaises(ValueError):
            notice_exclusive(bob, binding, sender_id='source')
        with self.assertRaises(ValueError):
            notice_exclusive(self.inbox, binding, sender_id='bob')
        self.assertEqual(notice_exclusive(self.inbox, binding, sender_id='source').binding(),
                         binding)
        self.assertEqual(notice_exclusive(self.inbox, binding, sender_id='source').binding(),
                         binding)

    def test_release_requires_exact_target_receipt(self):
        self.seed_four()
        binding, _, _ = self.exclusive('A', 'alice', 0, 2)
        receipt = withdraw_exclusive(self.inbox, binding, sender_id='source')
        with self.assertRaises(ValueError):
            release_exclusive(self.source, 'A', receipt, sender_id='bob')
        with self.assertRaises(ValueError):
            release_exclusive(self.source, 'B', receipt, sender_id='alice')
        with self.assertRaises(ValueError):
            release_exclusive(self.source, 'A', replace(receipt, stop=3), sender_id='alice')
        self.assertEqual(release_exclusive(self.source, 'A', receipt, sender_id='alice'), receipt)
        self.assertEqual(reserve_exclusive(self.source, 'F', 'C', 'bob', 0, 2, '可用').request_id, 'C')

    def test_signed_dispatch_stays_occupied(self):
        self.seed_four()
        binding, authorization, task = self.exclusive('A', 'alice', 0, 2)
        accept_exclusive(self.inbox, authorization, task, sender_id='source')
        for number in (0,):
            receive(self.inbox, 'A', scan(self.source, self.primary, task.task_id, number))
        result = sign(self.inbox, 'A')
        self.assertIsNotNone(find_publication_for_request(self.inbox, 'A'))
        with self.assertRaises(ValueError):
            reserve_exclusive(self.source, 'F', 'B', 'bob', 1, 3, '可用')
        with self.assertRaises(ValueError):
            reserve_exclusive(self.source, 'F', 'C', 'bob', 0, 1, '可用')
        self.assertEqual(read(self.inbox, result.result_id), result)


class OrdinaryFlowTests(LakeCase):
    def test_ordinary_registration_still_works(self):
        segments, receipts = self.all_received()
        self.assertEqual(tuple(receive(self.inbox, 'night', seg) for seg in segments), receipts)
        result = sign(self.inbox, 'night')
        self.assertEqual(result.total_liters, 25)


class WithdrawalAfterAcceptTests(LakeCase):
    def test_accepted_dispatch_can_still_be_withdrawn_if_unsigned(self):
        bind_destination(self.inbox, 'alice')
        revision = self.frozen('F', make_rows('F', (7, 7, 11, 7)))
        self.load(revision, 'main', self.primary, (0, 1), (2, 3))
        binding = reserve_exclusive(self.source, 'F', 'A', 'alice', 0, 2, '可用')
        task = assign_task(self.source, 'A', 'main', 'A-scan').assignment
        accept_exclusive(self.inbox, authorize_exclusive(binding), task, sender_id='source')
        receive(self.inbox, 'A', scan(self.source, self.primary, 'A-scan', 0))
        receipt = withdraw_exclusive(self.inbox, binding, sender_id='source')
        self.assertEqual(receipt.binding(), binding)
        with self.assertRaises(ValueError):
            sign(self.inbox, 'A')
        self.assertEqual(release_exclusive(self.source, 'A', receipt, sender_id='alice'), receipt)
        # The released range can go to the other library now.
        self.assertEqual(reserve_exclusive(self.source, 'F', 'B', 'bob', 0, 2, '可用').request_id, 'B')

    def test_reopen_replays_authorization_and_withdrawal(self):
        bind_destination(self.inbox, 'alice')
        revision = self.frozen('F', make_rows('F', (7, 7, 11, 7)))
        self.load(revision, 'main', self.primary, (0, 1), (2, 3))
        binding = reserve_exclusive(self.source, 'F', 'A', 'alice', 0, 2, '可用')
        authorization = authorize_exclusive(binding)
        task = assign_task(self.source, 'A', 'main', 'A-scan').assignment
        accepted = accept_exclusive(self.inbox, authorization, task, sender_id='source')
        self.reopen_receiving()
        self.assertEqual(accept_exclusive(self.inbox, authorization, task, sender_id='source'),
                         accepted)
        receipt = withdraw_exclusive(self.inbox, binding, sender_id='source')
        self.reopen_receiving()
        self.assertEqual(get_exclusive_withdrawal(self.inbox, 'A'), receipt)
        with self.assertRaises(ValueError):
            accept_exclusive(self.inbox, authorization, task, sender_id='source')

    def test_blocked_release_keeps_range_occupied(self):
        import sqlite3

        from exclusive_case import block_commit
        bind_destination(self.inbox, 'alice')
        revision = self.frozen('F', make_rows('F', (7, 7, 11, 7)))
        self.load(revision, 'main', self.primary, (0, 1), (2, 3))
        binding = reserve_exclusive(self.source, 'F', 'A', 'alice', 0, 2, '可用')
        receipt = withdraw_exclusive(self.inbox, binding, sender_id='source')
        with block_commit(self.source):
            try:
                release_exclusive(self.source, 'A', receipt, sender_id='alice')
            except sqlite3.DatabaseError as error:
                if error.sqlite_errorcode != sqlite3.SQLITE_AUTH:
                    raise
            else:
                self.fail('release commit was not blocked')
        self.assertFalse(self.source.in_transaction)
        with self.assertRaises(ValueError):
            reserve_exclusive(self.source, 'F', 'B', 'bob', 0, 2, '可用')
        self.assertEqual(release_exclusive(self.source, 'A', receipt, sender_id='alice'), receipt)
        self.assertEqual(reserve_exclusive(self.source, 'F', 'B', 'bob', 0, 2, '可用').request_id, 'B')
