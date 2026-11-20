import sqlite3

from publication.history import read
from publication.signing import sign
from publication.store import find_publication_for_request
from receiving.intake import receive
from tests.fixture import LakeCase, details_view


class PublicationTests(LakeCase):
    def test_v7_commit_failure_rolls_back_result_and_receipt(self):
        segments, receipts = self.all_received()
        # The AFTER INSERT trigger leaves a deferred violation at the actual COMMIT.
        # TEMP structures disappear with this connection, permitting normal retry after reopen.
        self.inbox.executescript('''
            CREATE TEMP TABLE allowed_issue (receipt_id INTEGER PRIMARY KEY);
            CREATE TEMP TABLE commit_gate (
                receipt_id INTEGER REFERENCES allowed_issue(receipt_id) DEFERRABLE INITIALLY DEFERRED
            );
            CREATE TEMP TRIGGER stop_issue AFTER INSERT ON main.issue_receipts
            BEGIN
                INSERT INTO commit_gate VALUES (NEW.receipt_id);
            END;
        ''')
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'FOREIGN KEY'):
            sign(self.inbox, 'night')
        self.assertFalse(self.inbox.in_transaction)
        self.assertEqual(self.write_counts(), (0, 0))
        self.assertIsNone(find_publication_for_request(self.inbox, 'night'))
        self.assertEqual(tuple(receive(self.inbox, 'night', seg) for seg in segments), receipts)
        self.reopen_receiving()
        self.assertEqual(self.write_counts(), (0, 0))
        self.assertEqual(tuple(receive(self.inbox, 'night', seg) for seg in segments), receipts)
        result = sign(self.inbox, 'night')
        self.assertEqual(details_view(result.details),
                         ((0, 0, '站点/F/0', 7), (1, 1, '站点/F/1', 11), (2, 2, '站点/F/2', 7)))
        self.assertEqual(result.total_liters, 25)
        self.assertEqual(self.write_counts(), (1, 1))
        self.reopen_receiving()
        self.assertEqual(read(self.inbox, result.result_id), result)
        self.assertEqual(sign(self.inbox, 'night'), result)
