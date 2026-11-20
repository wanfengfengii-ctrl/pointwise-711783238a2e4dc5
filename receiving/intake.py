from lake_db import write_transaction
from receiving.coverage import InvalidEvidence, check_position_facts, get_confirmed_snapshot
from receiving.receipts import find_accepted_segment, store_accepted_segment
from receiving.requests import find_assignment, get_receiving_request
from scan.segments import SegmentEvidence, verify_segment


class StaleAssignment(InvalidEvidence):
    pass


def receive(db, request_id, evidence):
    with write_transaction(db):
        entry = get_receiving_request(db, request_id)
        if not isinstance(evidence, SegmentEvidence):
            raise InvalidEvidence('expected completed segment evidence')
        if (evidence.request_id, evidence.revision_id) != (request_id, entry.request.revision_id):
            raise InvalidEvidence('evidence belongs to a different request or frozen revision')
        if not isinstance(evidence.task_id, str):
            raise InvalidEvidence('invalid evidence task ID')
        assignment = find_assignment(db, evidence.task_id)
        if assignment is None or not verify_segment(assignment, evidence):
            raise InvalidEvidence('evidence does not match a registered scan assignment')
        previous = find_accepted_segment(db, request_id, evidence.task_id, evidence.group_number)
        if previous is not None:
            if previous.evidence != evidence:
                raise InvalidEvidence('confirmed segment has a different payload')
            return previous.receipt
        if evidence.task_id != entry.active_task_id:
            raise StaleAssignment('a replaced task cannot add new evidence')
        snapshot = get_confirmed_snapshot(db, request_id)
        check_position_facts(entry.request, snapshot, evidence)
        return store_accepted_segment(db, evidence)
