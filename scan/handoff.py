from lake_db import write_transaction
from scan.requests import get_request
from scan.tasks import get_task, store_task_assignment


def begin_handoff(db, request_id, lost_task_id, standby_replica_id, task_id):
    """Create a successor for the same obligation using a verified standby."""
    with write_transaction(db):
        obligation = get_request(db, request_id)
        lost = get_task(db, lost_task_id)
        if lost.assignment.request_id != request_id or lost.state != 'lost':
            raise ValueError('handoff requires a lost task of this request')
        if lost.assignment.replica_id == standby_replica_id:
            raise ValueError('handoff requires a different replica')
        return store_task_assignment(db, obligation, standby_replica_id, task_id, lost_task_id)
