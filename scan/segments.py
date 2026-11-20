import hmac
import json
from dataclasses import dataclass, field, replace

from catalog.layouts import group_positions
from catalog.replicas import get_replica, read_replica_group
from catalog.revisions import FrozenRow, decode_frozen_row, encode_frozen_row
from lake_db import canonical_json
from scan.requests import get_request
from scan.tasks import record_task_state


@dataclass(frozen=True, slots=True)
class SegmentEvidence:
    request_id: str
    revision_id: str
    task_id: str
    replica_id: str
    group_number: int
    checked_positions: tuple[int, ...]
    hits: tuple[FrozenRow, ...]
    seal: bytes = field(repr=False)


def segment_body(evidence):
    return canonical_json([evidence.request_id, evidence.revision_id, evidence.task_id,
                           evidence.replica_id, evidence.group_number, evidence.checked_positions,
                           [encode_frozen_row(row) for row in evidence.hits]])


def restore_segment(body, seal):
    request_id, revision_id, task_id, replica_id, number, positions, hits = json.loads(body)
    return SegmentEvidence(request_id, revision_id, task_id, replica_id, number, tuple(positions),
                           tuple(map(decode_frozen_row, hits)), seal)


def _seal(assignment, evidence):
    return hmac.digest(assignment.seal_key, segment_body(evidence).encode('utf-8'), 'sha256')


def verify_segment(assignment, evidence):
    if (evidence.request_id, evidence.revision_id, evidence.task_id, evidence.replica_id) != (
            assignment.request_id, assignment.revision_id, assignment.task_id, assignment.replica_id):
        return False
    if (type(evidence.group_number) is not int or not 0 <= evidence.group_number < 2**63
            or type(evidence.checked_positions) is not tuple or type(evidence.hits) is not tuple
            or type(evidence.seal) is not bytes):
        return False
    try:
        return hmac.compare_digest(evidence.seal, _seal(assignment, evidence))
    except (AttributeError, ValueError, TypeError):
        return False


def scan(db, replica_db, task_id, group_number):
    assignment = record_task_state(db, task_id, 'running').assignment
    obligation = get_request(db, assignment.request_id)
    replica = get_replica(db, assignment.revision_id, assignment.replica_id)
    group = read_replica_group(replica_db, replica, group_number)
    checked = sorted((row for position, row in zip(group_positions(group), group.rows)
                      if obligation.start <= position < obligation.stop), key=lambda row: row.position)
    if not checked:
        return None
    evidence = SegmentEvidence(obligation.request_id, obligation.revision_id, task_id,
                               assignment.replica_id, group_number,
                               tuple(row.position for row in checked),
                               tuple(row for row in checked if row.quality == obligation.quality), b'')
    return replace(evidence, seal=_seal(assignment, evidence))
