from dataclasses import dataclass

from catalog.revisions import FrozenRow
from receiving.receipts import list_accepted_segments
from receiving.requests import get_receiving_request


class InvalidEvidence(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class CoverageSnapshot:
    checked_positions: frozenset[int]
    hits: tuple[FrozenRow, ...]

    def missing_positions(self, obligation):
        return tuple(position for position in range(obligation.start, obligation.stop)
                     if position not in self.checked_positions)


def check_position_facts(obligation, snapshot, evidence):
    positions = evidence.checked_positions
    if not positions or any(type(pos) is not int or not obligation.start <= pos < obligation.stop
                            for pos in positions):
        raise InvalidEvidence('invalid checked positions for the request')
    if any(left >= right for left, right in zip(positions, positions[1:])):
        raise InvalidEvidence('checked positions must be ordered and unique')
    checked = set(positions)
    hits = {}
    previous_position = -1
    for row in evidence.hits:
        if (not isinstance(row, FrozenRow) or type(row.position) is not int or row.position not in checked
                or row.position <= previous_position or row.quality != obligation.quality):
            raise InvalidEvidence('hits must be ordered, qualified and within the checked positions')
        hits[row.position] = row
        previous_position = row.position
    previous_hits = {row.position: row for row in snapshot.hits}
    for pos in snapshot.checked_positions.intersection(checked):
        if previous_hits.get(pos) != hits.get(pos):
            raise InvalidEvidence(f'conflicting confirmed facts at frozen position {pos}')


def get_confirmed_snapshot(db, request_id):
    get_receiving_request(db, request_id)
    checked = set()
    hits = {}
    for accepted in list_accepted_segments(db, request_id):
        checked.update(accepted.evidence.checked_positions)
        for row in accepted.evidence.hits:
            hits[row.position] = row
    return CoverageSnapshot(frozenset(checked), tuple(hits[pos] for pos in sorted(hits)))
