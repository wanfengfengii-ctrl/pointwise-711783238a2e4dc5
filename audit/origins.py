from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class OriginDifference:
    position: int
    expected: str
    actual: str


def compare_origins(expected, submitted):
    submitted_origins = {row.position: row.origin for row in submitted}
    return tuple(OriginDifference(row.position, row.origin, submitted_origins[row.position])
                 for row in expected if row.position in submitted_origins
                 and row.origin != submitted_origins[row.position])
