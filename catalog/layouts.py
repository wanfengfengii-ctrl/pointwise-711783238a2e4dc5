from dataclasses import dataclass

from catalog.revisions import FrozenRow


@dataclass(frozen=True, slots=True)
class LayoutGroup:
    number: int
    rows: tuple[FrozenRow, ...]


@dataclass(frozen=True, slots=True)
class ReplicaLayout:
    groups: tuple[LayoutGroup, ...]

    @classmethod
    def from_positions(cls, revision, numbered_positions):
        groups = []
        for number, positions in numbered_positions:
            members = []
            for position in positions:
                if type(position) is not int or not 0 <= position < len(revision.rows):
                    raise ValueError('layout position outside the frozen revision')
                members.append(revision.rows[position])
            groups.append(LayoutGroup(number, tuple(members)))
        return cls(tuple(groups))


def group_positions(group):
    return tuple(row.position for row in group.rows)


def validate_layout(revision, layout):
    if not isinstance(layout, ReplicaLayout) or type(layout.groups) is not tuple:
        raise ValueError('expected an immutable ReplicaLayout')
    numbers = set()
    positions = set()
    for group in layout.groups:
        if not isinstance(group, LayoutGroup) or type(group.rows) is not tuple or not group.rows:
            raise ValueError('expected a nonempty immutable LayoutGroup')
        if type(group.number) is not int or not 0 <= group.number < 2**63:
            raise ValueError('invalid replica group number')
        if group.number in numbers:
            raise ValueError(f'duplicate replica group {group.number}')
        numbers.add(group.number)
        for row in group.rows:
            if not isinstance(row, FrozenRow):
                raise ValueError('invalid replica member')
            position = row.position
            if type(position) is not int or not 0 <= position < len(revision.rows):
                raise ValueError('layout position outside the frozen revision')
            if position in positions:
                raise ValueError(f'layout repeats frozen position {position}')
            if (type(row.original_position) is not int
                    or (row.liters is not None and type(row.liters) is not int)
                    or row != revision.rows[position]):
                raise ValueError(f'layout content differs at frozen position {position}')
            positions.add(position)
    if len(positions) != len(revision.rows):
        missing = sorted(set(range(len(revision.rows))) - positions)
        raise ValueError(f'layout is missing frozen positions {missing}')
