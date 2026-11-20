from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ObservationRow:
    position: int
    origin: str
    liters: int | None
    quality: str


def encode_liters(value: int | None) -> str | None:
    if value is None:
        return None
    if type(value) is not int:
        raise ValueError('liters must be an integer or None')
    # Signed hex text handles integers beyond SQLite INTEGER and decimal conversion limits.
    return format(value, 'x')


def decode_liters(value: str | None) -> int | None:
    return None if value is None else int(value, 16)
