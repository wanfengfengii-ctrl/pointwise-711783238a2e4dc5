from intake.rows import ObservationRow, encode_liters


QUALITY_FLAGS = frozenset(('可用', '待核'))


def require_text(value, field):
    if not isinstance(value, str) or not value.strip() or '\x00' in value:
        raise ValueError(f'{field} must be nonblank text without NUL')
    try:
        value.encode('utf-8')
    except UnicodeEncodeError as error:
        raise ValueError(f'{field} must be valid Unicode text') from error


def validate_quality_flag(flag):
    if not isinstance(flag, str) or flag not in QUALITY_FLAGS:
        raise ValueError('unknown quality flag')


def validate_batch(batch_id, rows):
    require_text(batch_id, 'batch ID')
    ordered = tuple(rows)
    seen = set()
    for row in ordered:
        if not isinstance(row, ObservationRow):
            raise ValueError('expected an ObservationRow')
        if type(row.position) is not int or row.position < 0:
            raise ValueError('source position must be a nonnegative integer')
        if row.position in seen:
            raise ValueError(f'duplicate source position {row.position}')
        seen.add(row.position)
        require_text(row.origin, 'origin')
        validate_quality_flag(row.quality)
        encode_liters(row.liters)
    return ordered
