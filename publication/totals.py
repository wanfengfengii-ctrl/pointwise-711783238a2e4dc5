def ordered_details(snapshot):
    return tuple(sorted(snapshot.hits, key=lambda row: row.position))


def total_liters(details):
    return sum(row.liters for row in details if row.liters is not None)
