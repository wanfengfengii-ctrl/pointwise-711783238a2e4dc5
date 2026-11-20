from publication.store import get_publication


def read(db, result_id):
    """Retrieve the original committed package using only the receiving database."""
    return get_publication(db, result_id)
