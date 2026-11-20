from lake_db import write_transaction
from publication.store import find_publication_for_request, save_publication
from publication.totals import ordered_details, total_liters
from receiving.coverage import get_confirmed_snapshot
from receiving.requests import get_receiving_request


class IncompleteCoverage(ValueError):
    def __init__(self, request_id, missing_positions):
        self.request_id = request_id
        self.missing_positions = missing_positions
        super().__init__(f'{request_id}: unconfirmed frozen positions {missing_positions}')


def sign(db, request_id):
    with write_transaction(db):
        obligation = get_receiving_request(db, request_id).request
        already_signed = find_publication_for_request(db, request_id)
        if already_signed is not None:
            return already_signed
        snapshot = get_confirmed_snapshot(db, request_id)
        missing = snapshot.missing_positions(obligation)
        if missing:
            raise IncompleteCoverage(request_id, missing)
        details = ordered_details(snapshot)
        return save_publication(db, request_id, obligation.revision_id, details, total_liters(details))
