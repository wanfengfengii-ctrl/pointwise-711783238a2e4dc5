from lake_db import write_transaction
from publication.store import find_publication_for_request, save_publication
from publication.totals import ordered_details, total_liters
from receiving.coverage import get_confirmed_snapshot
from receiving.requests import (find_exclusive_authorization, find_exclusive_withdrawal,
                                get_destination, get_receiving_request)


class IncompleteCoverage(ValueError):
    def __init__(self, request_id, missing_positions):
        self.request_id = request_id
        self.missing_positions = missing_positions
        super().__init__(f'{request_id}: unconfirmed frozen positions {missing_positions}')


class WithdrawnDispatch(ValueError):
    pass


def sign(db, request_id):
    with write_transaction(db):
        obligation = get_receiving_request(db, request_id).request
        already_signed = find_publication_for_request(db, request_id)
        if already_signed is not None:
            return already_signed
        # Signing and withdrawal are mutually exclusive: whichever commits first wins.
        if find_exclusive_withdrawal(db, request_id) is not None:
            raise WithdrawnDispatch('a withdrawn dispatch can never be signed')
        # Only a dispatch that accepted its final authorization may publish, and only
        # through the named target library that holds that authorization.
        authorization = find_exclusive_authorization(db, request_id)
        if authorization is not None:
            _require_exclusive_terms(db, authorization, obligation)
        snapshot = get_confirmed_snapshot(db, request_id)
        missing = snapshot.missing_positions(obligation)
        if missing:
            raise IncompleteCoverage(request_id, missing)
        details = ordered_details(snapshot)
        return save_publication(db, request_id, obligation.revision_id, details, total_liters(details))


def _require_exclusive_terms(db, authorization, obligation):
    if authorization.binding().terms() != (obligation.revision_id, get_destination(db),
                                            obligation.start, obligation.stop, obligation.quality):
        raise ValueError('exclusive authorization does not match this target library')
