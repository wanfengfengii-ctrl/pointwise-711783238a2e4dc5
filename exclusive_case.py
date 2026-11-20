"""Independent-store fixtures for the Qingyuan exclusive dispatch handoff."""
import argparse
import json
import sqlite3
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from catalog.freeze import freeze
from catalog.layouts import ReplicaLayout
from catalog.replicas import initialize_replica_store, replicate
from catalog.revisions import get_revision
from intake.batches import ingest
from intake.rows import ObservationRow
from lake_cli import initialize_receiving, initialize_source
from lake_db import open_database
from publication.history import read
from publication.signing import IncompleteCoverage, sign
from publication.store import find_publication_for_request
from receiving.intake import receive
from receiving.requests import (accept_exclusive, authorize_exclusive, bind_destination,
                                get_exclusive_withdrawal, notice_exclusive, register_request,
                                withdraw_exclusive)
from scan.requests import (ExclusiveDispatch, UnsignedReceipt, get_exclusive_dispatch,
                           release_exclusive, request, reserve_exclusive)
from scan.segments import scan
from scan.tasks import assign_task


STORES = {
    "source": initialize_source,
    "primary": initialize_replica_store,
    "standby": initialize_replica_store,
    "alice": initialize_receiving,
    "bob": initialize_receiving,
}


@dataclass(frozen=True)
class Parcel:
    sender: str
    recipient: str
    body: object


class Mailbox:
    """The driver chooses which authenticated parcel is delivered and when."""

    def __init__(self):
        self.pending = []

    def send(self, sender, recipient, body):
        if sender not in STORES or recipient not in STORES:
            raise ValueError("unknown fixture endpoint")
        self.pending.append(Parcel(sender, recipient, body))

    def take(self, index=0):
        return self.pending.pop(index)


def open_store(directory, name):
    path = ":memory:" if directory is None else directory / f"{name}.sqlite"
    db = open_database(path)
    try:
        STORES[name](db)
    except BaseException:
        db.close()
        raise
    return db


@contextmanager
def block_commit(db):
    """Deny SQLite COMMIT for the operation run inside this context."""
    def authorize(action, detail, _second, _schema, _trigger):
        if action == sqlite3.SQLITE_TRANSACTION and detail == "COMMIT":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    db.set_authorizer(authorize)
    try:
        yield
    finally:
        db.set_authorizer(None)


def seed(source, primary, standby):
    rows = (
        ObservationRow(40, "北闸/A", 7, "可用"),
        ObservationRow(12, "北闸/B", 7, "可用"),
        ObservationRow(31, "南闸/C", 11, "可用"),
        ObservationRow(88, "东闸/D", 7, "可用"),
    )
    ingest(source, "F", rows)
    revision = get_revision(source, freeze(source, "F"))
    for name, db, groups in (
        ("primary", primary, ((0, 1), (2, 3))),
        ("standby", standby, ((0,), (1, 2), (3,))),
    ):
        layout = ReplicaLayout.from_positions(revision, enumerate(groups))
        replicate(source, db, "F", name, layout)


def v1(directory):
    with ExitStack() as stack:
        stores = {name: stack.enter_context(closing(open_store(directory, name))) for name in STORES}
        source = stores["source"]
        seed(source, stores["primary"], stores["standby"])
        mailbox = Mailbox()
        for name, replica, start, stop, group in (
            ("alice", "primary", 0, 2, 0), ("bob", "standby", 1, 3, 1),
        ):
            obligation = request(source, "F", name, start, stop, "可用")
            task = assign_task(source, name, replica, f"{name}-scan").assignment
            mailbox.send("source", name, (obligation, task))
            mailbox.send(replica, name, scan(source, stores[replica], task.task_id, group))
        for _ in range(2):
            registration, segment = mailbox.take(), mailbox.take()
            inbox = stores[registration.recipient]
            obligation, task = registration.body
            register_request(inbox, obligation, task)
            receive(inbox, obligation.request_id, segment.body)
        for name in ("source", "primary", "standby"):
            stores[name].close()

        alice, bob = stores["alice"], stores["bob"]
        first = sign(alice, "alice")
        with block_commit(bob):
            try:
                sign(bob, "bob")
            except sqlite3.DatabaseError as error:
                if error.sqlite_errorcode != sqlite3.SQLITE_AUTH:
                    raise
            else:
                raise AssertionError("commit was not blocked")
        if bob.in_transaction or find_publication_for_request(bob, "bob") is not None:
            raise AssertionError("failed commit published an ordinary review")
        second = sign(bob, "bob")
        actual = tuple((tuple(row.position for row in result.details), result.total_liters)
                       for result in (first, second))
        expected = (((0, 1), 14), ((1, 2), 18))
        if actual != expected:
            raise AssertionError(f"ordinary reviews: {actual!r} != {expected!r}")
        print(f"普通复验各自出账 {actual!r}，逻辑位置 1 可在两份成果中出现")


def _open_team(directory, stack):
    stores = {name: stack.enter_context(closing(open_store(directory, name))) for name in STORES}
    seed(stores["source"], stores["primary"], stores["standby"])
    bind_destination(stores["alice"], "alice")
    bind_destination(stores["bob"], "bob")
    return stores


def _dispatch(source, revision_id, dispatch_id, destination_id, start, stop, quality,
              replica_id, task_id):
    binding = reserve_exclusive(source, revision_id, dispatch_id, destination_id,
                                start, stop, quality)
    authorization = authorize_exclusive(binding)
    task = assign_task(source, binding.request_id, replica_id, task_id).assignment
    return binding, authorization, task


def _view(result):
    return [row.position for row in result.details], result.total_liters


def _expect_rejection(call, label):
    try:
        call()
    except (ValueError, LookupError):
        return
    raise AssertionError(f"{label} was not rejected")


def sign_first(directory):
    """Accept, receive fully, lose the sign reply and keep the range occupied."""
    with ExitStack() as stack:
        stores = _open_team(directory, stack)
        source, primary, standby = stores["source"], stores["primary"], stores["standby"]
        alice = stores["alice"]
        binding, authorization, task = _dispatch(source, "F", "A", "alice", 0, 2, "可用",
                                                 "primary", "alice-scan")
        alice_parcel = (authorization, task)
        accept_exclusive(alice, *alice_parcel, sender_id="source")
        try:
            sign(alice, "A")
        except IncompleteCoverage as pending:
            if pending.missing_positions != (0, 1):
                raise AssertionError(f"unexpected pending positions {pending.missing_positions!r}")
        else:
            raise AssertionError("signed before any segment was received")
        first_receipt = receive(alice, "A", scan(source, primary, "alice-scan", 0))

        _expect_rejection(lambda: reserve_exclusive(source, "F", "B", "bob", 1, 3, "可用"),
                          "overlapping standby dispatch")
        reserve_exclusive(source, "F", "S", "bob", 2, 3, "可用")

        # Disconnect the dispatch ledger and the replica copies before signing.
        source.close()
        primary.close()
        standby.close()
        signed = sign(alice, "A")  # succeeds; the reply parcel is lost on the way back

        source = stack.enter_context(closing(open_store(directory, "source")))
        _expect_rejection(lambda: reserve_exclusive(source, "F", "B2", "bob", 1, 3, "可用"),
                          "range held by the signed dispatch")
        _expect_rejection(lambda: reserve_exclusive(source, "F", "C", "bob", 0, 1, "可用"),
                          "signed dispatch still occupying its positions")
        # The standby may still take its own disjoint share alone.
        standby = stack.enter_context(closing(open_store(directory, "standby")))
        _, bob_auth, bob_task = _dispatch(source, "F", "S", "bob", 2, 3, "可用",
                                          "standby", "bob-scan")
        bob = stores["bob"]
        accept_exclusive(bob, bob_auth, bob_task, sender_id="source")
        bob_segment = scan(source, standby, "bob-scan", 1)
        if bob_segment is None or bob_segment.checked_positions != (2,):
            raise AssertionError("standby segment did not cover position 2")
        receive(bob, "S", bob_segment)
        bob_result = sign(bob, "S")
        if _view(bob_result) != ([2], 11):
            raise AssertionError(f"standby share: {_view(bob_result)!r}")

        alice.close()
        alice = stack.enter_context(closing(open_store(directory, "alice")))
        again = sign(alice, "A")
        collected = read(alice, again.result_id)
        if not (signed == again == collected) or _view(collected) != ([0, 1], 14):
            raise AssertionError(f"original result changed: {_view(collected)!r}")
        _expect_rejection(lambda: withdraw_exclusive(alice, binding, sender_id="source"),
                          "withdrawal after signature")
        if read(alice, again.result_id).receipt != signed.receipt:
            raise AssertionError("first issue receipt changed")
        summary = {"dispatch": "A", "positions": [0, 1], "liters": 14,
                   "standby_share": {"positions": [2], "liters": 11},
                   "release": "撤回拒绝，位置保持占用"}
        print(f"签发先成：甲领 {summary['positions']}，{summary['liters']} L；"
              f"乙 [1,3) 被拒、单独领 [2,3) 得 11 L；甲撤回无法归还，位置仍占用")
        return summary


def withdraw_first(directory):
    """Withdraw before the authorization arrives, release, then let the standby sign."""
    with ExitStack() as stack:
        stores = _open_team(directory, stack)
        source, standby = stores["source"], stores["standby"]
        alice, bob = stores["alice"], stores["bob"]
        binding, authorization, task = _dispatch(source, "F", "A", "alice", 0, 2, "可用",
                                                 "primary", "alice-scan")
        # The early notice already pinned this dispatch to alice; a notice sent to
        # the other library must be refused.
        outbox_binding, _, _ = _dispatch(source, "F", "X", "alice", 3, 4, "待核",
                                         "primary", "x-scan")
        notice_exclusive(alice, outbox_binding, sender_id="source")
        _expect_rejection(lambda: notice_exclusive(bob, outbox_binding, sender_id="source"),
                          "dispatch notice delivered to the wrong library")
        # The authorization stays cached; the withdrawal instruction reaches the target first.
        receipt = withdraw_exclusive(alice, binding, sender_id="source")  # reply lost

        alice.close()
        alice = stack.enter_context(closing(open_store(directory, "alice")))
        recovered = get_exclusive_withdrawal(alice, "A")
        if recovered != receipt or not isinstance(recovered, UnsignedReceipt):
            raise AssertionError("unsigned receipt was not recoverable after reopen")
        _expect_rejection(lambda: reserve_exclusive(source, "F", "B", "bob", 1, 3, "可用"),
                          "overlapping dispatch before release")
        accepted = release_exclusive(source, "A", recovered, sender_id="alice")
        if release_exclusive(source, "A", recovered, sender_id="alice") != accepted:
            raise AssertionError("release replay did not return the accepted receipt")

        bob_binding, bob_auth, bob_task = _dispatch(source, "F", "B", "bob", 1, 3, "可用",
                                                    "standby", "bob-scan")
        accept_exclusive(bob, bob_auth, bob_task, sender_id="source")
        receive(bob, "B", scan(source, standby, "bob-scan", 1))
        source.close()
        standby.close()
        bob_signed = sign(bob, "B")
        if _view(bob_signed) != ([1, 2], 18):
            raise AssertionError(f"standby result: {_view(bob_signed)!r}")

        source = stack.enter_context(closing(open_store(directory, "source")))
        _expect_rejection(lambda: accept_exclusive(alice, authorization, task, sender_id="source"),
                          "late authorization at the withdrawn target")
        _expect_rejection(lambda: accept_exclusive(alice, bob_auth, bob_task, sender_id="source"),
                          "authorization delivered to the wrong library")
        _expect_rejection(lambda: withdraw_exclusive(bob, binding, sender_id="source"),
                          "withdrawal at the wrong library")
        other = get_exclusive_dispatch(source, "B")
        wrong_dispatch = UnsignedReceipt("unsigned:B", "B", "B", "F", "bob", 1, 3, "可用")
        _expect_rejection(lambda: release_exclusive(source, "A", wrong_dispatch, sender_id="bob"),
                          "release for dispatch A with dispatch B's receipt")
        wrong_library = UnsignedReceipt("unsigned:A", "A", "A", "F", "bob", binding.start,
                                        binding.stop, binding.quality)
        _expect_rejection(lambda: release_exclusive(source, "A", wrong_library, sender_id="bob"),
                          "release naming a different target library")
        summary = {"dispatch": "A", "released": list(range(binding.start, binding.stop)),
                   "standby_positions": [1, 2], "standby_liters": 18,
                   "late_authorization": "拒绝"}
        print(f"撤回先成：甲交回未签凭据，资料库归还 [0,2)；乙领 [1,3) 断源签出 "
              f"{summary['standby_liters']} L；迟到授权被拒")
        return summary


def commit_gate(directory):
    """Block one commit each for signing and withdrawal, then retry the same terms."""
    with ExitStack() as stack:
        stores = _open_team(directory, stack)
        source, primary, alice = stores["source"], stores["primary"], stores["alice"]
        binding, authorization, task = _dispatch(source, "F", "A", "alice", 0, 2, "可用",
                                                 "primary", "alice-scan")
        accept_exclusive(alice, authorization, task, sender_id="source")
        for number in (0,):
            receive(alice, "A", scan(source, primary, "alice-scan", number))

        with block_commit(alice):
            try:
                sign(alice, "A")
            except sqlite3.DatabaseError as error:
                if error.sqlite_errorcode != sqlite3.SQLITE_AUTH:
                    raise
            else:
                raise AssertionError("signing commit was not blocked")
        if alice.in_transaction or find_publication_for_request(alice, "A") is not None:
            raise AssertionError("blocked signing left a partial publication")
        _expect_rejection(lambda: reserve_exclusive(source, "F", "H", "bob", 0, 1, "可用"),
                          "range held after a blocked signing commit")
        signed = sign(alice, "A")
        if sign(alice, "A") != signed:
            raise AssertionError("signing retry created a second finality")

        _dispatch(source, "F", "D", "alice", 2, 3, "可用", "primary", "d-scan")
        d_binding = get_exclusive_dispatch(source, "D")
        with block_commit(alice):
            try:
                withdraw_exclusive(alice, d_binding, sender_id="source")
            except sqlite3.DatabaseError as error:
                if error.sqlite_errorcode != sqlite3.SQLITE_AUTH:
                    raise
            else:
                raise AssertionError("withdrawal commit was not blocked")
        from receiving.requests import find_exclusive_withdrawal
        if alice.in_transaction or find_exclusive_withdrawal(alice, "D") is not None:
            raise AssertionError("blocked withdrawal left a partial receipt")
        receipt = withdraw_exclusive(alice, d_binding, sender_id="source")
        if withdraw_exclusive(alice, d_binding, sender_id="source") != receipt:
            raise AssertionError("withdrawal retry created a second receipt")
        release_exclusive(source, "D", receipt, sender_id="alice")

        _, pending_auth, pending_task = _dispatch(source, "F", "E", "alice", 3, 4, "待核",
                                                  "primary", "e-scan")
        accept_exclusive(alice, pending_auth, pending_task, sender_id="source")
        receive(alice, "E", scan(source, primary, "e-scan", 1))
        empty = sign(alice, "E")
        if (empty.details, empty.total_liters) != ((), 0):
            raise AssertionError(f"pending filter published {_view(empty)!r}")
        _expect_rejection(lambda: reserve_exclusive(source, "F", "G", "bob", 3, 4, "可用"),
                          "signed empty dispatch still occupying its position")
        summary = {"sign_retry": _view(signed), "withdraw_retry": receipt.receipt_id,
                   "empty_details": [], "empty_liters": 0}
        print(f"提交拦截：签发与撤回各失败一次后同参重试各得一个终局；[3,4) 待核签出空明细 0 L")
        return summary


def demo(directory):
    base = None if directory is None else Path(directory)
    results = {}
    for name, scenario in (("sign_first", sign_first), ("withdraw_first", withdraw_first)):
        target = None if base is None else base / name
        if target is not None and not target.exists():
            target.mkdir()
        results[name] = scenario(target)
    return results


SCENARIOS = {"v1": v1, "sign": sign_first, "withdraw": withdraw_first,
             "commit": commit_gate, "demo": demo}


def run_scenario(name, directory=None):
    """Run one named scenario inside a fresh or supplied temporary directory."""
    if directory is not None:
        return SCENARIOS[name](Path(directory))
    with TemporaryDirectory(prefix=f"qingyuan-exclusive-{name}-") as folder:
        return SCENARIOS[name](Path(folder))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=tuple(SCENARIOS))
    parser.add_argument("--memory", action="store_true",
                        help="run v1 entirely in memory (reopening scenarios need files)")
    args = parser.parse_args()
    if args.memory and args.scenario == "v1":
        result = SCENARIOS[args.scenario](None)
    else:
        with TemporaryDirectory(prefix="qingyuan-exclusive-") as directory:
            result = SCENARIOS[args.scenario](Path(directory))
    if result is not None:
        print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
