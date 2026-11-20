"""Independent-store fixtures for the Qingyuan exclusive dispatch handoff."""
import argparse
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
from publication.signing import sign
from publication.store import find_publication_for_request
from receiving.intake import receive
from receiving.requests import register_request
from scan.requests import request
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=("v1",))
    parser.add_argument("--memory", action="store_true")
    args = parser.parse_args()
    if args.memory:
        v1(None)
    else:
        with TemporaryDirectory(prefix="qingyuan-exclusive-") as directory:
            v1(Path(directory))


if __name__ == "__main__":
    main()
