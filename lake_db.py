import json
import sqlite3
from contextlib import contextmanager


def open_database(path):
    """Open an independent store with foreign keys and explicit writes."""
    db = sqlite3.connect(path, isolation_level=None)
    try:
        db.execute('PRAGMA foreign_keys = ON')
    except BaseException:
        db.close()
        raise
    return db


@contextmanager
def write_transaction(db):
    db.execute('BEGIN IMMEDIATE')
    try:
        yield
        db.commit()
    except BaseException:
        if db.in_transaction:
            db.rollback()
        raise


def initialize_schema(db, ddl):
    if db.in_transaction:
        raise RuntimeError('initialize outside a transaction')
    try:
        db.executescript('BEGIN IMMEDIATE;\n' + ddl)
        db.commit()
    except BaseException:
        if db.in_transaction:
            db.rollback()
        raise


def canonical_json(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))
