import argparse
import json
from contextlib import ExitStack, closing
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory

from audit.checks import audit
from audit.exports import format_audit_record
from audit.ledger import initialize_audit_ledger
from catalog.freeze import freeze
from catalog.layouts import ReplicaLayout
from catalog.replicas import initialize_replica_catalog, initialize_replica_store, replicate
from catalog.revisions import FrozenRow, get_revision, initialize_revisions
from intake.batches import ingest, initialize_batch_store
from intake.rows import ObservationRow
from lake_db import open_database
from publication.history import read
from publication.signing import IncompleteCoverage, sign
from publication.store import initialize_publications
from receiving.intake import StaleAssignment, receive
from receiving.receipts import initialize_segment_receipts
from receiving.requests import initialize_receiving_requests, register_assignment, register_request
from scan.handoff import begin_handoff
from scan.requests import initialize_scan_requests, request
from scan.segments import scan
from scan.tasks import assign_task, initialize_scan_tasks, record_task_state


SAMPLES = Path(__file__).with_name('samples')


def initialize_source(db):
    initialize_batch_store(db)
    initialize_revisions(db)
    initialize_replica_catalog(db)
    initialize_scan_requests(db)
    initialize_scan_tasks(db)


def initialize_receiving(db):
    initialize_receiving_requests(db)
    initialize_segment_receipts(db)
    initialize_publications(db)
    initialize_audit_ledger(db)


def initialize_directory(directory):
    directory.mkdir(parents=True, exist_ok=True)
    for name, initialize in (('source', initialize_source), ('primary', initialize_replica_store),
                             ('standby', initialize_replica_store), ('receiving', initialize_receiving)):
        with closing(open_database(directory / f'{name}.sqlite')) as db:
            initialize(db)


def _sample(name):
    return json.loads((SAMPLES / name).read_text(encoding='utf-8'))


def demo(directory):
    source_sample = _sample('frozen_f.json')
    review_sample = _sample('review_f.json')
    initialize_directory(directory)
    receiving_path = directory / 'receiving.sqlite'
    with ExitStack() as stack:
        db = stack.enter_context(closing(open_database(directory / 'source.sqlite')))
        primary = stack.enter_context(closing(open_database(directory / 'primary.sqlite')))
        standby = stack.enter_context(closing(open_database(directory / 'standby.sqlite')))
        inbox = stack.enter_context(closing(open_database(receiving_path)))
        batch_id = source_sample['batch_id']
        ingest(db, batch_id, (ObservationRow(**row) for row in source_sample['rows']))
        revision_id = freeze(db, batch_id)
        revision = get_revision(db, revision_id)
        for name, replica_db in (('primary', primary), ('standby', standby)):
            layout = ReplicaLayout.from_positions(revision, enumerate(source_sample[f'{name}_groups']))
            replicate(db, replica_db, revision_id, name, layout)
        request_id = source_sample['request_id']
        obligation = request(db, revision_id, request_id, source_sample['start'], source_sample['stop'],
                             source_sample['quality'])
        task = assign_task(db, request_id, 'primary', 'day-primary')
        register_request(inbox, obligation, task.assignment)
        first_segment = scan(db, primary, 'day-primary', 0)
        first_receipt = receive(inbox, request_id, first_segment)
        late = scan(db, primary, 'day-primary', 1)
        record_task_state(db, 'day-primary', 'lost')
        successor = begin_handoff(db, request_id, 'day-primary', 'standby', 'day-standby')
        register_assignment(inbox, successor.assignment)
        receive(inbox, request_id, scan(db, standby, 'day-standby', 1))
        try:
            premature = sign(inbox, request_id)
        except IncompleteCoverage as pending:
            if pending.missing_positions != (3,):
                raise RuntimeError(f'错误的待查位置 {pending.missing_positions}') from pending
        else:
            raise RuntimeError(f'全范围实收前签发 {premature.total_liters} L')
        try:
            receive(inbox, request_id, late)
        except StaleAssignment:
            pass
        receive(inbox, request_id, scan(db, standby, 'day-standby', 2))
        retained = receive(inbox, request_id, first_segment)
        signed = sign(inbox, request_id)

    with closing(open_database(receiving_path)) as inbox:
        collected = read(inbox, signed.result_id)
        record = audit(inbox, collected.result_id, (FrozenRow(**row) for row in review_sample['details']),
                       review_sample['total_liters'])
    expected = {'details': review_sample['details'], 'total_liters': review_sample['total_liters']}
    actual = {'details': [asdict(row) for row in signed.details], 'total_liters': signed.total_liters}
    downstream_expected = {'same_publication': True, 'same_first_receipt': True, 'same_issue_receipt': True,
                           'row_count': len(review_sample['details']), 'total_liters': review_sample['total_liters']}
    downstream_actual = {'same_publication': collected == signed, 'same_first_receipt': retained == first_receipt,
                         'same_issue_receipt': record.receipt == signed.receipt,
                         'row_count': record.row_count, 'total_liters': record.total_liters}
    if actual != expected or downstream_actual != downstream_expected:
        raise RuntimeError(f'抽查或核账不符 {actual!r} {downstream_actual!r}')
    print(format_audit_record(record))
    print('POINTWISE_SCENARIO ' + json.dumps({
        'case_id': 'v10', 'input': source_sample,
        'expected': expected, 'actual': actual,
        'downstream_expected': downstream_expected, 'downstream_actual': downstream_actual,
        'code_paths': ['scan/segments.py', 'receiving/intake.py', 'receiving/coverage.py',
                       'publication/signing.py', 'audit/checks.py'],
        'oracle': '手查样例位置 0、2、3 的标志和来源，按 7 + 11 + 7 计算，断源后比对原成果与原回执。'
    }, ensure_ascii=False, separators=(',', ':')))


def main():
    parser = argparse.ArgumentParser(description='青沅观测湖抽查台账')
    commands = parser.add_subparsers(dest='command', required=True)
    initialize = commands.add_parser('init', help='初始化或重开已有业务库')
    initialize.add_argument('directory', type=Path)
    run = commands.add_parser('demo', help='执行固定日批的接替、签发及断源核账')
    run.add_argument('--directory', type=Path, help='新建目录以保留本次演示数据库')
    args = parser.parse_args()
    if args.command == 'init':
        initialize_directory(args.directory)
    elif args.directory is not None:
        args.directory.mkdir()
        demo(args.directory)
    else:
        with TemporaryDirectory(prefix='qingyuan-demo-') as temporary:
            demo(Path(temporary))


if __name__ == '__main__':
    main()
