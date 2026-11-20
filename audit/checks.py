from audit.exports import AuditDifference, format_audit_mismatch
from audit.ledger import save_audit_record
from audit.origins import compare_origins
from catalog.revisions import FrozenRow
from lake_db import write_transaction
from publication.history import read


class AuditMismatch(ValueError):
    def __init__(self, result_id, differences):
        self.result_id = result_id
        self.differences = tuple(differences)
        super().__init__(format_audit_mismatch(result_id, self.differences))


def _check_details(expected, submitted):
    differences = []
    if len(expected) != len(submitted):
        differences.append(AuditDifference('行数', len(expected), len(submitted)))
    expected_positions = tuple(row.position for row in expected)
    actual_positions = tuple(row.position for row in submitted)
    if expected_positions != actual_positions:
        differences.append(AuditDifference('逻辑位置顺序', expected_positions, actual_positions))
    expected_by_position = {row.position: row for row in expected}
    for row in submitted:
        fixed = expected_by_position.get(row.position)
        if fixed is None:
            continue
        for field in ('original_position', 'liters', 'quality'):
            original = getattr(fixed, field)
            supplied = getattr(row, field)
            if original != supplied:
                differences.append(AuditDifference(field, original, supplied, row.position))
    differences.extend(AuditDifference('出处', diff.expected, diff.actual, diff.position)
                       for diff in compare_origins(expected, submitted))
    return differences


def audit(db, result_id, details, reported_total):
    submitted = tuple(details)
    with write_transaction(db):
        publication = read(db, result_id)
        for index, row in enumerate(submitted):
            if (not isinstance(row, FrozenRow) or type(row.position) is not int
                    or type(row.original_position) is not int
                    or (row.liters is not None and type(row.liters) is not int)):
                raise AuditMismatch(result_id, (AuditDifference('明细格式', '冻结观测行', f'交回第 {index + 1} 行'),))
        differences = _check_details(publication.details, submitted)
        recalculated = 0
        for row in submitted:
            if row.liters is not None:
                recalculated += row.liters
        if type(reported_total) is not int or recalculated != reported_total:
            differences.append(AuditDifference('上报合计', recalculated, reported_total))
        if recalculated != publication.total_liters:
            differences.append(AuditDifference('签发合计', publication.total_liters, recalculated))
        if differences:
            raise AuditMismatch(result_id, differences)
        return save_audit_record(db, publication, len(submitted), recalculated)
