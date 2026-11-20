from dataclasses import dataclass

from audit.ledger import AuditRecord


@dataclass(frozen=True, slots=True)
class AuditDifference:
    field: str
    expected: object
    actual: object
    position: int | None = None


def format_audit_mismatch(result_id, differences):
    lines = [f'成果 {result_id} 核账不符']
    for difference in differences:
        where = '' if difference.position is None else f'（逻辑位置 {difference.position}）'
        lines.append(f'{difference.field}{where} 应为 {difference.expected!r}，收到 {difference.actual!r}')
    return '\n'.join(lines)


def format_audit_record(record: AuditRecord):
    return (f'核账 {record.audit_id} 引用成果 {record.result_id}、原首执 {record.receipt.receipt_id}，'
            f'共 {record.row_count} 行，合计 {record.total_liters} L')
