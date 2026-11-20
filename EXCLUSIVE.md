# 独占抽查交班

资料库统一分配一个冻结批次内的逻辑位置。两个值班库各有独立数据库和持久库名。`README.md` 中的普通复验可以检查同一位置，原有签发、领取、核账和主备接替继续使用。

## 调度与办理入口

- `scan.requests.reserve_exclusive(source, revision_id, dispatch_id, destination_id, start, stop, quality)` 返回原派工绑定。冻结号、零基半开范围、质控标志、派工号和目标库均固定。原派工绑定公开 `request_id` 供现有扫描入口使用。全范围可分配时才登记，未结派工和已签派工均占位。范围检查须覆盖所有逻辑位置。同号同参返回原派工绑定，归还后重投原号也只领原绑定。新派工使用新号。
- `receiving.requests.bind_destination(inbox, destination_id)` 持久绑定本库名称，重复用同一配置可重开，异名拒绝。初始化沿用原库。`receiving.requests.accept_exclusive(inbox, authorization, primary, *, sender_id)` 在具名库验收最终授权及对应的原扫描主任务，成功返回 `ReceivingRequest`。允许在最终授权前交换其他交接消息，若采用这些消息，请公开其真实调用入口。已发出的授权可按原件延迟和重投。
- `receiving.intake.receive` 沿用实际扫描段与覆盖判据。`publication.signing.sign(inbox, request_id)` 在取得独占资格且覆盖完整时，返回原格式 `PublishedResult`，含固定正文、精确合计和首执。独占终局与成果及首执同次提交，只有目标库参与。完工后同号查询或重签返回原成果。普通登记和签发入口不得让独占任务绕过目标验收与终局。
- `receiving.requests.withdraw_exclusive(inbox, dispatch, *, sender_id)` 凭原派工绑定办理整单未签撤回，返回该目标库的未签凭据。同绑定重复办理返回原凭据。即使最终授权从未到库也可办理，成功后原授权始终无法登记或签发。若已经签发，撤回请求拒绝，原成果及首执仍可领取。
- `scan.requests.release_exclusive(source, dispatch_id, receipt, *, sender_id)` 凭该派工原目标库的确切未签凭据归还位置，返回受理的凭据。重投旧凭据只复领原归还结果，位置若已重新派给别单，现有归属保持。收不到回包时原范围继续占位。

`sender_id` 来自可信交接通道的实际发件端，夹具以 `Parcel.sender` 表示。派工授权与撤回指令从资料库发出，未签凭据从执行裁决的目标库发出。消息表示可自定，凭据须可按原绑定比对及复领。测试通道可调换真实来件、延迟、丢弃或重送回包。错库、异绑、同号异参和区间冲突抛 `ValueError`。旧接口的未知记录与覆盖不足异常沿用原约定。输入拒绝或提交失败保留此前状态。验收、签发、撤回和归还可分别重试，成功结果只在相应库提交后交付。

## 联合核验与演示

`python3 -B exclusive_case.py v1` 用 `alice`、`bob` 两个独立值班库验证普通复验。夹具 F 的四个冻结位置依次为 7、7、11、7 L，均为“可用”。甲的主副本覆盖组为 [0,2)、[2,4)，乙的备用副本覆盖组为 [0,1)、[1,3)、[3,4)。普通复验分别签出 14 L 和 18 L。脚本提供开库、种数、传输事件和提交拦截，供独占核验复用。

- 签发先成。甲得 [0,2)，实收后断开资料库与副本，签发成功但丢回包。资料端重开后拒绝乙的 [1,3)，允许乙单独领 [2,3)。甲库重开后仍可领 14 L 的原成果和首执，随后撤回无法归还位置。
- 撤回先成。缓存给甲的原授权，甲库先办理撤回并丢回包。重开甲库取回原未签凭据，资料库据此归还 [0,2)，乙可领 [1,3) 并断源签出 18 L。迟到甲授权不能登记或签发。把另一库来件或另一单的凭据用于该单应拒绝。
- 在目标办理签发和撤回时分别阻断一次提交，确认失败前的实收和源端占用保留，同参重试只有一个终局。对 [3,4) 用“待核”筛选，实收完整后签出空明细和 0 L，随后申请同一位置仍须拒绝。

交付时沿用 `.pointwise-runtime.json`。`version` 为 `1`，`kind` 为 `cli`，`setup` 为 argv 列表，`verify`、`demo` 各为一条 argv。`verify` 运行原回归及上述联合核验，任一核验失败时退出非零。`demo` 展示两种交班的实际成果与归还结果。原演示仍可用 `python3 -B -m lake_cli demo` 运行。

## 实现入口说明

- 原派工绑定 `scan.requests.ExclusiveDispatch` 由 `reserve_exclusive` 返回，`request_id` 与派工号一致，直接供 `scan.tasks.assign_task`、`scan.segments.scan` 和 `receiving.intake.receive` 使用。`get_exclusive_dispatch` 可按派工号复查原绑定。
- 最终授权由资料库侧 `receiving.requests.authorize_exclusive(dispatch)` 生成，交给 `accept_exclusive`。最终授权前交换的其它交接消息使用 `receiving.requests.notice_exclusive(inbox, dispatch, *, sender_id)` 登记，该入口同样只接受资料库来件与具名目标库，并可拒绝错库来件。
- 未签凭据是 `scan.requests.UnsignedReceipt`，可按原绑定比对，`receipt_id` 稳定可复领；`scan.requests.release_exclusive` 返回受理凭据，重复投递同凭据只复领原结果。
