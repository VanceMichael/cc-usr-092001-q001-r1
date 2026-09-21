# 领域约定

暴雨预警版本分发中枢围绕气象预警、订正版本与渠道回执保存可核对的业务记录。外部主体使用不含真实身份信息的稳定引用编号，时间采用带偏移量的 ISO 8601 字符串，材料只保存受控引用和 `sha256` 摘要。

## 事件信封

交换事件包含 `event_id`、`source_id`、`subject_ref`、`event_type`、`occurred_at`、`source_sequence`、`payload`、`payload_digest`。

* `event_id`：事件全局唯一编号；同一编号重复投递按幂等处理，重复但摘要/序号不一致按冲突拒绝（409）。
* `source_sequence`：**只在同一 `source_id` 内递增**。序号不新于来源已应用序号的消息为迟到消息：原样留痕（`apply_state=stale`）并保留哈希链位置，但不产生任何投影，绝不覆盖较新的决定。
* `occurred_at`：来源声明的发生时间，必须带时区偏移量；接收方保留原始发生时间和原始偏移，不用到达时间覆盖。跨午夜的有效期（`valid_from`/`valid_to`/`valid_for`）按该偏移计算与展示。
* `payload_digest`：发送方对 payload 规范化（键排序、无空白、不转义非 ASCII）后计算的 `sha256:` 摘要；接收方复算，不一致直接拒收。

## 事件类型

| 类型 | 语义 | payload 要点 |
| --- | --- | --- |
| `issue` | 首发预警 | `warning_level`、`areas[]`、雨量区间 `rainfall_mm:[min,max]`、`hourly_intensity_mm`、时效 `valid_from` + `valid_to`/`valid_for` |
| `amend` | 订正（范围增删、等级与雨强调整） | `areas[]` 为**订正后的完整范围**；未提供的雨量字段沿用上一版；退出范围的地区给 `withdrawn_reason` |
| `lift` | 解除 | `areas[]` 为空或缺省表示解除全部；可指定部分地区；`effective_at` 指定解除时刻 |

## 生效片段（fragment）

每个行政区在每次订正后形成一条可追溯的片段记录：

* `chain_version`：同一 `subject_ref` 内单调递增的版本号；
* `basis_event_id`：形成当前内容的决定事件（订正即指回该次 amend）；
* `replaces_fragment_id`：被替代的上一版片段，构成版本依据链；
* `closed_by_event_id` / `close_reason`：片段因何事件被替代（superseded）或解除（lifted）；
* 旧片段 `valid_to` 与新版本 `valid_from` 对齐，任一时间点一个地区至多命中一个有效片段，历史时刻可回放。

## 分发与确认

* 送达记录按“**依据事件 × 地区 × 渠道 × 岗位**”唯一（短信/广播/政务终端 + 值班/主管等岗位），渠道重试与重放事件都不会重复建单。
* 送达内容在建单时按原始时区定型入库；自动重试采用 30s/2m/5m/15m/30m 退避，超过 `max_attempts` 挂起等待人工补发；人工补发以 `request_id` 幂等，且不受自动次数上限限制。
* 接收确认（ACK）以 `ack_id` 幂等，且绑定 `(delivery_id, recipient_id)`，编号不得跨目标复用；未确认接收方在送达详情中列出。
* 全部分发状态落库：进程重启后 pending 与崩溃残留的 sending 都会继续处理。

## 不可篡改性

`events` 表为追加写台账（数据库触发器禁止 UPDATE/DELETE），每行保存 `chain_hash = sha256(上一行 chain_hash ‖ 规范信封)`。`GET /v1/integrity` 重算哈希链与载荷摘要，任何对历史版本的静默改写（改内容、删行、乱序）都会暴露。

示例内容仅用于说明字段形状，不代表真实人员、机构或业务结论。
