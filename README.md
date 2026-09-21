# 暴雨预警版本分发中枢

跨夜强降雨过程中，气象台会连续调整受影响市县、雨量区间与小时雨强。本服务独立接收带来源序号和签名摘要的预警发布、订正与解除消息，把重叠行政区拆成可追溯的生效片段，按地区、渠道和接收岗位生成唯一送达记录，并支持值班主管随时回放“某地区此刻处于何种预警、依据哪次订正形成、哪些接收方尚未确认”。

核心保证：

* **迟到不覆盖**：来源序号只在同源内单调前进，迟到旧序号消息仅留痕不生效；
* **跨夜时区正确**：所有时间保留来源原始偏移量，按绝对时刻比较；
* **全程幂等**：事件重放、渠道重试、人工补发（`request_id`）、接收确认（`ack_id`）重复提交无副作用；
* **重启可续**：分发状态全部落库，重启后 pending 与崩溃残留的 sending 继续处理；
* **历史不可静默改写**：事件台账追加写（触发器禁改禁删）+ 哈希链 + 载荷摘要自检。

## 目录

- `src/` 服务代码：`timeutil` / `canon` / `storage` / `events` / `dispatch` / `queries` / `channels` / `app`
- `scripts/migrate.py` 数据库初始化
- `contracts/` 外部交换字段示例（issue / amend / lift / ack）
- `docs/domain.md` 领域对象、时间与标识约定
- `tests/` 行为测试（unittest，32 个用例）

## 运行

```bash
make test      # 运行测试
make migrate   # 初始化 SQLite（默认 data/app.sqlite3）
make run       # 启动服务，默认 :8080
```

配置（环境变量）：`PORT`、`DATABASE_PATH`、`DISPATCH_INTERVAL`（后台重试扫描秒数，默认 5）。
Docker：`docker build -t warning-hub . && docker run -p 8080:8080 -v $PWD/data:/data warning-hub`。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/events` | 接入 issue/amend/lift 事件（校验摘要与来源序号） |
| GET  | `/v1/events` | 事件台账（含 stale 留痕，`?include_stale=false` 过滤） |
| PUT  | `/v1/admin/recipients` | 登记接收岗位（地区 × 渠道 sms/broadcast/gov_terminal × 岗位 × 地址） |
| GET  | `/v1/warnings/at?area_code=&at=` | 任一时刻某地区的生效预警与完整依据链（`at` 为带偏移 ISO 时间，缺省为现在） |
| GET  | `/v1/timeline?subject_ref=&area_code=` | 片段版本时间线 |
| GET  | `/v1/deliveries?area_code=&channel=` | 送达记录列表（含未确认人数） |
| GET  | `/v1/deliveries/{id}` | 送达详情：内容、尝试记录、已/未确认接收方 |
| POST | `/v1/deliveries/{id}/reissue` | 人工补发（body 带幂等 `request_id`、`operator_ref`） |
| POST | `/v1/acks` | 接收确认回执（`ack_id` 幂等，绑定送达与接收方） |
| POST | `/v1/dispatch/run` | 立即执行一次到期分发扫描（运维便利接口） |
| GET  | `/v1/integrity` | 哈希链与载荷摘要自检 |
| GET  | `/health` | 健康检查 |

### 最小流程

```bash
# 1. 登记岗位
curl -XPUT localhost:8080/v1/admin/recipients -H 'Content-Type: application/json' -d '{
  "recipient_id":"R-HZ-SMS-1","area_code":"330100","post":"duty",
  "channel":"sms","address":"13900000001"}'

# 2. 发布事件（payload_digest = "sha256:" + sha256(规范化 payload)）
curl -XPOST localhost:8080/v1/events -H 'Content-Type: application/json' -d @contracts/event.example.json

# 3. 查询 23:00 杭州的预警与依据
curl 'localhost:8080/v1/warnings/at?area_code=330100&at=2026-09-19T23:00:00%2B08:00'

# 4. 确认与自检
curl -XPOST localhost:8080/v1/acks -H 'Content-Type: application/json' -d @contracts/ack.example.json
curl localhost:8080/v1/integrity
```

默认渠道适配器（`src/channels.py` 的 LoggingChannel）只在内存记录发送动作；
真实部署时在此处接入短信网关、广播控制器与政务终端推送，实现同一 `Channel` 协议即可，其余分层无需改动。
