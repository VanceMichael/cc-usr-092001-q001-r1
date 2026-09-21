# 暴雨预警版本分发中枢

本项目用于管理气象预警、订正版本与渠道回执中的可核对的业务记录。服务接收带来源序号和签名摘要的发布、订正与解除消息，把重叠行政区拆成可追溯的生效片段，并按地区、渠道和接收岗位生成唯一送达记录；迟到消息不会覆盖较新的决定，跨午夜的时效按来报原始时区计算，渠道重试与人工补发保持幂等，服务重启后未完成的分发继续进行，历史版本不可被静默改写。

## 目录

- `contracts/` 保存外部交换字段示例（含可通过核验的真实摘要）。
- `docs/` 说明领域对象、时间和标识约定。
- `src/` 保存服务代码：`domain`（时间与摘要）、`store`（SQLite 与不可变约束）、`service`（接收与查询）、`dispatch`（渠道分发）、`api`（HTTP 接口）、`app`（入口）。
- `scripts/` 保存数据库初始化入口。
- `tests/` 保存行为检查。

## 运行

执行 `make test` 检查基础行为，执行 `make migrate` 初始化本地数据目录，执行 `make run` 启动服务。默认监听 `8080` 端口，健康检查地址为 `/health`。

配置通过环境变量传入，敏感值和本地数据库文件不得提交到仓库：

- `PORT`：监听端口，默认 `8080`。
- `DATABASE_PATH`：SQLite 文件路径，默认 `data/app.sqlite3`。
- `DISPATCH_INTERVAL_SECONDS`：后台派发轮询间隔，默认 `5`。

## 接口

所有请求与响应均为 JSON；时间参数必须带时区偏移（查询串中的 `+` 需编码为 `%2B`）。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/events` | 接收发布/订正/解除消息（幂等，字段见 `contracts/event.example.json`） |
| GET | `/events/{event_id}` | 查询单条事件的登记与判定结果 |
| POST | `/recipients` | 登记或更新接收岗位（`region_code`、`channel`、`post_ref`、`endpoint_ref`） |
| GET | `/subjects/{subject_ref}/versions` | 查询主题版本链 |
| GET | `/regions/{region_code}/state?at=<时间>` | 查询任一时刻该地区的预警状态及形成依据 |
| GET | `/regions/{region_code}/unconfirmed?channel=<渠道>` | 查询尚未确认的接收方，按渠道汇总 |
| GET | `/deliveries?region=&channel=&status=` | 查询送达记录 |
| GET | `/deliveries/{delivery_id}` | 查询送达详情（含尝试记录与确认回执） |
| POST | `/deliveries/{delivery_id}/confirm` | 登记确认回执（按 `receipt_id` 幂等） |
| POST | `/deliveries/{delivery_id}/resend` | 人工补发（已确认的为幂等空操作） |

错误响应统一为 `{"error": {"code": ..., "message": ...}}`，常见错误码：`digest_mismatch`（摘要不一致）、`event_conflict`（同号事件内容冲突）、`subject_exists` / `subject_not_found` / `subject_closed`（主题状态冲突）、`invalid_time`（时间缺失时区偏移）。
