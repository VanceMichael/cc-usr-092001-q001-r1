"""暴雨预警版本分发中枢 HTTP 服务。

路由：
  GET  /health
  POST /v1/events                     接入预警/订正/解除事件
  GET  /v1/events                     事件台账（含 stale 留痕）
  PUT  /v1/admin/recipients           登记/更新接收岗位
  GET  /v1/warnings/at?area_code&at   任一时刻某地区的生效预警与依据链
  GET  /v1/timeline?subject_ref&area  片段版本时间线
  GET  /v1/deliveries?area_code&channel
  GET  /v1/deliveries/{id}            送达详情（含未确认接收方）
  POST /v1/deliveries/{id}/reissue    人工补发（request_id 幂等）
  POST /v1/acks                       接收确认回执（ack_id 幂等）
  GET  /v1/integrity                  哈希链与摘要自检
"""

from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from . import events as events_mod
from . import queries
from .channels import Channel, default_channels
from .dispatch import DispatchError, Dispatcher
from .storage import Storage
from .timeutil import TimeError


def health_payload() -> dict[str, str]:
    """返回可供运行环境探测的服务状态。"""
    return {"status": "ok"}


class Service:
    """组装存储、分发器与后台重试线程。"""

    def __init__(self, store: Storage, channels: dict[str, Channel] | None = None,
                 dispatch_interval: float = 5.0, auto_dispatch: bool = True) -> None:
        self.store = store
        self.dispatcher = Dispatcher(store, channels=channels)
        self.dispatch_interval = dispatch_interval
        self._stop = threading.Event()
        self._worker: threading.Thread | None = None
        if auto_dispatch:
            self.start_worker()

    def start_worker(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        self._stop.clear()
        self._worker = threading.Thread(
            target=self._dispatch_loop, name="dispatch-worker", daemon=True
        )
        self._worker.start()

    def _dispatch_loop(self) -> None:
        while not self._stop.wait(self.dispatch_interval):
            try:
                self.dispatcher.run_due()
            except Exception:
                # 单次扫描失败不能杀死后台线程；下轮继续，状态全部在库中。
                continue

    def shutdown(self) -> None:
        self._stop.set()


def _make_handler(service: Service) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "WarningHub/1.0"

        # -- 工具 --------------------------------------------------------

        def _json(self, status: int, payload: dict | list) -> None:
            body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _body(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                raise events_mod.IngestionError("缺少 JSON 请求体")
            raw = self.rfile.read(length)
            try:
                data = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise events_mod.IngestionError(f"请求体不是合法 JSON：{exc}") from exc
            if not isinstance(data, dict):
                raise events_mod.IngestionError("请求体必须是 JSON 对象")
            return data

        def _query(self) -> dict[str, str]:
            parsed = parse_qs(urlsplit(self.path).query)
            return {k: v[-1] for k, v in parsed.items()}

        def log_message(self, format: str, *args: object) -> None:
            return

        # -- GET ---------------------------------------------------------

        def do_GET(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path.rstrip("/") or "/"
            try:
                if path == "/health":
                    self._json(200, health_payload())
                elif path == "/v1/events":
                    q = self._query()
                    self._json(200, queries.event_log(
                        service.store,
                        source_id=q.get("source_id"),
                        include_stale=q.get("include_stale", "true") != "false",
                    ))
                elif path == "/v1/warnings/at":
                    q = self._query()
                    if not q.get("area_code"):
                        self._json(400, {"error": "缺少查询参数 area_code"})
                        return
                    self._json(200, queries.warning_at(
                        service.store, q["area_code"], q.get("at")))
                elif path == "/v1/timeline":
                    q = self._query()
                    self._json(200, queries.timeline(
                        service.store,
                        subject_ref=q.get("subject_ref"), area_code=q.get("area_code"),
                    ))
                elif path == "/v1/deliveries":
                    q = self._query()
                    rows = service.dispatcher.list_pending(
                        area_code=q.get("area_code"), channel=q.get("channel"))
                    self._json(200, {"count": len(rows), "deliveries": rows})
                elif path.startswith("/v1/deliveries/"):
                    delivery_id = path.rsplit("/", 1)[-1]
                    self._json(200, service.dispatcher.delivery_detail(delivery_id))
                elif path == "/v1/integrity":
                    self._json(200, queries.verify_integrity(service.store))
                else:
                    self._json(404, {"error": f"未知路径：{path}"})
            except Exception as exc:
                self._error(exc)

        # -- POST / PUT --------------------------------------------------

        def do_POST(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path.rstrip("/") or "/"
            try:
                if path == "/v1/events":
                    result = events_mod.ingest(service.store, self._body())
                    status = 200 if result.apply_state.startswith("duplicate") else 201
                    self._json(status, {"accepted": True, **result.as_dict()})
                elif path == "/v1/acks":
                    self._json(200, service.dispatcher.record_ack(self._body()))
                elif path.startswith("/v1/deliveries/") and path.endswith("/reissue"):
                    delivery_id = path.split("/")[3]
                    body = self._body()
                    body["delivery_id"] = delivery_id
                    self._json(200, service.dispatcher.reissue(body))
                elif path == "/v1/dispatch/run":  # 管理便利接口：立即扫描一次
                    reports = service.dispatcher.run_due()
                    self._json(200, {"ran": len(reports),
                                     "results": [r.__dict__ for r in reports]})
                else:
                    self._json(404, {"error": f"未知路径：{path}"})
            except Exception as exc:
                self._error(exc)

        def do_PUT(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path.rstrip("/") or "/"
            try:
                if path == "/v1/admin/recipients":
                    self._json(200, service.dispatcher.register_recipient(self._body()))
                else:
                    self._json(404, {"error": f"未知路径：{path}"})
            except Exception as exc:
                self._error(exc)

        def _error(self, exc: Exception) -> None:
            if isinstance(exc, events_mod.ConflictError):
                self._json(409, {"error": str(exc)})
            elif isinstance(exc, (events_mod.IngestionError, DispatchError, TimeError)):
                self._json(400, {"error": str(exc)})
            else:
                self._json(500, {"error": f"内部错误：{exc}"})

    return Handler


def build_server(host: str, port: int, store: Storage,
                 channels: dict[str, Channel] | None = None,
                 dispatch_interval: float | None = None,
                 auto_dispatch: bool = True) -> ThreadingHTTPServer:
    interval = dispatch_interval if dispatch_interval is not None else float(
        os.environ.get("DISPATCH_INTERVAL", "5"))
    service = Service(store, channels=channels, dispatch_interval=interval,
                      auto_dispatch=auto_dispatch)
    httpd = ThreadingHTTPServer((host, port), _make_handler(service))
    httpd.service = service  # type: ignore[attr-defined]
    return httpd


def main() -> None:
    port = int(os.environ.get("PORT", "8080"))
    db_path = os.environ.get("DATABASE_PATH", "data/app.sqlite3")
    store = Storage(db_path)
    httpd = build_server("0.0.0.0", port, store, channels=default_channels())
    print(f"暴雨预警版本分发中枢监听 0.0.0.0:{port}，数据库 {db_path}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.service.shutdown()  # type: ignore[attr-defined]
        httpd.server_close()
        store.close()


if __name__ == "__main__":
    main()
