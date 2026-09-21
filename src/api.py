"""HTTP 接口层：把 JSON 请求映射到核心服务。"""

from __future__ import annotations

import json
import re
import traceback
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

from .dispatch import Dispatcher
from .domain import ServiceError, parse_moment
from .service import AlertService

# 查询串里未编码的 "+" 会被当作空格，值班端直接粘贴时间时常见，做兼容。
_BROKEN_OFFSET_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2} \d{2}:\d{2}$")


def make_handler(
    service: AlertService, dispatcher: Dispatcher | None = None
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "AlertHub/1.0"

        def do_GET(self) -> None:
            self._handle("GET")

        def do_POST(self) -> None:
            self._handle("POST")

        def log_message(self, format: str, *args: object) -> None:
            return

        # ----------------------------------------------------------
        def _handle(self, method: str) -> None:
            try:
                status, payload = self._route(method)
            except ServiceError as exc:
                status, payload = exc.status, {
                    "error": {"code": exc.code, "message": exc.message}
                }
            except Exception:  # noqa: BLE001 - 兜底，避免把堆栈暴露给调用方
                traceback.print_exc()
                status, payload = 500, {
                    "error": {"code": "internal", "message": "服务内部错误"}
                }
            self._send_json(status, payload)

        def _route(self, method: str) -> tuple[int, dict]:
            parsed = urlparse(self.path)
            parts = [p for p in parsed.path.split("/") if p]
            query = parse_qs(parsed.query)

            if method == "GET" and parts == ["health"]:
                return 200, {"status": "ok"}
            if method == "POST" and parts == ["events"]:
                return 201, service.ingest_event(self._read_json())
            if method == "GET" and len(parts) == 2 and parts[0] == "events":
                return 200, service.get_event(parts[1])
            if method == "POST" and parts == ["recipients"]:
                return 201, service.register_recipient(self._read_json())
            if method == "GET" and len(parts) == 3 and parts[0] == "subjects" and parts[2] == "versions":
                return 200, service.subject_versions(parts[1])
            if method == "GET" and len(parts) == 3 and parts[0] == "regions" and parts[2] == "state":
                at = self._parse_at(query)
                return 200, service.region_state(parts[1], at)
            if method == "GET" and len(parts) == 3 and parts[0] == "regions" and parts[2] == "unconfirmed":
                return 200, service.unconfirmed(parts[1], channel=self._first(query, "channel"))
            if method == "GET" and parts == ["deliveries"]:
                return 200, service.list_deliveries(
                    region=self._first(query, "region"),
                    channel=self._first(query, "channel"),
                    status=self._first(query, "status"),
                )
            if method == "GET" and len(parts) == 2 and parts[0] == "deliveries":
                return 200, service.get_delivery(parts[1])
            if method == "POST" and len(parts) == 3 and parts[0] == "deliveries" and parts[2] == "confirm":
                return 200, service.confirm_delivery(parts[1], self._read_json())
            if method == "POST" and len(parts) == 3 and parts[0] == "deliveries" and parts[2] == "resend":
                outcome = service.resend_delivery(parts[1])
                if dispatcher is not None and outcome.get("resend") == "scheduled":
                    dispatcher.run_once()
                return 200, service.get_delivery(parts[1])
            raise ServiceError(404, "not_found", "接口不存在")

        # ----------------------------------------------------------
        @staticmethod
        def _first(query: dict, name: str) -> str | None:
            values = query.get(name)
            return values[0] if values else None

        def _parse_at(self, query: dict):
            raw = self._first(query, "at")
            if raw is None:
                return None
            if _BROKEN_OFFSET_RE.match(raw):
                raw = raw.replace(" ", "+")
            return parse_moment(raw, "at")

        def _read_json(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            try:
                body = json.loads(raw.decode("utf-8")) if raw else {}
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ServiceError(400, "invalid_json", "请求体不是合法 JSON") from exc
            if not isinstance(body, dict):
                raise ServiceError(400, "invalid_body", "请求体必须是 JSON 对象")
            return body

        def _send_json(self, status: int, payload: dict) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler
