"""提供项目的基础健康检查服务。"""

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def health_payload() -> dict[str, str]:
    """返回可供运行环境探测的服务状态。"""
    return {"status": "ok"}


class Handler(BaseHTTPRequestHandler):
    """处理基础 HTTP 请求。"""

    def do_GET(self) -> None:
        if self.path != "/health":
            self.send_error(404)
            return
        body = json.dumps(health_payload()).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    port = int(os.environ.get("PORT", "8080"))
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
