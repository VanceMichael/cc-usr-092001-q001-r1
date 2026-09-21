"""服务入口：装配存储、核心服务、分发器与 HTTP 接口。"""

from __future__ import annotations

import os
import threading
from http.server import ThreadingHTTPServer

from .api import make_handler
from .dispatch import Dispatcher, LoggingAdapter
from .service import CHANNELS, AlertService
from .store import Store


def health_payload() -> dict[str, str]:
    """返回可供运行环境探测的服务状态。"""
    return {"status": "ok"}


def build(database_path: str) -> tuple[Store, AlertService, Dispatcher]:
    """装配服务组件；启动时回收中断的派发，保证重启后继续进行。"""
    store = Store(database_path)
    service = AlertService(store)
    dispatcher = Dispatcher(store, {channel: LoggingAdapter() for channel in CHANNELS})
    dispatcher.recover_inflight()
    return store, service, dispatcher


def _dispatch_loop(dispatcher: Dispatcher, stop: threading.Event, interval: float) -> None:
    while not stop.is_set():
        dispatcher.run_once()
        stop.wait(interval)


def main() -> None:
    port = int(os.environ.get("PORT", "8080"))
    database_path = os.environ.get("DATABASE_PATH", "data/app.sqlite3")
    interval = float(os.environ.get("DISPATCH_INTERVAL_SECONDS", "5"))

    store, service, dispatcher = build(database_path)
    stop = threading.Event()
    worker = threading.Thread(
        target=_dispatch_loop, args=(dispatcher, stop, interval), daemon=True
    )
    worker.start()
    server = ThreadingHTTPServer(("0.0.0.0", port), make_handler(service, dispatcher))
    try:
        server.serve_forever()
    finally:
        stop.set()
        worker.join(timeout=2)
        store.close()


if __name__ == "__main__":
    main()
