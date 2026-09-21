"""渠道发送适配器。

真实部署在这里接入短信网关、广播控制器和政务终端推送；
默认实现只做本地记录（LoggingChannel），测试用 ScriptedChannel 注入成败。
适配器只负责“把定型内容送到地址列表”，不做任何业务判断。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Callable, Protocol


class ChannelError(RuntimeError):
    """渠道暂时不可用，应按退避计划重试。"""


@dataclass
class SendResult:
    provider_ref: str
    accepted: int
    detail: dict = field(default_factory=dict)


class Channel(Protocol):
    name: str

    def send(self, *, delivery: dict, addresses: list[dict]) -> SendResult:
        """addresses 为 [{recipient_id, channel, address}]。失败抛 ChannelError。"""
        ...


class LoggingChannel:
    """默认适配器：把发送动作记入内存列表，始终成功。"""

    def __init__(self, name: str) -> None:
        self.name = name
        self.sent: list[dict] = []
        self._lock = threading.Lock()
        self._seq = 0

    def send(self, *, delivery: dict, addresses: list[dict]) -> SendResult:
        with self._lock:
            self._seq += 1
            ref = f"{self.name}-local-{self._seq}"
            self.sent.append(
                {
                    "provider_ref": ref,
                    "delivery_id": delivery["delivery_id"],
                    "title": delivery["title"],
                    "addresses": [a["address"] for a in addresses],
                }
            )
        return SendResult(provider_ref=ref, accepted=len(addresses))


class ScriptedChannel:
    """测试适配器：按脚本返回成败，记录调用次数。"""

    def __init__(self, name: str, failures: int = 0) -> None:
        self.name = name
        self.remaining_failures = failures
        self.calls: list[dict] = []
        self._lock = threading.Lock()

    def send(self, *, delivery: dict, addresses: list[dict]) -> SendResult:
        with self._lock:
            self.calls.append(
                {"delivery_id": delivery["delivery_id"], "n": len(addresses)}
            )
            if self.remaining_failures > 0:
                self.remaining_failures -= 1
                raise ChannelError(f"{self.name} 模拟失败（剩余将失败 {self.remaining_failures} 次）")
        return SendResult(
            provider_ref=f"{self.name}-script-{len(self.calls)}",
            accepted=len(addresses),
        )


ChannelFactory = Callable[[str], Channel]


def default_channels() -> dict[str, Channel]:
    return {
        "sms": LoggingChannel("sms"),
        "broadcast": LoggingChannel("broadcast"),
        "gov_terminal": LoggingChannel("gov_terminal"),
    }
