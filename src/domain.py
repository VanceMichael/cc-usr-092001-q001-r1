"""领域基础类型：时间解析、摘要校验与统一错误约定。"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime


class ServiceError(Exception):
    """携带 HTTP 状态码与业务错误码的领域错误。"""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


_OFFSET_RE = re.compile(r"(Z|[+-]\d{2}:\d{2})$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


@dataclass(frozen=True)
class Moment:
    """一个携带原始时区偏移的时间点。

    业务时间一律以来报中的偏移量为准：epoch 用于绝对先后比较，
    raw 与 offset_minutes 原样保留，不得用服务本地时区或到达时间改写。
    """

    raw: str
    epoch: int
    offset_minutes: int


def parse_moment(value: object, field: str) -> Moment:
    """解析带偏移量的 ISO 8601 时间；缺失时区偏移的一律拒绝。"""
    if not isinstance(value, str) or not _OFFSET_RE.search(value):
        raise ServiceError(
            422, "invalid_time", f"{field} 必须是带时区偏移的 ISO 8601 字符串"
        )
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ServiceError(422, "invalid_time", f"{field} 不是合法的 ISO 8601 时间") from exc
    if parsed.tzinfo is None:
        raise ServiceError(422, "invalid_time", f"{field} 缺少时区偏移")
    offset = parsed.utcoffset()
    if offset is None:
        raise ServiceError(422, "invalid_time", f"{field} 缺少时区偏移")
    return Moment(
        raw=value,
        epoch=int(parsed.timestamp()),
        offset_minutes=int(offset.total_seconds() // 60),
    )


def canonical_json(value: object) -> str:
    """生成用于摘要计算的规范 JSON 表示（键排序、无空白、保留中文）。"""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_digest(payload: object) -> str:
    """按规范 JSON 计算 payload 的 sha256 摘要。"""
    return "sha256:" + hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def require_digest(payload: object, digest: object) -> str:
    """校验 payload_digest 的格式与内容，不一致时拒绝接收。"""
    if not isinstance(digest, str) or not _DIGEST_RE.match(digest):
        raise ServiceError(422, "invalid_digest", "payload_digest 必须是 sha256:<64位十六进制>")
    if digest != compute_digest(payload):
        raise ServiceError(422, "digest_mismatch", "payload_digest 与 payload 内容不一致")
    return digest
