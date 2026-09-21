"""规范化 JSON 与 sha256 摘要。

发送方对载荷做规范化（键排序、无空白、不转义非 ASCII）后计算 sha256，
接收方用同一规则复算，字节级一致才接受。这样同一份内容无论传输时
如何排版，摘要都稳定；任何对历史内容的改动都会导致摘要不符。
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any


class CanonicalError(ValueError):
    """载荷无法被规范化（类型不受支持或含 NaN/Infinity）。"""


def canonical_json(obj: Any) -> str:
    return json.dumps(
        obj,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_bytes(obj: Any) -> bytes:
    try:
        return canonical_json(obj).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CanonicalError(f"载荷无法规范化：{exc}") from exc


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_payload(payload: Any) -> str:
    """返回 ``sha256:<hex>`` 形式的载荷摘要。"""
    return "sha256:" + sha256_hex(canonical_bytes(payload))


def verify_digest(payload: Any, claimed: str) -> bool:
    if not isinstance(claimed, str) or not claimed.startswith("sha256:"):
        return False
    actual = digest_payload(payload)
    # 常量时间比较，避免摘要校验被计时侧信道利用。
    return hmac.compare_digest(actual, claimed.strip().lower())


def link_hash(prev_chain_hash: str, material: bytes) -> str:
    """计算哈希链的下一环：sha256(上一环 || 规范化事件信封)。"""
    return "sha256:" + sha256_hex(prev_chain_hash.encode("ascii") + b"\n" + material)
