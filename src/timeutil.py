"""时间处理：保留原始时区偏移的时刻与 ISO 8601 时长。

外部消息中的时间一律带偏移量（如 ``2026-09-19T23:40:00+08:00``）。
系统内部按统一的 Unix 秒比较先后，跨午夜的有效期因此天然正确；
原始偏移量随记录保留，用于回放和展示，绝不替换为服务器本地时区。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


class TimeError(ValueError):
    """时间字符串无法解析或缺少时区信息。"""


@dataclass(frozen=True, order=True)
class Instant:
    """一个绝对时刻，附带来源声明的原始 UTC 偏移（分钟）。"""

    unix_seconds: float
    tz_offset_minutes: int

    @property
    def offset_hhmm(self) -> str:
        sign = "+" if self.tz_offset_minutes >= 0 else "-"
        minutes = abs(self.tz_offset_minutes)
        return f"{sign}{minutes // 60:02d}:{minutes % 60:02d}"

    def iso_with_original_offset(self) -> str:
        """按原始偏移量渲染 ISO 8601 字符串。"""
        offset = timezone(timedelta(minutes=self.tz_offset_minutes))
        text = datetime.fromtimestamp(self.unix_seconds, tz=offset).isoformat(timespec="seconds")
        return text

    def iso_utc(self) -> str:
        text = datetime.fromtimestamp(self.unix_seconds, tz=timezone.utc).isoformat(
            timespec="seconds"
        )
        return text.replace("+00:00", "Z")


def parse_instant(value: str) -> Instant:
    """解析带偏移量的 ISO 8601 字符串，拒绝朴素时间。"""
    if not isinstance(value, str):
        raise TimeError("时间必须是字符串")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise TimeError(f"无法解析时间：{value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TimeError(f"时间必须携带时区偏移量：{value!r}")
    offset_minutes = int(parsed.utcoffset().total_seconds() // 60)
    return Instant(parsed.timestamp(), offset_minutes)


_DURATION_RE = re.compile(
    r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$"
)


def parse_duration_seconds(value: str | int | float) -> float:
    """解析 ISO 8601 时长（PnDTnHnMnS）或裸秒数。"""
    if isinstance(value, (int, float)):
        seconds = float(value)
    elif isinstance(value, str):
        match = _DURATION_RE.fullmatch(value.strip())
        if not match:
            raise TimeError(f"无法解析时长：{value!r}")
        days, hours, minutes, secs = match.groups()
        seconds = (
            int(days or 0) * 86400
            + int(hours or 0) * 3600
            + int(minutes or 0) * 60
            + float(secs or 0)
        )
    else:
        raise TimeError(f"无法解析时长：{value!r}")
    if seconds <= 0:
        raise TimeError("时长必须为正数")
    return seconds
