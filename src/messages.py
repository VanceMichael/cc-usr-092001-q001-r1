"""渠道消息渲染。

送达内容在创建送达记录时一次性定型并入库，之后不可变；
跨午夜的时间窗按事件携带的原始时区偏移渲染。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


def _format_window(ts: float, offset_minutes: int) -> str:
    tz = timezone(timedelta(minutes=offset_minutes))
    return datetime.fromtimestamp(ts, tz).strftime("%Y-%m-%d %H:%M")


def _level_line(detail: dict) -> list[str]:
    lines = [f"预警等级：{detail['level']}"]
    rmin, rmax = detail.get("rainfall_min_mm"), detail.get("rainfall_max_mm")
    if rmin is not None or rmax is not None:
        lines.append(f"累计雨量（毫米）：{rmin if rmin is not None else '?'}–{rmax if rmax is not None else '?'}")
    if detail.get("hourly_intensity_mm") is not None:
        lines.append(f"最大小时雨强（毫米/小时）：{detail['hourly_intensity_mm']}")
    return lines


def render_warning(fragment: dict, area_name: str) -> tuple[str, str]:
    """生成预警/订正后的标题与正文。fragment 为投影行字典。"""
    title = f"【暴雨预警】{area_name} {fragment['level']}"
    lines = [
        f"{area_name}暴雨预警生效（版本 v{fragment['chain_version']}）",
        f"预警编号：{fragment['subject_ref']}",
        *_level_line(fragment),
        f"生效：{_format_window(fragment['valid_from_unix'], fragment['valid_from_offset_minutes'])}",
    ]
    if fragment.get("valid_to_unix") is not None:
        lines.append(
            f"失效：{_format_window(fragment['valid_to_unix'], fragment['valid_to_offset_minutes'])}"
        )
    body = "\n".join(lines)
    return title, body


def render_notice(kind: str, fragment: dict, area_name: str, reason: str | None) -> tuple[str, str]:
    """生成解除/撤销范围通知，防止继续转发已失效区域。"""
    if kind == "lift":
        title = f"【预警解除】{area_name} 暴雨预警已解除"
        head = f"{area_name}暴雨预警已解除（原版本 v{fragment['chain_version']}）"
        tail = f"解除依据：{reason}" if reason else "请停止转发此前预警范围。"
    else:
        title = f"【范围撤销】{area_name} 已移出本次预警范围"
        head = f"{area_name}已不在最新订正（v{fragment['chain_version']}之后）的预警范围内"
        tail = f"原因：{reason}" if reason else "请勿继续转发该地区的旧版预警。"
    body = "\n".join([head, f"预警编号：{fragment['subject_ref']}", tail])
    return title, body
