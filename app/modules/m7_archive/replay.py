"""归档原件的证据重放：解析原件并复算原桶。

不重新实现时间解释（M2）也不改变请求去重（M3）：
- JSON 行解析与隔离口径直接复用 M1 ``parse``；
- 小时桶口径直接复用 M4 ``bucket``/``hour_key``（右端开、UTC 比较）。

冷热两侧读出的原件字节经各自摘要校验后进入同一重放函数，
因此同一批次归档前后复算出的桶必须逐字节一致。
"""
from __future__ import annotations

import json

from app.modules.m1_logs.core import parse as m1_parse
from app.modules.m4_buckets.core import bucket as m4_bucket
from app.modules.m4_buckets.core import hour_key as m4_hour_key


def replay_events(lines: list[dict]) -> dict:
    """把回读的原件行（含 source_line/text/digest）重放为事件与隔离行。"""
    # 用换行拼接恢复 M1 期望的物理行布局；source_line 已由归档侧逐行保留
    text = "\n".join(item["text"] for item in sorted(lines,
                                                     key=lambda x: x["source_line"]))
    parsed = m1_parse(text)
    accepted = parsed["rows"]
    quarantine = parsed["quarantine"]
    return {"events": accepted, "quarantine": quarantine}


def rebuild_buckets(lines: list[dict]) -> dict:
    """复算原桶：返回桶明细及守恒计数。"""
    replayed = replay_events(lines)
    events = replayed["events"]
    buckets = m4_bucket(events)
    bucketed = sum(item["count"] for item in buckets)
    return {
        "buckets": buckets,
        "input_count": len(events) + len(replayed["quarantine"]),
        "valid_count": len(events),
        "quarantine_count": len(replayed["quarantine"]),
        "bucketed_count": bucketed,
        "conserved": bucketed == len(events),
        "quarantine": replayed["quarantine"],
        "hour_key_sample": m4_hour_key(events[0]["timestamp"]) if events else None,
    }


def canonical_buckets(buckets: list[dict]) -> str:
    """桶结果的规范 JSON 文本，用于冷热两侧一致性比对。"""
    return json.dumps(buckets, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))
