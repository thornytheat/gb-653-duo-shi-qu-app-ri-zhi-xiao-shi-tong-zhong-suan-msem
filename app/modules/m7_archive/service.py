"""M7 服务层：把 HTTP 载荷编排为 ArchiveStore 动作，并附加原桶复算。"""
from __future__ import annotations

from . import replay
from .core import ArchiveError, ArchiveStore


def get_store(request) -> ArchiveStore:
    store = getattr(request.app.state, "archive_store", None)
    if store is None:  # pragma: no cover - 装配失败时的明确报错
        raise ArchiveError("ARCHIVE_NOT_CONFIGURED", "冷热归档未配置")
    return store


def ingest(store: ArchiveStore, payload: dict) -> dict:
    batch_id = str(payload["batch_id"])
    text = payload.get("text", "")
    source_name = str(payload.get("source_name", "api"))
    ingest_key = payload.get("ingest_key")
    return store.ingest(batch_id, text, source_name=source_name,
                        ingest_key=ingest_key)


def migrate(store: ArchiveStore, payload: dict) -> dict:
    return store.migrate(str(payload["batch_id"]))


def readback(store: ArchiveStore, payload: dict) -> dict:
    return store.readback(str(payload["batch_id"]))


def verify(store: ArchiveStore, payload: dict) -> dict:
    return store.verify_archive(str(payload["batch_id"]))


def rebuild(store: ArchiveStore, payload: dict) -> dict:
    """回读原件（冷件会逐帧校验）并复算原桶，附冷/热位置与摘要。"""
    view = store.readback(str(payload["batch_id"]))
    result = replay.rebuild_buckets(view["lines"])
    view.pop("lines", None)
    return {**view, **result,
            "buckets_canonical": replay.canonical_buckets(result["buckets"])}


def status(store: ArchiveStore) -> dict:
    return store.status()


def batches(store: ArchiveStore) -> dict:
    return {"batches": store.list_batches()}
