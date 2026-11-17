"""M7 冷热归档：接入实际日志、字节限额、迁移校验、回读复算、
并发计数、中断恢复、失败保热、损坏报错、幂等与整批拒绝。"""
import threading

import pytest

from app.errors import DomainError
from app.modules.m7_archive.core import (
    ArchiveStore, ColdStorage, STATE_HOT, STATE_MIGRATING, STATE_ARCHIVED,
    STATE_FAILED, TASK_DONE,
)

LOG_TEXT = (
    '{"request_id":"a1","timestamp":"2026-11-15T01:05:00Z","service":"pay","status":200}\n'
    '{"request_id":"a2","timestamp":"2026-11-15T01:59:00Z","service":"pay","status":500}\n'
    '\n'
    '{"request_id":"a3","timestamp":"2026-11-15T02:05:00Z","service":"pay","status":200}'
)


def make_store(tmp_path, hot_quota=512, cold_quota=4096):
    return ArchiveStore(str(tmp_path / "archive.db"),
                        ColdStorage(tmp_path / "cold"),
                        hot_quota_bytes=hot_quota,
                        cold_quota_bytes=cold_quota)


# ----------------------------- 接入与容量口径 ----------------------------- #
def test_ingest_keeps_physical_line_numbers_and_bytes(tmp_path):
    store = make_store(tmp_path)
    view = store.ingest("b1", LOG_TEXT, source_name="test")
    assert view["state"] == STATE_HOT
    assert view["line_count"] == 3            # 空行跳过，物理行号不重排
    rb = store.readback("b1")
    assert [line["source_line"] for line in rb["lines"]] == [1, 2, 4]
    assert rb["location"] == "HOT"
    # 在线占用 = 各原件行 UTF-8 字节之和
    expect_bytes = sum(len(line.encode("utf-8"))
                       for line in LOG_TEXT.splitlines() if line.strip())
    assert rb["hot_bytes"] == expect_bytes
    assert store.status()["online_bytes"] == expect_bytes


def test_hot_quota_rejects_whole_batch_without_half_batch(tmp_path):
    store = make_store(tmp_path, hot_quota=40)  # 远小于整批
    with pytest.raises(DomainError) as exc:
        store.ingest("big", LOG_TEXT)
    assert exc.value.code == "HOT_QUOTA_EXCEEDED"
    assert store.list_batches() == []          # 不留半批
    assert store.status()["hot_used_bytes"] == 0
    # 小批次仍可正常接入
    store.ingest("small", '{"request_id":"x"}')
    assert len(store.list_batches()) == 1


def test_ingest_key_idempotent_no_double_bucketing(tmp_path):
    store = make_store(tmp_path)
    first = store.ingest("b1", LOG_TEXT, ingest_key="key-1")
    second = store.ingest("ignored-id", LOG_TEXT, ingest_key="key-1")
    assert second["idempotent"] is True
    assert second["batch_id"] == first["batch_id"]
    assert len(store.list_batches()) == 1
    assert store.status()["hot_used_bytes"] == first["hot_bytes"]


# ----------------------------- 迁移、校验与状态机 ----------------------------- #
def test_migrate_archived_frees_hot_and_cold_readback_matches(tmp_path):
    store = make_store(tmp_path)
    before = store.ingest("b1", LOG_TEXT)
    result = store.migrate("b1")
    assert result["state"] == STATE_ARCHIVED
    assert result["verified_lines"] == before["line_count"]
    assert result["cold_bytes"] > 0
    # 热件已释放，在线占用归零，冷占用增长
    status = store.status()
    assert status["hot_used_bytes"] == 0
    assert status["cold_used_bytes"] >= result["cold_bytes"]
    assert status["states"][STATE_ARCHIVED]["batches"] == 1
    assert status["states"][STATE_HOT]["batches"] == 0
    # 冷件回读：逐帧校验后原文逐行一致（含行号、摘要）
    rb = store.readback("b1")
    assert rb["location"] == "COLD"
    assert [line["source_line"] for line in rb["lines"]] == [1, 2, 4]
    assert "\n".join(line["text"] for line in rb["lines"]) == "\n".join(
        line for line in LOG_TEXT.splitlines() if line.strip())
    verify = store.verify_archive("b1")
    assert verify["ok"] is True and verify["verified_lines"] == 3


def test_migrate_idempotent_retry_does_not_duplicate(tmp_path):
    store = make_store(tmp_path)
    store.ingest("b1", LOG_TEXT)
    first = store.migrate("b1")
    second = store.migrate("b1")                 # 重试
    assert second["idempotent"] is True
    assert second["archive_id"] == first["archive_id"]
    arc_files = list((tmp_path / "cold").glob("*.loghour.arc"))
    assert len(arc_files) == 1                  # 不重复归档
    rb = store.readback("b1")
    assert len(rb["lines"]) == 3                # 不重复入桶


def test_cold_quota_rejects_and_keeps_hot(tmp_path):
    store = make_store(tmp_path, cold_quota=8)  # 连归档头都不够
    store.ingest("b1", LOG_TEXT)
    with pytest.raises(DomainError) as exc:
        store.migrate("b1")
    assert exc.value.code == "COLD_QUOTA_EXCEEDED"
    view = store.readback("b1")
    assert view["state"] == STATE_HOT and view["location"] == "HOT"
    assert len(view["lines"]) == 3              # 热件完整
    assert list((tmp_path / "cold").glob("*")) == []


def test_interrupted_migration_resumes_from_verified_progress(tmp_path):
    store = make_store(tmp_path)
    store.ingest("b1", LOG_TEXT)
    # 手工驱动到“迁移中”，并只完成前两行，模拟进程中断
    db = store.connect()
    batch = store._get_batch(db, "b1")
    store._begin_migration(db, batch)
    db.commit()
    db.close()
    db = store.connect()
    batch = store._get_batch(db, "b1")
    assert batch["state"] == STATE_MIGRATING and batch["archive_id"]
    row = db.execute(
        "SELECT source_line,raw,digest,byte_count FROM archive_lines "
        "WHERE batch_id='b1' ORDER BY source_line LIMIT 1").fetchone()
    store.cold.append_frame(batch["archive_id"], row["source_line"],
                            bytes(row["raw"]), row["digest"])
    db.execute(
        "UPDATE archive_lines SET task_state=? WHERE batch_id='b1' "
        "AND source_line=?", (TASK_DONE, row["source_line"]))
    db.execute("UPDATE archive_batches SET verified_lines=1 WHERE batch_id='b1'")
    db.commit()
    db.close()
    # 此时状态停在 MIGRATING，热件完整
    assert store.list_batches()[0]["state"] == STATE_MIGRATING
    assert len(store.readback("b1")["lines"]) == 3
    # 恢复：从已校验进度继续，不重写第一帧
    resumed = store.migrate("b1")
    assert resumed["state"] == STATE_ARCHIVED
    assert resumed["verified_lines"] == 3
    arc = store.cold.read_archive(resumed["archive_id"])
    assert [f["source_line"] for f in arc["frames"]] == [1, 2, 4]
    assert len(arc["frames"]) == 3              # 第一帧未重复追加


def test_crash_between_cold_write_and_db_commit_does_not_duplicate_frame(tmp_path):
    """崩溃窗口：第一帧已落冷件，但库里该行仍是 PENDING。"""
    store = make_store(tmp_path)
    store.ingest("b1", LOG_TEXT)
    db = store.connect()
    batch = store._get_batch(db, "b1")
    store._begin_migration(db, batch)
    db.commit()
    db.close()

    db = store.connect()
    batch = store._get_batch(db, "b1")
    row = db.execute(
        "SELECT source_line,raw,digest FROM archive_lines "
        "WHERE batch_id='b1' ORDER BY source_line LIMIT 1").fetchone()
    # 冷帧写入成功，但刻意不更新 task_state（模拟提交前崩溃）
    store.cold.append_frame(batch["archive_id"], row["source_line"],
                            bytes(row["raw"]), row["digest"])
    db.close()

    # 库里三行仍全是 PENDING，冷件已有 1 帧
    view = store.list_batches()[0]
    assert view["state"] == STATE_MIGRATING and view["verified_lines"] == 0
    recovered = store.migrate("b1")
    assert recovered["state"] == STATE_ARCHIVED
    # 进度不重复计数：恰好 3 行；冷字节按 DONE 行原件字节汇总
    assert recovered["verified_lines"] == 3
    expect_cold_payload = sum(
        len(line.encode("utf-8"))
        for line in LOG_TEXT.splitlines() if line.strip())
    assert recovered["cold_bytes"] >= expect_cold_payload
    arc = store.cold.read_archive(recovered["archive_id"])
    assert len(arc["frames"]) == 3                      # 没有重复帧
    assert [f["source_line"] for f in arc["frames"]] == [1, 2, 4]
    assert store.readback("b1")["location"] == "COLD"


def test_reconcile_after_partial_done_does_not_double_count(tmp_path):
    """库中已有 1 行 DONE + 1 个崩溃帧（冷件有、库 PENDING），对账后计数仍准确。"""
    store = make_store(tmp_path)
    store.ingest("b1", LOG_TEXT)
    db = store.connect()
    batch = store._get_batch(db, "b1")
    store._begin_migration(db, batch)
    db.commit(); db.close()

    db = store.connect()
    batch = store._get_batch(db, "b1")
    rows = db.execute(
        "SELECT source_line,raw,digest,byte_count FROM archive_lines "
        "WHERE batch_id='b1' ORDER BY source_line").fetchall()
    # 第一行：冷帧 + 库 DONE（正常进度）
    store.cold.append_frame(batch["archive_id"], rows[0]["source_line"],
                            bytes(rows[0]["raw"]), rows[0]["digest"])
    db.execute("UPDATE archive_lines SET task_state=? WHERE batch_id='b1' "
               "AND source_line=?", (TASK_DONE, rows[0]["source_line"]))
    # 第二行：冷帧已写，但库仍 PENDING（崩溃窗口）
    store.cold.append_frame(batch["archive_id"], rows[1]["source_line"],
                            bytes(rows[1]["raw"]), rows[1]["digest"])
    db.execute("UPDATE archive_batches SET verified_lines=1 WHERE batch_id='b1'")
    db.commit(); db.close()

    recovered = store.migrate("b1")
    assert recovered["state"] == STATE_ARCHIVED
    assert recovered["verified_lines"] == 3
    arc = store.cold.read_archive(recovered["archive_id"])
    assert len(arc["frames"]) == 3
    done_bytes = sum(len(line.encode("utf-8"))
                     for line in LOG_TEXT.splitlines() if line.strip())
    assert recovered["cold_bytes"] >= done_bytes


def test_failed_switch_keeps_hot_and_retry_recovers(tmp_path, monkeypatch):
    store = make_store(tmp_path)
    store.ingest("b1", LOG_TEXT)
    # 让最终切换时的整份读回校验失败 => FAILED 且热件保留
    original = store.cold.read_archive

    def corrupt_on_finalize(archive_id):
        result = original(archive_id)
        if len(result["frames"]) == 3:
            result["frames"][0]["digest"] = "0" * 64
        return result

    monkeypatch.setattr(store.cold, "read_archive", corrupt_on_finalize)
    with pytest.raises(DomainError) as exc:
        store.migrate("b1")
    assert exc.value.code == "COLD_ARCHIVE_CORRUPT"
    view = store.readback("b1")
    assert view["state"] == STATE_FAILED
    assert view["location"] == "HOT" and len(view["lines"]) == 3  # 热件保留
    # 撤销故障后重试：从已校验进度恢复并成功归档
    monkeypatch.undo()
    recovered = store.migrate("b1")
    assert recovered["state"] == STATE_ARCHIVED
    assert store.readback("b1")["location"] == "COLD"


def test_failed_retry_rebuilds_missing_cold_from_hot(tmp_path, monkeypatch):
    store = make_store(tmp_path)
    store.ingest("b1", LOG_TEXT)
    original = store.cold.read_archive

    def corrupt_on_finalize(archive_id):
        result = original(archive_id)
        if len(result["frames"]) == 3:
            result["frames"][0]["digest"] = "0" * 64
        return result

    monkeypatch.setattr(store.cold, "read_archive", corrupt_on_finalize)
    with pytest.raises(DomainError):
        store.migrate("b1")
    assert store.list_batches()[0]["state"] == STATE_FAILED
    # 冷件被外部清理掉，热件仍完整
    for p in (tmp_path / "cold").glob("*.loghour.arc"):
        p.unlink()
    monkeypatch.undo()
    recovered = store.migrate("b1")
    assert recovered["state"] == STATE_ARCHIVED
    assert recovered["verified_lines"] == 3
    rb = store.readback("b1")
    assert rb["location"] == "COLD" and len(rb["lines"]) == 3


def test_corrupt_cold_archive_raises_explicit_error(tmp_path):
    store = make_store(tmp_path)
    store.ingest("b1", LOG_TEXT)
    migrated = store.migrate("b1")
    path = store.cold.path_for(migrated["archive_id"])
    blob = bytearray(path.read_bytes())
    blob[-20] ^= 0xFF                          # 翻转冷件中的字节
    path.write_bytes(bytes(blob))
    with pytest.raises(DomainError) as exc:
        store.readback("b1")
    assert exc.value.code == "COLD_ARCHIVE_CORRUPT"
    with pytest.raises(DomainError) as exc2:
        store.verify_archive("b1")
    assert exc2.value.code == "COLD_ARCHIVE_CORRUPT"


def test_archive_id_path_traversal_rejected(tmp_path):
    cold = ColdStorage(tmp_path / "cold")
    with pytest.raises(DomainError) as exc:
        cold.path_for("../escape")
    assert exc.value.code == "ARCHIVE_ID_INVALID"


def test_length_prefixed_frames_survive_embedded_newlines(tmp_path):
    # 原件行内含转义换行（JSON 字符串里的 \n），物理切分仍按外层换行
    text = '{"request_id":"a","note":"line1\\nline2"}\n{"request_id":"b"}'
    store = make_store(tmp_path)
    store.ingest("b1", text)
    store.migrate("b1")
    rb = store.readback("b1")
    assert rb["location"] == "COLD"
    assert rb["lines"][0]["text"] == '{"request_id":"a","note":"line1\\nline2"}'
    assert rb["lines"][1]["text"] == '{"request_id":"b"}'
    # 冷件为二进制长度前缀结构，魔数不是旧的纯文本行格式
    arc_path = store.cold.path_for(rb["archive_id"])
    assert arc_path.read_bytes().startswith(b"LOGHOUR-ARCHIVE/1\n")


def test_corrupt_length_prefix_detected(tmp_path):
    store = make_store(tmp_path)
    store.ingest("b1", LOG_TEXT)
    migrated = store.migrate("b1")
    path = store.cold.path_for(migrated["archive_id"])
    blob = bytearray(path.read_bytes())
    # 魔数(18) + 元数据长度前缀(8) 之后是首个数据帧的长度前缀，翻转其高位字节
    blob[26] ^= 0x80
    path.write_bytes(bytes(blob))
    with pytest.raises(DomainError) as exc:
        store.readback("b1")
    assert exc.value.code == "COLD_ARCHIVE_CORRUPT"


# ----------------------------- 原桶复算：不改时间/去重 ----------------------------- #
def test_rebuild_buckets_identical_before_and_after_archiving(tmp_path):
    from app.modules.m7_archive import replay
    store = make_store(tmp_path)
    store.ingest("b1", LOG_TEXT)
    hot_view = store.readback("b1")
    hot_buckets = replay.rebuild_buckets(hot_view["lines"])
    store.migrate("b1")
    cold_view = store.readback("b1")
    cold_buckets = replay.rebuild_buckets(cold_view["lines"])
    assert replay.canonical_buckets(hot_buckets["buckets"]) == \
        replay.canonical_buckets(cold_buckets["buckets"])
    # 守恒：入桶计数 = 有效事件数；隔离行沿用 M1 口径
    assert cold_buckets["conserved"] is True
    assert cold_buckets["input_count"] == 3
    # 右端开口径未被改动：01:59 仍在 01 桶，02:05 在 02 桶
    hours = {b["hour"]: b for b in cold_buckets["buckets"]}
    assert hours["2026-11-15T01:00:00Z"]["count"] == 2
    assert hours["2026-11-15T01:00:00Z"]["errors"] == 1
    assert hours["2026-11-15T02:00:00Z"]["count"] == 1


def test_replay_preserves_m1_quarantine_evidence(tmp_path):
    from app.modules.m7_archive import replay
    store = make_store(tmp_path)
    text = '{"request_id":"ok1","timestamp":"2026-11-15T01:00:00Z","service":"pay","status":200}\n{"request_id":'  # 第二行截断
    store.ingest("b1", text)
    rb = store.readback("b1")
    result = replay.rebuild_buckets(rb["lines"])
    assert result["quarantine_count"] == 1
    assert result["quarantine"][0]["reason"] == "TRUNCATED"
    assert result["valid_count"] == 1
    assert result["bucketed_count"] == 1


# ----------------------------- 并发计数一致 ----------------------------- #
def test_concurrent_ingest_and_migrate_keeps_counts_consistent(tmp_path):
    store = make_store(tmp_path, hot_quota=10_000, cold_quota=1_000_000)
    for i in range(6):
        store.ingest(f"b{i}", LOG_TEXT)
    errors = []

    def worker_migrate():
        for i in range(6):
            try:
                store.migrate(f"b{i}")
            except DomainError as exc:        # 配额/状态冲突只能是明确领域错误
                errors.append(exc.code)

    def worker_ingest():
        for i in range(6, 12):
            try:
                store.ingest(f"b{i}", LOG_TEXT)
            except DomainError as exc:
                errors.append(exc.code)

    t1 = threading.Thread(target=worker_migrate)
    t2 = threading.Thread(target=worker_ingest)
    t1.start(); t2.start(); t1.join(); t2.join()
    assert errors == []
    batches = store.list_batches()
    assert len(batches) == 12
    archived = [b for b in batches if b["state"] == STATE_ARCHIVED]
    hot = [b for b in batches if b["state"] == STATE_HOT]
    assert len(archived) == 6 and len(hot) == 6
    status = store.status()
    # 在线热字节 = 各热批原件字节之和；计数没有丢失或重复
    expect_hot = sum(b["hot_bytes"] for b in hot)
    assert status["hot_used_bytes"] == expect_hot
    assert status["hot_line_count"] == sum(b["line_count"] for b in hot)
    # 每个已归档批次冷件都可校验且行数齐全
    for b in archived:
        v = store.verify_archive(b["batch_id"])
        assert v["ok"] and v["verified_lines"] == b["line_count"]


def test_concurrent_same_batch_migrate_runs_once(tmp_path):
    store = make_store(tmp_path)
    store.ingest("b1", LOG_TEXT)
    outcomes = []

    def worker():
        outcomes.append(store.migrate("b1"))

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert all(o["state"] == STATE_ARCHIVED for o in outcomes)
    assert len(list((tmp_path / "cold").glob("*.loghour.arc"))) == 1
    assert len(store.readback("b1")["lines"]) == 3
