"""M7 冷热归档：纯标准库核心。

冷归档文件是只追加的证据容器（不引入压缩以外的外部依赖，这里不压缩、不改写），
全部采用长度前缀帧，原件字节可以是任意字节（含换行）而无歧义：

    魔数帧   "LOGHOUR-ARCHIVE/1\\n"
    元数据帧 8 字节大端长度 + 元数据 JSON（UTF-8）
    数据帧   8 字节大端长度 + 帧头 JSON（UTF-8）
             8 字节大端长度 + 原件原始字节
             8 字节大端长度 + 帧 sha256 的十六进制（ASCII）

帧头记录物理行号、原件字节数与行 sha256；帧 sha256 覆盖
``帧头 JSON 字节 + 原件字节 + 校验摘要字节``。读取时逐帧复算，
损坏立即抛 ``COLD_ARCHIVE_CORRUPT``，不返回任何半截数据。
热库存同一批原始行的字节，迁移前后逐行 sha256 一致。

本模块不解释时间（M2 不变）、不做请求去重（M3 不变）：
桶复算直接复用 M4 的 ``hour_key``/``bucket``，输入仍是原始 JSON 行解析出的事件。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import struct
import threading
from datetime import datetime, timezone
from pathlib import Path

from app.errors import DomainError

MAGIC = b"LOGHOUR-ARCHIVE/1\n"
LEN_PREFIX = 8  # 每段 8 字节大端无符号长度前缀

# 批次生命周期（也是回读接口对外的状态口径）
STATE_HOT = "HOT"              # 待迁移：原件全部驻留在线热库
STATE_MIGRATING = "MIGRATING"  # 迁移中：冷件逐帧追加并校验，热件仍完整保留
STATE_ARCHIVED = "ARCHIVED"    # 已归档：冷件校验通过，热件已按字节释放
STATE_FAILED = "FAILED"        # 切换失败：冷件不完整/损坏，热件原样保留

# 归档任务（行级）进度状态
TASK_PENDING = "PENDING"
TASK_DONE = "DONE"


class ArchiveError(DomainError):
    """冷热归档领域错误，code 为稳定原因码。"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def line_digest(raw: bytes) -> str:
    """单行原件的稳定摘要；热库与冷帧用同一口径。"""
    return sha256_hex(raw)


# --------------------------------------------------------------------------- #
# 冷后端：文件系统上的只追加归档，按 archive_id 一文件
# --------------------------------------------------------------------------- #
class ColdStorage:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, archive_id: str) -> Path:
        # 只允许文件名安全的编号，杜绝路径穿越
        if not archive_id or not all(c.isalnum() or c in "-_" for c in archive_id):
            raise ArchiveError("ARCHIVE_ID_INVALID", f"归档编号非法: {archive_id!r}")
        return self.root / f"{archive_id}.loghour.arc"

    def init_archive(self, archive_id: str, meta: dict) -> Path:
        """新建归档并写入魔数与元数据；已存在则报错（不覆盖旧证据）。"""
        path = self.path_for(archive_id)
        if path.exists():
            raise ArchiveError("ARCHIVE_EXISTS", f"归档已存在，禁止覆盖: {archive_id}")
        meta_bytes = json.dumps(meta, ensure_ascii=False,
                                sort_keys=True).encode("utf-8")
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "wb") as fh:
            fh.write(MAGIC)
            fh.write(_frame(meta_bytes))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        return path

    def append_frame(self, archive_id: str, source_line: int, raw: bytes,
                     row_digest: str) -> dict:
        """追加一帧并返回帧描述；写完即读回校验，损坏当场报错。"""
        path = self.path_for(archive_id)
        if not path.exists():
            raise ArchiveError("ARCHIVE_NOT_FOUND", f"归档不存在: {archive_id}")
        # header 内的行摘要必须与载荷复算一致，否则拒绝写冷件
        if line_digest(raw) != row_digest:
            raise ArchiveError("DIGEST_MISMATCH",
                               f"第 {source_line} 行摘要不一致，拒绝写冷件")
        header = {"source_line": source_line, "bytes": len(raw),
                  "digest": row_digest, "frame_sha256": None}
        header_json = json.dumps(header, ensure_ascii=False, sort_keys=True,
                                 separators=(",", ":")).encode("utf-8")
        checksum = sha256_hex(header_json + raw)
        header["frame_sha256"] = checksum
        header_json = json.dumps(header, ensure_ascii=False, sort_keys=True,
                                 separators=(",", ":")).encode("utf-8")
        checksum_bytes = checksum.encode("ascii")
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "wb") as dst, open(path, "rb") as src:
            dst.write(src.read())
            dst.write(_frame(header_json))
            dst.write(_frame(raw))
            dst.write(_frame(checksum_bytes))
            dst.flush()
            os.fsync(dst.fileno())
        os.replace(tmp, path)
        return {"source_line": source_line, "bytes": len(raw),
                "digest": row_digest, "frame_sha256": checksum}

    def read_archive(self, archive_id: str) -> dict:
        """读回整个归档并逐帧校验。任何字节损坏都抛 COLD_ARCHIVE_CORRUPT。"""
        path = self.path_for(archive_id)
        if not path.exists():
            raise ArchiveError("ARCHIVE_NOT_FOUND", f"归档不存在: {archive_id}")
        with open(path, "rb") as fh:
            blob = fh.read()
        stream = _ByteStream(blob)
        try:
            if stream.read(len(MAGIC)) != MAGIC:
                raise ArchiveError("COLD_ARCHIVE_CORRUPT",
                                   f"归档 {archive_id} 魔数损坏，原件不可信")
            meta = json.loads(stream.read_frame().decode("utf-8"))
            frames = []
            while not stream.exhausted():
                header_json = stream.read_frame()
                raw = stream.read_frame()
                checksum_bytes = stream.read_frame()
                header = json.loads(header_json.decode("utf-8"))
                rebuilt_header = dict(header)
                rebuilt_header["frame_sha256"] = None
                rebuilt_json = json.dumps(
                    rebuilt_header, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":")).encode("utf-8")
                expect = sha256_hex(rebuilt_json + raw)
                if checksum_bytes != expect.encode("ascii"):
                    raise ArchiveError(
                        "COLD_ARCHIVE_CORRUPT",
                        f"归档 {archive_id} 第 {header.get('source_line')} 行"
                        "帧校验和不符，原件不可信")
                if header.get("bytes") != len(raw) or \
                        line_digest(raw) != header.get("digest"):
                    raise ArchiveError(
                        "COLD_ARCHIVE_CORRUPT",
                        f"归档 {archive_id} 第 {header.get('source_line')} 行"
                        "原件摘要或长度不符，原件不可信")
                frames.append({"source_line": header["source_line"], "raw": raw,
                               "digest": header["digest"], "bytes": len(raw)})
        except (TruncatedArchive, UnicodeDecodeError, json.JSONDecodeError,
                KeyError, struct.error) as exc:
            raise ArchiveError("COLD_ARCHIVE_CORRUPT",
                               f"归档 {archive_id} 帧损坏或截断: {exc}") from exc
        return {"archive_id": archive_id, "meta": meta, "frames": frames}

    def used_bytes(self) -> int:
        total = 0
        for p in self.root.glob("*.loghour.arc"):
            total += p.stat().st_size
        return total

    def frame_lines(self, archive_id: str) -> dict[int, str]:
        """返回冷件中已存在帧的 {source_line: digest}，用于中断恢复去重。

        逐帧做完整校验；冷件损坏时同样抛 COLD_ARCHIVE_CORRUPT，
        不会把一个损坏的归档当成“可继续追加”。
        """
        return {f["source_line"]: f["digest"]
                for f in self.read_archive(archive_id)["frames"]}


class TruncatedArchive(Exception):
    pass


def _frame(segment: bytes) -> bytes:
    """长度前缀帧：8 字节大端长度 + 段字节。"""
    return struct.pack(">Q", len(segment)) + segment


class _ByteStream:
    def __init__(self, blob: bytes):
        self.blob = blob
        self.pos = 0

    def read(self, n: int) -> bytes:
        if self.pos + n > len(self.blob):
            raise TruncatedArchive("字节不足，容器被截断")
        out = self.blob[self.pos:self.pos + n]
        self.pos += n
        return out

    def read_frame(self) -> bytes:
        (length,) = struct.unpack(">Q", self.read(LEN_PREFIX))
        return self.read(length)

    def exhausted(self) -> bool:
        return self.pos >= len(self.blob)


# --------------------------------------------------------------------------- #
# 热库 + 归档登记：SQLite，进程内串行化 + 事务，保证并发计数一致
# --------------------------------------------------------------------------- #
class ArchiveStore:
    def __init__(self, db_path: str, cold: ColdStorage,
                 hot_quota_bytes: int, cold_quota_bytes: int):
        self.db_path = db_path
        self.cold = cold
        if hot_quota_bytes <= 0 or cold_quota_bytes <= 0:
            raise ValueError("配额必须为正整数")
        self.hot_quota_bytes = int(hot_quota_bytes)
        self.cold_quota_bytes = int(cold_quota_bytes)
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        # 进程内互斥：SQLite 串行写 + 同一把锁覆盖“检查-写入-计数”复合动作
        self._lock = threading.RLock()
        with self.connect() as db:
            self._create(db)

    def connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.db_path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA foreign_keys=ON")
        return db

    @staticmethod
    def _create(db: sqlite3.Connection) -> None:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS archive_batches (
                batch_id      TEXT PRIMARY KEY,
                state         TEXT NOT NULL,
                source_name   TEXT NOT NULL,
                hot_bytes     INTEGER NOT NULL,
                line_count    INTEGER NOT NULL,
                content_digest TEXT NOT NULL,
                archive_id    TEXT,
                cold_bytes    INTEGER NOT NULL DEFAULT 0,
                verified_lines INTEGER NOT NULL DEFAULT 0,
                error_code    TEXT,
                created_at    TEXT NOT NULL,
                updated_at    TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS archive_lines (
                batch_id    TEXT NOT NULL,
                source_line INTEGER NOT NULL,
                raw         BLOB NOT NULL,
                digest      TEXT NOT NULL,
                byte_count  INTEGER NOT NULL,
                task_state  TEXT NOT NULL DEFAULT 'PENDING',
                PRIMARY KEY (batch_id, source_line),
                FOREIGN KEY (batch_id) REFERENCES archive_batches(batch_id)
            );
            CREATE TABLE IF NOT EXISTS archive_ingest (
                ingest_key TEXT PRIMARY KEY,
                batch_id   TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )

    # ----------------------------- 容量口径 ----------------------------- #
    def hot_used(self, db: sqlite3.Connection | None = None) -> int:
        own = db is None
        db = db or self.connect()
        try:
            return int(db.execute(
                "SELECT COALESCE(SUM(byte_count),0) FROM archive_lines").fetchone()[0])
        finally:
            if own:
                db.close()

    def cold_used(self) -> int:
        return self.cold.used_bytes()

    def status(self) -> dict:
        with self._lock, self.connect() as db:
            rows = db.execute(
                "SELECT state, COUNT(*) AS batches, COALESCE(SUM(hot_bytes),0) AS bytes "
                "FROM archive_batches GROUP BY state").fetchall()
            per_state = {r["state"]: {"batches": r["batches"], "bytes": int(r["bytes"])}
                         for r in rows}
            hot = self.hot_used(db)
            line_total = db.execute(
                "SELECT COUNT(*) FROM archive_lines").fetchone()[0]
            archived_line_total = db.execute(
                "SELECT COALESCE(SUM(line_count),0) FROM archive_batches "
                "WHERE state='ARCHIVED'").fetchone()[0]
        cold = self.cold_used()
        return {
            "hot_quota_bytes": self.hot_quota_bytes,
            "hot_used_bytes": hot,
            "hot_free_bytes": self.hot_quota_bytes - hot,
            "cold_quota_bytes": self.cold_quota_bytes,
            "cold_used_bytes": cold,
            "cold_free_bytes": self.cold_quota_bytes - cold,
            "online_bytes": hot,
            "hot_line_count": line_total,
            "archived_line_count": int(archived_line_total),
            "states": {
                STATE_HOT: per_state.get(STATE_HOT, {"batches": 0, "bytes": 0}),
                STATE_MIGRATING: per_state.get(STATE_MIGRATING,
                                               {"batches": 0, "bytes": 0}),
                STATE_ARCHIVED: per_state.get(STATE_ARCHIVED,
                                              {"batches": 0, "bytes": 0}),
                STATE_FAILED: per_state.get(STATE_FAILED, {"batches": 0, "bytes": 0}),
            },
        }

    # ----------------------------- 原件接入 ----------------------------- #
    def ingest(self, batch_id: str, text: str | bytes,
               source_name: str = "api", ingest_key: str | None = None) -> dict:
        """接入一批实际日志原件。

        - 按物理行号保留非空行（与 M1 一致跳过空行，但行号不重排）；
        - 热配额不足时整批拒绝，不留下半批（先算账再写库）；
        - ingest_key 幂等：重复提交同一 key 直接返回既有批次，不重复入桶。
        """
        if isinstance(text, str):
            payload = text.encode("utf-8")
        else:
            payload = bytes(text)
        rows = _split_physical_lines(payload)
        if not rows:
            raise ArchiveError("EMPTY_BATCH", "批次为空，没有可归档的原件行")
        line_records = [(line_no, raw, line_digest(raw), len(raw))
                        for line_no, raw in rows]
        batch_bytes = sum(r[3] for r in line_records)
        content_digest = sha256_hex(b"\n".join(r[1] for r in line_records))
        now = utc_now()
        with self._lock:
            with self.connect() as db:
                if ingest_key is not None:
                    hit = db.execute(
                        "SELECT batch_id FROM archive_ingest WHERE ingest_key=?",
                        (ingest_key,)).fetchone()
                    if hit:
                        return self._batch_view(db, hit["batch_id"], idempotent=True)
                exists = db.execute(
                    "SELECT 1 FROM archive_batches WHERE batch_id=?",
                    (batch_id,)).fetchone()
                if exists:
                    raise ArchiveError("BATCH_EXISTS", f"批次已存在: {batch_id}")
                if self.hot_used(db) + batch_bytes > self.hot_quota_bytes:
                    raise ArchiveError(
                        "HOT_QUOTA_EXCEEDED",
                        f"热配额不足: 需要 {batch_bytes} 字节，"
                        f"剩余 {self.hot_quota_bytes - self.hot_used(db)} 字节，整批拒绝")
                db.execute(
                    "INSERT INTO archive_batches(batch_id,state,source_name,hot_bytes,"
                    "line_count,content_digest,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (batch_id, STATE_HOT, source_name, batch_bytes,
                     len(line_records), content_digest, now, now))
                db.executemany(
                    "INSERT INTO archive_lines(batch_id,source_line,raw,digest,"
                    "byte_count,task_state) VALUES(?,?,?,?,?,?)",
                    [(batch_id, ln, raw, dg, n, TASK_PENDING)
                     for ln, raw, dg, n in line_records])
                if ingest_key is not None:
                    db.execute(
                        "INSERT OR IGNORE INTO archive_ingest(ingest_key,batch_id,"
                        "created_at) VALUES(?,?,?)",
                        (ingest_key, batch_id, now))
                view = self._batch_view(db, batch_id, idempotent=False)
            return view

    # ----------------------------- 迁移 ----------------------------- #
    def migrate(self, batch_id: str) -> dict:
        """把一个热批次按字节限额迁入冷归档（可中断、可恢复、幂等）。"""
        with self._lock:
            with self.connect() as db:
                batch = self._get_batch(db, batch_id)
                state = batch["state"]
                if state == STATE_ARCHIVED:
                    # 重试不重复归档：直接回显既有结果
                    return self._batch_view(db, batch_id, idempotent=True)
                if state == STATE_FAILED:
                    # 上次切换失败，热件保留；允许从已校验进度重试。
                    # 若冷件已被外部移除，则重建归档并把行级进度重置为 PENDING，
                    # 靠热件重新逐帧迁出（热件在 FAILED 态始终完整）。
                    self._recover_failed(db, batch)
                elif state == STATE_HOT:
                    self._begin_migration(db, batch)
                elif state == STATE_MIGRATING:
                    pass  # 中断恢复：沿用同一归档继续补帧
                db.commit()
                return self._drive_migration(batch_id)

    def _recover_failed(self, db: sqlite3.Connection,
                        batch: sqlite3.Row) -> None:
        archive_id = batch["archive_id"]
        cold_missing = not (archive_id and self.cold.path_for(archive_id).exists())
        now = utc_now()
        if cold_missing:
            meta = {
                "archive_id": archive_id or f"arc-{batch['batch_id']}",
                "batch_id": batch["batch_id"],
                "source_name": batch["source_name"],
                "content_digest": batch["content_digest"],
                "line_count": batch["line_count"],
                "created_at": now,
                "rule_version": "archive/1",
                "recovered": True,
            }
            self.cold.init_archive(meta["archive_id"], meta)
            db.execute(
                "UPDATE archive_lines SET task_state=? WHERE batch_id=?",
                (TASK_PENDING, batch["batch_id"]))
            db.execute(
                "UPDATE archive_batches SET state=?, archive_id=?, "
                "verified_lines=0, cold_bytes=0, error_code=NULL, updated_at=? "
                "WHERE batch_id=?",
                (STATE_MIGRATING, meta["archive_id"], now, batch["batch_id"]))
        else:
            self._to_migrating(db, batch)

    def _begin_migration(self, db: sqlite3.Connection,
                         batch: sqlite3.Row) -> None:
        # 冷配额预检：整批语义，不够就拒绝且不建半份冷件、热件不动
        need = self._cold_footprint(db, batch["batch_id"])
        free = self.cold_quota_bytes - self.cold.used_bytes()
        if need > free:
            raise ArchiveError(
                "COLD_QUOTA_EXCEEDED",
                f"冷配额不足: 整批约需 {need} 字节，剩余 {free} 字节，拒绝迁移")
        archive_id = f"arc-{batch['batch_id']}"
        now = utc_now()
        meta = {
            "archive_id": archive_id,
            "batch_id": batch["batch_id"],
            "source_name": batch["source_name"],
            "content_digest": batch["content_digest"],
            "line_count": batch["line_count"],
            "created_at": now,
            "rule_version": "archive/1",
        }
        self.cold.init_archive(archive_id, meta)
        db.execute(
            "UPDATE archive_batches SET state=?, archive_id=?, updated_at=? "
            "WHERE batch_id=?",
            (STATE_MIGRATING, archive_id, now, batch["batch_id"]))

    def _to_migrating(self, db: sqlite3.Connection, batch: sqlite3.Row) -> None:
        # FAILED -> MIGRATING：归档文件可能残缺，保留它用于排查，进度以库里
        # 已 DONE 的帧为准；冷文件损坏会在补帧/读回时明确报错。
        now = utc_now()
        db.execute(
            "UPDATE archive_batches SET state=?, error_code=NULL, updated_at=? "
            "WHERE batch_id=?",
            (STATE_MIGRATING, now, batch["batch_id"]))

    def _cold_footprint(self, db: sqlite3.Connection, batch_id: str) -> int:
        # 魔数 + 元数据帧 + 每帧三段长度前缀（帧头/载荷/校验摘要）的保守上界
        rows = db.execute(
            "SELECT byte_count FROM archive_lines WHERE batch_id=?",
            (batch_id,)).fetchall()
        payload = sum(r["byte_count"] for r in rows)
        per_frame_overhead = 256
        return len(MAGIC) + 1 + 512 + payload + len(rows) * per_frame_overhead

    def _drive_migration(self, batch_id: str) -> dict:
        """逐帧迁移；每帧事务提交，中断后从最后一个 DONE 继续。

        崩溃窗口（冷帧已落盘、库 DONE 未提交）恢复时，先与冷件已有帧对账：
        冷件里已校验存在的行直接补标 DONE，绝不重复追加同一帧。
        """
        try:
            while True:
                with self.connect() as db:
                    batch = self._get_batch(db, batch_id)
                    if batch["state"] != STATE_MIGRATING:
                        break
                    self._reconcile_cold_frames(db, batch)
                    row = db.execute(
                        "SELECT source_line,raw,digest,byte_count FROM archive_lines "
                        "WHERE batch_id=? AND task_state=? ORDER BY source_line LIMIT 1",
                        (batch_id, TASK_PENDING)).fetchone()
                    if row is None:
                        self._finalize_migration(db, batch)
                        break
                    # 写冷帧（内部读回校验），成功后才标记 DONE
                    self.cold.append_frame(batch["archive_id"], row["source_line"],
                                           bytes(row["raw"]), row["digest"])
                    db.execute(
                        "UPDATE archive_lines SET task_state=? WHERE batch_id=? "
                        "AND source_line=?",
                        (TASK_DONE, batch_id, row["source_line"]))
                    db.execute(
                        "UPDATE archive_batches SET verified_lines=verified_lines+1,"
                        "cold_bytes=cold_bytes+?, updated_at=? WHERE batch_id=?",
                        (row["byte_count"], utc_now(), batch_id))
        except ArchiveError:
            self._mark_failed(batch_id)
            raise
        except Exception as exc:  # 磁盘/IO 等：失败保热件，明确报错
            self._mark_failed(batch_id)
            raise ArchiveError("MIGRATION_IO_ERROR",
                               f"迁移中断，热件已保留: {exc}") from exc
        with self.connect() as db:
            return self._batch_view(db, batch_id, idempotent=False)

    def _reconcile_cold_frames(self, db: sqlite3.Connection,
                               batch: sqlite3.Row) -> None:
        """把冷件中已存在且校验通过的帧补登为 DONE（恢复去重）。

        崩溃窗口里冷帧已落盘但行级状态仍 PENDING；这里补登 DONE，并把
        verified_lines/cold_bytes 校正为“冷件实际帧数/字节”，避免重复计数。
        """
        arc_path = self.cold.path_for(batch["archive_id"])
        if not arc_path.exists():
            return
        cold_frames = self.cold.frame_lines(batch["archive_id"])  # 损坏即抛错
        if not cold_frames:
            return
        placeholders = ",".join("?" for _ in cold_frames)
        rows = db.execute(
            f"SELECT source_line,digest,byte_count FROM archive_lines "
            f"WHERE batch_id=? AND task_state=? AND source_line IN ({placeholders})",
            (batch["batch_id"], TASK_PENDING, *cold_frames.keys())).fetchall()
        reconciled = 0
        reconciled_bytes = 0
        for row in rows:
            if cold_frames.get(row["source_line"]) != row["digest"]:
                raise ArchiveError(
                    "COLD_ARCHIVE_CORRUPT",
                    f"冷件第 {row['source_line']} 行摘要与热件不符，拒绝继续迁移")
            db.execute(
                "UPDATE archive_lines SET task_state=? WHERE batch_id=? "
                "AND source_line=?",
                (TASK_DONE, batch["batch_id"], row["source_line"]))
            reconciled += 1
            reconciled_bytes += row["byte_count"]
        if reconciled:
            # 补登后以全部 DONE 行为准重算字节，并把进度对齐冷件真实帧数，
            # 杜绝崩溃窗口导致的重复累加
            done_bytes = db.execute(
                "SELECT COALESCE(SUM(byte_count),0) FROM archive_lines "
                "WHERE batch_id=? AND task_state=?",
                (batch["batch_id"], TASK_DONE)).fetchone()[0]
            db.execute(
                "UPDATE archive_batches SET verified_lines=?, cold_bytes=?, "
                "updated_at=? WHERE batch_id=?",
                (len(cold_frames), int(done_bytes), utc_now(),
                 batch["batch_id"]))

    def _finalize_migration(self, db: sqlite3.Connection,
                            batch: sqlite3.Row) -> None:
        """整份冷件读回复验通过后才删热件并切换 ARCHIVED（切换失败保留热件）。"""
        arc = self.cold.read_archive(batch["archive_id"])
        if len(arc["frames"]) != batch["line_count"]:
            raise ArchiveError("COLD_ARCHIVE_CORRUPT",
                               "冷件帧数与批次数不符，拒绝切换，热件保留")
        digests_db = {r["source_line"]: r["digest"] for r in db.execute(
            "SELECT source_line,digest FROM archive_lines WHERE batch_id=?",
            (batch["batch_id"],)).fetchall()}
        digests_arc = {f["source_line"]: f["digest"] for f in arc["frames"]}
        if digests_db != digests_arc:
            raise ArchiveError("COLD_ARCHIVE_CORRUPT",
                               "冷件逐行摘要与热件不一致，拒绝切换，热件保留")
        content = b"\n".join(f["raw"] for f in sorted(arc["frames"],
                                                       key=lambda x: x["source_line"]))
        if sha256_hex(content) != batch["content_digest"]:
            raise ArchiveError("COLD_ARCHIVE_CORRUPT",
                               "冷件整体摘要不符，拒绝切换，热件保留")
        cold_bytes = self.cold.path_for(batch["archive_id"]).stat().st_size
        now = utc_now()
        # 先把状态切到 ARCHIVED 再删热行，同事务；任何异常都不提交 => 热件保留
        db.execute(
            "UPDATE archive_batches SET state=?, cold_bytes=?, "
            "verified_lines=line_count, error_code=NULL, updated_at=? WHERE batch_id=?",
            (STATE_ARCHIVED, cold_bytes, now, batch["batch_id"]))
        db.execute("DELETE FROM archive_lines WHERE batch_id=?", (batch["batch_id"],))

    def _mark_failed(self, batch_id: str) -> None:
        try:
            with self.connect() as db:
                db.execute(
                    "UPDATE archive_batches SET state=?, error_code=?, updated_at=? "
                    "WHERE batch_id=?",
                    (STATE_FAILED, "MIGRATION_INTERRUPTED", utc_now(), batch_id))
        except sqlite3.Error:
            pass  # 进程级崩溃时状态由下次 migrate 依据 DONE 帧恢复

    # ----------------------------- 回读 / 复算 ----------------------------- #
    def readback(self, batch_id: str) -> dict:
        """回读批次原件：热批次读热库，已归档批次逐帧校验冷件后返回。"""
        with self._lock, self.connect() as db:
            batch = self._get_batch(db, batch_id)
            if batch["state"] in (STATE_HOT, STATE_MIGRATING, STATE_FAILED):
                rows = db.execute(
                    "SELECT source_line,raw,digest,byte_count,task_state "
                    "FROM archive_lines WHERE batch_id=? ORDER BY source_line",
                    (batch_id,)).fetchall()
                lines = [{"source_line": r["source_line"],
                          "text": bytes(r["raw"]).decode("utf-8"),
                          "digest": r["digest"], "bytes": r["byte_count"],
                          "task_state": r["task_state"]} for r in rows]
                return self._batch_view(db, batch_id, lines=lines,
                                        location="HOT")
            arc = self.cold.read_archive(batch["archive_id"])  # 损坏即抛错
            lines = [{"source_line": f["source_line"],
                      "text": f["raw"].decode("utf-8"),
                      "digest": f["digest"], "bytes": f["bytes"],
                      "task_state": TASK_DONE}
                     for f in sorted(arc["frames"], key=lambda x: x["source_line"])]
            return self._batch_view(db, batch_id, lines=lines, location="COLD")

    def verify_archive(self, batch_id: str) -> dict:
        """只校验不回显全文：冷件损坏明确报错。"""
        with self._lock, self.connect() as db:
            batch = self._get_batch(db, batch_id)
            if batch["state"] != STATE_ARCHIVED:
                raise ArchiveError("BATCH_NOT_ARCHIVED",
                                   f"批次 {batch_id} 尚未归档，当前状态 {batch['state']}")
            arc = self.cold.read_archive(batch["archive_id"])
        return {"batch_id": batch_id, "archive_id": batch["archive_id"],
                "state": STATE_ARCHIVED, "verified_lines": len(arc["frames"]),
                "line_count": batch["line_count"],
                "ok": len(arc["frames"]) == batch["line_count"]}

    def list_batches(self) -> list[dict]:
        with self._lock, self.connect() as db:
            rows = db.execute(
                "SELECT batch_id,state,source_name,hot_bytes,line_count,"
                "archive_id,cold_bytes,verified_lines,error_code,created_at,updated_at "
                "FROM archive_batches ORDER BY created_at, batch_id").fetchall()
            return [dict(r) for r in rows]

    # ----------------------------- 内部工具 ----------------------------- #
    @staticmethod
    def _get_batch(db: sqlite3.Connection, batch_id: str) -> sqlite3.Row:
        row = db.execute("SELECT * FROM archive_batches WHERE batch_id=?",
                         (batch_id,)).fetchone()
        if row is None:
            raise ArchiveError("BATCH_NOT_FOUND", f"批次不存在: {batch_id}")
        return row

    def _batch_view(self, db: sqlite3.Connection, batch_id: str,
                    lines: list[dict] | None = None, location: str | None = None,
                    idempotent: bool = False) -> dict:
        batch = self._get_batch(db, batch_id)
        view = {
            "batch_id": batch["batch_id"],
            "state": batch["state"],
            "source_name": batch["source_name"],
            "hot_bytes": batch["hot_bytes"],
            "line_count": batch["line_count"],
            "content_digest": batch["content_digest"],
            "archive_id": batch["archive_id"],
            "cold_bytes": batch["cold_bytes"],
            "verified_lines": batch["verified_lines"],
            "error_code": batch["error_code"],
            "created_at": batch["created_at"],
            "updated_at": batch["updated_at"],
            "idempotent": idempotent,
        }
        if location is not None:
            view["location"] = location
        if lines is not None:
            view["lines"] = lines
        return view


def _split_physical_lines(payload: bytes) -> list[tuple[int, bytes]]:
    """按物理行号切分；跳过空行但保留原始行号（与 M1 证据口径一致）。"""
    out = []
    for line_no, line in enumerate(payload.splitlines(), 1):
        if not line.strip():
            continue
        out.append((line_no, line))
    return out
