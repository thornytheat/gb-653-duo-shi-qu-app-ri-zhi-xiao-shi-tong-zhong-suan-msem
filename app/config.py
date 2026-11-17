from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    db_path: str = "./data/loghour.db"
    rule_version: str = "2026.11"
    timezone: str = "Asia/Shanghai"
    # M7 冷热归档：热库存原件字节的在线限额，冷库存归档文件的字节限额
    archive_db_path: str = "./data/archive.db"
    cold_root: str = "./data/cold"
    hot_quota_bytes: int = 1_048_576
    cold_quota_bytes: int = 4_194_304

    @classmethod
    def from_env(cls):
        return cls(
            db_path=os.getenv("DB_PATH", cls.db_path),
            rule_version=os.getenv("RULE_VERSION", cls.rule_version),
            archive_db_path=os.getenv("ARCHIVE_DB_PATH", cls.archive_db_path),
            cold_root=os.getenv("COLD_ROOT", cls.cold_root),
            hot_quota_bytes=int(os.getenv("HOT_QUOTA_BYTES", cls.hot_quota_bytes)),
            cold_quota_bytes=int(os.getenv("COLD_QUOTA_BYTES",
                                          cls.cold_quota_bytes)),
        )
