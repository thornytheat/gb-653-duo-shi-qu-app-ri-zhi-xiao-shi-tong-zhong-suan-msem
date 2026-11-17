REASONS = {
    "BAD_COLUMNS": "字段数量不符", "EMPTY_VALUE": "必要值为空",
    "DUPLICATE_CONFLICT": "同一业务键取值冲突", "TRUNCATED": "尾记录不完整",
    "INVALID_VALUE": "值不符合定义", "STALE_VERSION": "提交版本已过期",
    # M7 冷热归档
    "EMPTY_BATCH": "批次为空，没有可归档原件",
    "BATCH_EXISTS": "批次编号已存在",
    "BATCH_NOT_FOUND": "批次不存在",
    "BATCH_NOT_ARCHIVED": "批次尚未归档，无法校验冷件",
    "HOT_QUOTA_EXCEEDED": "热库字节限额不足，整批拒绝",
    "COLD_QUOTA_EXCEEDED": "冷库字节限额不足，拒绝迁移",
    "ARCHIVE_EXISTS": "归档文件已存在，禁止覆盖",
    "ARCHIVE_NOT_FOUND": "归档文件不存在",
    "ARCHIVE_ID_INVALID": "归档编号含非法字符",
    "COLD_ARCHIVE_CORRUPT": "冷归档损坏，原件不可信",
    "DIGEST_MISMATCH": "行摘要不一致，拒绝写入冷件",
    "MIGRATION_IO_ERROR": "迁移发生 IO 错误，热件已保留",
    "MIGRATION_INTERRUPTED": "迁移中断，可从已校验进度恢复",
    "ARCHIVE_NOT_CONFIGURED": "冷热归档未配置",
}

def reason(code: str) -> str:
    return REASONS.get(code, "未登记原因")
