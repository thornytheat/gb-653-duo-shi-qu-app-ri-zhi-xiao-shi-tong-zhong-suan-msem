# 数据字典

| 字段 | 类型 | 可空 | 定义 |
|---|---|---:|---|
| `run_id` | string | 否 | 输入摘要派生的稳定运行编号 |
| `source_line` | integer | 否 | 原文件物理行号 |
| `reason_code` | string | 是 | 隔离或冲突的稳定原因码 |
| `raw_value` | string | 是 | 未归一的原始证据 |
| `normalized_value` | string | 是 | 按项目规则得到的值 |
| `rule_version` | string | 否 | 计算规则版本 |

字符串编号逐字保留。金额与计量值用 `Decimal`。时间值带偏移并以 UTC 参与比较。区间统一采用左闭右开。

## M7 冷热归档字段

| 字段 | 类型 | 可空 | 定义 |
|---|---|---:|---|
| `batch_id` | string | 否 | 一批原件的稳定批次编号 |
| `state` | string | 否 | `HOT` 待迁移 / `MIGRATING` 迁移中 / `ARCHIVED` 已归档 / `FAILED` 切换失败 |
| `source_line` | integer | 否 | 原件物理行号，跳过空行但不重排 |
| `digest` | string | 否 | 单行原件 UTF-8 字节的 sha256 十六进制 |
| `frame_sha256` | string | 否 | 冷帧（帧头+载荷）的 sha256，读回逐帧复算 |
| `hot_bytes` | integer | 否 | 批次原件在线热库占用字节 |
| `cold_bytes` | integer | 否 | 归档文件实际字节（仅归档后有值） |
| `verified_lines` | integer | 否 | 已通过校验的行数，中断恢复的进度口径 |
| `archive_id` | string | 是 | 冷归档文件编号 `arc-<batch_id>` |
| `ingest_key` | string | 是 | 接入幂等键，同键重复提交复用既有批次 |

冷归档帧容器魔数为 `LOGHOUR-ARCHIVE/1`，文件 `*.loghour.arc`；配额口径为字节，
超限抛 `HOT_QUOTA_EXCEEDED` / `COLD_QUOTA_EXCEEDED`，冷件损坏抛 `COLD_ARCHIVE_CORRUPT`。
