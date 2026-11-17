# 深度登记 多时区应用日志小时桶重算

        - 规模：中型
        - 核心规则：事件先归一到 UTC，DST 重叠时保留 fold；小时桶右端开，水位只能前进且迟到事件仍进入修订。
        - 使用者与工作流：导入随仓样例，逐模块复核，最后导出单一证据清单。

        ## 功能模块

        | 模块 | 使用者动作 | 入口 | 核心文件 | 测试 |
        |---|---|---|---|---|
        | M1 日志采信 | 运行样例并复核证据 | `/api/logs/import` / `/logs` | `app/modules/m1_logs/core.py` `app/modules/m1_logs/service.py` | `tests/test_m1_logs.py` |
| M2 时间解析 | 运行样例并复核证据 | `/api/time/normalize` / `/time` | `app/modules/m2_time/core.py` `app/modules/m2_time/service.py` | `tests/test_m2_time.py` |
| M3 事件去重 | 运行样例并复核证据 | `/api/events/dedupe` / `/events` | `app/modules/m3_events/core.py` `app/modules/m3_events/service.py` | `tests/test_m3_events.py` |
| M4 小时分桶 | 运行样例并复核证据 | `/api/buckets/rebuild` / `/buckets` | `app/modules/m4_buckets/core.py` `app/modules/m4_buckets/service.py` | `tests/test_m4_buckets.py` |
| M5 迟到修订 | 运行样例并复核证据 | `/api/watermark/advance` / `/watermark` | `app/modules/m5_watermark/core.py` `app/modules/m5_watermark/service.py` | `tests/test_m5_watermark.py` |
| M6 批次对账 | 运行样例并复核证据 | `/api/export/buckets.csv` / `/runs` | `app/modules/m6_runs/core.py` `app/modules/m6_runs/service.py` | `tests/test_m6_runs.py` |
| M7 冷热归档 | 接入实际日志、按字节限额迁出旧批次并回读复算 | `/api/archive/migrate` / `/archive` | `app/modules/m7_archive/core.py` `app/modules/m7_archive/service.py` `app/modules/m7_archive/replay.py` | `tests/test_m7_archive.py` `tests/test_m7_archive_api.py` |

        ## 陷阱登记

        | TRAP | 现象 | 朴素实现 | 本项目处理 | 处理位置 | 测试 |
        |---|---|---|---|---|---|
        | TRAP-03 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m1_logs/core.py` | `tests/test_m1_logs.py::test_trap_1` |
| TRAP-05 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m2_time/core.py` | `tests/test_m2_time.py::test_trap_1` |
| TRAP-07 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m3_events/core.py` | `tests/test_m3_events.py::test_trap_1` |
| TRAP-09 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m4_buckets/core.py` | `tests/test_m4_buckets.py::test_trap_1` |
| TRAP-11 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m5_watermark/core.py` | `tests/test_m5_watermark.py::test_trap_1` |
| TRAP-12 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m6_runs/core.py` | `tests/test_m6_runs.py::test_trap_1` |
| TRAP-13 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m1_logs/core.py` | `tests/test_m1_logs.py::test_trap_2` |
| TRAP-14 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m2_time/core.py` | `tests/test_m2_time.py::test_trap_2` |
| TRAP-16 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m3_events/core.py` | `tests/test_m3_events.py::test_trap_2` |
| TRAP-23 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m4_buckets/core.py` | `tests/test_m4_buckets.py::test_trap_2` |
| TRAP-30 | 容量超限只留小时桶会丢证 | 删原件只留聚合 | 原件按字节限额迁冷归档，帧帧带 sha256，可回读复算 | `app/modules/m7_archive/core.py` | `tests/test_m7_archive.py::test_migrate_archived_frees_hot_and_cold_readback_matches` |
| TRAP-31 | 迁移中断产生半批/重复帧 | 崩溃后重跑整批或重复追加 | 逐帧登记 DONE，恢复时与冷件对账去重 | `app/modules/m7_archive/core.py` | `tests/test_m7_archive.py::test_crash_between_cold_write_and_db_commit_does_not_duplicate_frame` |
| TRAP-32 | 冷件静默损坏仍被采信 | 读回不校验 | 逐帧复算帧 sha256 与行摘要，损坏明确报 `COLD_ARCHIVE_CORRUPT` | `app/modules/m7_archive/core.py` | `tests/test_m7_archive_api.py::test_cold_corruption_returns_explicit_422` |
| TRAP-33 | 配额不足留下半批 | 先写后算账 | 先算整批字节再写库，超限整批拒绝 | `app/modules/m7_archive/core.py` | `tests/test_m7_archive.py::test_hot_quota_rejects_whole_batch_without_half_batch` |
| TRAP-34 | 切换失败误删热件 | 先删热件后验冷件 | 冷件整份复验通过后同事务切换并删热行，失败保热置 `FAILED` | `app/modules/m7_archive/core.py` | `tests/test_m7_archive.py::test_failed_switch_keeps_hot_and_retry_recovers` |

        ## 对账规则

        - 输入记录数 = 有效记录数 + 隔离记录数 + 空记录数。
        - 核心数值或事件数在转换、判定和导出之间保持守恒。
        - 同一输入摘要重跑复用原运行编号，不增加结果行。
        - 归档前后同一批原件复算的小时桶逐字节一致；在线热字节 = 各热批原件字节之和，迁移释放与冷占用可对账。
        - 迁移与接入在进程内串行化，并发下计数不丢不重；`ingest_key` 与重复迁移幂等。

        ## 已知不覆盖

        - 生产系统连接、身份认证和外部密钥不在演示范围内。
