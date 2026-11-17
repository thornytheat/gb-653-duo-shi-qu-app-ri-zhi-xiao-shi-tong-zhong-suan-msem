# 多时区应用日志小时桶重算

        面向一线复核人员的单服务数据工作台。样例数据随仓库提供，结论保留行号、原因码和原值。

        ## 业务模块

        - M1 日志采信：`/api/logs/import`
- M2 时间解析：`/api/time/normalize`
- M3 事件去重：`/api/events/dedupe`
- M4 小时分桶：`/api/buckets/rebuild`
- M5 迟到修订：`/api/watermark/advance`
- M6 批次对账：`/api/export/buckets.csv`
- M7 冷热归档：`/api/archive/*`（页面 `/archive`）

        ## 核心规则

        事件先归一到 UTC，DST 重叠时保留 fold；小时桶右端开，水位只能前进且迟到事件仍进入修订。

        ## 冷热归档（M7）

        在线原件按字节限额（`HOT_QUOTA_BYTES`）驻留热库（SQLite BLOB），旧批次整体迁入冷归档目录
        （`COLD_ROOT`，受 `COLD_QUOTA_BYTES` 约束）。

        - 冷件为只追加的带校验帧容器（`*.loghour.arc`）：每帧记录物理行号、原件字节、行 sha256 与帧 sha256；
          回读逐帧复算，损坏直接返回 `COLD_ARCHIVE_CORRUPT`（HTTP 422），不返回半截数据。
        - 迁移逐帧提交并登记已校验进度，中断后从最后一个已校验帧恢复；冷帧已落盘但数据库未提交的崩溃窗口，
          恢复时按冷件去重，不重复归档、不重复入桶。
        - 整份冷件读回复验通过后，才在同一事务内切换 `ARCHIVED` 并删除热行；切换失败保留全部热件并置 `FAILED`。
        - 热/冷配额不足均整批拒绝，不留下半批；`ingest_key` 与已归档批次的重复迁移均幂等。
        - 回读区分 `HOT`/`COLD` 来源；`/api/archive/rebuild-buckets` 用归档原件复用 M1 解析与 M4 分桶复算原桶，
          不重新解释时间、不改变请求去重。
        - 四态口径：待迁移 `HOT`、迁移中 `MIGRATING`、已归档 `ARCHIVED`、切换失败 `FAILED`；
          在线与冷占用见 `/api/archive/status`。

        ## 启动

        ```bash
        docker compose up --build
        ```

        浏览器访问 `http://localhost:18212`。运行测试：

        ```bash
        docker compose exec -T app pytest -q
        ```

        ## 样例流程

        依次运行六个模块；最后在批次页复核输入、有效、隔离与输出合计。同一输入摘要重复执行时复用原 `run_id`。

        ## 已知不覆盖

        不处理加密文件与需要外部密钥的对象；不连接真实生产存储。
