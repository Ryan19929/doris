# 提速第一批：保留分区的快照装载、合并空拍、任务完成即运行

> 对应 `restore-r2-speed` 分支。实测依据：2 GB/副本、3 表 × 200 分区、3 副本、表级 CCR 同步（`__keep_on_local__`），默认配置下方案 T_total 151～160 s，其中 FE 3 秒节拍的空等约 60 s、每 BE 单线程的 MOVE 池约 40 s/BE；调优配置（1 秒节拍、16 个快照线程）下 91/90 s，MOVE 阶段仍占 36～42 s。两项代码改动各对准其中一块。

## 1. 保留分区由快照任务装载到位（只影响复用路径）

**现状。** 原子恢复里判为保留（kept）的分区，每个副本仍然：CREATE 建空暂存 tablet → RESTORE_DIGEST → MAKE_SNAPSHOT（把原表 tablet 的 rowset 文件硬链接到快照目录）→ COMMITTING 的 MOVE（`convert_rowset_ids` 改 rowset id、`remove_all`、逐文件链接、重载 tablet）。MOVE 池每个 BE 固定 1 个线程（`agent_server.cpp`），3000 个副本约 40 s/BE。增量分区的本地底座已经在快照阶段由 `make_local_source_snapshot_and_load` 装载到位（证据 `LOCAL_LOADED` 随 tablet meta 落盘），COMMITTING 只追加增量。

**改动。** 保留分区走同一条 BE 路径：

- FE 的快照任务多一个 optional 字段 `TSnapshotRequest.restore_kept_load`（配置 `restore_reuse_kept_load_at_snapshot`，默认 true）；BE 入口条件改为 `restore_local_source && (restore_incremental || restore_kept_load)`，装载后返回一个空的快照目录（`restore_incremental=true` 的空目录请求）。
- 提交阶段的 MOVE 请求**不变**。BE 的 `move_with_restore_stage` 在 `.hdr.increment` 检查之后、`convert_rowset_ids` 之前多一个分支：目录里没有 `<tablet_id>.hdr`、没有 `.hdr.increment`、没有数据文件 → 读 tablet meta 里的证据，`job_id` 相同、stage 为 `LOCAL_LOADED`、`max_version == base_version` 则返回 OK（零 IO），否则 `[RESTORE_NOT_RETRYABLE]`。普通恢复的快照目录一定有 tablet meta，这个分支对原有路径是死代码。
- FE 在 `commit()` 给保留分区的 DirMoveTask 打一个只在内存里的标记，`finishDirMoveTask` 据此把它的失败按本地来源任务处理（不可重试错误立即取消，其他错误 `restore_local_source_task_max_retry` 次后取消）；普通 MOVE 的失败仍无限重发。
- 纵深防御：装载入口按 tablet 路径互斥（`LocalSnapshotLock`），两个任务同时装载同一个 tablet 时第二个看到证据后跳过；本地来源快照的超时取 `restore_local_source_snapshot_timeout_s`（默认 6 小时）与作业超时中的较大者：清扫线程不看装载是否在进行，短于作业超时会在跨盘慢装载时中途删掉源快照（代码审查 P1-1）。
- 可观测：`TDownloadStats.local_source_loaded_tablets` → `SHOW RESTORE` 的 `atomic_local.loaded_tablets`；心跳能力值升为 3（不被要求）。

**为什么 BE 按目录内容自描述，而不是 FE 加字段。** 同一个副本的快照和 MOVE 总在同一个 BE 上，所以无论 FE 是哪个版本、BE 在作业中途升级与否，BE 自己做过什么就知道怎么收尾：能力 2 的 FOLLOWER FE 接管后发的普通 MOVE 在新 BE 上同样变成确认，不会卡到超时；快照时是旧 BE（做了带 tablet meta 的完整快照）、提交时是新 BE，照旧整体替换，不会取消。FE 不需要持久化任何新字段，回滚没有兼容问题。

**中断场景。** 快照任务重发由 `already_loaded` 分支吸收；FE 在 SNAPSHOTING 之前重启是从 PENDING 用新的暂存 tablet 重做整个作业（DOWNLOAD 之前没有编辑日志），装载的工作白做一遍但结果正确，第一套暂存 tablet 由 tablet report 删除；FE 在 DOWNLOAD/COMMIT 之后重启只重发 MOVE，由确认吸收；BE 在装载中途崩溃，崩在 `remove_all` 之后、重载之前时 RocksDB 里仍是初始 meta，重发重做，崩在重载之后时有证据，重发跳过；装载后、确认前被 compaction 不影响（证据随 meta 保留，版本不变）；被克隆覆盖由调度器保护挡住（保护集合本来就包含保留的原子分区），万一发生则证据丢失、确认失败、作业取消、原表不受影响。

**收益（推测，待 dwcat 复测）。** 保留分区的装载从单线程 MOVE 池挪进 5（调优 16）线程的快照池；文件操作数不减少（`convert_rowset_ids` 本身就是再链接 + 删旧链接）。调优配置下 MOVE 阶段 36～42 s → 0% 改写约 6 s、30% 改写约 12～15 s（增量分区的追加仍在 MOVE 池），快照阶段每表 +4～8 s。

## 2. 状态机合并空拍（改 Doris 原有路径，默认关闭）

`BackupHandler` 每拍（`backup_handler_update_interval_millis`，默认 3 秒）对每个作业调一次 `run()`，`run()` 每拍只执行一个状态函数。keep_on_local 备份在快照完成后还要 4 拍（UPLOAD_SNAPSHOT、UPLOADING、SAVE_META、UPLOAD_INFO 各一拍，都不等 BE）；恢复在快照完成后要 1 拍发下载任务、下载完成后要 1 拍发 MOVE，没有要下载的东西时 DOWNLOADING 还白等 1 拍。每张表 6～7 拍，三表串行就是 18～21 拍。

`backup_restore_merge_idle_ticks`（默认 false）打开后，`run()` 的 switch 外包一层循环：状态函数执行后，若 `status` 正常、状态变了、新状态不需要等 BE（或等待集合已空），就在同一拍继续；备份一拍最多 5 个状态，恢复 4 个。状态函数本身和它们写的编辑日志一处不改，所以日志条目与顺序逐条相同，`replayRun` 零改动；恢复在每个合并步之前都重做 `checkIfNeedCancel` 和暂停调试点的检查；`status` 不复位，UPLOAD_INFO 失败仍靠下一拍重试。关闭时循环只跑一次，逐字节等价于原行为。

`backup_restore_run_on_task_finish`（默认 false）：某状态等待的最后一个 BE 任务回报时，作业调用 `BackupHandler.nudge(dbId, jobId)`，由 handler 的一个线程立刻跑一次 `run()`，省掉等下一拍的平均半拍。只记 id，执行前重新查找作业，只在本 FE 仍是 Master、作业仍是库里当前作业且未结束时才运行，所以回放替换或淘汰的作业对象不会被运行。

## 3. 测试

- BE 单测 `restore_digest_test.cpp` 的 `RestoreKeptLoadTest.*`：装载写证据并返回空目录、重发跳过、同 tablet 并发只装一次、空目录的 MOVE 确认 / 拒绝另一作业 / 另一阶段 / 另一版本 / 证据被清、带 tablet meta 的快照仍整体替换、compaction 后确认、MoW 装载 → compaction → 确认。
- FE 单测 `RestoreReuseTest`：快照任务带 `restore_kept_load`、MOVE 请求不变但任务被标记、保留分区 MOVE 的失败收敛、`loaded_tablets` 汇总、合并空拍下恢复在同一拍发下载和 MOVE、暂停调试点、nudge 的去重 / 非 Master / 对象被替换；`BackupJobTest`：keep_on_local 备份一拍走到 FINISHED 且日志序列不变、S3 路径在 UPLOADING 停下、UPLOAD_INFO 失败下一拍重试。
- 回归 `test_backup_restore_kept_load`、`test_backup_restore_merge_idle_ticks`，以及 `test_backup_restore_atomic_local_source_docker` 新增的 FE 在 SNAPSHOTING 重启、BE 在装载中途重启。
