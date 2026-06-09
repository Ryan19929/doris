# Doris `_binlog` 回收机制分析：tablet 均衡与删除场景

## 范围

本文分析 BE tablet 目录下的 `_binlog` 子目录在 tablet 被均衡、迁移、删除时的文件与元数据回收机制。

这里的 `_binlog` 指 CCR/`STATEMENT_AND_SNAPSHOT` binlog 使用的本地文件目录，主要保存 rowset segment 及倒排索引文件的 binlog 副本或硬链接。`binlog<Row>` 使用独立 row binlog rowset 元数据与文件路径，本文只在清理线程的兜底逻辑中提及，不展开分析。

## 文件和元数据布局

对于一个 tablet，CCR binlog 文件放在 tablet 物理路径下：

```text
<root_path>/data/<shard>/<tablet_id>/<schema_hash>/_binlog/
```

`Tablet::get_segment_filepath()` 会把某个 rowset 的第 `N` 个 segment 映射为：

```text
<tablet_path>/_binlog/<rowset_id>_<N>.dat
```

普通 segment 文件生成 binlog 文件时，`BetaRowset::add_to_binlog()` 会在同一 tablet 目录内创建 `_binlog`，并把 rowset segment、倒排索引文件硬链接到 `_binlog`。因此在同一个 tablet 路径内，`_binlog` 文件通常和普通 rowset 文件共享 inode；删除 `_binlog` 路径名一般不会删除普通 rowset 路径名，但会减少一个硬链接引用。

binlog 元数据保存在 BE 本地 OlapMeta/RocksDB 中，关键前缀包括：

```text
binlog_meta_<tablet_uid>_<version>_<rowset_id>
binlog_data_<tablet_uid>_<version>_<rowset_id>
```

文件和元数据是两层生命周期：

- `_binlog` 文件负责实际可下载的数据。
- `binlog_meta_`/`binlog_data_` 负责从 binlog version 定位 rowset、segment 数、rowset meta 等信息。
- 合法 GC 需要同时删除文件和元数据；只手动删文件会留下短期元数据残留。

## 正常 binlog GC

正常的 binlog GC 由 FE `BinlogGcer` 发起。它每 15 秒运行一次，调用 `BinlogManager.gc()` 计算 tombstone，然后按 BE 维度发送 `BinlogGcTask`。

GC 是否产生 tombstone 由 binlog 配置和 syncer lock 共同决定：

- `binlog.ttl_seconds`
- `binlog.max_bytes`
- `binlog.max_history_nums`
- CCR/syncer 对 commit seq 的锁定

BE 收到 `GC_BINLOG` task 后，`gc_binlog_callback()` 把 tablet/version 收集成 `tablet_id -> version`，再调用 `StorageEngine::gc_binlogs()`。如果 tablet 还在 BE 内存中，最终进入 `Tablet::gc_binlogs(version)`。

`Tablet::gc_binlogs()` 的顺序是：

1. 以当前 tablet uid 构造 `binlog_meta_` key 范围。
2. 找出小于等于 GC version 的 binlog meta。
3. 根据 meta 中的 rowset id 和 segment 数推导 `_binlog` 文件路径。
4. 逐个 `unlink()` binlog segment 和 index 文件。
5. 如果文件删除没有失败，则删除对应的 `binlog_meta_` 和 `binlog_data_` key。

一个重要细节是，`Tablet::gc_binlogs()` 对 `ENOENT` 直接跳过。这意味着如果 `_binlog` 文件已经被手动删除，下一次合法 GC 仍有机会继续删除对应元数据。但在这之前，如果还有读取或 CCR 拉取这些 binlog，会遇到文件不存在。

## tablet 均衡：跨 BE clone

tablet 均衡创建新副本时，BE clone 流程会主动携带 `_binlog`。

源 BE 创建 snapshot 时，`EngineCloneTask::_make_snapshot()` 设置 `is_copy_binlog=true`。`SnapshotManager` 在生成 snapshot 后，如果目标 tablet 开启 CCR binlog，会：

1. 遍历一致版本 rowset。
2. 调用 `Tablet::get_rowset_binlog_metas()` 收集这些版本对应的 binlog meta。
3. 把 meta 写入 snapshot 目录的 `rowset_binlog_metas.pb`。
4. 把源 tablet `_binlog` 中的 segment/index 文件链接到 snapshot 目录，文件名变为：

```text
<rowset_id>_<segment>.binlog
<rowset_id>_<segment>.binlog-index
```

目标 BE 下载 snapshot 后，`EngineCloneTask` 会：

1. 读取并删除 clone 目录里的 `rowset_binlog_metas.pb`。
2. 如果包含 binlog meta，则在目标 tablet 目录下创建 `_binlog`。
3. 遇到 `.binlog` 文件时，把后缀转换回 `.dat`，并 hard link 到目标 `_binlog`。
4. 遇到 `.binlog-index` 文件时，把后缀转换回 `.idx`，并 hard link 到目标 `_binlog`。
5. 调用 `Tablet::ingest_binlog_metas()` 把 binlog meta 导入目标 BE 的 OlapMeta。

因此，均衡新增副本后，目标副本拥有自己的 `_binlog` 路径和本地 binlog meta。源副本的 `_binlog` 不会因为 clone 成功而立即删除；它继续受两类机制管理：

- 如果源副本仍存在，按正常 binlog GC 删除过期 version。
- 如果源副本后续因均衡完成被删除，随 tablet drop/trash 流程回收。

## tablet 均衡：本机 storage migration

本机磁盘迁移和跨 BE clone 类似，也会迁移 `_binlog`，但实现上使用 copy 而不是 hard link。

`EngineStorageMigrationTask::_copy_index_and_data_files()` 会：

1. 复制 rowset 普通数据和索引文件到目标路径。
2. 对每个 rowset version 调用 `Tablet::get_rowset_binlog_metas()`。
3. 逐个复制 `_binlog` segment 文件到迁移目标目录，文件名为 `<rowset_id>_<segment>.binlog`。
4. 复制 binlog index 文件到迁移目标目录。
5. 汇总并写出 `rowset_binlog_metas.pb`。

迁移完成后，新 tablet 路径会拥有对应数据文件、binlog 文件和 meta。旧 tablet 路径如果没有对应有效 tablet meta，会被路径 GC 检测到并移动到 trash。相关兜底逻辑在 `TabletManager::try_delete_unused_tablet_path()`：当路径存在但 meta 不存在或不匹配时，将整个 schema hash 目录移动到 trash。

## tablet 删除和表删除

tablet 删除不是直接 `rm -rf` tablet 目录，而是分阶段完成。

`TabletManager::drop_tablet()` 进入 `_drop_tablet()` 后：

1. 从 tablet map 中移除 tablet。
2. 把 tablet 状态设置为 `TABLET_SHUTDOWN`。
3. 保存 tablet meta。
4. 把 tablet 放入 `_shutdown_tablets` 列表。

后续 `TabletManager::start_trash_sweep()` 批量处理 `_shutdown_tablets`。当 tablet 不再被其他线程引用时，`_move_tablet_to_trash()` 会：

1. 确认 RocksDB 中的 tablet meta 仍为 `TABLET_SHUTDOWN` 且 tablet uid 匹配。
2. 在 tablet 目录中保存一份 `.hdr`。
3. 调用 `DataDir::move_to_trash(tablet_path)`。
4. 删除 RocksDB 中的 tablet meta。

`DataDir::move_to_trash()` 的行为由 `trash_file_expire_time_sec` 决定：

- `trash_file_expire_time_sec <= 0`：直接删除 tablet 目录。
- `trash_file_expire_time_sec > 0`：把整个 tablet 目录 rename 到 `<root_path>/trash/<timestamp>.<counter>/<tablet_id>/<schema_hash>`。

因为 `_binlog` 是 tablet 目录的子目录，所以 tablet 删除时 `_binlog` 会随整个 tablet 目录一起进入 trash 或被直接删除。此时不需要单独等 binlog TTL。

## `DROP TABLE FORCE` 与 binlog TTL

`DROP TABLE ... FORCE` 只影响 FE recycle bin 的表回收时间。`CatalogRecycleBin::recycleTable()` 在 `isForceDrop=true` 时把 recycle time 置为 0，表示表不可恢复并应立即回收。

它不会把 `binlog.ttl_seconds` 改成 0，也不会让历史 binlog 立即按 TTL 过期。`DROP_TABLE` 本身仍会写入一条 binlog，`DropTableRecord` 只保存 commit seq、db id、table id、table name、raw SQL 等信息，不保存 `forceDrop`。

因此 force drop 后常见现象是：

- 表在 FE 语义上已经不可恢复。
- BE 上对应 tablet 目录可能还没被 drop/trash 线程处理。
- `_binlog` 可能短时间仍在原 tablet 目录，或已经随 tablet 目录进入 trash。
- 这些残留通常等待的是 tablet/trash 清理，不是等待 binlog TTL 自动改成立即过期。

还有一个细节：如果 FE 的 binlog GC 在表已经从 catalog 中移除后才尝试发送 GC task，`BinlogGcer.sendDbGcInfoToBe()` 可能因找不到 table 或 partition 而跳过该表。BE 侧如果 tablet 已经从内存中移除，`StorageEngine::gc_binlogs()` 也会看到 `tablet_id not found` 并跳过。因此，drop 后 `_binlog` 的物理回收主要依赖 tablet/trash 清理链路，而不是 binlog GC。

## trash 清理

BE 垃圾清扫线程周期性调用 `StorageEngine::start_trash_sweep()`。它会：

1. 扫描 snapshot 目录。
2. 扫描 trash 目录。
3. 如果磁盘使用率超过 guard space，则 trash expire 按 0 处理，加速清理。
4. 调用 `TabletManager::start_trash_sweep()` 将 `_shutdown_tablets` 移入 trash。
5. 清理无效 rowset meta、binlog meta、delete bitmap、pending publish info 等。

trash 目录删除由 `_do_sweep()` 完成。目录名中的时间戳用于判断是否超过 `trash_file_expire_time_sec`。当磁盘水位较高时，`start_trash_sweep()` 会用 0 作为 trash expire，使 trash 中可识别的目录立即进入删除判断。

## 元数据兜底清理

除了 `Tablet::gc_binlogs()` 正常删除当前 tablet 的 binlog meta，BE 垃圾清扫还会调用 `StorageEngine::_clean_unused_binlog_metas()`。

该函数遍历所有 `binlog_meta_`，解析 `BinlogMetaEntryPB`。如果 meta 指向的 tablet 已经不存在，就记录对应 suffix，并调用 `RowsetMetaManager::remove_binlog()` 删除 `binlog_meta_` 和 `binlog_data_`。

这个逻辑只能清理 BE 本地 OlapMeta 中的残留 binlog meta；它不负责扫描和删除孤立 `_binlog` 文件。孤立文件的回收仍依赖 tablet 目录进入 trash 或路径 GC。

## 场景总结

| 场景 | `_binlog` 文件如何产生或迁移 | 回收触发 | 主要风险点 |
| --- | --- | --- | --- |
| 正常写入并发布 | `BetaRowset::add_to_binlog()` 在 tablet 内 hard link segment/index 到 `_binlog` | FE binlog GC 下发 `GC_BINLOG`，BE `Tablet::gc_binlogs()` unlink 文件并删 meta | 手动删会使未过期 binlog 读取失败 |
| 跨 BE clone/均衡新增副本 | 源 snapshot hard link `.binlog`，目标下载后 hard link 到目标 `_binlog` 并导入 meta | 新副本按正常 binlog GC；旧副本按正常 GC 或后续 tablet drop | 源副本不会因 clone 成功立即释放 `_binlog` |
| 本机 storage migration | 复制 `_binlog` 文件和 `rowset_binlog_metas.pb` 到目标路径 | 旧路径通过 path GC/tablet trash 回收；新路径按正常 GC | 迁移中断可能留下待 path GC 的旧目录 |
| `DROP TABLE FORCE` | 不新建 `_binlog`，现有 `_binlog` 属于被 drop tablet | tablet drop 后整个 tablet 目录进入 trash 或直接删除 | 等的是 tablet/trash 清理，不是 binlog TTL 变 0 |
| tablet 已从内存移除 | `StorageEngine::gc_binlogs()` 找不到 tablet 会跳过 | trash/path GC 和 `_clean_unused_binlog_metas()` 兜底 | 文件和 meta 清理可能不同步完成 |

## 运维判断

如果表或 tablet 仍然存在，不建议手动删除 `_binlog`。即使 `_binlog` 是硬链接，Doris 元数据仍可能引用它，CCR 或 binlog 下载会失败。

如果表已经 `DROP TABLE ... FORCE`，且确认不需要恢复或 CCR 继续消费这张表，对应 `_binlog` 已没有业务价值。此时更稳妥的做法是：

1. 等待 BE 日志出现 `begin drop tablet`、`set tablet to shutdown state`、`successfully move tablet to trash`。
2. 如果目录已经在 `<root_path>/trash/`，优先删除确认归属的 trash 子目录。
3. 如果仍在正常 tablet 路径，只删除已经确认属于被 drop tablet 的 `_binlog` 或整个 tablet 目录，不要按全局 `find ... -name _binlog` 批量删除。
4. 删除文件后，允许后续 `_clean_unused_binlog_metas()` 或正常 GC 清理残留 meta。

## 关键源码位置

- `_binlog` 文件路径：`be/src/storage/tablet/tablet.cpp`，`Tablet::get_segment_filepath()`
- 写入时 hard link：`be/src/storage/rowset/beta_rowset.cpp`，`BetaRowset::add_to_binlog()`
- snapshot 携带 binlog：`be/src/storage/snapshot/snapshot_manager.cpp`
- clone 导入 binlog：`be/src/storage/task/engine_clone_task.cpp`
- storage migration 复制 binlog：`be/src/storage/task/engine_storage_migration_task.cpp`
- FE binlog GC：`fe/fe-core/src/main/java/org/apache/doris/binlog/BinlogGcer.java`
- BE binlog GC task：`be/src/agent/task_worker_pool.cpp`，`gc_binlog_callback()`
- BE binlog 文件和 meta 删除：`be/src/storage/tablet/tablet.cpp`，`Tablet::gc_binlogs()`
- tablet drop/trash：`be/src/storage/tablet/tablet_manager.cpp`，`TabletManager::_drop_tablet()`、`_move_tablet_to_trash()`
- trash 删除策略：`be/src/storage/data_dir.cpp`，`DataDir::move_to_trash()`；`be/src/storage/storage_engine.cpp`，`StorageEngine::start_trash_sweep()`、`_do_sweep()`
- 无效 binlog meta 兜底清理：`be/src/storage/storage_engine.cpp`，`StorageEngine::_clean_unused_binlog_metas()`
