# Backup/Restore 大规模元数据内存优化开发计划

## 文档状态

- 状态：执行中（核心实现、兼容矩阵、四阶段中断重启、checkpoint/image 跨配置恢复、
  Follower/Observer replay、运行中角色切换、特殊元数据矩阵和 200 万压力测试已完成；
  20 万五阶段、大 RestoreJob、journal size counting 对照、最新 master rebase 和拆分 PR
  定向自验证已完成；上游提交与完整 CI 未完成）
- 目标分支：Apache Doris `master`
- 相关实现：
  - HYDCP/hy-doris#49：RestoreJob Guava Table/Multimap 流式 JSON 序列化
  - HYDCP/hy-doris#63：BackupMeta/Table 流式 JSON、Replica 剥离及 journal size 计数优化
  - HYDCP/hy-doris#66：本地 snapshot 目录生命周期、输入侧流式压缩及 RPC 大小保护
  - apache/doris#65321：从 BackupMeta 剥离无用 Replica
- 本地实现分支（均未合入 `master`）：
  - PR 1：`codex/backup-strip-replica-info` @ `3ec7ff3f374`
  - PR 2：`codex/backup-journal-size-counting` @ `f71760e5d7a`
  - PR 3：`codex/streaming-gson-foundation` @ `bdacd53ee31`
  - PR 4：`codex/restore-job-streaming` @ `e599cf60f1b`
  - PR 5：`codex/backup-meta-streaming` @ `a3a5578602e`
  - 验证分支：`codex/backup-memory-benchmark` @ `dd2c4b437a4`

PR 1—5 已完成本地实现、分支拆分和最新 `master` rebase，但不代表已提交上游、通过完整 CI
或可以合入 `master`。验证分支中的 wrapper 复用、Gson 可选能力探测缓存、OOM 传播修复和
受控 spill 已经按职责回迁 PR 3/PR 5，并在 Linux 官方 thirdparty 环境完成 rebase 后定向复测；
单 FE/单 BE 的
真实 BACKUP/RESTORE E2E、RestoreJob 在四个运行阶段的 FE 重启续跑，以及 streaming writer
生成 checkpoint 后由 legacy reader 加载、Leader streaming writer → Follower/Observer legacy
reader 的实时 replay，以及 DOWNLOADING 阶段主从切换均已完成；colocate、动态分区、Replica
保留开关、Cloud 元数据子类型、20 万 tablet 快速基准、200 万 tablet 七阶段压力矩阵和
60 万 snapshot mapping 的 RestoreJob reader 三次独立 fork 对照也已完成。
真实 Cloud BACKUP/RESTORE 需要 Cloud 集群与 MetaService，当前经典集群不具备该条件。

本文用于跟踪上述优化向 Apache Doris `master` 的移植、拆分、验证和发布。计划中的每个 PR 必须能够独立审查、独立验证，并在出现问题时独立回滚。

## 当前进度快照（2026-07-20）

### 实现与验证

| 维度 | 当前结论 | 证据 |
| --- | --- | --- |
| 核心实现 | PR 1—5 已完成拆分、最新 master rebase 和个人远端正式分支更新 | 五个职责分支均有独立提交历史、备份 ref 和回退边界 |
| Streaming 基础设施 | 已完成 | PR 3 兼容矩阵 20/20，通过 wrapper 复用和 Gson capability probe 缓存消除逐对象异常分配 |
| RestoreJob | 状态矩阵、四阶段 restart、Follower/Observer replay 和 failover 已完成 | DOWNLOADING 停原 Leader 后 legacy Follower 接管至 FINISHED；旧 Leader 成功回归 |
| BackupMeta/Table | 三阶段实现完成 | streaming deep copy/持久化、受控 spill、兼容性与异常清理；PR 5 矩阵 16/16、spill helper 11/11 |
| 容量与性能 | 20 万重复矩阵、200 万压力矩阵、大 RestoreJob 和 journal size 对照完成 | 20 万五阶段 peak delta 降低 5.78%—88.19%；Restore reader 降低 80.17%；512 MiB size check 不再保留完整 buffer |
| 真实功能 | 单 FE/单 BE E2E、四阶段中断重启、checkpoint 跨配置恢复和特殊元数据矩阵完成 | Restore 均 FINISHED，数据校验不变；colocate/动态分区及 Replica true/false 通过，Cloud 子类型完成定向 UT |
| 上游就绪度 | 尚未满足 | rebase 后定向 FE UT 均通过；仍缺 PR 模板整理、上游提交和完整 CI |

### 相关 PR 状态

以下是 2026-07-20 的状态快照，后续以 GitHub 实时状态为准：

- HYDCP/hy-doris#49 已合并，是 RestoreJob streaming 的 3.1 参考实现。
- HYDCP/hy-doris#63 已批准但仍为 Open/Blocked，是 Backup 组合方案的参考实现，不直接替代
  面向 `master` 的 PR 1—5 拆分。
- HYDCP/hy-doris#66 已合并，解决本地 snapshot 目录清理、并发 pin、输入侧流式压缩和 Thrift
  尺寸保护。它与 RestoreJob Gson streaming 共享“输入侧流式处理”原则，但没有复用 TypeAdapter
  或 JSON spill；压缩结果仍以完整 `byte[]` 驻留 FE heap，因此不能视为 RestoreJob JVM 峰值问题
  的完整解法。
- apache/doris#65321 仍为 Draft/Open，需要 review。Compile、FE UT、BE UT 和 P0 等主要检查已通过，
  External Regression 与 `cloud_p0` 仍需处理或证明与改动无关。
- PR 2—5 尚未向 Apache Doris 创建上游 PR，当前只存在个人远端实现分支。

## 未来机会与优先级

### P0：先关闭持久化正确性风险

1. [已完成] RestoreJob 在 `CREATING`、`SNAPSHOTING`、`DOWNLOADING`、`COMMITTING` 阶段
   分别执行 FE 重启，任务均继续运行至正确终态。
2. [已完成] 覆盖 `EditLog.loadJournal`、`BackupHandler.replayAddJob`、image load 和 checkpoint
   线程，确认 legacy/streaming 两种 payload 都能真实 replay。
3. [已完成] Master streaming 写入、Follower/Observer legacy replay、节点间配置不一致，以及
   DOWNLOADING 阶段角色切换后的运行时回退。
4. [已完成] 补齐 colocate、动态分区、CloudTablet/CloudReplica 持久化子类型、
   `reserve_replica=true/false`、`backup_meta_reserve_replica_info=true/false` 和 20 万 tablet
   BackupHandler checkpoint image。真实 CloudRestoreJob E2E 留待具备 MetaService 的 Cloud 环境执行。

P0 正确性门禁、200 万 tablet 容量门禁、计划内性能对照、最新 master rebase 和拆分 PR
定向自验证已完成，但在完整 CI 通过前，
仍不默认开启 streaming，也不把 Cloud 子类型单元矩阵等同于真实 CloudRestoreJob E2E。

### P1：建立可用于上游评审的性能证据

1. [已完成] 执行 200 万 tablets × 3 replicas 七阶段压力基准，16 GiB heap 下均无 OOM，
   fixture 与 spill 文件均无残留。
2. [已完成] 对 spill 修正后的 20 万 tablet before/after 五阶段矩阵各运行三次独立 fork，报告
   sampled peak、耗时、GC 和 retained heap 中位数；`journal_replay` 另有 JFR allocation 归因。
3. [已完成] 增加大 RestoreJob benchmark，覆盖 snapshot mapping 和 commit mapping 主导场景；
   reader 的 streaming/legacy 对照各运行三次独立 fork，并报告中位数。
4. [已完成] 补充 journal size counting 的旧缓冲/计数流三次独立 fork，对照 heap、retained、
   GC 和耗时，并校验精确序列化字节数。

### P1：延伸 #66 的 snapshot RPC 内存治理

#66 已避免压缩前把完整原始文件加载进堆，但 `ByteArrayOutputStream` 扩容、`toByteArray()` 复制和
Thrift `byte[]` 仍会使完整压缩结果驻留堆中。后续机会应作为独立设计处理：

1. 根据 FE heap 和 RPC 并发度设置保守的响应上限，而不是只依赖约 2 GiB 的协议边界。
2. 评估 file-backed/spillable 压缩输出，先消除 `ByteArrayOutputStream` 扩容和复制峰值。
3. 如果要从根本上移除最终完整 `byte[]`，需要设计分块或流式 snapshot RPC；这属于协议改造，
   不应混入 PR 3—5 的 Gson persistence 系列。
4. bounded output、计数和 spill 可以在语义稳定后提炼共享基础设施，但不能为了复用而合并错误
   语义：JSON 持久化要求格式兼容，snapshot RPC 还受到 Thrift 消息边界约束。

### P2：按可审查顺序进入 Apache Doris master

1. 先整理 apache/doris#65321：删除 Cloud 模式下必然失败的 BACKUP 用例、处理 External
   Regression、补全 PR 模板并 rebase 最新 `master`。
2. PR 2 可独立提交；随后严格按 PR 3 → PR 4 → PR 5 提交，避免同时修改 Gson 和
   `AbstractJob` 造成审查/回滚困难。
3. 每个 PR 独立执行定向测试、完整 FE 门禁和相关 regression，不把组合验证结果直接当成拆分 PR
   的 CI 结果。
4. P0 replay、200 万压力和配置回退已经通过；完成稳定 before/after、rebase 和完整 CI 后，
   才提交 PR 6 或讨论默认开启。

## 背景

大规模 BACKUP/RESTORE 会同时放大持久化对象大小和 FE 序列化峰值内存。已确认的主要来源包括：

1. BackupMeta 保存完整的 `Replica` 对象，但 Restore 会根据 `ReplicaAllocation` 和目标 Backend 重建 Replica，不读取备份中的 Replica 对象。
2. Gson 多态适配器通过 `JsonElement` DOM 完成分发，序列化大型 Table、Partition、Tablet、Replica 对象图时会创建大量临时对象。
3. RestoreJob 中的 Guava `Table` 和 `Multimap` 字段通过树模式序列化，`snapshotInfos`、`restoredVersionInfo` 等大字段会显著增加堆内存峰值。
4. 部分写入路径先生成完整 JSON `String`，再转换成 UTF-8 字节并写入 `DataOutput`，同时保留字符数组、String 和字节数组。
5. `BDBJEJournal.exceedMaxJournalSize()` 为计算 journal 大小而完整序列化并缓冲对象，实际写 journal 时又执行一次序列化。

历史基准中，20 万 tablets、每 tablet 3 replicas 的 BackupJob journal payload 在 Replica 剥离后约从 119 MB 降至 23 MB。完整方案需要同时降低最终 payload、长期持有内存和序列化过程中的瞬时内存。

## 目标

1. BackupMeta 默认不持久化 Restore 不使用的 Replica 对象。
2. RestoreJob 的大型 Guava 集合使用流式 JSON 适配器。
3. BackupMeta 和 Table 元数据的多态分发不再构造完整 JSON DOM。
4. 大型 JSON 可以直接以长度前缀 UTF-8 格式写入 `DataOutput`，不生成完整中间 String。
5. Table deep copy 使用流式 JSON 管道，降低 `OlapTable.selectiveCopy()` 的峰值内存。
6. journal size 检查只统计字节数，不保留完整序列化结果。
7. 保持现有 editlog、image、BackupMeta 和 BackupJobInfo 的磁盘及 wire 格式兼容。
8. 每项优化都有运行时回退路径和可复现的性能数据。

## 非目标

1. 不修改 BE snapshot 文件格式或上传协议。
2. 不改变 BACKUP/RESTORE 的状态机和任务调度语义。
3. 不在本计划中引入新的压缩格式或提升 meta version。
4. 不把内部序列化开关暴露为新的 BACKUP/RESTORE SQL property，除非 master reviewer 明确要求。
5. 不在同一个 PR 中同时提交所有优化。

## 总体拆分

| 顺序 | PR | 主要内容 | 依赖 | 当前状态 |
| --- | --- | --- | --- | --- |
| 1 | Replica 剥离 | 修复并完善 apache/doris#65321 | 无 | 已 rebase；定向 FE UT 16/16；完整 CI 未完成 |
| 2 | Journal size 计数 | 使用计数流替代完整缓冲 | 无 | 已 rebase；定向 FE UT 2/2、512 MiB 三次对照通过；上游未提交 |
| 3 | Streaming Gson 基础设施 | Guava 与多态 TypeAdapter 的流式实现 | 无 | 已 rebase；兼容矩阵 20/20、JFR 和五阶段三次对照完成；完整 CI 未完成 |
| 4 | RestoreJob 流式序列化 | 迁移 HYDCP/hy-doris#49 的 Restore 优化 | PR 3 | 已重建到完整 PR 3；定向 FE UT 7/7，重启、replay/failover 和大对象对照完成 |
| 5 | BackupMeta/Table 流式序列化 | 迁移 HYDCP/hy-doris#63 的 Backup 优化 | PR 1、3、4 | 已去重重建；定向 FE UT 27/27，真实 E2E、特殊元数据和 200 万压力通过 |
| 6 | 默认开启与最终验证 | 根据兼容性和压力测试结果开启默认配置 | PR 1—5 | 未开始 |

PR 1 和 PR 2 可以独立推进。PR 3 合入后再依次提交 PR 4 和 PR 5，避免多个 PR 同时修改 `GsonUtils`、`RuntimeTypeAdapterFactory` 和 `AbstractJob`。

## PR 1：从 BackupMeta 剥离 Replica

建议标题：

```text
[fix](backup) Strip replica info from backup meta
```

基于 Draft PR apache/doris#65321 继续完善。

### 实现范围

- 在 detached backup table copy 上清除 LocalTablet 和 CloudTablet 的 Replica。
- 保留 tablet 数量、顺序、ID、PartitionInfo 和 ReplicaAllocation。
- 保留 `backup_meta_reserve_replica_info` 作为旧行为回退开关。
- 将过宽的 `clearReplicas()` API 改为用途明确的 `clearReplicasForBackup()`，或采用等价的受限命名。
- 不修改 live catalog Tablet，也不修改 snapshot task 的 Replica 选择逻辑。

### 必须修复的问题

- [ ] 删除 `cloud_p0` 中必然失败的 BACKUP 用例。Cloud 模式下 `BackupCommand` 明确拒绝 BACKUP。
- [ ] 将端到端用例放入普通 `backup_restore` suite。
- [ ] 更新 PR 中遗留的 `close #xxx`、`Related PR: #xxx`、checklist 和 Release note。
- [ ] rebase 最新 `master` 后重新执行完整 CI。

### 测试

- [ ] LocalTablet selective copy 默认剥离 Replica。
- [ ] CloudTablet 清理当前 `replica` 和 legacy `replicas` 字段。
- [ ] 原始 catalog table 的 Replica 数量不变。
- [ ] `backup_meta_reserve_replica_info=true` 保留 Replica。
- [ ] 非 Backup selective copy 保留 Replica。
- [ ] `BackupMeta.write/read` 在两种配置下均可 round-trip。
- [ ] 普通模式执行 BACKUP、DROP/TRUNCATE、RESTORE，并通过 `order_qt` 校验数据。
- [ ] 覆盖 `reserve_replica=true/false`。

### 合入门槛

- Restore 不依赖备份 Replica 的结论有单测和端到端测试证明。
- FE UT、Cloud UT、P0、backup_restore、cloud_p0 全部通过。
- 不存在未解释的 CI failure。
- PR 描述明确说明：该 PR 降低 copy 完成后的 retained heap 和持久化大小，但尚未消除 `DeepCopy.copy()` 本身的峰值内存。

## PR 2：Journal size 检查改用计数流

建议标题：

```text
[improvement](fe) Avoid buffering journal entries when checking size
```

### 实现范围

- 将 `BDBJEJournal.exceedMaxJournalSize()` 中的 `DataOutputBuffer` 替换为 `CountingDataOutputStream` 和 null sink。
- 保持实际 journal 写入路径不变。
- 序列化错误必须继续向调用方返回，不能吞掉异常或在错误后继续写入。

### 测试

- [x] 同一 `JournalEntity` 在旧缓冲实现和计数实现下得到完全相同的大小。
- [x] 覆盖小于、等于和大于 1 GB 限制的边界。
- [x] 覆盖 Writable 写入异常。
- [x] 实现不再创建 `DataOutputBuffer`，计数结果写入 null sink。
- [ ] 记录旧缓冲与计数流的实际 heap/allocation 对比。

### 合入门槛

- 不改变 journal 内容及 1 GB 限制语义。
- 对 BackupJob、RestoreJob 和普通小 journal 均无行为变化。

## PR 3：Streaming Gson 基础设施

建议标题：

```text
[improvement](fe) Add streaming Gson adapters for large metadata
```

### 实现范围

- 基于 master 当前 `GsonUtilsBase`、`GsonUtilsCatalog` 分层实现 Guava `Table`/`Multimap` 流式 TypeAdapter。
- 扩展 `RuntimeTypeAdapterFactory`，支持通过 `JsonReader`/`JsonWriter` 进行多态流式分发。
- 支持 type 字段不在 JSON 对象首字段的输入。
- 支持具有 default subtype 的 legacy payload；缺少 type 字段时，将已经消费的首字段回放给默认 delegate。
- 保留 tree mode，作为测试基线和运行时回退路径。
- 本 PR 不修改 BackupJob/RestoreJob 的业务状态机或持久化入口。

### 兼容性要求

对于同一对象，tree mode 和 streaming mode 必须产生语义相同且可双向读取的 JSON。对于声明为 byte-compatible 的路径，必须直接比较原始序列化字节，而不只比较反序列化后的对象。

### 测试矩阵

- [x] tree writer → tree reader。
- [x] tree writer → streaming reader。
- [x] streaming writer → tree reader。
- [x] streaming writer → streaming reader。
- [x] null、empty、单元素和大规模 Table/Multimap。
- [x] 嵌套泛型和不同 key/value 类型。
- [x] type 字段在首位、中间和末尾。
- [x] legacy payload 缺少 type 字段。
- [x] 未知 type、重复 type、截断 JSON 和错误字段类型明确失败。
- [x] `GsonSerializationTest` 覆盖 master 注册的所有相关适配器。

### 合入门槛

- 基础设施测试不依赖 Backup/Restore 集群。
- 不新增用户可见 SQL 属性。
- 不改变现有 JSON schema 和字段名。
- FE 全量单测和 checkstyle 通过。

## PR 4：RestoreJob 流式 JSON 序列化

建议标题：

```text
[improvement](restore) Stream RestoreJob JSON serialization
```

### 实现范围

- 对 RestoreJob 中的大型 Guava `Table`/`Multimap` 字段启用 PR 3 的适配器。
- 重点覆盖 `snapshotInfos`、`restoredVersionInfo` 及同类字段。
- 保留内部 FE config 以选择 streaming 或 legacy tree mode。
- reader 必须能够读取两种 mode 生成的内容。
- 配置只控制实现选择，不改变持久化协议。

### Replay 与生命周期测试

- [x] PENDING、CREATING、SNAPSHOTING 状态四象限写入/读取。
- [x] DOWNLOAD、DOWNLOADING、COMMIT、COMMITTING 大映射状态四象限写入/读取。
- [x] FINISHED、CANCELLED 终态四象限写入/读取。
- [x] `JournalEntity.readFields` 对 legacy/streaming 两种输出和两种 reader 配置兼容。
- [x] `EditLog.loadJournal`/`BackupHandler.replayAddJob` 自动化 replay；legacy/streaming writer ×
  reader 四组 PENDING → COMMIT 路径均通过。
- [x] Master 两个 streaming config 开启写入后，两个 config 均关闭的 Follower 实时 replay。
- [x] Observer legacy replay、streaming checkpoint image 重启加载和三 electable FE 角色切换。
- [x] 配置开启写入后关闭配置读取。
- [x] 配置关闭写入后开启配置读取。
- [x] FE 在 CREATING、SNAPSHOTING、DOWNLOADING、COMMITTING 阶段重启后，RestoreJob 均从
  对应的 PENDING、DOWNLOAD 或 COMMIT journal 继续运行并到达 FINISHED。

### 双向兼容矩阵

| 写入方 | 读取方 | 要求 |
| --- | --- | --- |
| 旧版本 tree writer | 新版本 streaming reader | 必须支持 |
| 新版本 streaming writer | 当前 master tree reader | 必须支持 |
| streaming config 开启 | streaming config 关闭 | 必须支持 |
| streaming config 关闭 | streaming config 开启 | 必须支持 |

### 合入门槛

- 不提升 meta version。
- 不改变 RestoreJob JSON 字段和状态机。
- RestoreJob、CloudRestoreJob、BackupHandler 相关单测全部通过。
- 至少提供一组大 RestoreJob 的内存与耗时对比。

## PR 5：BackupMeta/Table 流式序列化

建议标题：

```text
[improvement](backup) Stream backup table metadata serialization
```

### 实现范围

- 为 Table、Partition、Tablet、Replica 的多态 factory 启用 streaming dispatch。
- 增加直接面向 `DataInput`/`DataOutput` 的长度前缀 UTF-8 JSON helper。
- 将 `Text.writeString(GSON.toJson(object))` 替换为不构造完整 String 的流式写入，同时保持已有长度前缀格式。
- 将 `Table`、`OlapTable`、`BackupMeta`、`BackupJobInfo` 和相关读写路径迁移到 streaming helper。
- 用流式 JSON deep copy 替换 `OlapTable.selectiveCopy()` 中的完整 DOM/String deep copy。
- 复用 PR 1 的 Replica 剥离，不再次引入同名配置或重复实现。
- 复用 PR 4 的 config-driven adapter dispatch，避免并行存在多套 mode scope。

### PR 内部提交顺序

PR 5 保持为一个上游 PR，但拆成四个可独立 review/revert 的 commit：

1. length-prefixed UTF-8 JSON helper 与隔离单测。
2. Table/Partition/Tablet/Replica 多态 streaming dispatch。
3. BackupMeta、Table、OlapTable selective copy、BackupJobInfo 调用点迁移。
4. AbstractJob、BackupJob、RestoreJob 三类 image/editlog 读写入口迁移。

长度前缀 writer 只序列化一次到 spillable UTF-8 buffer：默认最多在堆中保留 8 MiB，超过阈值
后写入临时文件；长度确定后再写 4-byte length 并回放 payload。不能用“先计数、再序列化”的
双遍方案，因为 job 在两次遍历之间可能变化。reader 使用只覆盖声明 length 的 bounded
`DataInput` stream，不为整个 payload 分配连续 `byte[]`，并明确处理负 length、截断和尾随内容。
关闭配置时，各生产调用点必须走原 `Text`/String/`DeepCopy` legacy 路径。

原始 64 KiB segmented buffer 只限制单块数组大小，仍在堆中保留整份 JSON，不能称为真正的
bounded-memory writer。20 万 tablet 对照测试暴露该设计缺陷后，验证分支已改为 spillable buffer；
该修正必须通过远端 FE UT、容量复测和临时文件清理验证后才能回迁 PR 5。

### 持久化测试

- [x] 定向 FE UT（spill 修正前）：31/31 通过。
- [x] spillable helper 单测：11/11 通过，覆盖强制 spill、原始字节一致、round-trip、payload
  上限、close 后失败、序列化/目标写入失败清理、不可用临时目录及 suppressed cleanup 异常。
- [x] spill 修正后的 FE 定向测试、Checkstyle 与 20 万 tablet 五阶段复测。

- [x] Table/OlapTable legacy/streaming writer × reader 四象限及原始字节一致。
- [x] BackupMeta 文件 legacy/streaming writer × reader 四象限及原始字节一致。
- [x] BackupJobInfo 文件四象限、原始字节及 UTF-8 行为不变。
- [x] BackupJob editlog write/read/replay，覆盖 `JournalEntity.readFields`、`EditLog.loadJournal` 和
  `BackupHandler.replayAddJob`。
- [x] streaming writer 生成真实 checkpoint image 后，关闭两个 streaming config 并重启，legacy
  reader 成功加载 image，RestoreJob 终态和 10 万行表数据完整。
- [x] 默认 subtype、兼容 label 和非首位 type 的 legacy Table/Partition/Tablet/Replica JSON 可读取。
- [x] Replica 剥离后的空 LocalTablet/CloudTablet 可被两种 reader 配置读取。
- [x] 部分分区、rollup、colocate、动态分区和 Cloud Tablet 元数据 round-trip；前四项已完成真实
  BACKUP/RESTORE，CloudTablet/CloudReplica 由 legacy/streaming 单元兼容矩阵覆盖，真实 Cloud
  BACKUP/RESTORE 仍需 MetaService 环境。

### 兼容矩阵验证记录（2026-07-17）

- PR 3 `46d6fa0aae4`：`RuntimeTypeAdapterFactoryStreamingTest` 与
  `GsonUtilsBaseStreamingCollectionTest` 共 20/20，通过四象限、2000 项复杂 Table/Multimap、
  type 首/中/末、default subtype 和错误输入矩阵；Checkstyle 0。
- PR 4 `7ad57e168b0`：Linux 官方 thirdparty、Maven 3.9.14、JDK 17 下
  `BackupRestoreJobStreamingJsonConfigTest` 5/5，25-module reactor `BUILD SUCCESS`；覆盖全部
  9 个 RestoreJob 状态、1024-cell 活跃状态、压缩 CloudRestoreJob 和 `JournalEntity.readFields`。
- PR 4/PR 5 验证分支于 2026-07-20 使用 Maven 3.9.9、JDK 17 补充
  `EditLog.loadJournal`/`BackupHandler.replayAddJob` 和 `BackupHandler.write/readFields` image
  四象限测试；目标用例 7/7、25-module reactor `BUILD SUCCESS`。
- PR 5 `02a5d1017a7`：同一 Linux 环境运行 DeepCopy、TableMeta、Restore/Backup job、
  BackupMeta、BackupJobInfo 五类测试共 16/16，25-module reactor `BUILD SUCCESS`；另有
  `LengthPrefixedJsonStreamTest` 11/11。这里的 replay 证据只到 `JournalEntity.readFields`，不包含
  `EditLog.loadJournal`、`BackupHandler`、Follower 或重启路径。

### 合入门槛

- 声明 byte-compatible 的所有路径都有原始字节比较测试。
- 20 万 tablets × 3 replicas 在 `-Xmx2g` 基准下完成 selective copy、BackupMeta write 和 BackupJob editlog write。
- streaming config 关闭时可以回退 legacy tree path。
- BACKUP、RESTORE、FE restart 和 journal replay 端到端测试通过。

## PR 6：默认开启和最终验证

如果 PR 3—5 为降低首次合入风险而默认关闭 streaming，则使用一个只修改默认配置的小 PR 开启功能。开启前必须满足：

- [ ] 完整 CI 连续通过。
- [x] 新旧 writer/reader 兼容矩阵全部通过。
- [x] 20 万 tablet 快速基准稳定通过。
- [x] 200 万 tablet 压力基准无 OOM。
- [x] Master/Follower/Observer 重启、replay 和运行中角色切换验证。
- [x] streaming writer → legacy reader 的单 FE restart/image load、Follower/Observer replay 和
  角色切换演练成功。
- [ ] PR 描述包含优化前后数据和已知边界。

## 性能基准方案

### 数据规模

1. 快速基准：20 万 tablets × 3 replicas，FE `-Xmx2g`。
2. 压力基准：200 万 tablets × 3 replicas，使用与生产问题相近的 FE heap 配置。

测试对象需要固定表结构、分区数、index 数、tablet 顺序和 replica allocation，以保证不同实现之间可重复比较。

### 记录指标

| 阶段 | 指标 |
| --- | --- |
| `OlapTable.selectiveCopy()` | 峰值 heap、总分配量、耗时 |
| BackupMeta write/read | 峰值 heap、耗时、文件大小 |
| BackupJob editlog write/replay | 峰值 heap、耗时、journal payload 大小 |
| RestoreJob write/read/replay | 峰值 heap、耗时、payload 大小 |
| BackupHandler image write/replay | 峰值 heap、耗时、image payload 大小 |
| journal size check | 峰值 heap、序列化次数、耗时 |

推荐使用固定 JVM 参数和 JFR/async-profiler allocation 结果，并在每次测试前完成同样的 warm-up。PR 中必须同时提供 before/after，而不能只给优化后的数据。

### 当前执行结果（2026-07-16）

- `ba63859065a` 下，20 万 tablets × 3 replicas、`full_streaming`、每阶段独立 JVM、
  `-Xms2g/-Xmx2g` 的 `selective_copy`、`backup_meta_write`、`backup_meta_read`、
  `journal_write`、`journal_replay` 均完成。该结果仅证明容量快速基准可运行，不代表优化前后收益。
- `legacy` 和 `strip_replicas` 的 `selective_copy` 均在 2 GiB 下 OOM，证明 Replica 在完整
  `DeepCopy` 后再清除不能解决 copy 瞬时峰值；streaming deep copy 才越过该阶段容量门槛。
- 初始对照中，整份 segmented JSON 仍驻留堆内，部分 streaming writer 的 sampled heap peak
  高于 legacy。该结果阻止 PR 5 按原设计进入 `master`，并促成 `7ea1088deaa` 的 spillable
  buffer 修正。
- `DeepCopy.copy()` 会吞掉由反射包装的 `OutOfMemoryError` 并返回 `null`；验证分支
  `6ffd6ff6b89` 已改为原样传播 `Error`。远端 `DeepCopyTest` 2/2 通过，20 万 legacy case
  能输出结构化 `status=oom` 后以 OOME 失败。
- 当前远端机器同时运行其他 FE/BE 进程，单次 10 ms heap sampler 和耗时数据会受 GC/调度影响。
  在获得隔离资源、至少三次 fork 中位数和 JFR allocation 数据前，不使用这些数字声明性能比例。
- 计划内性能证据、最新 master rebase 和拆分 PR 定向自验证已完成；后续重点转为 PR 模板、
  上游提交和完整 CI。

正式 Maven heap 参数为 `-Dfe.ut.max.heap=2g`，固定初始堆可额外使用
`-Dfe.ut.extra.jvm.args=-Xms2g`；不能再使用 `-DargLine` 覆盖 fe-core 的默认 heap。

### Wrapper 复用、capability probe 缓存与受控 spill 复测（2026-07-20）

验证分支在 `ef668e62d5b`（受控 spill 已存在，但每个对象新建 reader/writer wrapper）和
`8b830a04599`（wrapper pool + capability probe cache + Doris 临时目录 spill）之间执行同口径
对照。数据规模为 20 万 tablets × 3 replicas，`full_streaming`，每个阶段使用独立 JVM 和
`-Xms2g/-Xmx2g`。五个阶段、两个版本各运行 3 次，共 30 个 benchmark JVM；下表均为中位数。

| 阶段 | 优化前 peak delta | 优化后 peak delta | peak 变化 | 优化前 elapsed | 优化后 elapsed | elapsed 变化 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `selective_copy` | 1,366,549,496 | 1,287,525,504 | -5.78% | 62,551 ms | 15,157 ms | -75.77% |
| `backup_meta_write` | 1,316,712,808 | 841,793,024 | -36.07% | 9,367 ms | 3,239 ms | -65.42% |
| `backup_meta_read` | 1,287,651,328 | 152,043,520 | -88.19% | 6,484 ms | 639 ms | -90.14% |
| `journal_write` | 1,315,670,912 | 868,023,144 | -34.02% | 10,109 ms | 3,266 ms | -67.69% |
| `journal_replay` | 1,286,602,752 | 153,092,096 | -88.10% | 7,039 ms | 706 ms | -89.97% |

| 阶段 | 优化前 retained delta | 优化后 retained delta | 优化前 GC count/time | 优化后 GC count/time |
| --- | ---: | ---: | ---: | ---: |
| `selective_copy` | 54,259,976 | 54,336,776 | 16 / 344 ms | 7 / 116 ms |
| `backup_meta_write` | 13,251,136 | 13,220,008 | 3 / 16 ms | 2 / 14 ms |
| `backup_meta_read` | 41,208,024 | 41,069,936 | 1 / 25 ms | 0 / 0 ms |
| `journal_write` | 13,232,728 | 13,242,024 | 3 / 17 ms | 2 / 13 ms |
| `journal_replay` | 41,211,416 | 41,074,552 | 1 / 35 ms | 0 / 0 ms |

两版本使用的五阶段 benchmark 代码没有差异；对照只改变 streaming 基础设施实现。测试仍在共享
主机上顺序执行，elapsed 会受背景 CPU 调度影响，因此耗时比例作为实验室方向性证据，不作为
生产 SLA。reader 的峰值和 GC 结论同时得到下一节独立 JFR allocation 归因支持；10 ms sampled
peak 仍不是精确总分配量。

- 五个阶段均在 2 GiB heap 下通过；BackupMeta 与 journal payload 分别保持 19,802,039 和
  19,802,331 bytes，说明 wrapper 复用、capability probe 缓存与 spill 路径没有改变持久化字节规模。
- 优化前后 retained delta 基本不变，说明最终 Table/BackupJob 对象图没有变化；主要收益来自减少
  瞬时 wrapper、异常栈和中间对象。两个 reader 阶段测量区间 GC 均从 1 次降为 0。
- 大集合单测中连续处理 2,000 个多态对象，每个方向只构造 1 个 wrapper；另有嵌套深度上限
  64、异常恢复、释放强引用和 8 线程隔离测试。
- spill 文件位于 `Config.tmp_dir/backup_restore_json_spill`，使用单次 UUID `CREATE_NEW` 和
  `DELETE_ON_CLOSE` channel。Linux OpenJDK 17 探针确认打开、回放、关闭期间目录均无可见残留；
  本次 30 个 200k JVM 运行结束后 Java tmp 和 Doris spill 文件计数均为 0。
- 合并后的定向测试在目标机通过：`fe-common` 25/25、`fe-core` 16/16，完整 25 模块 reactor
  `BUILD SUCCESS`，Checkstyle 0 violations；补充临时目录失败测试后 spill suite 为 11/11。
- 本机 `run-fe-ut.sh` 因该 worktree 缺少 `thirdparty/installed/bin/protoc` 未能启动；同一测试已在
  用户提供的 Linux 目标机用预编译 thirdparty 完成。该环境问题不计为代码失败。
- 单独加入 wrapper pool 时，`journal_replay` 没有获得峰值收益。随后 JFR 定位到每次 wrapper
  acquire 仍反射探测 Gson 可选设置并创建 `NoSuchMethodException`；该问题已由
  `8b830a04599` 修复，完整结果见下一节。

### Journal replay JFR 根因与修复（2026-07-17）

为严格隔离变量，JFR 对比使用相同的 200k×3、2 GiB、`journal_replay` case：

| 版本 | 变更 | peak delta | elapsed | 测量区间 GC | JFR sampled allocation |
| --- | --- | ---: | ---: | ---: | ---: |
| `ef668e62d5b` | pool 前 | 1,286,602,752 | 6,015 ms | 1 | 1.783 GiB |
| `a582951621c` | 仅 wrapper pool | 1,288,699,904 | 6,349 ms | 1 | 1.335 GiB |
| `8b830a04599` | pool + capability probe cache | 152,043,520 | 745 ms | 0 | 0.140 GiB |

JFR 证明这是两个叠加的逐对象分配问题：

1. pool 前，`EnteredObjectJsonReader` 每次调用 `JsonReader` 构造器；其内部数组相关采样分配约
   1,013.4 MiB。加入 pool 后该调用栈消失。
2. pool 后，每次 acquire 仍通过 `Class.getMethod()` 探测 Gson 2.10 不存在的
   `getStrictness`/`getNestingLimit` 等方法。失败反射生成的
   `NoSuchMethodException → Throwable.fillInStackTrace` 占采样分配约 1,080.9 MiB（79.06%），
   抵消了 wrapper pool 的峰值收益。
3. `8b830a04599` 将四项可选能力在类初始化时各探测一次；不存在时缓存 no-op copier，存在时
   缓存 Method，实际调用异常仍转为 `AssertionError`。修复后的主要分配只剩字段名 String、
   `LocalTablet`、集合和 `MaterializedIndex.gsonPostProcess()` 所需 HashMap 节点。

修复前两个版本都把固定 1.2 GiB G1 Eden 填满并触发一次 young GC，因此 10 ms heap sampler
得到几乎相同峰值；修复后测量区间分配低于 Eden 水位，不再触发 GC。无 JFR 的三个独立 fork
进一步复现：peak delta 为 152,043,520 / 153,092,096 / 152,043,520 bytes，elapsed 为
696 / 655 / 733 ms，三次 GC delta 均为 0，临时文件残留为 0。

定向单测验证 2,000 个对象写入/读取前后 capability probe 总数始终为 4；
`RuntimeTypeAdapterFactoryStreamingTest` 11/11、Checkstyle 0 violations。

### Journal size counting 对照（2026-07-20）

验证分支合入 PR 2 的实际 `BDBJEJournal.countJournalSize()`，并新增不由普通 Surefire 自动发现的
手动 benchmark。测试使用固定 512 MiB `Writable` payload，JournalEntity 另写入 2-byte opcode；
旧模式复现 `DataOutputBuffer(128)` + `DatabaseEntry`，新模式调用计数流。两种模式各运行 3 个
独立 JVM，固定 `-Xms2g/-Xmx2g`，以下为中位数：

| 模式 | peak delta | retained delta | elapsed | GC count/time | retained buffer capacity |
| --- | ---: | ---: | ---: | ---: | ---: |
| 完整缓冲 | 807,900,128 | 537,910,520 | 913 ms | 1 / 3 ms | 536,871,936 |
| 计数流 | 0 | 7,744 | 12 ms | 0 / 0 ms | 0 |
| 变化 | -100.00% sampled delta | -99.9986% | -98.69% | -100% | -100% |

六轮均精确得到 536,870,914 bytes，证明计数流没有改变 JournalEntity 字节数；`peak delta=0`
表示 10 ms sampler 没有观察到高于基线的 heap 增长，不表示绝对零分配。该 synthetic payload
刻意隔离 size-check buffer，不包含 BackupJob Gson 序列化成本，因此只能量化 PR 2 消除的完整
buffer，不能替代前述真实 BackupJob 五阶段矩阵。8 MiB 冒烟的 buffered/counting 两种模式也
通过；`BDBJEJournalSizeTest` 2/2、Checkstyle 0 violations，临时目录和 spill 残留均为 0。

### 最新 master rebase 与拆分 PR 自验证（2026-07-20）

五个职责分支从共同旧基点更新到 `upstream/master` @ `8460676f3fc`。PR 1、PR 2、PR 3 的
`range-diff` 逐提交等价；PR 4 只回放 RestoreJob 专属提交到完整 PR 3 之上；PR 5 使用
“新 PR 4 + 新 PR 1”组合基线重建，Git 自动去除重复的 DeepCopy、Restore 兼容修复和 replay
测试提交。重建后的 PR 5 在本方案涉及的生产文件上与已完成压力测试的验证分支一致，额外差异
仅来自最新 master 已合入的 Cloud timeout 和 Repository URI 修复。

为避免在验证通过前覆盖个人远端正式分支，先推送五个 `codex/rebase-test-*-20260720` 候选分支。
在 `192.168.9.44` 的独立 worktree 中使用仓库标准 `run-fe-ut.sh` 逐分支验证，结果如下：

| 分支 | 定向测试 | 结果 |
| --- | --- | ---: |
| PR 1 | `BackupMetaTest`、`OlapTableTest`、`CloudTabletTest`、`DeepCopyTest` | 16/16 |
| PR 2 | `BDBJEJournalSizeTest` | 2/2 |
| PR 3 | streaming collection 与 runtime type adapter 测试 | 20/20 |
| PR 4 | RestoreJob 序列化、Journal/EditLog 与 image replay 兼容矩阵 | 7/7 |
| PR 5 | spill helper、BackupMeta/JobInfo、Table metadata 与组合兼容矩阵 | 27/27 |

五轮 Maven reactor 均为 `BUILD SUCCESS`，Checkstyle 无 violation。验证通过后，使用带精确旧
hash 保护的 `--force-with-lease` 更新五个个人远端 `codex/*` 正式分支；作为 apache/doris#65321
head 的 `origin/backup-strip-replica-info` 保持不变。原测试集群在验证后 FE HTTP 和 BE health 均
返回 200；占用 2.9 GiB 的临时 worktree 已清理。此次只推送个人 remote，未创建或更新 Apache
upstream PR。

### 真实集群 MinIO E2E（2026-07-17）

在 `codex/backup-memory-benchmark` @ `8b830a04599` 上使用 no-AVX2 ASAN 构建，部署一个 FE、
一个 BE，并以独立 MinIO bucket 作为 S3 repository。通过仓库标准脚本
`./run-regression-test.sh --run -d backup_restore -s <suite>` 执行三轮真实 E2E，共 4 个 suite，
failed/fatal/skipped 均为 0：

- `test_backup_restore` 与匹配到的 `test_backup_connectivity_failed`：repository 创建成功，
  BACKUP 完整经过 `PENDING` 至 `FINISHED`，RESTORE 完整经过 `PENDING` 至 `FINISHED`，恢复后
  查询结果一致；临时表 backup 分支也通过。
- `test_backup_restore_partition`：备份 `p1`—`p6`，随后分两轮恢复 `p1`—`p3`、`p4`—`p6`，
  两轮均到达 `FINISHED`，排序查询结果正确。
- `test_backup_restore_mv`：恢复后的 `DESC ALL` 保留 rollup 索引，`EXPLAIN` 显示 CBO 成功选择
  恢复后的物化视图。

实验结束后 FE、BE 与 MinIO 均存活，日志扫描未发现 ASAN 或运行时 fatal。该结果证明普通表、
分区元数据和 rollup/MV 元数据在真实 S3-compatible 存储路径下可备份、恢复和查询；在当时它不替代
RestoreJob 中途重启、journal replay、checkpoint、多 FE 和 200 万 tablet 容量验证，这些结果见后续章节。

上述回归 suite 运行时，`enable_backup_restore_job_streaming_json` 和
`enable_table_meta_streaming_json` 均为 `false`，因此它们是 legacy 路径的真实功能基线，不能
单独作为 streaming persistence 的证据。

### Streaming journal 与 checkpoint 重启验证（2026-07-20）

在同一单 FE、单 BE、MinIO 环境中，将两个 streaming config 同时置为 `true`，备份包含 4 个
分区、32 个 tablet、10 万行的 `restart_table`。BACKUP 从 PENDING 运行至 FINISHED 后删除原表，
发起 RESTORE，并在 SHOW RESTORE 首次进入 CREATING 时停止 FE。重启时运行时 config 回落为
`false`，FE 日志显示从该 RestoreJob 的 PENDING journal 加载，随后依次完成 CREATING、
SNAPSHOTING、DOWNLOAD、DOWNLOADING、COMMIT、COMMITTING 和 FINISHED。恢复结果为：

- `COUNT(*) = 100000`
- `MIN(id) = 0`
- `MAX(id) = 99999`
- `SUM(id) = 4999950000`

随后再次将两个 config 置为 `true`，把 `edit_log_roll_num` 临时降为 1 并生成标记 journal，
leader checkpoint 线程成功生成并自检 `image.48201`。关闭两个 config、恢复 roll 阈值并二次
重启后，主线程明确从 `image.48201` 加载；SHOW RESTORE 仍为 FINISHED，以上四项数据校验保持
不变。该实验覆盖了 streaming writer → legacy reader 的真实 journal replay 和 checkpoint
image load。

完成 Follower 实验并恢复单 FE 拓扑后，复用同一 10 万行快照，分别在 SNAPSHOTING、
DOWNLOADING、COMMITTING 状态停止 FE。三次重启后的日志分别显示从 PENDING、DOWNLOAD、
COMMIT 持久化状态恢复；与先前 CREATING 中断一起，四个目标阶段均续跑至 FINISHED。每轮的
行数、ID 边界和 SUM 校验都与上述结果一致。由此单 FE 运行中重启矩阵已闭环；Follower、
Observer 和节点角色切换结果见后续两节。

### Follower replay 与节点间配置不一致（2026-07-20）

在 Docker bridge 独立 IP 上增加一个 Follower，与 Leader 使用相同服务端口和独立的 meta/log
目录。两节点 `Alive=true` 后，先完成一轮同配置 BACKUP/RESTORE replay，并由 Leader 生成
`image.48438`、成功推送至 1 个 Follower；Follower 重启日志明确从该 image 加载，集群恢复后
journal 差距保持在 1—2。

由于 `ADMIN SET FRONTEND CONFIG` 会传播动态配置，同配置 replay 不能作为配置不一致证据。
因此第二轮通过各 FE 本地配置接口固定并在测试前后核对：

- Leader：`enable_backup_restore_job_streaming_json=true`、
  `enable_table_meta_streaming_json=true`
- Follower：上述两个配置均为 `false`

随后使用新 label `codex_follower_mismatch_snapshot` 完成 BACKUP 和 RESTORE。Follower 的 replayer
日志记录了 BACKUP 的 PENDING、UPLOAD_SNAPSHOT、SAVE_META、UPLOAD_INFO、FINISHED，以及
RestoreJob 的 PENDING、DOWNLOAD、COMMIT、FINISHED；无反序列化或 replay 异常。最终两节点均
Alive，Follower journal 仅落后 2，恢复数据仍为 100000 行、ID 0—99999、总和 4999950000。

该实验完成 Master streaming writer → Follower legacy reader 的真实 edit log replay；Observer
和节点角色切换的独立验证见下一节。Follower restart/image load 已完成，但该次重启时两节点
config 相同，跨配置 image load 的证据来自上一节的单 FE 实验。

### Observer replay、checkpoint 与运行中角色切换（2026-07-20）

在 Docker bridge 独立 IP 上增加 Observer，确认 `Role=OBSERVER`、`Join=true`、`Alive=true`。
使用各节点本地配置接口固定 Leader 的两个 streaming config 为 `true`、Observer 均为 `false`，
通过新 label `codex_observer_mismatch_snapshot` 完成 BACKUP/RESTORE。Observer replayer 日志完整
记录 BACKUP 的 PENDING、UPLOAD_SNAPSHOT、SAVE_META、UPLOAD_INFO、FINISHED，以及 RestoreJob
的 PENDING、DOWNLOAD、COMMIT、FINISHED；测试前后配置未变化，journal 差距为 2，10 万行
数据校验保持不变。

随后临时把 Leader 的 `edit_log_roll_num` 降为 1，生成 `image.51002` 并成功推送至 Observer。
Observer 在两个 streaming config 仍为 `false` 时重启，主线程明确从 `image.51002` 加载；节点
恢复 Alive 后 journal 差 1，数据校验仍为 100000 行、ID 0—99999、总和 4999950000。

角色切换使用原 Leader、两个新增 Follower 和一个 Observer。原 Leader 两个 streaming config
为 `true`，两个候选 Follower 均为 `false`。复用上述快照发起 RESTORE，在 DOWNLOADING 状态
停止原 Leader；`172.17.0.8` 成为新 Leader，其日志显示从 PENDING、DOWNLOAD journal replay，
随后完成任务并得到相同数据结果。旧 Leader 重启后作为 Follower 回归，最终一个 Leader、两个
Follower、一个 Observer 均 Alive，journal 差距为 1。该实验完成 streaming writer → legacy
reader 在真实选主和运行中任务接管路径上的验证。

### 特殊元数据与大型 BackupHandler image（2026-07-20）

测试前发现回归入口连接的是旧 Follower，节点级 streaming config 没有落到新 Leader。后续通过
新 Leader `172.17.0.8` 的本地配置接口直接设置并复核
`enable_backup_restore_job_streaming_json=true` 和
`enable_table_meta_streaming_json=true`，避免把 legacy 基线误记为 streaming 结果。

- 动态分区分别在 `reserve_dynamic_partition_enable=true/false` 下完成真实
  BACKUP → MinIO → RESTORE。55 个年度分区和 20 行数据保持完整；`true` 保留动态分区属性，
  `false` 按预期关闭 `dynamic_partition.enable`。legacy 基线和 streaming 路径均通过。
- colocate 普通表和分区/MV 两组 suite 均通过。覆盖默认、`reserve_colocate=false/true`、
  colocate group 冲突和 bucket 数不一致；保留场景恢复 COLOCATE plan，12 个 tablet 健康，
  `ColocateMismatchNum=0`。
- `backup_meta_reserve_replica_info=false` 是 streaming E2E 的默认路径；随后在实际 Leader 置为
  `true`，再次完成动态分区 BACKUP/RESTORE，并在测试后恢复为 `false`。Restore SQL 的
  `reserve_replica=true` 也在上述路径中执行。
- `BackupMetaTest` 2/2 和 `TableMetaStreamingJsonTest` 4/4 通过，完整 25-module reactor
  `BUILD SUCCESS`。前者覆盖保留开关的 true/false、legacy/streaming reader/writer、
  LocalTablet/CloudTablet 和 LocalReplica/CloudReplica；后者覆盖 CloudPartition、CloudTablet、
  CloudReplica 等显式多态子类型。当前为经典集群、没有 MetaService，因此不声称完成真实
  CloudRestoreJob E2E。

验证分支新增 `handler_image_write` 和 `handler_image_replay` 两个独立 JVM benchmark stage，
通过 `BackupHandler.write/readFields` 处理包含 20 万 tablets × 3 replicas 的 BackupJob image。
两阶段均在 `-Xms2g/-Xmx2g` 下通过，reader 使用 streaming writer fixture 并关闭两个 streaming
配置，验证 legacy reader 可加载 image，且精确回放 20 万 tablets、Replica 按配置剥离：

| 阶段 | peak delta | retained delta | elapsed | payload |
| --- | ---: | ---: | ---: | ---: |
| `handler_image_write` | 471,067,456 | 13,198,600 | 4,332 ms | 19,802,346 |
| `handler_image_replay` | 1,133,510,656 | 41,107,168 | 1,812 ms | 19,802,346 |

这组结果证明大型 BackupHandler checkpoint image 在 2 GiB 容量门槛下可以写入并跨配置回放；
机器仍与多 FE/BE 共享资源，因此单次 sampled peak 和耗时只作为容量证据，不用于声明性能比例。

### 200 万 tablet 七阶段压力矩阵（2026-07-20）

在同一验证分支上执行 200 万 tablets × 3 replicas、`full_streaming`、每阶段独立 JVM 的压力矩阵。
为避免共享机器发生系统级 OOM，测试前仅停止本工作树的主 FE/BE，把 available memory 从约
9.8 GiB 提升至 25 GiB；其他目录的 Doris 服务未停止。所有阶段固定 `-Xms16g/-Xmx16g`，
均为 JUnit 1/1、25-module reactor `BUILD SUCCESS`，没有发生 JVM 或宿主机 OOM：

| 阶段 | peak delta | retained delta | elapsed | payload |
| --- | ---: | ---: | ---: | ---: |
| `selective_copy` | 10,290,792,288 | 423,274,312 | 144,417 ms | - |
| `backup_meta_write` | 3,546,221,152 | 13,210,504 | 16,558 ms | 198,002,041 |
| `backup_meta_read` | 1,509,949,440 | 410,023,936 | 4,742 ms | 198,002,041 |
| `journal_write` | 3,622,000,736 | 13,312,032 | 15,421 ms | 198,002,333 |
| `journal_replay` | 1,509,949,440 | 410,026,712 | 5,018 ms | 198,002,333 |
| `handler_image_write` | 3,596,424,736 | 13,317,552 | 14,562 ms | 198,002,348 |
| `handler_image_replay` | 8,959,033,344 | 411,016,648 | 18,455 ms | 198,002,348 |

`handler_image_replay` 使用 streaming writer fixture，并将 reader 的 Table/Job streaming 配置
同时关闭，精确回放 200 万 tablets 且 Replica 数为 0。七阶段结束后
`doris-backup-memory-benchmark-*` 临时目录和 `backup_restore_json_spill` 文件计数均为 0。
暂停的主 FE/BE 随后恢复，FE HTTP 返回 200，BE 进程正常。

这些数字证明 16 GiB 容量门槛可通过，不代表稳定的优化比例。机器仍与其他 FE/BE 共享资源，
且每阶段只运行一次；before/after 比例仍需在隔离资源上多次独立 fork 并结合 JFR/GC 数据报告。

### 大 RestoreJob write/replay（2026-07-20）

验证分支新增 `restore_job_write` 和 `restore_job_replay` 两个独立 JVM benchmark stage。
测试对象包含 20 万 tablets × 3 replicas 对应的 60 万条 `snapshotInfos`，以及 7.5 万条
`restoredVersionInfo`；固定 `-Xms2g/-Xmx2g`。fixture 始终由 streaming writer 生成，reader
分别打开和关闭 RestoreJob streaming 配置，因此两组读取相同的 79,663,052-byte payload。
每轮均校验 journal opcode、两类 mapping 的精确数量，以及首、中、末三条 SnapshotInfo 的全部字段。

streaming 和 legacy reader 各运行三次独立 JVM，以下为中位数：

| reader | peak delta | retained delta | elapsed | GC count | GC time |
| --- | ---: | ---: | ---: | ---: | ---: |
| streaming | 366,569,136 | 303,224,584 | 2,083 ms | 7 | 213 ms |
| legacy | 1,848,151,744 | 303,194,496 | 7,970 ms | 49 | 1,584 ms |
| 变化 | -80.17% | +0.01% | -73.86% | -85.71% | -86.55% |

同规模 streaming write 单轮也在 2 GiB 下通过：peak delta 1,366,615,208 bytes，retained delta
25,377,632 bytes，耗时 7,394 ms，payload 为 79,663,052 bytes。reader 对照表明优化主要消除了
legacy tree 反序列化的瞬时对象图和 GC 压力；最终 RestoreJob live graph 不变，因此两组 retained
heap 基本一致。测试仍在共享主机执行，三次独立 fork 中位数可作为当前 reader A/B 证据，但在
没有 JFR allocation 数据前不把 sampled peak 等同于精确总分配量。

全部运行均恢复 60 万条 snapshot mapping 和 7.5 万条 version mapping；测试后
`doris-backup-memory-benchmark-*` 临时目录及 spill 文件计数均为 0，主 FE HTTP 返回 200，
主 BE 进程正常。

### Branch 3.1 参考基线（2026-07-17）

在 `branch-3.1-backup-mem` @ `b8c24f5404e` 上，通过未 push 的纯测试分支
`codex/branch31-memory-benchmark` 移植同一 benchmark。测试规模仍为 20 万 tablets × 3 replicas、
`full_streaming`、每阶段独立 JVM、`-Xms2g/-Xmx2g`。五阶段各执行一次，均为 JUnit 1/1、
返回码 0：

| 阶段 | peak delta | retained delta | elapsed | payload |
| --- | ---: | ---: | ---: | ---: |
| `selective_copy` | 1,446,544,544 | 109,967,704 | 16,319 ms | - |
| `backup_meta_write` | 948,333,360 | 17,707,160 | 4,398 ms | 23,602,277 |
| `backup_meta_read` | 766,509,056 | 92,259,440 | 1,090 ms | 23,602,277 |
| `journal_write` | 1,013,149,376 | 17,739,096 | 4,390 ms | 23,602,569 |
| `journal_replay` | 766,509,056 | 92,259,520 | 1,260 ms | 23,602,569 |

Branch 3.1 的 `RuntimeTypeAdapterFactory` 仍能看到逐对象创建
`TypeFieldInjectingJsonWriter`/`EnteredObjectJsonReader` 的代码，因此分配放大逻辑存在；但本次容量
测试证明它没有在该分支的 200k×3 场景下造成 2 GiB OOM。Branch 3.1 与 master 的对象模型、
Gson 版本、payload 大小和测试图构造 API 不同，不能把两组绝对峰值直接作为性能优劣比较。
要量化 branch 3.1 上 wrapper pool 的净收益，仍需把 pool 单独移植后做同分支 A/B 和 allocation
profile。

### 验收基线

- Replica 剥离后的 BackupJob/BackupMeta payload 应显著小于旧格式；历史参考值为 119 MB → 23 MB。
- 20 万 tablet 快速基准不得在 `-Xmx2g` 下 OOM。
- streaming 实现不能产生第二份完整 JSON String、JsonElement tree 或与 payload 等大的 size-check buffer。
- CPU 时间不得出现无法解释的明显回退。
- 所有兼容路径的输出必须可由对端 reader 正确读取。

## 配置和回退策略

计划保留以下内部配置能力：

1. `backup_meta_reserve_replica_info`
   - `false`：默认剥离 Replica。
   - `true`：恢复旧 BackupMeta 行为。

2. Backup/Restore Job streaming JSON 开关
   - 控制 Guava Table/Multimap 的 streaming 或 tree 实现。
   - 不改变 JSON schema。

3. Table metadata streaming JSON 开关
   - 控制 Table/Partition/Tablet/Replica 的 streaming 或 tree 多态分发。
   - 不改变 JSON schema。

配置设计必须明确 writer、reader、Master、Follower、Observer 和 checkpoint 线程的行为。如果配置只控制 writer，reader 应自动兼容两种输出；如果配置同时控制 reader，则所有 FE 节点都必须具备一致且可操作的回退方式。

RestoreJob 接入不使用仅包围 `AbstractJob.read()` 的 ThreadLocal scope。checkpoint/image 可以经过
`AbstractJob.read()`，但 editlog replay 还会直接经过 `BackupJob.read()`、`RestoreJob.read()`；仅在
`AbstractJob` 建立 scope 会漏掉 follower replay。PR 4 由同一个内部 FE config 直接驱动外层多态
adapter 和 RestoreJob 字段级 adapter，且关闭配置时两层都选择 legacy delegate，保证开关能作为
真实的 reader/writer 故障回退，而不只是内存 A/B 开关。

建议首次合入 streaming 路径时默认关闭，完成一轮兼容性和压力验证后由 PR 6 单独开启。若 reviewer 不接受默认关闭的 dormant path，则 PR 3—5 必须在默认开启前完成 PR 6 的全部验收项目。

## 风险清单

| 风险 | 影响 | 缓解措施 |
| --- | --- | --- |
| streaming 输出字段顺序或格式改变 | 旧 FE 无法 replay/restore | 原始字节比较和新写旧读测试 |
| default subtype 回放错误 | legacy catalog JSON 读取失败 | 缺少 type 字段的专门测试 |
| 运行时配置在不同 FE 上不一致 | checkpoint/replay 使用不同路径 | 明确 writer/reader 语义并测试多 FE |
| 清理 Replica 误作用于 live tablet | catalog 数据损坏 | 仅操作 detached copy，使用专用 API 命名 |
| Backup 与 Restore 同时修改 AbstractJob | PR 冲突或出现两套逻辑 | 严格按照 PR 3 → 4 → 5 顺序合入 |
| 大对象测试只验证功能未验证峰值 | OOM 问题未真正解决 | 固定 `-Xmx`、记录 allocation、GC 后 retained heap 和容量阈值 |
| segmented buffer 保留整份 JSON | writer 仍按 payload 大小占用堆 | 超过 8 MiB spill 到临时文件，并验证清理与磁盘失败语义 |
| spill 临时文件因进程硬退出残留 | FE 临时目录被长期占用 | 使用 `DELETE_ON_CLOSE` channel；继续在 E2E/硬退出测试中验证目标文件系统语义 |
| benchmark 与其他服务共享主机 | 耗时和 sampled heap 数据失真 | 容量测试可先执行；性能比例必须在隔离环境重复并取中位数 |
| CI 环境不支持特定命令 | 用例确定性失败 | 按 suite 能力放置测试，Cloud 只保留可执行路径 |

## 执行顺序与跟踪

### 阶段 A：修复现有 Draft

- [x] 完成 PR 1 定向 FE UT（16/16）和描述草案。
- [x] rebase 到 `upstream/master` @ `8460676f3fc` 并完成 Linux 定向验证。
- [ ] 将 PR 1 的全局 DeepCopy Error 传播提交拆出，或经审查确认保留在 PR 1。
- [ ] 在最终更新 Draft 前 rebase 当前 master（2026-07-21 仅新增 1 个无关 Iceberg regression 提交）。
- [ ] 重跑 buildall 和相关 regression。
- [ ] 请求 backup/restore maintainer review。

### 阶段 B：独立低风险优化

- [x] 完成 PR 2 实现和边界测试（定向 FE UT 2/2）。
- [x] 记录 512 MiB payload 下旧缓冲与计数流各三次独立 fork 的内存、GC 和耗时对比。
- [ ] 合入后确认无 journal 相关回归。

### 阶段 C：流式基础设施

- [x] 提取并完成 master 适配后的通用实现分支。
- [x] 完成 PR 3 基础设施兼容矩阵（20/20）。
- [ ] 合入后再创建 Restore/Backup 业务 PR。

### 阶段 D：Restore

- [x] 完成 PR 4 本地实现分支。
- [x] 完成全部状态与配置切换的单元兼容矩阵（5/5）。
- [x] 验证 RestoreJob replay 和中途重启。
- [x] 提供大 RestoreJob streaming/legacy reader 三次独立 fork 对照，以及 streaming write
  的 2 GiB 容量数据。

### 阶段 E：Backup

- [x] 从组合实现中拆出 PR 1、2、3、4 的独立职责。
- [x] 完成 PR 5 流式 deep copy、持久化迁移与 spillable buffer 本地实现。
- [x] 完成 Table/BackupMeta/BackupJobInfo/job 压缩路径兼容矩阵（16/16）。
- [x] 完成单 FE/单 BE 的普通表、分区表、MV/rollup BACKUP/RESTORE 端到端验证。
- [x] 完成 RestoreJob 中途重启、journal replay 和 checkpoint 验证。

### 阶段 F：默认开启

- [x] 完成 20 万 tablet `full_streaming` 七阶段单次容量快速基准，包含 BackupHandler image
  write/replay。
- [x] 完成 wrapper 复用前后 reader/replay 三次 fork 对照和 spill 残留验证。
- [x] 完成 `journal_replay` JFR allocation 根因分析、修复及三次复测。
- [x] 完成大 RestoreJob streaming/legacy reader 三次独立 fork 对照。
- [x] 完成 spill 修正后的 20 万五阶段 before/after 三次重复基准，并结合 `journal_replay`
  JFR allocation 分析。
- [x] 完成 journal size counting 的 512 MiB 旧缓冲/计数流三次对照。
- [x] 完成 200 万 tablet 压力基准。
- [x] 完成多 FE replay 和回退演练。
- [ ] 提交 PR 6 或在 reviewer 同意后确认默认开启。

## 完成定义

只有满足以下全部条件，计划才视为完成：

1. PR 1—5 已合入 Apache Doris master；如采用分阶段启用，PR 6 也已合入。
2. Replica 剥离、RestoreJob streaming、Table metadata streaming 和 journal size counting 均有独立测试。
3. 新旧 writer/reader 双向兼容矩阵全部通过。
4. BACKUP、RESTORE、FE restart、Follower replay 和 checkpoint 测试通过。
5. 20 万和 200 万 tablet 基准结果已经记录在对应 PR 中。
6. 所有运行时开关都有明确的作用范围、默认值和回退操作。
7. 没有未解释的 CI failure、未更新的模板占位符或与实际测试不一致的 checklist。

## 剩余预计周期

截至 2026-07-20，在不计算 reviewer 等待和全量 CI 排队的情况下，剩余上游整理预计需要
0.5—1.5 个工作日；如补真实 CloudRestoreJob E2E，另需 0.5—1 日：

- 如上游要求真实 CloudRestoreJob 证据，在具备 MetaService 的 Cloud 环境补测：0.5—1 日。
- PR 模板、上游提交顺序和 CI 问题整理：0.5—1.5 日。

上述项目可以部分并行，但不能通过并行省略合入门禁；默认开启的决定仍以后续容量和 replay 结果
为准。
