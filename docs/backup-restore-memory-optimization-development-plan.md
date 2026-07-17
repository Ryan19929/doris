# Backup/Restore 大规模元数据内存优化开发计划

## 文档状态

- 状态：执行中（本地实现与定向验证阶段，尚未满足提交上游或合入条件）
- 目标分支：Apache Doris `master`
- 相关实现：
  - HYDCP/hy-doris#49：RestoreJob Guava Table/Multimap 流式 JSON 序列化
  - HYDCP/hy-doris#63：BackupMeta/Table 流式 JSON、Replica 剥离及 journal size 计数优化
  - apache/doris#65321：从 BackupMeta 剥离无用 Replica
- 本地实现分支（均未合入 `master`）：
  - PR 1：`codex/backup-strip-replica-info` @ `04cb7272895`
  - PR 2：`codex/backup-journal-size-counting` @ `c9b930bcfec`
  - PR 3：`codex/streaming-gson-foundation` @ `7c92546e7ac`
  - PR 4：`codex/restore-job-streaming` @ `c1840fd2ff3`
  - PR 5：`codex/backup-meta-streaming` @ `2d0a750a0d2`
  - 验证分支：`codex/backup-memory-benchmark` @ `8b830a04599`

PR 1—5 已完成本地实现和分支拆分，但不代表已提交上游、通过完整 CI 或可以合入
`master`。验证分支包含 benchmark、OOM 传播修复和 spillable buffer 设计修正，需完成远端
复测后再回迁到对应生产分支。

本文用于跟踪上述优化向 Apache Doris `master` 的移植、拆分、验证和发布。计划中的每个 PR 必须能够独立审查、独立验证，并在出现问题时独立回滚。

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
| 1 | Replica 剥离 | 修复并完善 apache/doris#65321 | 无 | 本地实现分支完成；完整门禁未完成 |
| 2 | Journal size 计数 | 使用计数流替代完整缓冲 | 无 | 本地实现完成；定向 FE UT 2/2 通过 |
| 3 | Streaming Gson 基础设施 | Guava 与多态 TypeAdapter 的流式实现 | 无 | 本地实现分支完成；完整兼容矩阵未完成 |
| 4 | RestoreJob 流式序列化 | 迁移 HYDCP/hy-doris#49 的 Restore 优化 | PR 3 | 本地实现分支完成；Restore E2E/重启未完成 |
| 5 | BackupMeta/Table 流式序列化 | 迁移 HYDCP/hy-doris#63 的 Backup 优化 | PR 1、3、4 | 本地实现与 spill 修正完成；远端复测中 |
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

- [ ] tree writer → tree reader。
- [ ] tree writer → streaming reader。
- [ ] streaming writer → tree reader。
- [ ] streaming writer → streaming reader。
- [ ] null、empty、单元素和大规模 Table/Multimap。
- [ ] 嵌套泛型和不同 key/value 类型。
- [ ] type 字段在首位、中间和末尾。
- [ ] legacy payload 缺少 type 字段。
- [ ] 未知 type、重复 type、截断 JSON 和错误字段类型明确失败。
- [ ] `GsonSerializationTest` 覆盖 master 注册的所有相关适配器。

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

- [ ] PENDING 状态写入、读取和 replay。
- [ ] SNAPSHOTING 状态写入、读取和 replay。
- [ ] DOWNLOAD/SNAPSHOT/DOWNLOAD_FINISHED 等包含大映射的状态。
- [ ] FINISHED/CANCELLED 终态 replay。
- [ ] Master 写入后 Follower replay。
- [ ] 配置开启写入后关闭配置读取。
- [ ] 配置关闭写入后开启配置读取。
- [ ] FE 重启后 RestoreJob 能继续运行或保持正确终态。

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

- [ ] Table/OlapTable 旧写新读、新写旧读。
- [ ] BackupMeta 文件旧写新读、新写旧读。
- [ ] BackupJobInfo 文件内容及 UTF-8 行为不变。
- [ ] BackupJob editlog write/read/replay。
- [ ] image/checkpoint 中包含大型 OlapTable 时可正常生成和加载。
- [ ] 默认 subtype 的 legacy Table/Partition/Tablet/Replica JSON 可读取。
- [ ] Replica 剥离后的空 LocalTablet/CloudTablet 可被新旧 reader 读取。
- [ ] 部分分区、rollup、colocate、动态分区和 Cloud Tablet 元数据 round-trip。

### 合入门槛

- 声明 byte-compatible 的所有路径都有原始字节比较测试。
- 20 万 tablets × 3 replicas 在 `-Xmx2g` 基准下完成 selective copy、BackupMeta write 和 BackupJob editlog write。
- streaming config 关闭时可以回退 legacy tree path。
- BACKUP、RESTORE、FE restart 和 journal replay 端到端测试通过。

## PR 6：默认开启和最终验证

如果 PR 3—5 为降低首次合入风险而默认关闭 streaming，则使用一个只修改默认配置的小 PR 开启功能。开启前必须满足：

- [ ] 完整 CI 连续通过。
- [ ] 新旧 writer/reader 兼容矩阵全部通过。
- [ ] 20 万 tablet 快速基准稳定通过。
- [ ] 200 万 tablet 压力基准无 OOM。
- [ ] 至少完成一次 Master/Follower/Observer 重启及 replay 验证。
- [ ] 配置回退演练成功。
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
- 未完成：spill 修正后 before/after 矩阵、RestoreJob 大对象 benchmark、200 万压力基准、真实
  BACKUP/RESTORE E2E、FE 重启和多 FE replay。

正式 Maven heap 参数为 `-Dfe.ut.max.heap=2g`，固定初始堆可额外使用
`-Dfe.ut.extra.jvm.args=-Xms2g`；不能再使用 `-DargLine` 覆盖 fe-core 的默认 heap。

### Wrapper 复用与受控 spill 复测（2026-07-17）

验证分支在 `ef668e62d5b`（每个对象新建 reader/writer wrapper）和
`a40946deb14c`（wrapper pool + Doris 临时目录 spill）之间执行同口径对照。数据规模为
20 万 tablets × 3 replicas，`full_streaming`，每个阶段使用独立 JVM 和
`-Xms2g/-Xmx2g`。reader 两个阶段各运行 3 次并取 sampled heap peak 中位数；其余阶段目前
只有单次结果，因此不能用来声明稳定的性能比例。

| 阶段 | 优化前 peak delta | 优化后 peak delta | 变化 | 结论 |
| --- | ---: | ---: | ---: | --- |
| `selective_copy` | 1,382,918,656 | 1,361,285,632 | -1.56% | 容量通过；峰值小幅下降 |
| `backup_meta_write` | 1,316,725,552 | 1,316,084,512 | -0.05% | 峰值基本不变 |
| `backup_meta_read`（3 次中位数） | 1,287,651,328 | 1,170,210,816 | -9.12% | reader wrapper 复用有效 |
| `journal_write` | 1,316,256,064 | 1,315,268,896 | -0.08% | 峰值基本不变 |
| `journal_replay`（3 次中位数） | 1,287,651,328 | 1,288,699,904 | +0.08% | 峰值不变，仍有其他主导内存 |

- 五个阶段均在 2 GiB heap 下通过；BackupMeta 与 journal payload 分别保持 19,802,039 和
  19,802,331 bytes，说明 wrapper 复用与 spill 路径没有改变持久化字节规模。
- 大集合单测中连续处理 2,000 个多态对象，每个方向只构造 1 个 wrapper；另有嵌套深度上限
  64、异常恢复、释放强引用和 8 线程隔离测试。
- spill 文件位于 `Config.tmp_dir/backup_restore_json_spill`，使用单次 UUID `CREATE_NEW` 和
  `DELETE_ON_CLOSE` channel。Linux OpenJDK 17 探针确认打开、回放、关闭期间目录均无可见残留；
  200k 五阶段运行结束后 Java tmp 和 Doris spill 文件计数均为 0。
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

- [ ] 完成 PR 1 测试和描述修复。
- [ ] rebase 最新 master。
- [ ] 重跑 buildall 和相关 regression。
- [ ] 请求 backup/restore maintainer review。

### 阶段 B：独立低风险优化

- [x] 完成 PR 2 实现和边界测试（定向 FE UT 2/2）。
- [ ] 记录旧缓冲与计数流的内存对比。
- [ ] 合入后确认无 journal 相关回归。

### 阶段 C：流式基础设施

- [x] 提取并完成 master 适配后的通用实现分支。
- [ ] 完成 PR 3 全部兼容性测试。
- [ ] 合入后再创建 Restore/Backup 业务 PR。

### 阶段 D：Restore

- [x] 完成 PR 4 本地实现分支。
- [ ] 验证 RestoreJob replay 和中途重启。
- [ ] 提供大 RestoreJob before/after 数据。

### 阶段 E：Backup

- [x] 从组合实现中拆出 PR 1、2、3、4 的独立职责。
- [x] 完成 PR 5 流式 deep copy、持久化迁移与 spillable buffer 本地实现。
- [ ] 完成 BACKUP/RESTORE/restart 端到端验证。

### 阶段 F：默认开启

- [x] 完成 20 万 tablet `full_streaming` 五阶段单次容量快速基准。
- [x] 完成 wrapper 复用前后 reader/replay 三次 fork 对照和 spill 残留验证。
- [x] 完成 `journal_replay` JFR allocation 根因分析、修复及三次复测。
- [ ] 完成 spill 修正后的 20 万 before/after 重复基准与 JFR allocation 分析。
- [ ] 完成 200 万 tablet 压力基准。
- [ ] 完成多 FE replay 和回退演练。
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

## 预计周期

在不计算 reviewer 等待和全量 CI 排队的情况下，预计需要 10—15 个工作日：

- PR 1：1—2 日。
- PR 2：1 日。
- PR 3：3—4 日。
- PR 4：2—3 日。
- PR 5：4—6 日。
- 最终压力测试和默认开启：2—3 日，可与 PR review 部分并行。
