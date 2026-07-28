# Backup/Restore 内存优化 PR1—PR5 原理与审查指南

## 1. 文档目的

本文面向代码作者、内部审查者和 Apache Doris reviewer，解释 PR1—PR5 为什么需要拆分、每个 PR
解决哪一层内存放大、哪些持久化语义必须保持不变，以及审查时应重点验证什么。

如果还不熟悉 FE、Tablet、Replica、journal、image、Gson 或 streaming，建议先读
`backup-restore-memory-optimization-beginner-guide.md`。

本文不是 PR 合并状态记录。实时进度、提交 hash 和完整实验记录见
`backup-restore-memory-optimization-development-plan.md`；PR1 的 Replica 剥离细节另见
`backup-meta-replica-stripping-principle.md`。

### 1.1 评审背景：这组改动从哪里来

这不是一次单点 OOM 修复，也不是要重新设计 BACKUP/RESTORE 文件格式。问题出现在 tablet 数量很大
时：同一份 FE 元数据会依次经过 catalog deep copy、BackupJob/RestoreJob 持久化、edit log
size preflight、checkpoint/image 和 BackupMeta 文件写入。旧实现会在不同阶段额外保留 Replica 对象、
JSON DOM、完整 JSON String 和 byte buffer，因此最终 payload 尚未超过 FE heap 时，瞬时分配和
Old Gen 占用就可能先触发 Full GC 或 OOM。

最初的 branch-3.1 实验覆盖过 5 万级 tablet 的 RestoreJob，以及约 200 万 tablet、每 tablet
3 副本的 BACKUP 生产规模。后者的旧格式单条 edit log 按实测比例外推可能超过 BDBJE 的 1 GiB
journal 限制。这里引用这些规模是为了解释问题为什么值得处理；branch-3.1 的实测结果不能直接当作
master 上每个拆分 PR 的独立收益承诺。

方案经历了三个阶段：

1. [HYDCP/hy-doris#49](https://github.com/HYDCP/hy-doris/pull/49) 先在 branch-3.1 优化
   RestoreJob：为 `snapshotInfos`、`restoredVersionInfo` 和 job 多态分发引入 streaming Gson
   路径，并验证旧写新读、新写新读和重启 replay。它是当前 PR3、PR4 的主要来源；
2. [HYDCP/hy-doris#63](https://github.com/HYDCP/hy-doris/pull/63) 在 branch-3.1 把方案扩展到
   BACKUP，组合了 Replica 剥离、Table 元数据 streaming、长度前缀 I/O 和 journal 计数。它提供了
   完整链路和性能证据，但一个提交同时改变多个持久化层，范围不适合作为 master 的最终 review
   单元；
3. [apache/doris#65321](https://github.com/apache/doris/pull/65321) 是面向 master 的第一个
   Draft，只保留 detached BackupMeta 中 Replica 剥离这一项。后续工作不直接把 #63 整体搬到
   master，而是重新整理为 PR1—PR5。

拆成五个 PR 的原因不是代码量，而是五类改动的正确性证明不同：PR1 要证明数据确实不会被 Restore
消费；PR2 要证明只计数仍与真实 journal 字节数一致；PR3 要证明通用 adapter 的 JSON schema 和
兼容回退不变；PR4 要证明 RestoreJob replay 安全；PR5 要证明外层 byte format、bounded read 和
spill 生命周期安全。独立拆分后，每一项都可以单独 review、测试和回滚。

### 1.2 本轮 reviewer 要判断什么

本轮评审的目标是确认这套 master 拆分方案是否满足以下三个条件：

1. 功能语义不变：BACKUP 仍从 live catalog 选择 snapshot Replica，RESTORE 仍按目标集群重新创建
   Tablet/Replica；
2. 持久化兼容：不新增 journal opcode、不提升 meta version，已有 journal、image 和 BackupMeta
   仍可读取，开关不一致和回滚场景可 replay；
3. 内存峰值逐层降低：每个 PR 只认领自己消除的那一层放大，不把组合分支的总收益归到单个 PR。

本轮不试图改变 BE snapshot 数据格式、BACKUP/RESTORE SQL 接口或状态机，也不在 PR1—PR5 中直接
决定 streaming 默认开启。默认值策略留到 PR6；真实 CloudRestoreJob + MetaService E2E 仍是可选的
补充验证，不应被误写成已经完成。

### 1.3 当前可供代码评审的分支快照

以下是 2026-07-28 的个人 remote 快照。它们用于逐项 review，不代表已经完成最终 rebase 或满足
Apache CI：

| 计划 PR | 分支 | 快照 commit | 依赖 |
| --- | --- | --- | --- |
| PR1 | `codex/backup-strip-replica-info` | `3ec7ff3f374` | 无 |
| PR2 | `codex/backup-journal-size-counting` | `f71760e5d7a` | 无 |
| PR3 | `codex/streaming-gson-foundation` | `bdacd53ee31` | 无 |
| PR4 | `codex/restore-job-streaming` | `e599cf60f1b` | PR3 |
| PR5 | `codex/backup-meta-streaming` | `a3a5578602e` | PR1、PR3、PR4 |

截至该快照，五个分支相对本地跟踪的 `upstream/master` 均落后 75 个提交，因此 reviewer 可以先审
设计边界和实现逻辑，但最终逐 PR diff、CI 和 merge 判断应在 rebase 后再做。PR1 分支还包含一项
全局 DeepCopy `Error` 传播修改，建议先拆成独立前置 PR。Apache Draft #65321 的
`backup-strip-replica-info` 是另一条尚未更新的 remote 分支，不应与上述 `codex/` 快照混为一谈。

## 2. 总体结论

这组优化处理的是同一条持久化链路上的五种不同放大：

```text
live catalog / BackupJob / RestoreJob
        |
        |  PR1: 删除 Restore 不使用的 Replica 对象
        v
需要持久化的对象图
        |
        |  PR3/PR4: 避免 Guava 集合和多态对象先变成 JsonElement DOM
        |  PR5: 避免完整 JSON String/byte[]，大 payload 受控 spill
        v
4-byte length + UTF-8 JSON
        |
        |  PR2: journal size preflight 只计数，不再保留第二份完整 payload
        v
BDBJE journal / FE image / BackupMeta 文件
```

五个 PR 的职责如下：

| PR | 核心问题 | 直接收益 | 依赖 |
| --- | --- | --- | --- |
| PR1 | BackupMeta 保存无用 Replica | 缩小 retained object graph 和最终 payload | 无 |
| PR2 | size check 为计数而完整缓冲 | 消除 preflight 的第二份完整 byte buffer | 无 |
| PR3 | Guava/多态 adapter 先构造 JSON DOM | 提供 schema-compatible streaming adapter 基础设施 | 无 |
| PR4 | RestoreJob 大 Table 字段和 job 多态分发使用 tree mode | 降低 RestoreJob write/read/replay 临时对象 | PR3 |
| PR5 | Table/BackupMeta/job 外层仍构造 String，且 deep copy 峰值高 | 长度前缀直写、bounded read、spill、streaming deep copy | PR1、PR3、PR4 |

PR1 和 PR2 可以独立审查、独立合入。PR3 是基础设施，PR4 和 PR5 应严格按 PR3 → PR4 → PR5
提交。当前 streaming 开关默认关闭；是否默认开启作为 PR6 单独决策。

## 3. 必须保持的系统不变量

reviewer 不应只检查“新 reader 能读新 writer”。这组代码必须同时满足：

1. 不改变 BACKUP/RESTORE 状态机和 BE snapshot 文件格式；
2. 不提升 meta version，不引入新的 journal opcode；
3. 保持现有 `4-byte signed length + UTF-8 JSON bytes` 外层格式；
4. 保持 JSON 字段名、类型 label、default subtype 和兼容 label；
5. legacy writer、streaming writer、legacy reader、streaming reader四象限互通；
6. Leader/Follower/Observer 的运行时开关不一致时仍可 replay；
7. streaming image 可以在关闭开关后由 legacy reader 加载；
8. 写入失败、输入截断、非法 length 和 spill 失败必须明确报错，不能继续使用部分数据；
9. 所有临时 spill 文件在成功、序列化失败、目标写失败和 close 失败路径上都必须清理；
10. 每个 PR 都能独立回滚，不依赖未合入的业务状态变化。

## 4. 旧链路为什么会放大内存

### 4.1 对象图放大

BackupMeta 保存完整 `OlapTable -> Partition -> MaterializedIndex -> Tablet -> Replica`。Restore 会
重建 Tablet 和 Replica，因此具体 Replica 对象是长期持有和持久化的冗余数据。

### 4.2 JSON DOM 放大

旧的 `RuntimeTypeAdapterFactory` 通过 `delegate.toJsonTree()` 或先读成 `JsonElement` 来注入/查找
类型字段。大型 Table、Partition、Tablet、Replica 和 RestoreJob Guava Table 会同时产生业务对象图
和 JSON DOM 对象图。

### 4.3 String/byte[] 放大

`Text.writeString(GSON.toJson(value))` 至少需要完整 JSON String，随后还需要 UTF-8 bytes。reader
通常先分配完整 byte[]/String，再交给 Gson 构造目标对象。

### 4.4 preflight 重复缓冲

`BDBJEJournal.exceedMaxJournalSize()` 旧实现把 JournalEntity 写进 `DataOutputBuffer`，只为读取长度；
真正写 journal 时会再次序列化。这使大 BackupJob/RestoreJob 在最差时多保留一份完整 payload。

### 4.5 deep copy 在清理之前到达峰值

`OlapTable.selectiveCopy()` 的 legacy 路径先完整序列化并读回整个 Table，再从副本中删除不需要的
内容。因此“copy 后清理 Replica”可以缩小 retained graph，却不能降低第一次 copy 的瞬时峰值。

## 5. PR1：从 BackupMeta 剥离 Replica

### 原理

BACKUP 有两条不同链路：snapshot task 从 live Tablet 选择 Replica；BackupMeta 则保存 detached
table copy。PR1 只清理后者。`OlapTable.selectiveCopy(..., isForBackup=true)` 完成 deep copy 后，
在 `backup_meta_reserve_replica_info=false` 时调用 `clearReplicasForBackup()`：

- LocalTablet 把 detached `replicas` 置为空列表；
- CloudTablet 同时清空当前 `replica` 和 legacy `replicas`；
- live table、tablet 拓扑、ID、顺序、PartitionInfo 和 ReplicaAllocation 不变。

RESTORE 的 `resetIdsForRestore()` 会清除旧 tablet、分配新 ID，并根据目标集群和 ReplicaAllocation
创建新 Replica。RESTORE 的 SQL 属性 `reserve_replica` 保留的是 ReplicaAllocation，不是具体
Replica 对象。

### 兼容与回退

- JSON schema 和 meta version 不变；
- 新 reader 仍可读取带 Replica 的旧 BackupMeta；
- `backup_meta_reserve_replica_info=true` 恢复旧写入内容；
- 新备份默认省略 Replica，因此属于明确的内部行为变化。

### 审查重点

1. `clearReplicasForBackup()` 是否只有 detached copy 调用点；
2. snapshot task 是否在 live catalog 上完成 Replica 选择；
3. Cloud 当前字段和 legacy 字段是否都清理；
4. `reserve_replica=true` 是否仍能恢复正确的副本分配；
5. PR 文案是否明确 initial `DeepCopy.copy()` 峰值仍存在。

### 当前待决项

PR1 分支还包含一个全局 `DeepCopy` Error 传播提交：反射包装的 OutOfMemoryError 不再被转换为
`null`。逻辑正确，但作用于所有 DeepCopy 调用方，建议拆成独立前置 PR，避免扩大 PR1 范围。

## 6. PR2：Journal size check 改为计数流

### 原理

旧实现：

```text
JournalEntity.write(DataOutputBuffer) -> 保留完整 byte[] -> DatabaseEntry.getSize()
```

新实现：

```text
JournalEntity.write(CountingDataOutputStream(OutputStream.nullOutputStream())) -> long count
```

它仍然真实执行同一个 `JournalEntity.write()`，所以包括 opcode、长度前缀和 payload 在内的字节数
与真实写入一致；区别只是底层 sink 不保存字节。

1 GiB 判断从 `(1 << 30)` 明确为 `1L << 30`，并保持原来的严格 `size > limit` 语义。Writable
抛出的 IOException 原样传播。

### 审查重点

1. 是否统计完整 JournalEntity，而不是只统计 Writable body；
2. 边界是否仍为“等于限制允许，大于限制拒绝”；
3. counter 是否为 long，避免大 payload 计数溢出；
4. 底层是否真正使用 null sink，没有隐藏 buffer；
5. serialization exception 是否继续失败而不是返回错误大小。

### 性能证据

固定 512 MiB Writable 的三次独立 JVM 中，两种实现均得到 536,870,914 bytes。旧缓冲中位 retained
约 537.9 MB，新计数流约 7.7 KB；该实验只隔离 size-check buffer，不代表完整 BackupJob
序列化成本。

## 7. PR3：Streaming Gson 基础设施

### Guava Table/Multimap

PR3 使用 `TypeAdapterFactory` 直接操作 JsonReader/JsonWriter，保持旧 JSON shape，但不先创建完整
JsonElement。adapter 从字段声明解析泛型参数，并由 Gson 获取 row key、column key、value 或 map
adapter；null、empty、嵌套泛型和运行时 value 类型均在兼容矩阵中覆盖。

### 多态 streaming dispatch

旧多态写入通常是：

```text
delegate.toJsonTree(value) -> JsonObject 添加 clazz -> Streams.write()
```

新路径用 `TypeFieldInjectingJsonWriter` 在 delegate 开始写对象时把类型字段注入为首字段。读取
canonical type-first JSON 时，`EnteredObjectJsonReader` 把已经进入对象的 reader 交给正确 subtype
delegate，不构造 DOM。若历史输入的 type 不在首位或缺失，则进入 `readLegacyObject()` 兼容慢路径，
为当前对象构造 JsonObject 后再调用 legacy delegate；它保证兼容，但不声称旧非 canonical 输入也
完全 streaming。

必须处理的历史输入包括：

- type 字段位于首位、中间或末尾；
- type 字段缺失但 factory 存在 default subtype；
- compatible subtype label；
- unknown/duplicate type、截断 JSON 和错误字段类型。

### wrapper pool 和能力探测缓存

高 tablet 数场景会创建海量多态 wrapper。PR3 使用 per-thread、per-root-stream、LIFO wrapper pool，
最多缓存 64 层；释放时清除 payload 强引用，root stream 使用 WeakReference。

Gson 2.10/2.11 的 JsonReader/JsonWriter 可选设置 API 不同。反射能力探测只在类初始化时执行并缓存
copier/no-op，不能在每个对象 acquire 时通过 `Class.getMethod()` 制造 NoSuchMethodException。

### 审查重点

1. streaming 开关关闭时是否完全走 legacy tree delegate；
2. writer 是否只向 JSON object 注入一次 type 字段；
3. reader 的 type-first streaming 主路径，以及非首位/default subtype 的 legacy fallback 是否都不丢字段；
4. wrapper 异常退出后是否仍按 LIFO 释放并清除引用；
5. `JsonReaderInternalAccess` hook 是否只解包自定义 wrapper，不改变普通 JsonReader；
6. Gson 版本不存在的可选方法是否缓存 no-op，真实调用失败是否明确抛错；
7. byte-compatible 路径是否比较原始 bytes，而不只比较反序列化对象。

PR3 本身不修改 Backup/Restore 状态机和持久化入口，是可独立验证的基础设施 PR。

## 8. PR4：RestoreJob 流式 JSON

### 原理

RestoreJob 的 `snapshotInfos` 和 `restoredVersionInfo` 是大型 Guava Table。PR4 为这两个字段安装
config-driven adapter，并为 AbstractJob 的多态 factory 启用 PR3 streaming dispatch。

`enable_backup_restore_job_streaming_json=false` 时，字段和 job 多态分发均使用 legacy adapter；
开启时改用 streaming adapter。两种模式输出相同 schema，开关选择实现而不是协议版本。

PR4 仍使用现有外层 Text/String 持久化入口，因此它主要消除 Guava Table 和多态 dispatch 的 DOM，
不是最终的 DataOutput 直写方案。外层长度前缀直写由 PR5 完成。

### 为什么 replay 安全

开关不写入 payload，也不是 reader 选择格式的 version bit。安全性来自两种 adapter 维护相同 JSON
shape，所以：

- legacy writer → streaming reader；
- streaming writer → legacy reader；
- Leader streaming → Follower/Observer legacy；
- streaming checkpoint image → 关闭开关后的 legacy load

都必须成立。压缩 RestoreJob 的 marker/压缩路径仍保留。

### 审查重点

1. 字段级 adapter 与 AbstractJob 多态 adapter 是否使用同一 config；
2. PENDING 到 FINISHED/CANCELLED 的所有状态是否覆盖；
3. JournalEntity、EditLog.loadJournal、BackupHandler replay 和 image load 是否都覆盖；
4. 配置在节点间不一致或重启后回落时是否仍能读取；
5. CloudRestoreJob subtype 是否在多态注册和单测中存在；
6. PR 文案是否避免宣称已经移除完整外层 String。

### 性能证据

包含 60 万条 snapshot mapping 和 7.5 万条 version mapping 的 79.7 MB RestoreJob payload，三次
独立 JVM 中位数显示 streaming reader 相对 legacy reader sampled peak delta 下降约 80.17%，耗时
下降约 73.86%；两者 retained heap 基本相同，符合“减少临时对象而不改变最终 live graph”的预期。

## 9. PR5：BackupMeta/Table 与外层持久化流式化

### 9.1 保持长度前缀格式

`Text.writeString(json)` 的外层格式是 4-byte length 加 UTF-8 JSON。PR5 的
`LengthPrefixedJsonStream` 保持这个格式，因此 legacy reader 仍能读取 streaming writer 的结果，
streaming reader 也能读取 legacy writer 的结果。

### 9.2 为什么 writer 需要 spill

目标 DataOutput 通常不能回写开头的 length。以下两个简单方案都有问题：

- 先完整写到 byte[]：重新引入与 payload 等大的 heap buffer；
- 先计数、再序列化：遍历两次，耗时翻倍，而且可变 job 两次遍历可能产生不同内容。

PR5 采用单遍序列化的 spillable buffer：

1. 前 8 MiB 保存在内存；
2. 超过阈值后写入 `Config.tmp_dir/backup_restore_json_spill`；
3. JSON 完成后获得精确 int length；
4. 写入 length，再以 64 KiB 块回放 payload；
5. close 时删除 spill，所有失败路径执行 cleanup，并把 cleanup failure 作为 suppressed exception。

这使 destination 在序列化完成前不被修改，也避免双遍序列化。

### 9.3 bounded reader

reader 只向 Gson 暴露声明 length 范围内的 DataInput：

- negative length 明确失败；
- truncated input 抛 EOFException；
- Gson 未消费完整 payload 明确失败；
- 不为整个 payload 分配连续 byte[]；
- reader close 不关闭上层 journal/image 所拥有的 DataInput。

### 9.4 业务迁移

PR5 把 streaming helper 应用于 Table、OlapTable、BackupMeta、BackupJobInfo、AbstractJob、BackupJob
和 RestoreJob 的相关 write/read；Table/Partition/Tablet/Replica 多态 factory 使用 PR3 dispatch；
`OlapTable.selectiveCopy()` 在 table streaming 开关开启时使用 spillable JSON stream deep copy。

两个主要开关默认均为 false：

| 开关 | 控制范围 |
| --- | --- |
| `enable_table_meta_streaming_json` | Table/BackupMeta、catalog 多态 dispatch、streaming deep copy |
| `enable_backup_restore_job_streaming_json` | BackupJob/RestoreJob 外层持久化和 job/field adapters |

### 审查重点

1. streaming 和 Text writer 的原始 bytes 是否一致；
2. compressed job marker 是否在 streaming reader 中正确区分和回放；
3. writer 是否只序列化一次，是否在序列化成功前不写 destination；
4. 8 MiB 只是内存阈值，不是 payload 上限；真正上限是否受 int length 约束；
5. spill path 是否位于 Doris tmp_dir，而不是 JVM 全局临时目录；
6. 成功、serializer failure、destination failure、目录不可用和 close failure 是否都清理；
7. config=false 是否完整走原 Text/String/DeepCopy 路径；
8. BackupMeta、BackupJobInfo、journal、image 和 selective copy 是否分别有四象限测试；
9. 组合 PR 是否复用 PR1/PR4，而没有重复 config、adapter 或 replay 逻辑。

## 10. 跨 PR 兼容矩阵

| 写入方 | 读取方 | 必须结果 |
| --- | --- | --- |
| legacy Text + tree adapter | legacy reader | 基线不变 |
| legacy Text + tree adapter | streaming reader | 成功 |
| length-prefixed streaming + streaming adapter | legacy Text reader | 成功 |
| length-prefixed streaming + streaming adapter | streaming reader | 成功 |
| Leader streaming | Follower/Observer legacy | journal 实时 replay 成功 |
| streaming checkpoint | restart 后 config=false | image load 成功 |
| compressed RestoreJob | 两类 reader | marker 和压缩 payload 成功 |
| Replica stripped BackupMeta | 两类 table reader | tablet 拓扑保留、Replica 为空 |

“能反序列化”还不够。测试必须校验 job 状态、mapping 数量、Tablet 顺序、ReplicaAllocation、恢复
数据行数/边界/聚合值，以及临时文件无残留。

## 11. 已完成的主要验证

- 重放到 2026-07-20 master 后，PR1—PR5 定向 FE 测试分别为 16/16、2/2、20/20、7/7、27/27；
- 五轮 Maven reactor 均 `BUILD SUCCESS`，Checkstyle 无 violation；
- 单 FE/单 BE MinIO BACKUP/RESTORE、分区表、MV/rollup 通过；
- RestoreJob 在 CREATING、SNAPSHOTING、DOWNLOADING、COMMITTING 中断后均恢复至 FINISHED；
- Leader streaming → Follower/Observer legacy replay、checkpoint/image 跨配置加载和运行中选主通过；
- 动态分区、colocate、Replica 保留开关和 Cloud 元数据 subtype 矩阵通过；
- 20 万 tablet 重复矩阵、200 万 tablet 七阶段容量矩阵和 spill 残留检查通过；
- 真实 CloudRestoreJob + MetaService E2E 尚未完成，不能用 Cloud subtype UT 替代这一结论；
- 各拆分 PR 的正式 Apache CI 仍未完成，组合验证不能直接当作独立 PR CI 结果。

## 12. 推荐 reviewer 顺序

1. 先审 PR1 的“Replica 是否确实无用”和 detached-copy 边界；
2. 独立审 PR2 的精确计数和异常语义；
3. 审 PR3 的 JSON schema、default subtype、wrapper 生命周期和 Gson 版本兼容；
4. 审 PR4 的 RestoreJob 字段范围、状态/replay 矩阵和跨节点配置不一致；
5. 审 PR5 的外层 byte compatibility、single-pass spill、bounded reader 和调用点迁移；
6. PR1—PR5 合入并完成 CI 后，再决定 PR6 是否默认开启两个 streaming config。

## 13. 当前需要 reviewer 明确决定的问题

1. PR1 的 DeepCopy Error 传播是否拆为独立前置 PR；
2. 是否接受先合入默认关闭的 PR3—PR5，再通过 PR6 默认开启；
3. 是否要求在合入前补真实 CloudRestoreJob + MetaService E2E；
4. 是否要求将内部开关加入用户文档，或保持为仅用于回退的 FE config；
5. 性能数字哪些作为容量证据，哪些可以进入 PR 的 before/after 声明。

## 14. 总审查结论

五个 PR 的拆分边界合理：PR1 减少数据量，PR2 消除重复缓冲，PR3 提供通用且可回退的 adapter，
PR4 先覆盖 RestoreJob，PR5 再完成 BackupMeta/Table 和外层 I/O。当前未发现必须推翻总体方案的
逻辑问题。合入前的主要工作不是继续叠加功能，而是处理 PR1 的 DeepCopy 范围、逐 PR rebase/CI、
reviewer 对默认关闭策略的决定，以及可选的真实 Cloud E2E。
