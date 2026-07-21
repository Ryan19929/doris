# BackupMeta Replica 剥离原理与审查指南

## 1. 结论先行

PR1 的核心做法是：BACKUP 仍然从 live catalog tablet 选择可用 Replica 并创建 BE snapshot task，
但写入 BackupMeta 的 detached table copy 不再保存具体 Replica 对象，只保留恢复表结构所需的
Partition、Index、Tablet 拓扑和 ReplicaAllocation。

这个改动不会让 RESTORE 复用原 Replica。现有 RESTORE 本来就会重新分配 table/index/partition/tablet/
replica ID，并根据目标集群和 ReplicaAllocation 创建新的 Replica。备份中的 Replica ID、Backend ID、
状态和统计信息没有参与该过程。

代码逻辑本身成立。需要特别注意两个边界：

1. Replica 是在 `DeepCopy.copy()` 完成之后才从副本中清除，因此 PR1 能降低 BackupMeta 的 retained
   heap、持久化体积和后续序列化成本，但不能消除第一次 deep copy 的瞬时内存峰值。
2. 当前 PR1 分支还包含一个全局 `DeepCopy` Error 传播修改。它是正确性修复，但不属于 Replica
   剥离本身，建议拆成独立前置 PR，或在 PR 描述中明确解释为什么必须同批提交。

## 2. 为什么 BackupMeta 中的 Replica 是冗余数据

OlapTable 的元数据对象图大致如下：

```text
OlapTable
  -> Partition
    -> MaterializedIndex
      -> Tablet
        -> Replica
```

BackupMeta 历史上直接保存完整 Table 对象，因此 Replica 随对象图被隐式写入。它并不是专门设计的
备份格式字段，也不是恢复数据文件的来源。

BACKUP 实际包含两条不同的数据链路：

```text
数据快照链路
live OlapTable -> live Tablet -> 选择可用 Replica -> BE SnapshotTask

元数据链路
live OlapTable -> DeepCopy -> detached OlapTable -> BackupMeta -> __meta / journal / image
```

数据快照链路必须读取 live Replica，因为 FE 要选择 Backend 执行 snapshot。元数据链路只需要描述
恢复后的逻辑结构，不需要记住源集群上的具体 Replica。

## 3. PR1 改变了什么

入口位于 `BackupJob.prepareBackupMetaForOlapTableWithoutLock()`：

```text
olapTable.selectiveCopy(reservedPartitions, VISIBLE, true)
```

`OlapTable.selectiveCopy()` 先创建完整 detached copy，然后只在下面两个条件同时成立时清理 Replica：

```text
isForBackup == true
backup_meta_reserve_replica_info == false
```

因此：

- 普通 truncate 等非 BACKUP 的 selective copy 不受影响；
- live catalog table 不受影响；
- snapshot task 的 Replica 选择不受影响；
- 设置 `backup_meta_reserve_replica_info=true` 可以恢复旧的 BackupMeta 内容；
- Tablet 数量、顺序、ID、PartitionInfo 和 ReplicaAllocation 均保留。

LocalTablet 和 CloudTablet 的内部布局不同，所以清理方式也不同：

- LocalTablet：将 detached tablet 的 `replicas` 替换为空列表；
- CloudTablet：同时清空当前单值字段 `replica` 和旧格式兼容字段 `replicas`。

API 命名为 `clearReplicasForBackup()`，目的是强调它只能用于 detached backup copy。这个限制目前由
调用链保证，而不是由类型系统强制保证。

## 4. 为什么 RESTORE 不需要备份中的 Replica

RESTORE 加载 BackupMeta 中的 OlapTable 后，会进入 `OlapTable.resetIdsForRestore()`。其核心行为是：

1. 为 table、index 和 partition 分配新 ID；
2. 读取并保留逻辑 tablet 数量和顺序；
3. 清除备份 table 中原有 tablet 实例；
4. 为每个逻辑 tablet 创建新的 Tablet 和 tablet ID；
5. 根据目标集群的 ReplicaAllocation、colocate backend sequence 或可用 Backend 创建新 Replica；
6. 为每个 Replica 分配新 ID。

因此即使旧 BackupMeta 中带有 Replica，这些对象也会随着旧 tablet 被丢弃。

容易混淆的是下面两个开关：

| 配置或属性 | 发生阶段 | 真正含义 |
| --- | --- | --- |
| `backup_meta_reserve_replica_info` | BACKUP 写元数据 | 是否把源集群的具体 Replica 对象写进 BackupMeta |
| RESTORE `reserve_replica` | RESTORE 重建表 | 是否保留原 ReplicaAllocation，而不是是否复用 Replica 对象 |

即使 `reserve_replica=true`，RESTORE 仍会创建新的 Tablet 和 Replica；它只保留副本数量及 Tag 分配
规则。PR1 的 alias restore 回归用例特意使用 `reserve_replica=true`，用于证明这两个概念相互独立。

## 5. 兼容性

PR1 没有引入新的 BackupMeta 格式、meta version 或压缩协议。JSON 对象结构保持不变，只是 Replica
集合变为空，CloudTablet 的 Replica 字段为空或不再输出。

兼容方向如下：

- 新代码读取旧备份：旧备份中的 Replica 仍可正常反序列化；
- 新代码读取新备份：空 Replica 的 LocalTablet/CloudTablet 可以正常 round-trip；
- 恢复旧行为：动态设置 `backup_meta_reserve_replica_info=true` 后，新 BACKUP 再次写入 Replica；
- RESTORE 行为：无论 BackupMeta 是否包含 Replica，都走同一套新 ID、新 Backend 重建流程。

该开关是 FE 内部、mutable、master-only 配置，不新增 SQL BACKUP/RESTORE 属性。

## 6. 内存收益和明确限制

20 万 tablets、每 tablet 3 replicas 的历史容量基准中，移除 Replica 后的 BackupJob journal payload
约从 119 MB 降到 23 MB，体积下降约 81%。这个数据说明 Replica 占持久化对象图的大部分，但不能
被解释成 PR1 单独降低了 81% 的全过程峰值内存。

PR1 的直接收益是：

- 减小 BackupMeta、BackupJob journal 和 image 中长期持有的对象图；
- 减小 `__meta`、journal 和 image 的最终持久化字节；
- 减少 deep copy 完成之后各次序列化/反序列化需要处理的对象数量。

PR1 不解决：

- `DeepCopy.copy()` 在清理前完整复制 Replica 所产生的瞬时峰值；
- Gson 多态适配器构造 JsonElement DOM 的临时对象；
- 完整 JSON String 和 UTF-8 byte[] 同时驻留；
- journal size check 的重复序列化。

在 2 GiB heap 的 20 万 tablet 测试中，legacy 和仅 `strip_replicas` 的 `selective_copy` 都会 OOM；
只有后续 streaming deep copy 方案能越过这个阶段。因此 PR1 应被描述为“缩小 retained metadata 和
持久化载荷”，不能单独宣称“解决大表 BACKUP OOM”。

## 7. `DeepCopy` Error 传播提交

当前 PR1 分支的第三个提交修改了 `DeepCopy.copy()`：反射调用的 read 方法如果抛出 Error，
`Method.invoke()` 会把它包装为 InvocationTargetException。旧代码捕获 Exception 后返回 `null`，
导致 OutOfMemoryError 被误报成普通 deep-copy 失败；新代码会重新抛出原始 Error。

这个修改符合“致命 VM 错误不能降级为普通业务失败”的原则，单测也验证了同一个
OutOfMemoryError 实例被传播。但是它会影响所有 DeepCopy 调用方，不只 BACKUP。

审查建议：

- 首选：拆成独立、小型前置 PR，PR1 只保留 Replica 剥离和对应测试；
- 备选：继续放在 PR1，但 PR 标题、Problem Summary、Behavior changed 和 Release note 必须显式
  覆盖这一全局行为变化。

## 8. 已有测试覆盖

| 测试 | 覆盖内容 |
| --- | --- |
| `OlapTableTest` | 默认剥离、保留旧行为、非 BACKUP copy 不剥离、live table 不变 |
| `BackupMetaTest` | Local/Cloud × reserve true/false 的 BackupMeta write/read round-trip |
| `CloudTabletTest` | CloudTablet 当前 Replica 字段被清空 |
| `DeepCopyTest` | 反射包装的 OutOfMemoryError 原样传播 |
| `test_backup_restore_alias` | 在剥离配置下 BACKUP，并以 `reserve_replica=true` 恢复为 alias 后校验数据 |

最新重放分支已在 Linux 上执行上述四个 FE UT 类，结果 16/16；Maven checkstyle 和 25 模块 reactor
构建通过。新增 regression case 已编写，但仍应由正式 regression/Apache CI 执行后再勾选通过。

## 9. 审查清单

建议重点检查：

1. `clearReplicasForBackup()` 是否只有 detached copy 调用点；
2. snapshot task 是否始终从 live OlapTable 选择 Replica；
3. LocalTablet、CloudTablet 以及 Cloud legacy 字段是否都被覆盖；
4. Tablet 拓扑和 ReplicaAllocation 是否保持不变；
5. `reserve_replica=true/false` 是否都不依赖具体 Replica 对象；
6. 新旧 BackupMeta 是否双向可读；
7. PR 文案是否明确 initial DeepCopy 峰值仍然存在；
8. 是否接受把全局 DeepCopy Error 传播留在 PR1。

## 10. 当前审查结论

- 正确性：Replica 剥离链路成立，未发现破坏 BACKUP/RESTORE 语义的代码问题；
- 数据安全：清理作用于 detached copy，live catalog 和 BE snapshot task 不受影响；
- 兼容性：格式不升级，旧行为有动态回退开关；
- 性能表述：可以声明持久化载荷和 retained metadata 显著下降，不能声明 PR1 单独解决 copy 峰值；
- 合入建议：先拆分或明确解释 DeepCopy 全局行为修改，再更新 Draft PR 并跑正式 regression/CI。
