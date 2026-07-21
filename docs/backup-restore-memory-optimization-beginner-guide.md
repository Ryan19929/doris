# Backup/Restore 内存优化入门帮助：用一次“搬家”理解 PR1—PR5

## 1. 这份文档适合谁

如果你知道 Doris 有 BACKUP 和 RESTORE，但不熟悉 FE 内存、journal、image、Gson、TypeAdapter，
建议先读本文，再读面向 reviewer 的技术指南。

读完后，你应该能回答五个问题：

1. 为什么表的数据没有变，FE 仍可能因为“元数据”而 OOM；
2. 为什么 BackupMeta 里的 Replica 可以删除；
3. “流式 JSON”到底流在哪里，为什么能省内存；
4. 为什么还需要临时文件 spill；
5. PR1—PR5 各自解决什么，为什么不能揉成一个大 PR。

先记住一句话：

> 这组优化没有改变备份的数据文件，也没有改变恢复流程；它改变的是 FE 保存、复制和读取“大型说明书”的方式。

## 2. 用“公司搬家”建立直觉

可以把一次 Doris BACKUP 想成公司搬家：

- 表中的真实数据文件，是需要搬走的箱子；
- Table、Partition、Tablet 等元数据，是箱子清单；
- Replica 是“这个箱子目前放在哪几个仓库、由谁保管”的现场记录；
- BackupMeta 是随货物保存的搬家清单；
- RESTORE 是在新办公室按清单重新摆放箱子；
- FE 是负责制作清单、记录进度和恢复工作的搬家指挥中心。

旧实现的问题不是“箱子太大”，而是指挥中心在制作清单时同时保留了很多份完整草稿：

1. 清单里写入了新办公室根本用不到的旧仓库保管记录；
2. 为了给不同类型的物品加标签，先把整份清单复制成一棵临时对象树；
3. 又把整份清单拼成一个大字符串，再转成字节；
4. 为了判断清单是否超过 1 GiB，再复制一份完整字节；
5. 读取时也可能先把整份清单一次性搬进内存。

小表时这些副本不明显。20 万个 Tablet、每个 3 个 Replica 时，临时清单和正式清单会同时占用
数 GiB heap，最终触发 OOM 或频繁 GC。

PR1—PR5 就是在逐层删除这些多余副本。

## 3. 先认识十个词

### 3.1 FE

Frontend。它保存 catalog、调度 BACKUP/RESTORE、写 journal 和 image。本系列优化主要发生在 FE，
不是 BE 数据文件格式优化。

### 3.2 Table、Partition、Index、Tablet

它们描述一张 Doris 表如何组织。可以简单理解为：

```text
Table
  -> Partition
    -> MaterializedIndex
      -> Tablet
```

RESTORE 必须知道这些结构，否则无法重建表。

### 3.3 Replica

一个 Tablet 可以有多个副本。Replica 记录副本 ID、Backend、版本、状态和统计信息等。

关键点：源集群上的具体 Replica 不等于目标集群恢复后的 Replica。RESTORE 会在目标集群重新选择
Backend，并创建新的 Replica ID。

### 3.4 ReplicaAllocation

它描述“需要多少个副本、不同 Tag 各需要多少个”，例如 3 副本。它是恢复表结构需要的规则。

Replica 是某一次实际摆放结果；ReplicaAllocation 是摆放规则。RESTORE 需要后者，不需要前者。

### 3.5 BackupMeta

备份中的表结构说明书。它保存 Table、Partition、Index、Tablet 等信息，RESTORE 用它重建表。

### 3.6 Journal

FE 的增量操作日志。BACKUP/RESTORE job 的状态变化需要写入 journal，Follower 也通过 replay journal
跟上 Leader。

### 3.7 Image / checkpoint

FE catalog 的阶段性完整快照。可以理解为“把很多 journal 汇总成一张当前状态全景图”。FE 重启时
会先加载 image，再 replay 后续 journal。

### 3.8 JSON 和 Gson

JSON 是持久化元数据使用的文本结构；Gson 是 Java 对象和 JSON 之间的转换工具。

### 3.9 Heap、峰值和 retained heap

- heap：JVM 管理的内存；
- 峰值：某一瞬间最多占了多少；
- retained heap：操作结束后，业务对象仍长期持有多少。

减少 retained heap 不一定能降低执行过程中的瞬时峰值，这正是理解 PR1 和 PR5 区别的关键。

### 3.10 Streaming 和 spill

- streaming：边读边处理、边生成边写，不先构造完整中间对象；
- spill：内存超过阈值后，把临时内容放到磁盘，完成后删除。

这里的 streaming 是 FE 内部 JSON 处理方式，不是网络流式 RPC，也不是 BE 数据文件传输。

## 4. 一次 BACKUP 实际做了什么

只看与本系列有关的简化流程：

```text
1. 从 live catalog 找到表
2. 从 live Tablet 选择可用 Replica
3. 给对应 BE 发送 SnapshotTask，生成真实数据快照
4. 复制一份 Table 元数据，形成 detached copy
5. 把 detached copy 放进 BackupMeta
6. 把 BackupJob 状态写入 journal/image
7. 把 BackupMeta 和数据快照上传到 repository
```

第 2 步必须使用 Replica，因为要知道去哪个 BE 做数据快照。

第 4—7 步不需要继续保存源集群 Replica。恢复时只要知道有多少 Tablet、每个 Tablet 应按什么
ReplicaAllocation 创建副本即可。

这就是 PR1 安全性的核心：它没有在第 2 步删除 Replica，而是在第 4 步得到独立副本后清理。

## 5. 一次 RESTORE 实际做了什么

简化流程如下：

```text
读取 BackupMeta
  -> 重建 Table / Partition / Index
  -> 为 Tablet 分配新 ID
  -> 根据 ReplicaAllocation 和目标集群 Backend 创建新 Replica
  -> 下载并提交数据快照
  -> 表恢复完成
```

RESTORE 不会把源 Replica 原样放回目标集群。原因很直观：

- 目标集群可能没有相同 Backend；
- 原 Replica ID 可能与目标集群 ID 冲突；
- 目标集群的 Tag、可用节点和 colocate 布局可能不同。

因此备份中的具体 Replica 对象是“旧办公室仓库地址”，恢复需要的是“每件物品需要几份”的规则。

## 6. PR1：删除恢复不使用的 Replica

### 原来怎么做

BackupMeta 保存完整 Table 对象图，所以每个 Tablet 的所有 Replica 也被保存。

例如 20 万 Tablet × 3 Replica，会多保存 60 万个 Replica 对象。

### 现在怎么做

`OlapTable.selectiveCopy()` 先得到 detached copy，然后在 BACKUP 用途且
`backup_meta_reserve_replica_info=false` 时清空副本中的 Replica：

```text
live table:      Tablet -> Replica A, B, C   保持不变
backup copy:     Tablet -> empty             写入 BackupMeta
```

### 为什么安全

- SnapshotTask 已从 live table 选择 Replica；
- 清理的是 detached copy，不是 live catalog；
- Tablet 数量、顺序、ID 和 ReplicaAllocation 都保留；
- RESTORE 本来就创建新 Tablet 和新 Replica。

### 两个容易混淆的配置

| 名称 | 含义 |
| --- | --- |
| `backup_meta_reserve_replica_info=true` | BACKUP 继续把具体 Replica 写进 BackupMeta，恢复旧行为 |
| RESTORE `reserve_replica=true` | 保留原 ReplicaAllocation，不是复用具体 Replica |

### PR1 没解决什么

Replica 是在 deep copy 完成后才删除。也就是说，复制过程中仍曾经创建完整 Replica 对象图。

所以 PR1 能缩小最终 BackupMeta、journal 和 retained heap，但不能单独解决第一次 deep copy 的
瞬时 OOM。这个问题由 PR5 的 streaming deep copy 处理。

### 一个直观数字

历史容量基准中，20 万 Tablet × 3 Replica 的 BackupJob journal payload 在剥离后约从 119 MB
降到 23 MB。这个数字说明最终清单小了约 81%，不能理解为整个 BACKUP 峰值也下降了 81%。

## 7. PR2：只计算 journal 大小，不保存整份副本

### 为什么需要 size check

Doris 在写 journal 前要判断单条 journal 是否超过 1 GiB。这个检查本身是合理的。

### 原来怎么做

```text
把 JournalEntity 完整写进 DataOutputBuffer
  -> 得到完整 byte[]
  -> 读取 byte[].length
  -> 丢掉 buffer
  -> 真正写 journal 时再序列化一次
```

这相当于为了给一本书称重，先复印整本书，再数复印纸。

### 现在怎么做

```text
JournalEntity.write(CountingDataOutputStream)
  -> 每写一个字节只增加计数
  -> 字节发送到 null sink，不保存内容
```

它仍执行真实的 `JournalEntity.write()`，所以 opcode、长度前缀和正文都会被准确计数。

### 为什么安全

- 相同对象走相同 write 方法；
- 小于、等于、大于限制的边界有测试；
- 等于 1 GiB 仍沿用旧语义，不算超限；
- Writable 抛出 IOException 时继续失败，不会拿错误计数继续运行。

### 一个直观数字

固定 512 MiB payload 的实验中，两种方式都得到 536,870,914 bytes。旧方式长期保留约 537.9 MB
buffer，新方式约 7.7 KB。

PR2 只消除“大小检查的副本”，不会让 JSON 序列化本身消失。

## 8. PR3：让 Gson 不再先造完整 JSON 树

### 什么是 JSON DOM

假设最终 JSON 是：

```json
{"clazz":"LocalTablet","id":100,"replicas":[]}
```

tree mode 不会直接写这些字符，而是先在内存里创建类似下面的对象：

```text
JsonObject
  -> "clazz" -> JsonPrimitive
  -> "id" -> JsonPrimitive
  -> "replicas" -> JsonArray
```

几十万个 Tablet、Replica 会产生几十万个 JsonObject、JsonArray、字符串和 map 节点。最终业务对象
还在，临时 JSON 树又复制了一遍信息。

### 为什么多态对象更麻烦

Tablet 可能是 LocalTablet 或 CloudTablet。JSON 里需要 `clazz` 字段告诉 reader 应创建哪个 Java
子类。旧 adapter 常见做法是先把完整对象转成 JsonObject，再插入 `clazz`。

### streaming adapter 怎么做

writer 在 delegate 开始写对象时，直接先写入 `clazz`，随后把其余字段继续写到同一个 JsonWriter：

```text
beginObject
  -> 写 clazz
  -> delegate 继续写 id、replicas...
endObject
```

canonical 新 JSON 会把 `clazz` 写在第一位。reader 看到首字段就是 `clazz` 时，选择正确 subtype
adapter，再通过 wrapper 从当前输入位置继续读取；这条主路径不需要先构造完整 JsonObject。

### 为什么 reader 不能只看第一个字段

历史 JSON 不保证 `clazz` 一定位于第一位，有些旧 payload 甚至没有 type 字段，而是依赖 default
subtype。为了兼容这些输入，reader 会进入 legacy 慢路径，把当前这个对象暂时恢复成 JsonObject，
找到类型或选择 default subtype 后再交给旧 delegate。因此“兼容”不等于“所有旧输入也完全无 DOM”。

reader 必须支持：

- `clazz` 在第一、中间或最后；
- 没有 `clazz` 时选择默认 subtype；
- 已经读过的字段不能丢失，legacy 慢路径要把它们保存在兼容 JsonObject 中；
- 未知 type、重复 type、截断 JSON 必须明确失败。

### wrapper 为什么还要复用

即使 wrapper 很小，200 万 Tablet 也可能创建数百万个 wrapper。PR3 使用线程内对象池复用它们，
释放时清空对 payload 的引用，最多缓存 64 层嵌套。

测试还发现 Gson 不同版本的可选方法如果每次都用反射探测，会不断创建
NoSuchMethodException。最终实现只在类初始化时探测一次，之后复用结果。

### PR3 为什么单独提交

它只提供通用 JSON 基础设施，不修改 BACKUP/RESTORE 状态机，也不立即迁移所有持久化入口。
这样 reviewer 可以先确认 JSON 兼容性，再审业务接入。

## 9. PR4：先把 RestoreJob 的大字段接到 streaming adapter

### RestoreJob 里什么最大

两个典型字段是：

- `snapshotInfos`：记录 tablet、Backend 和 snapshot 的映射；
- `restoredVersionInfo`：记录恢复分区的版本映射。

它们是 Guava Table。Tablet 数量大时，这两个字段会非常大。

### PR4 做了什么

- 给大 Guava Table 字段安装 PR3 的 streaming adapter；
- 给 AbstractJob 的 BackupJob/RestoreJob/CloudRestoreJob 多态分发启用 streaming；
- 用 `enable_backup_restore_job_streaming_json` 在 legacy 和 streaming adapter 之间切换。

### 为什么开关不写进数据格式

两种 adapter 输出相同 JSON schema。开关只是选择“如何生成/读取同一份 JSON”，不是格式版本。

因此 Leader 开启、Follower 关闭，或者写入时开启、重启后恢复 false，都应该可以读取。

### PR4 和 PR5 的区别

PR4 仍使用外层 `GSON.toJson()` 和 Text/String 写入。它去掉的是 Guava Table 和 job 多态分发产生的
JsonElement DOM，但最终完整 JSON String 还存在。

PR5 才会把外层 String 也移除，并直接处理长度前缀 DataInput/DataOutput。

### 一个直观数字

79.7 MB RestoreJob、60 万 snapshot mapping、7.5 万 version mapping 的三次独立 JVM 对照中，
streaming reader 的 sampled peak 中位数比 legacy reader 低约 80.17%，耗时低约 73.86%。

两边 retained heap 几乎相同，因为最终 RestoreJob 对象内容没有变化；减少的是读取过程中的临时树。

## 10. PR5：移除完整 String，并在必要时 spill 到磁盘

### 为什么不能直接边生成 JSON 边写 DataOutput

现有格式要求先写 4-byte length，再写 JSON：

```text
[JSON 长度][JSON bytes]
```

开始生成 JSON 时还不知道最终长度，而 journal/image 的 DataOutput 通常不能回到开头修改 length。

### 三种方案对比

#### 方案 A：全部放进 byte[]

先生成完整 byte[]，知道长度后再写。简单，但大 payload 会重新占满 heap。

#### 方案 B：先计数，再生成第二次

第一遍只计数，第二遍真正写。内存小，但序列化两次。更严重的是 BackupJob/RestoreJob 可能在两遍
之间变化，导致计数长度与第二遍内容不一致。

#### 方案 C：单遍生成 + spill

PR5 选择这个方案：

```text
开始序列化
  -> 前 8 MiB 放内存
  -> 超过 8 MiB 后写 Doris tmp_dir 下的临时文件
  -> 序列化完成，得到精确 length
  -> 写 length
  -> 以 64 KiB 块回放 JSON
  -> 关闭并删除临时文件
```

这样只序列化一次，也不会在序列化成功前污染目标 DataOutput。

### 8 MiB 是什么

它只是“何时从内存转移到磁盘”的阈值，不是最大 JSON 大小。小对象不会碰磁盘，大对象的内存
buffer 被限制在较小范围。

### reader 怎么避免一次性分配

reader 先读取 length，然后给 Gson 一个只能看到这 length 个字节的 bounded stream：

- length 为负数：失败；
- 实际数据少于 length：失败；
- JSON 解析完成但还有未消费字节：失败；
- 不创建与整个 payload 同大小的连续 byte[]；
- 关闭 reader 不会关闭外层 journal/image 输入。

### PR5 迁移哪些地方

- Table / OlapTable；
- BackupMeta；
- BackupJobInfo；
- AbstractJob / BackupJob / RestoreJob；
- Table、Partition、Tablet、Replica 的多态 JSON；
- `OlapTable.selectiveCopy()` 的 deep copy。

`enable_table_meta_streaming_json=false` 和
`enable_backup_restore_job_streaming_json=false` 时仍走 legacy 路径，便于快速回退。

### 为什么临时文件不会变成永久垃圾

spill 使用 Doris `tmp_dir/backup_restore_json_spill`。成功、Gson 序列化失败、目标写失败、目录不可用
和 close 失败都有清理测试；如果主异常发生后清理也失败，清理异常会作为 suppressed exception
保留，不能静默丢失。

## 11. 把五个 PR 串起来看

假设有 20 万 Tablet、每个 3 Replica：

### 只有 PR1

最终 BackupMeta 很小，但 deep copy 过程中仍复制了 60 万 Replica，初始峰值仍可能 OOM。

### 再有 PR2

journal size check 不再额外保留一份完整 byte buffer，但第一次 JSON 生成仍然昂贵。

### 再有 PR3

具备不构造完整 DOM 的工具，但业务入口尚未全部使用。

### 再有 PR4

RestoreJob 大映射和 job subtype 不再构造完整 DOM，但外层 String 仍存在。

### 最后有 PR5

Table/BackupMeta/job 外层可以直接按长度前缀读写，大对象 spill，selective copy 也使用 streaming
deep copy。五种内存放大才形成闭环。

可以记成：

```text
PR1 少写无用内容
PR2 不为计数复制内容
PR3 提供边读边写工具
PR4 先优化 RestoreJob
PR5 完成 BackupMeta/Table 和外层 I/O
```

## 12. 为什么要拆成五个 PR

如果一次提交全部修改，reviewer 很难回答：

- payload 变小是 Replica 删除还是 streaming 导致；
- JSON 不兼容是 adapter、外层 length 还是业务字段导致；
- replay 失败应回滚哪一层；
- 内存收益来自哪项变化。

拆分后：

- PR1、PR2 可以独立合入和回滚；
- PR3 只审通用 JSON 机制；
- PR4 只审 RestoreJob 接入；
- PR5 只审 Table/BackupMeta、外层 I/O 和 spill；
- 出现问题时可以精确关闭 config 或回滚单个 PR。

## 13. 兼容性为什么是本方案的第一优先级

FE 集群升级不是所有节点同时完成。可能出现：

- Leader 是新代码，Follower 是旧配置；
- 写 journal 时 config=true，重启后 config=false；
- image 由 streaming writer 生成，却由 legacy reader 加载；
- RestoreJob 正处于 DOWNLOADING，Leader 发生切换。

所以测试不能只做“新 writer → 新 reader”，必须覆盖：

| Writer | Reader | 要求 |
| --- | --- | --- |
| legacy | legacy | 原行为不变 |
| legacy | streaming | 能读旧内容 |
| streaming | legacy | 能回退 |
| streaming | streaming | 新路径正常 |

还要验证 journal replay、checkpoint image、Follower/Observer 和运行中选主。只做 Gson round-trip
不能证明集群升级安全。

## 14. 普通读者如何审查这组代码

不需要先理解所有 Gson 内部实现，可以按下面顺序提问。

### 第一步：确认数据有没有被错误删除

- PR1 删除的是 detached copy 还是 live table？
- Tablet 拓扑和 ReplicaAllocation 是否还在？
- SnapshotTask 是否仍能找到 live Replica？

### 第二步：确认磁盘格式有没有变化

- 外层是否仍为 4-byte length + UTF-8 JSON？
- 字段名和 subtype label 是否不变？
- legacy/streaming 原始 bytes 是否有直接比较？

### 第三步：确认错误会不会被吞掉

- Writable 失败是否向上抛；
- negative length、截断、尾随数据是否失败；
- spill 目录不可写是否失败；
- 失败后临时文件是否删除。

### 第四步：确认不是只测了工具函数

- 是否经过 JournalEntity 和 EditLog replay；
- 是否经过 BackupHandler image；
- 是否真的执行 BACKUP/RESTORE；
- 是否做过重启和 Follower/Observer 配置不一致。

### 第五步：确认性能数字没有被夸大

- 单个 PR 的收益是否来自单变量对照；
- sampled peak 是否被错误写成精确 allocation；
- 容量测试是否被写成稳定性能比例；
- 组合方案的收益是否被错误归到 PR1 或 PR4。

## 15. 常见问题

### 为什么不直接给 FE 增加 heap？

增大 heap 只能推迟问题。对象数量随 Tablet 和 Replica 线性增长，还会增加 GC 停顿和故障恢复时间。
去掉不必要的对象和完整缓冲才是可扩展解法。

### 为什么不只做压缩？

压缩能减小最终字节，但压缩前可能已经构造了完整对象树、String 和 byte[]。如果压缩输出本身也
放在完整 byte[] 中，峰值仍然存在。这组优化处理的是压缩之前和读回时的内存生命周期。

### streaming 会改变 JSON 吗？

设计目标是不改变。它改变生成和读取方式，而不是字段名或语义。声明 byte-compatible 的路径会
直接比较原始 bytes。

### spill 会把用户数据写到临时目录吗？

spill 内容是 FE 正在持久化的 JSON 元数据，不是 BE tablet 数据文件。文件位于 Doris tmp_dir，
只在 payload 较大时存在，并在回放完成或失败清理后删除。

### 为什么 PR3—PR5 默认关闭？

默认关闭便于先合入兼容基础设施，通过独立 CI 和 reviewer 验证后再由 PR6 决定开启。这样出现问题
时可以动态回到 legacy 路径。若社区不接受 dormant path，则需要在合入前完成 PR6 的全部门禁。

### CloudTablet 有单测，是否等于 Cloud RESTORE 已验证？

不等于。当前完成的是 CloudTablet/CloudReplica 等元数据 subtype 兼容测试；真实 CloudRestoreJob
还需要具备 MetaService 的 Cloud 环境。

### PR2 已经用 counting stream，为什么 PR5 writer 不先 count？

场景不同。PR2 只判断大小，允许丢弃内容；PR5 必须真正写内容。如果 PR5 先 count 再写，就会
序列化两次，而且可变 job 可能两次内容不同，所以它使用单遍 spill。

## 16. 当前方案还有哪些未完成事项

- PR1 的全局 DeepCopy Error 传播修改建议拆为独立前置 PR；
- PR1—PR5 仍需各自完成 Apache CI，不能只依赖组合分支验证；
- reviewer 需要决定是否接受 streaming 默认关闭，并在 PR6 开启；
- 如果上游要求，需要补真实 CloudRestoreJob + MetaService E2E；
- 性能数据进入 PR 时，需要区分单变量收益、方向性实验和容量门槛。

## 17. 五句话总结

1. BACKUP 需要 live Replica 做数据快照，但 BackupMeta 不需要保存这些 Replica。
2. journal size check 只需要数字，不应该保留整份 payload。
3. streaming adapter 直接读写 JSON，避免再造一棵完整 JsonElement 树。
4. PR4 先优化 RestoreJob 内部大字段，PR5 再移除外层 String，并用 spill 控制大 payload 内存。
5. 所有新旧路径必须保持相同格式，并通过 replay、重启、Follower/Observer 和 checkpoint 验证。

## 18. 下一步阅读

理解本文后，可以按以下顺序继续：

1. `backup-restore-memory-optimization-review-guide.md`：中文版技术审查指南；
2. `backup-restore-memory-optimization-review-guide-en.md`：Apache reviewer 英文版；
3. `backup-meta-replica-stripping-principle.md`：PR1 Replica 剥离专项；
4. `backup-restore-memory-optimization-development-plan.md`：提交 hash、测试矩阵和实验数据。
