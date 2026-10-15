# Restore / CCR 全量同步的本地数据复用

目标是让 Restore 和 CCR 全量同步时，目标端已有且内容一致的数据不再重复下载。证明不了一致就照常下载，结果与完整下载相同；复用相关的能力默认关闭。注意"复用开关全关"不等于没有任何新行为：备份文件清单（Manifest）默认会生成，文件集合与大小校验默认开启，备份侧会多一次文件清单的计算和存储，开销见 [manifest-design.md](./manifest-design.md)。

## 代码位置

本分支 `restore-r2`，基于 apache/doris master `14c957e048b`。

## 文档

| 文档 | 内容 |
| --- | --- |
| [overview.md](./overview.md) | 总体方案：目标与原则、地基 / R0 / R2 三部分的架构、决策门、路线与风险 |
| [manifest-design.md](./manifest-design.md) | 地基：版本化 Manifest、强摘要与备份完整性校验的设计 |
| [r0-observation-design.md](./r0-observation-design.md) | R0 观测：统计既有血缘复用的硬链接数、下载数与未匹配原因 |
| [r2-prototype-research.md](./r2-prototype-research.md) | R2 原型调研：现有校验和读取路径的代码核对，以及逻辑摘要的实现选型 |
| [r2-detailed-design.md](./r2-detailed-design.md) | R2 详细设计：分区级逻辑摘要复用、增量补齐（含 MoR 与带 DELETE 条件的 MoW）、原子恢复路径 |
| [incremental-digest-research.md](./incremental-digest-research.md) | 摘要增量化调研：按 rowset 缓存摘要并在 compaction 时继承，使摘要开销与变化量成正比 |
| [timing-experiment.md](./timing-experiment.md) | 端到端耗时实验设计，以及测试环境下已完成的摸底与补充实验结果 |
| [stuck-append-analysis.md](./stuck-append-analysis.md) | 原子恢复 + 增量补齐卡在 COMMITTING 的根因分析 |
| [stuck-append-fix-plan.md](./stuck-append-fix-plan.md) | 上述问题的修复方案（已两轮审查） |

## 复用范围（首版）

- **只复用 Duplicate 表。** Unique 表（MoW、MoR）的删除标记行、被覆盖的旧版本等隐藏状态不在摘要里，摘要相同的两个分区，之后同一笔写入的结果可能不同，所以默认整体下载，原因记为 `HIDDEN_STATE_MOW`、`HIDDEN_STATE_MOR`。FE 配置 `restore_reuse_allow_hidden_state_models`（默认 false）可以恢复旧行为，仅供实验，不保证一致。
- **默认每个复用的分区都有摘要证明。** `restore_reuse_default_check_level` 默认 `full`；`off`、`sample` 默认也按 `full` 执行，除非打开 `restore_reuse_allow_unverified_keep`（默认 false，仅供实验和观测，不提供一致性保证）。`SHOW RESTORE` 里同时显示请求的档位和实际生效的档位。
- **谱系戳带来源集群 ID。** 备份记录源端集群 ID，谱系戳随之记录，两边都有且不相等时不作为候选。集群 ID 是初始化时生成的随机整数，不是全局唯一标识，只用来缩小候选范围，不当作证明；任何一边缺失（旧备份、旧谱系戳）时仍可作为候选，由摘要兜底。
- **"查询结果相同"的含义。** 摘要证明的是在备份版本上的可见数据相同，不含隐藏列（如 `__DORIS_VERSION_COL__`，它随写入历史而不同，`show_hidden_columns=true` 时可见）。

## 当前状态

已实现：

- 谱系戳；
- 备份完整性校验；
- 复用观测；
- 分区级复用（逻辑摘要）；
- 增量补齐（实现上含 MoR、带 DELETE 条件的 MoW，但首版按上面的范围，默认只对 Duplicate 表生效）；
- 原子恢复路径；
- 摘要缓存修复。

**已知未修复**：原子恢复 + 增量补齐在多副本下，可能因副本修复竞争卡在 COMMITTING。根因和修复方案见 [stuck-append-analysis.md](./stuck-append-analysis.md) 和 [stuck-append-fix-plan.md](./stuck-append-fix-plan.md)，修复方案已审查、尚未实现。

测试环境的实测结论：灾备来回切换演练中，下载量减少约 71%，错误复用为 0；在下载较慢的单机测试环境、数据量较大时，端到端耗时加速约 2～3 倍，数据量较小（约 2 GB）时因固定开销占大头只有约 1.2～1.6 倍。目标规模和万兆网络下的收益仍需复测。

## 部署与回滚

先说明保护机制，再说步骤。

**自动避开混跑。** FE 和 BE 在心跳里上报复用能力（`restore_reuse_capability`，本版本为 1，旧版本不带这个字段，按 0 处理）。恢复作业开始判定复用时，检查所有 FOLLOWER FE（Master 自己算，Observer 不会接管作业，不检查）和所有存活的 BE：任何一个能力小于 1，或者主节点还没收到它的心跳，本作业就不复用，整体下载，不下发摘要任务、本地来源快照和增量补齐，并在 `SHOW RESTORE` 的 `ReuseEstimate` 里记录原因 `MIXED_VERSIONS`。开关是 FE 配置 `restore_reuse_require_capability`（可动态修改，默认 true）；关掉它等于不检查，只用于实验。这个检查只在作业开始时做一次，作业进行中发生的升级、降级或切主，由下面的文件层保护和步骤兜底。

**文件层保护。**

- 只含增量的快照，其元数据文件命名为 `<tablet_id>.hdr.increment`，目录里没有 `<tablet_id>.hdr`。旧版本 BE 的整体替换找不到 `.hdr` 会直接失败，不会把增量当成完整快照去替换 tablet；新版本 BE 的整体替换发现 `.hdr.increment` 同样拒绝，返回不可重试错误，tablet 不变。
- 快照与备份清单不符，BE 以 `INTERNAL_ERROR` 回报，错误信息带前缀 `[RESTORE_MANIFEST_MISMATCH]`，FE 按前缀识别并立即取消作业。没有新增 thrift 状态码，旧 FE 收到的是它认识的错误，不会出错。

**升级。**

1. 先升级所有 BE，再升级所有 FE。升级期间保持复用开关关闭（`enable_restore_partition_reuse`、`enable_restore_incremental_append`、`enable_restore_atomic_reuse`）。
2. 全部节点升级完成后，确认心跳已经收到（`restore_reuse_require_capability` 为 true 时，作业会自己判断），再打开复用开关。

**回滚。**

1. 先关闭上面三个复用开关。
2. 等待或取消所有在途的恢复作业：用 `SHOW RESTORE` 检查，确认没有处于 PENDING 到 COMMITTING 之间的作业，必要时 `CANCEL RESTORE`。关闭开关不会撤销进行中作业已经做出的复用决定，所以这一步不能省。
3. 先回滚 FE，再回滚 BE。
4. 回滚可能丢失谱系戳：旧版本 FE 写镜像或者重写包含分区、表的编辑日志时，会丢掉它不认识的字段（谱系戳、来源等）。丢失是安全的，只是失去复用依据；再次升级后的第一个恢复作业会整体下载，之后重新建立谱系戳。BE 上的 `restore_stage` 可能被旧版本保留，也可能丢失，二者都没有害处：判定时会核对作业 ID 和基础版本，证据丢失时作业被取消。

**作业期间意外切到旧 FE。** 旧 FE 不认识复用相关的字段，会把增量作业当作普通恢复继续下发整体替换。此时由文件层保护兜底：BE 找不到 `.hdr` 会让任务失败，不会冲掉本地数据。旧 FE 会一直重发任务直到作业超时，所以发现后应该手动 `CANCEL RESTORE`；原子恢复下原表不受影响，非原子恢复按“重新恢复一次”处理。保留分区那一侧不会出现数据错误，只是没有按预期保留。升级后重新恢复一次即可。
