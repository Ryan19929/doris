# Restore / CCR 全量同步的本地数据复用

目标是让 Restore 和 CCR 全量同步时，目标端已有且内容一致的数据不再重复下载。证明不了一致就照常下载，结果与完整下载相同；所有新能力默认关闭。

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

## 当前状态

已实现：

- 谱系戳；
- 备份完整性校验；
- 复用观测；
- 分区级复用（逻辑摘要）；
- 增量补齐（含 MoR、带 DELETE 条件的 MoW）；
- 原子恢复路径；
- 摘要缓存修复。

**已知未修复**：原子恢复 + 增量补齐在多副本下，可能因副本修复竞争卡在 COMMITTING。根因和修复方案见 [stuck-append-analysis.md](./stuck-append-analysis.md) 和 [stuck-append-fix-plan.md](./stuck-append-fix-plan.md)，修复方案已审查、尚未实现。

测试环境的实测结论：灾备来回切换演练中，下载量减少约 71%，错误复用为 0；在下载较慢的单机测试环境、数据量较大时，端到端耗时加速约 2～3 倍，数据量较小（约 2 GB）时因固定开销占大头只有约 1.2～1.6 倍。目标规模和万兆网络下的收益仍需复测。
