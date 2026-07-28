# Backup/Restore 内存优化压测报告

## 一句话结论

在 8 GiB FE 堆下，优化版已稳定完成 8192 Tablets 的 Backup 并实际触发磁盘
spill，而 Doris 4.1.3 正式包在冷 FE 下执行同规模 Backup 时越过 7.5 GiB
安全线，但两者的 8192 Tablets Restore 仍超出当前安全内存范围。

## 1. 测试目的

本报告记录 P1–P5 Backup/Restore 内存优化组合方案的压力测试结果。测试重点是
FE 元数据序列化、反序列化和重放，而不是大数据量传输吞吐。

主要验证以下问题：

1. 元数据达到多大规模时，8 MiB streaming 缓冲区会 spill 到磁盘？
2. 在 8 GiB FE 堆下，Backup 和 Restore 分别可以安全运行到什么规模？
3. 大快照能否在 streaming 开关切换后保持兼容？
4. 取消任务或重启 FE 后，是否遗留 catalog 状态、锁、spill 文件或仓库对象？
5. 与未包含这些优化的 Doris 4.1.3 正式包相比，实际安全边界有什么变化？

## 2. 优化版测试构建与环境

| 项目 | 值 |
|---|---|
| 日期 | 2026-07-28 |
| 主机 | `192.168.9.44` |
| 测试 worktree | `/home/doris-integration-test` |
| 源分支 | `codex/backup-restore-memory-integration` |
| 远程测试 worktree 提交 | `fcfd5f5dadb` |
| 对应个人 remote 提交 | `f5f7e764b3b5c6c56a307f8dfdd0b9510ac8a9e8` |
| 部署方式 | 单 FE + 单 BE，存算一体 |
| FE 堆 | `-Xms8192m -Xmx8192m`，G1 |
| FE 端口 | HTTP 31030，query 32030，RPC 32020，edit log 32010 |
| BE 端口 | HTTP 31040，heartbeat 32050，BE 32060，BRPC 31060 |
| 对象存储 | 已有本地 MinIO，每轮使用独立仓库前缀 |
| 表结构 | 整数 Range 分区，每个源表写入 1 行 |
| 副本数 | 1 |
| Streaming 阈值 | 每个序列化 JSON payload 为 8 MiB |
| 验证的默认分区上限 | 4096 个 Range 分区 |

测试主机同时运行其他 Doris 进程。采样器根据绝对输出路径定位目标 FE 和 BE，
只采集目标进程的资源数据。

压力测试开始前，目标进程状态约为：

- FE RSS：2.1 GiB。
- FE live heap：1.1 GiB。
- BE RSS：8.3 GiB。
- 主机 `MemAvailable`：32 GiB。
- `/home` 可用空间：7 GiB。

## 3. 安全限制

本轮测试目标是确定安全运行边界，不主动制造 OOM：

| 资源 | 停止条件 |
|---|---:|
| FE RSS | 7500 MiB |
| BE RSS | 18000 MiB |
| 主机 `MemAvailable` | 16 GiB |
| `/home` 可用空间 | 4 GiB |
| 单次 Backup 或 Restore | 900 秒 |

达到停止条件后，测试程序会执行 `CANCEL BACKUP` 或 `CANCEL RESTORE`，验证清理
结果，并停止继续扩大规模。

## 4. 测试负载与采样方法

每个阶段执行以下操作：

1. 创建包含 `N` 个整数 Range 分区的表。
2. 512、2048 和 4096 Tablets 阶段，每个分区使用 1 个 bucket。
3. 写入 1 行数据，确保 Restore 正确性不只依赖元数据判断。
4. 执行 streaming Backup 并等待 `FINISHED`。
5. 对同一张表执行 legacy Backup 并等待 `FINISHED`。
6. Restore streaming 快照并验证：
   - 行数为 1；
   - 分区数等于目标值；
   - Restore 任务达到 `FINISHED`。
7. 下一档边界使用 4096 个分区、每分区 2 个 bucket，共 8192 Tablets。

资源采样间隔为 20 ms，记录：

- `/proc/<pid>/status` 中目标 FE、BE 的 RSS；
- 主机 `MemAvailable`；
- `/home` 可用空间；
- FE 打开的、目标路径包含 `backup_restore_json_` 的文件描述符；
- 通过 `/proc/<fe-pid>/fd` 观察到的最大 spill 文件大小。

每次取消任务后以及最终清理后，都会再次检查 spill 目录。

## 5. 优化版原始结果

RSS 是每个操作期间观察到的进程高水位。各阶段顺序执行，因此数值包含前序阶段
已经触碰并驻留的页面，不能当作完全隔离的冷启动 A/B 对比。

| 分区数 | Buckets | Tablets | 操作 | 路径 | 时间（秒） | FE RSS 峰值（MiB） | BE RSS 峰值（MiB） | 主机最小可用内存（MiB） | `/home` 最小可用空间（GiB） | 最大 spill（MiB） | 结果 |
|---:|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---|
| 512 | 1 | 512 | Backup | streaming | 25.981 | 2760.3 | 8725.2 | 31628.9 | 6.89 | 0 | FINISHED |
| 512 | 1 | 512 | Backup | legacy | 27.638 | 3041.7 | 8749.2 | 31284.2 | 6.88 | 0 | FINISHED |
| 512 | 1 | 512 | Restore | streaming | 30.643 | 3346.4 | 8927.3 | 30767.3 | 6.87 | 0 | FINISHED；512 分区；1 行 |
| 2048 | 1 | 2048 | Backup | streaming | 64.345 | 4222.7 | 9056.0 | 29704.4 | 6.82 | 0 | FINISHED |
| 2048 | 1 | 2048 | Backup | legacy | 59.033 | 4394.3 | 9104.4 | 29475.4 | 6.78 | 0 | FINISHED |
| 2048 | 1 | 2048 | Restore | streaming | 74.216 | 4488.3 | 9340.2 | 29010.1 | 6.74 | 0 | FINISHED；2048 分区；1 行 |
| 4096 | 1 | 4096 | Backup | streaming | 110.393 | 5340.1 | 9714.4 | 28117.8 | 6.65 | 0 | FINISHED |
| 4096 | 1 | 4096 | Backup | legacy | 108.457 | 5442.4 | 9587.5 | 27811.5 | 6.58 | 0 | FINISHED |
| 4096 | 1 | 4096 | Restore | streaming | 147.612 | 6315.0 | 9823.9 | 26831.8 | 6.50 | 15.84 | FINISHED；4096 分区；1 行 |
| 4096 | 2 | 8192 | Backup | streaming | 204.317 | 6538.4 | 10242.5 | 26039.3 | 6.30 | 8.48 | FINISHED |
| 4096 | 2 | 8192 | Backup | legacy | 184.073 | 7333.0 | 10189.1 | 25416.2 | 5.72 | 0 | FINISHED |
| 4096 | 2 | 8192 | Restore | streaming | 未完成 | 7500.5 | 未保留 | 高于 16 GiB | 高于 4 GiB | 取消前已观察到 | FE RSS 达到安全线后取消 |

DDL 准备时间：

| 分区数 | Buckets | Tablets | 建表及写入时间（秒） |
|---:|---:|---:|---:|
| 512 | 1 | 512 | 3.596 |
| 2048 | 1 | 2048 | 14.726 |
| 4096 | 1 | 4096 | 31.292 |
| 4096 | 2 | 8192 | 32.528 |

## 6. 优化版边界分析

### 6.1 Backup 边界

- Streaming Backup 已完成 8192 Tablets。
- Backup 期间观察到的最大 spill 文件为 8.48 MiB。
- 这直接证明单个 Backup 表元数据 payload 已超过 8 MiB 内存阈值，并成功通过
  spill 路径继续运行。
- 同规模 legacy Backup 也完成，但顺序进程的 RSS 高水位达到 7333 MiB，已接近
  7500 MiB 安全线。

Streaming 与 legacy 的时间差不能当作严格性能对比：两个操作顺序执行，对象存储
状态不同，并且两次操作之间没有重启进程。

### 6.2 Restore 边界

- Streaming Restore 已完成 4096 Tablets。
- Restore 期间观察到的最大 spill 文件为 15.84 MiB。
- 恢复后的表包含 4096 个分区和预期的源数据。
- 重启 FE 后尝试了 8192 Tablets Restore。
- FE RSS 达到 7,680,516 KiB 后立即取消。
- FE 没有崩溃，也没有出现 OOM 或致命 GC 诊断。

因此，在本次 8 GiB FE 堆和 ASAN 测试部署中：

- Backup 至少已验证到 8192 Tablets。
- Restore 已验证到 4096 Tablets。
- 8192 Tablets Restore 超出设定的安全内存范围。

这是当前环境下的安全边界，不是 Doris 的通用硬限制。

## 7. 正确性、重放与清理验证

压力测试前已完成以下功能 E2E 矩阵：

- legacy writer → streaming reader：通过；
- streaming writer → legacy reader：通过；
- Backup/Restore 数据行数和 checksum：通过；
- streaming Truncate：通过；
- 关闭开关后重启 FE 并执行 EditLog replay：通过；
- 官方 `test_backup_restore_db` 回归测试：10 张表全部通过；
- 定向 FE 测试：71/71 通过；
- 完整 FE、BE 构建：通过。

压力测试结束后：

- 达到安全线后取消的 Restore 任务释放了任务状态和锁；
- 没有遗留压力测试数据库；
- 没有遗留压力测试仓库；
- 没有遗留 spill 文件；
- 4 个 MinIO 压力测试前缀全部删除；
- FE、BE 已重启，以释放 JVM 和 allocator 驻留页面；
- Backend 恢复为 `Alive=true`；
- 未发现 `OutOfMemoryError`、G1 evacuation failure、to-space exhausted 或
  FE `FATAL` 记录；
- `/home` 可用空间当时恢复到约 17 GiB。

清理后目标进程状态：

- FE RSS：约 2.1 GiB；
- FE live heap：约 0.57 GiB；
- BE RSS：约 4.8 GiB；
- Backend 版本：`doris-0.0.0-fcfd5f5dadb`。

## 8. 优化版测试限制

1. 当前是单 FE、单 BE 部署，没有覆盖真实 Cloud MetaService 集群。
2. BE 使用 ASAN 构建，因此绝对 BE 内存不代表 Release 构建。
3. 测试重点是元数据基数，不衡量大数据对象吞吐、压缩比或网络饱和度。
4. 每次只运行一个 Backup 或 Restore。
5. RSS 是进程驻留内存高水位，不等于单次操作的精确 live object 大小。
6. 8192 Tablets Restore 在安全线主动取消，没有进行破坏性 OOM 搜索。

## 9. Doris 4.1.3 未优化基线

### 9.1 正式包与隔离部署

未优化基线使用适合该主机的正式发布包：

| 项目 | 值 |
|---|---|
| 下载地址 | `https://download.selectdb.com/apache-doris-4.1.3-bin-x64-noavx2.tar.gz` |
| 压缩包大小 | 3,597,300,943 字节 |
| SHA-512 | `40254ee5201c74b89d376d23d30ecbda2f563a2509399ca98c9ed1108b2d8de6a5df650a27f6fe61be2409094f4ff70007d4fe3a0278f2195fc9998072bf1bf9` |
| FE、BE 报告的版本 | `doris-4.1.3-rc02-7126cf65d96` |
| 部署目录 | `/home/doris-4.1.3-baseline` |
| FE 端口 | HTTP 37030，query 38030，RPC 38020，edit log 38010 |
| BE 端口 | HTTP 37040，heartbeat 38050，BE 38060，BRPC 37060 |
| FE 堆 | `-Xms8192m -Xmx8192m`，G1 |
| 对象存储 | 使用相同 MinIO，每轮使用独立仓库前缀 |

下载地址的文件名为 4.1.3，但运行时版本字符串包含 `4.1.3-rc02`。报告同时保留
这两个信息，避免将实际运行提交误认为另一个构建。

基线和优化版使用同一主机、相同表结构、1 行源数据、相同副本数、MinIO、20 ms
采样间隔、超时时间和安全限制。基线使用独立的 FE 元数据目录、BE 存储目录、
进程和端口范围。

基线 BE 是 Release 二进制，而优化版 BE 是 ASAN 构建。因此，绝对 BE RSS 和
操作耗时只做记录，不用于直接宣称优化收益。两边 FE 堆大小相同。

第一个正式测量阶段开始前，基线主机状态：

- CPU：32 个逻辑 CPU，Intel Xeon E5-2470 2.30 GHz。
- 内存：总计 96,476 MiB，可用 27,533 MiB。
- Swap：关闭。
- 解压后 `/home` 可用空间：约 19.84 GiB。
- FE RSS：1547.1 MiB；FE live heap：206,214 KiB。
- BE RSS：969.8 MiB。

### 9.2 基线冒烟测试

正式压测前先执行 16 分区、16 Tablets 的 Backup/Restore：

| 操作 | 时间（秒） | FE RSS 峰值（MiB） | BE RSS 峰值（MiB） | 结果 |
|---|---:|---:|---:|---|
| Backup | 16.076 | 1432.5 | 958.9 | FINISHED |
| Restore | 20.118 | 1514.4 | 965.9 | FINISHED；16 分区；16 Tablets；1 行 |

进入正式阶段前，冒烟测试数据库、仓库和 MinIO 前缀均已删除。

### 9.3 基线顺序测试结果

第一轮复用同一个 FE 进程，并按照与优化版相同的顺序逐级增加元数据规模。每个
阶段使用新的数据库、仓库和对象存储前缀，阶段结束后立即清理。

| 分区数 | Buckets | Tablets | 操作 | 时间（秒） | 阶段开始 FE RSS（MiB） | FE RSS 峰值（MiB） | BE RSS 峰值（MiB） | 主机最小可用内存（MiB） | `/home` 最小可用空间（GiB） | 结果 |
|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---|
| 512 | 1 | 512 | Backup | 21.679 | 1554.7 | 1640.0 | 1008.8 | 27198.6 | 19.82 | FINISHED |
| 512 | 1 | 512 | Restore | 21.566 | 同阶段 | 1733.0 | 992.2 | 27121.1 | 19.81 | FINISHED；512 分区；512 Tablets；1 行 |
| 2048 | 1 | 2048 | Backup | 40.839 | 1750.0 | 3142.2 | 1122.9 | 25561.0 | 19.77 | FINISHED |
| 2048 | 1 | 2048 | Restore | 38.076 | 同阶段 | 4155.1 | 1060.9 | 24578.1 | 19.73 | FINISHED；2048 分区；2048 Tablets；1 行 |
| 4096 | 1 | 4096 | Backup | 51.689 | 4197.8 | 6937.5 | 1314.3 | 21621.4 | 19.61 | FINISHED |
| 4096 | 1 | 4096 | Restore | 57.113 | 同阶段 | 7286.9 | 1158.4 | 21292.2 | 19.51 | FINISHED；4096 分区；4096 Tablets；1 行 |
| 4096 | 2 | 8192 | Backup | 87.451 | 7292.8 | 7498.0 | 1432.4 | 20953.2 | 19.36 | FINISHED |
| 4096 | 2 | 8192 | Restore | 未完成 | 同阶段 | 至少 7503.0 | 未保留 | 高于 16 GiB | 高于 4 GiB | FE RSS 达到 7,683,088 KiB 后安全取消 |

DDL 准备时间：

| 分区数 | Buckets | Tablets | 建表及写入时间（秒） |
|---:|---:|---:|---:|
| 512 | 1 | 512 | 1.206 |
| 2048 | 1 | 2048 | 4.016 |
| 4096 | 1 | 4096 | 8.442 |
| 4096 | 2 | 8192 | 8.922 |

顺序测试完成 4096 Tablets 后，FE RSS 仍为 7,467,092 KiB，但
`jcmd GC.heap_info` 报告的 Java 已用堆只有 1,035,083 KiB。这一差异说明顺序
RSS 是驻留页面高水位，不能当作下一次操作的 live object 大小。

### 9.4 冷 FE 边界测试

另外执行两轮测试：创建源表前只重启基线 FE，BE 和存储保持不变，用于区分单次
操作的分配压力与顺序测试的驻留页面历史。

| 分区数 | Buckets | Tablets | 操作 | 时间（秒） | 阶段开始 FE RSS（MiB） | DDL 后 FE RSS（MiB） | FE RSS 峰值（MiB） | 结果 |
|---:|---:|---:|---|---:|---:|---:|---:|---|
| 4096 | 1 | 4096 | Backup | 59.101 | 1816.1 | 2027.0 | 3226.3 | FINISHED |
| 4096 | 1 | 4096 | Restore | 53.113 | 同阶段 | 同阶段 | 4699.7 | FINISHED；4096 分区；4096 Tablets；1 行 |
| 4096 | 2 | 8192 | Backup | 未完成 | 2819.1 | 3366.8 | 至少 7695.9 | FE RSS 达到 7,880,588 KiB 后安全取消 |
| 4096 | 2 | 8192 | Restore | 未开始 | — | — | — | Backup 未完成 |

冷 FE 的 8192 Tablets 测试在 Backup 阶段越过安全线，尚未提交 Restore。
FE 保持存活，可以继续接受清理语句；未出现实际 `OutOfMemoryError`、G1
evacuation failure、to-space exhausted 或致命进程记录。数据库、仓库和 MinIO
前缀均成功删除。

### 9.5 优化版与正式版边界对比

#### 9.5.1 FE 堆大小与已验证 Tablets 规模

当前可以对外表述的容量结果如下：

| FE 堆 | 实现 | Backup 已验证规模 | Backup 不安全点 | Restore 已验证规模 | Restore 不安全点 |
|---:|---|---:|---:|---:|---:|
| 8 GiB | Doris 4.1.3 正式版 | 4096 Tablets | 冷 FE 的 8192 Tablets 在 Backup 阶段达到安全线 | 4096 Tablets | 顺序态 8192 Tablets 达到安全线 |
| 8 GiB | P1–P5 优化版 | 8192 Tablets | 尚未测到下一个档位 | 4096 Tablets | 8192 Tablets 达到安全线 |

因此，在本轮离散测试档位中：

- Backup 的**已验证规模从 4096 提升到 8192 Tablets，即提升到 2 倍**。
- Restore 的已验证规模仍为 4096 Tablets，当前没有观察到容量档位提升。
- 这表示“已经完成正确性验证的规模”扩大 2 倍，不表示正式版的精确极限一定是
  4096；其真实极限位于本轮已通过和已失败档位之间。

如果评审只需要一句定量结果，可以表述为：

> 在相同 8 GiB FE 堆和 7.5 GiB RSS 安全线下，优化版将 Backup 已验证规模从
> 4096 提升到 8192 Tablets，而 Restore 仍维持在 4096 Tablets。

#### 9.5.2 可观察的内存差值

8192 Tablets Backup 的观测结果为：

| 实现与状态 | FE RSS 观测值 | 结果 |
|---|---:|---|
| Doris 4.1.3 正式版，冷 FE | 至少 7695.9 MiB | 达到安全线并取消，Backup 未完成 |
| P1–P5 优化版，顺序态 | 6538.4 MiB | Backup 完成 |

优化版完成任务时的 RSS 比正式版未完成任务时已经观察到的 RSS 少
**1157.5 MiB（约 1.13 GiB，参考降幅 15.04%）**。由于正式版在完成前已经取消，
它的完整峰值只会等于或高于该观测值；但两次运行的 FE 进程历史不同，因此
15.04% 只能作为跨运行参考值，不能作为严格受控的内存降幅结论。

#### 9.5.3 边界解释

在 8 GiB FE 堆下直接观察到的安全边界：

| 路径与进程状态 | Backup 边界 | Restore 边界 |
|---|---|---|
| 优化版顺序测试 | 8192 Tablets 完成；观察到 8.48 MiB spill | 4096 完成；观察到 15.84 MiB spill；8192 安全取消 |
| 4.1.3 正式版顺序测试 | 8192 完成，RSS 达到 7498.0 MiB | 4096 完成；8192 安全取消 |
| 4.1.3 正式版冷 FE 测试 | 4096 完成；8192 Backup 安全取消 | 4096 完成；8192 未进入 Restore |

优化版测试证明 Backup 和 Restore 可以在不把整个序列化 JSON payload 保留在
单个堆缓冲区中的情况下跨越 8 MiB 阈值。正式版冷 FE 测试给出了实际未优化失败
点：8192 Tablets Backup 越过 7.5 GiB RSS 安全线，而优化版 streaming Backup
完成了 8192 Tablets，并使用 8.48 MiB spill 文件。

这说明在当前测试环境中，优化后的 Backup 安全范围更宽，但不能将其描述为严格
吞吐基准或通用的百分比内存降幅，原因包括：

1. 正式版和优化版来自不同源码提交。
2. 优化版 BE 使用 ASAN，正式版 BE 不使用。
3. 主机为共享环境，两轮测试的初始内存和磁盘状态不同。
4. 顺序 RSS 包含以前已经触碰的页面。
5. 优化版 8192 Backup 与正式版冷 FE 8192 Backup 的进程历史不同。

两个路径的 8192 Tablets Restore 都超出 8 GiB 安全范围。Streaming 消除了完整
JSON 字符串或缓冲区的峰值，但没有消除 catalog deep copy、表重建、replay 和
任务状态的全部内存开销。

### 9.6 基线清理与原始数据归档

基线测试结束后：

- 所有测试数据库和仓库均不存在；
- 包括冒烟测试在内的 7 个 MinIO 基线前缀全部删除；
- 两个达到安全线后取消的任务均释放数据库状态并允许清理；
- 基线 FE、BE 已停止；
- 独立安装包、解压目录、元数据和 BE 存储目录已删除；
- `/home` 可用空间从约 20 GiB 恢复到 29 GiB；
- 优化版测试集群和端口未被修改。

基线原始数据归档包含 15 份日志，以及：

- 正式包 SHA-512 文件；
- 所有原始日志的 SHA-256 清单；
- 测试前、测试后环境快照；
- 每阶段的 `CONTEXT`、`STAGE` 和 `RESULT` JSON 行；
- 每阶段后的 RSS、`jcmd GC.heap_info`、磁盘、Backend 和错误扫描输出。

本地归档目录：

`/private/tmp/doris-4.1.3-baseline-raw`

从测试主机复制后，已根据 `MANIFEST.sha256` 逐项验证全部文件。

## 10. 最终结论

1. P1–P5 组合方案的 streaming JSON 磁盘 spill 路径已在真实 Backup 和 Restore
   中触发，不再只是单元测试覆盖。
2. 在本次 8 GiB FE 环境中，优化版 Backup 已验证到 8192 Tablets，正式版冷 FE
   在同规模 Backup 阶段达到安全线。
3. 优化版与正式版的 4096 Tablets Backup/Restore 均能完成并通过数据校验。
4. 8192 Tablets Restore 在两种路径下仍超出当前安全范围，后续优化应重点关注
   catalog deep copy、Restore 表重建和任务状态峰值，而不仅是 JSON buffer。
5. 本轮未发现 OOM、进程崩溃、数据损坏、锁泄漏、spill 文件泄漏或对象存储
   前缀残留。
