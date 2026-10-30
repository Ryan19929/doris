# 字段号登记

本地数据复用在社区已有的 thrift 结构、proto 消息和枚举里新增的字段，统一用私有号段，从 7601 起，每个结构里依次递增。社区自己的字段号是从小往上加的（目前最大不超过 1010），私有号段避开它们，以后升级到社区版本或社区新增字段时不会撞号。号段 76xx 与 BE 的错误码 `RESTORE_MANIFEST_MISMATCH`（-7600）同属本功能。

新定义的结构（如 `TRestoreDigestReq`、`TDownloadStats`、`RestoreStagePB`）完全归本功能所有，内部仍从 1 编号。

**改号前**（`9bee08bc11e` 及更早）用的是紧接社区最大号的号码，社区之后合入的行级 TTL（apache/doris#65858）占用了其中的 `TabletMetaPB` 38、`TabletMetaCloudPB` 43、`TFinishTaskRequest` 21。改号前的版本只在测试环境上运行过，没有上线；测试环境上用旧号写下的 tablet meta 会以未知字段的形式保留，不要拿这样的数据去升级社区版本，重建即可。

| 文件 | 结构 | 号 | 字段 |
|---|---|---|---|
| AgentService.thrift | `TUploadReq` | 7601 | `write_manifest` |
| AgentService.thrift | `TRemoteTabletSnapshot` | 7601 | `manifest_root` |
|  |  | 7602 | `incremental` |
| AgentService.thrift | `TDownloadReq` | 7601 | `manifest_roots` |
|  |  | 7602 | `incremental_ranges` |
| AgentService.thrift | `TSnapshotRequest` | 7601 | `compute_digest` |
|  |  | 7602 | `compute_logical_digest` |
|  |  | 7603 | `restore_incremental` |
|  |  | 7604 | `restore_local_source` |
|  |  | 7605 | `job_id` |
|  |  | 7606 | `write_manifest` |
|  |  | 7607 | `restore_kept_load` |
| AgentService.thrift | `TMoveDirReq` | 7601 | `incremental` |
|  |  | 7602 | `backup_logical_digest` |
|  |  | 7603 | `local_source_loaded` |
| AgentService.thrift | `TAgentTaskRequest` | 7601 | `restore_digest_req` |
| MasterService.thrift | `TFinishTaskRequest` | 7601 | `manifest_roots` |
|  |  | 7602 | `manifest_root` |
|  |  | 7603 | `manifest_verified_tablets` |
|  |  | 7604 | `manifest_digest_checked` |
|  |  | 7605 | `download_stats` |
|  |  | 7606 | `logical_digest` |
|  |  | 7607 | `prefix_digest_roots` |
| FrontendService.thrift | `TFrontendPingFrontendResult` | 7601 | `restoreReuseCapability` |
| HeartbeatService.thrift | `TBackendInfo` | 7601 | `restore_reuse_capability` |
| Types.thrift | 枚举 `TTaskType` | 7601 | `RESTORE_DIGEST` |
| olap_file.proto | `TabletMetaPB` | 7601 | `restore_stage` |
| olap_file.proto | `TabletMetaCloudPB` | 7601 | `restore_stage` |

新增字段时：在本表登记，取该结构里的下一个号；回合到其他分支（如 hy-doris branch-3.1）时号码与本表一致。
