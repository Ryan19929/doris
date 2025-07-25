// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.cloud.transaction;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.proto.Cloud.AbortSubTxnRequest;
import org.apache.doris.cloud.proto.Cloud.AbortSubTxnResponse;
import org.apache.doris.cloud.proto.Cloud.AbortTxnRequest;
import org.apache.doris.cloud.proto.Cloud.AbortTxnResponse;
import org.apache.doris.cloud.proto.Cloud.AbortTxnWithCoordinatorRequest;
import org.apache.doris.cloud.proto.Cloud.AbortTxnWithCoordinatorResponse;
import org.apache.doris.cloud.proto.Cloud.BeginSubTxnRequest;
import org.apache.doris.cloud.proto.Cloud.BeginSubTxnResponse;
import org.apache.doris.cloud.proto.Cloud.BeginTxnRequest;
import org.apache.doris.cloud.proto.Cloud.BeginTxnResponse;
import org.apache.doris.cloud.proto.Cloud.CheckTxnConflictRequest;
import org.apache.doris.cloud.proto.Cloud.CheckTxnConflictResponse;
import org.apache.doris.cloud.proto.Cloud.CleanTxnLabelRequest;
import org.apache.doris.cloud.proto.Cloud.CleanTxnLabelResponse;
import org.apache.doris.cloud.proto.Cloud.CommitTxnRequest;
import org.apache.doris.cloud.proto.Cloud.CommitTxnResponse;
import org.apache.doris.cloud.proto.Cloud.GetCurrentMaxTxnRequest;
import org.apache.doris.cloud.proto.Cloud.GetCurrentMaxTxnResponse;
import org.apache.doris.cloud.proto.Cloud.GetDeleteBitmapUpdateLockRequest;
import org.apache.doris.cloud.proto.Cloud.GetDeleteBitmapUpdateLockResponse;
import org.apache.doris.cloud.proto.Cloud.GetTxnIdRequest;
import org.apache.doris.cloud.proto.Cloud.GetTxnIdResponse;
import org.apache.doris.cloud.proto.Cloud.GetTxnRequest;
import org.apache.doris.cloud.proto.Cloud.GetTxnResponse;
import org.apache.doris.cloud.proto.Cloud.LoadJobSourceTypePB;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.proto.Cloud.MetaServiceResponseStatus;
import org.apache.doris.cloud.proto.Cloud.PrecommitTxnRequest;
import org.apache.doris.cloud.proto.Cloud.PrecommitTxnResponse;
import org.apache.doris.cloud.proto.Cloud.RemoveDeleteBitmapUpdateLockRequest;
import org.apache.doris.cloud.proto.Cloud.RemoveDeleteBitmapUpdateLockResponse;
import org.apache.doris.cloud.proto.Cloud.SubTxnInfo;
import org.apache.doris.cloud.proto.Cloud.TableStatsPB;
import org.apache.doris.cloud.proto.Cloud.TabletIndexPB;
import org.apache.doris.cloud.proto.Cloud.TxnInfoPB;
import org.apache.doris.cloud.proto.Cloud.TxnStatusPB;
import org.apache.doris.cloud.proto.Cloud.UniqueIdPB;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugPointUtil.DebugPoint;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.event.DataChangeEvent;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.persist.BatchRemoveTransactionsOperation;
import org.apache.doris.persist.BatchRemoveTransactionsOperationV2;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CalcDeleteBitmapTask;
import org.apache.doris.thrift.TCalcDeleteBitmapPartitionInfo;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.SubTransactionState;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionCommitFailedException;
import org.apache.doris.transaction.TransactionIdGenerator;
import org.apache.doris.transaction.TransactionNotFoundException;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.transaction.TransactionUtil;
import org.apache.doris.transaction.TxnCommitAttachment;
import org.apache.doris.transaction.TxnStateCallbackFactory;
import org.apache.doris.transaction.TxnStateChangeCallback;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CloudGlobalTransactionMgr implements GlobalTransactionMgrIface {
    private static final Logger LOG = LogManager.getLogger(CloudGlobalTransactionMgr.class);
    private static final String NOT_SUPPORTED_MSG = "Not supported in cloud mode";

    class CommitCostTimeStatistic {
        long waitCommitLockCostTimeMs = 0;
        long waitDeleteBitmapLockCostTimeMs = 0;
        long calculateDeleteBitmapCostTimeMs = 0;

        long commitToMsCostTimeMs = 0;

        public long getCommitToMsCostTimeMs() {
            return commitToMsCostTimeMs;
        }

        public void setCommitToMsCostTimeMs(long commitToMsCostTimeMs) {
            this.commitToMsCostTimeMs = commitToMsCostTimeMs;
        }

        public long getCalculateDeleteBitmapCostTimeMs() {
            return calculateDeleteBitmapCostTimeMs;
        }

        public void setCalculateDeleteBitmapCostTimeMs(long calculateDeleteBitmapCostTimeMs) {
            this.calculateDeleteBitmapCostTimeMs = calculateDeleteBitmapCostTimeMs;
        }

        public long getWaitCommitLockCostTimeMs() {
            return waitCommitLockCostTimeMs;
        }

        public void setWaitCommitLockCostTimeMs(long waitCommitLockCostTimeMs) {
            this.waitCommitLockCostTimeMs = waitCommitLockCostTimeMs;
        }

        public long getWaitDeleteBitmapLockCostTimeMs() {
            return waitDeleteBitmapLockCostTimeMs;
        }

        public void setWaitDeleteBitmapLockCostTimeMs(long waitDeleteBitmapLockCostTimeMs) {
            this.waitDeleteBitmapLockCostTimeMs = waitDeleteBitmapLockCostTimeMs;
        }
    }

    private TxnStateCallbackFactory callbackFactory;
    private final Map<Long, Long> subTxnIdToTxnId = new ConcurrentHashMap<>();
    private Map<Long, AtomicInteger> waitToCommitTxnCountMap = new ConcurrentHashMap<>();
    private Map<Long, CommitCostTimeStatistic> commitCostTimeStatisticMap = new ConcurrentHashMap<>();

    // dbId -> tableId -> txnId
    private Map<Long, Map<Long, Long>> lastTxnIdMap = Maps.newConcurrentMap();
    // dbId -> txnId -> signature
    private Map<Long, Map<Long, Long>> txnLastSignatureMap = Maps.newConcurrentMap();

    public CloudGlobalTransactionMgr() {
        this.callbackFactory = new TxnStateCallbackFactory();
    }

    public void setEditLog(EditLog editLog) {
        //do nothing
    }

    @Override
    public TxnStateCallbackFactory getCallbackFactory() {
        return callbackFactory;
    }

    @Override
    public void addDatabaseTransactionMgr(Long dbId) {
        // do nothing in cloud mode
    }

    @Override
    public void removeDatabaseTransactionMgr(Long dbId) {
        // do nothing in cloud mode
    }

    @Override
    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TxnCoordinator coordinator,
            LoadJobSourceType sourceType, long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException {
        return beginTransaction(dbId, tableIdList, label, null, coordinator, sourceType, -1, timeoutSecond);
    }

    @Override
    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TUniqueId requestId,
            TxnCoordinator coordinator, LoadJobSourceType sourceType, long listenerId, long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException {

        LOG.info("try to begin transaction, dbId: {}, label: {}", dbId, label);
        if (Config.disable_load_job) {
            throw new AnalysisException("disable_load_job is set to true, all load jobs are prevented");
        }

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        if (!coordinator.isFromInternal) {
            InternalDatabaseUtil.checkDatabase(db.getFullName(), ConnectContext.get());
        }

        MTMVUtil.checkModifyMTMVData(db, tableIdList, ConnectContext.get());

        switch (sourceType) {
            case BACKEND_STREAMING:
                checkValidTimeoutSecond(timeoutSecond, Config.max_stream_load_timeout_second,
                        Config.min_load_timeout_second, sourceType);
                break;
            default:
                checkValidTimeoutSecond(timeoutSecond, Config.max_load_timeout_second,
                        Config.min_load_timeout_second, sourceType);
        }

        BeginTxnResponse beginTxnResponse = null;
        int retryTime = 0;

        try {
            Preconditions.checkNotNull(coordinator);
            Preconditions.checkNotNull(label);
            FeNameFormat.checkLabel(label);

            TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
            txnInfoBuilder.setDbId(dbId);
            txnInfoBuilder.addAllTableIds(tableIdList);
            txnInfoBuilder.setLabel(label);
            txnInfoBuilder.setListenerId(listenerId);

            if (requestId != null) {
                UniqueIdPB.Builder uniqueIdBuilder = UniqueIdPB.newBuilder();
                uniqueIdBuilder.setHi(requestId.getHi());
                uniqueIdBuilder.setLo(requestId.getLo());
                txnInfoBuilder.setRequestId(uniqueIdBuilder);
            }

            txnInfoBuilder.setCoordinator(TxnUtil.txnCoordinatorToPb(coordinator));
            txnInfoBuilder.setLoadJobSourceType(LoadJobSourceTypePB.forNumber(sourceType.value()));
            txnInfoBuilder.setTimeoutMs(timeoutSecond * 1000);
            txnInfoBuilder.setPrecommitTimeoutMs(Config.stream_load_default_precommit_timeout_second * 1000);

            final BeginTxnRequest beginTxnRequest = BeginTxnRequest.newBuilder()
                    .setTxnInfo(txnInfoBuilder.build())
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .build();

            while (retryTime < Config.metaServiceRpcRetryTimes()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("retryTime:{}, beginTxnRequest:{}", retryTime, beginTxnRequest);
                }
                beginTxnResponse = MetaServiceProxy.getInstance().beginTxn(beginTxnRequest);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("retryTime:{}, beginTxnResponse:{}", retryTime, beginTxnResponse);
                }

                if (beginTxnResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                LOG.info("beginTxn KV_TXN_CONFLICT, retryTime:{}", retryTime);
                backoff();
                retryTime++;
            }

            Preconditions.checkNotNull(beginTxnResponse);
            Preconditions.checkNotNull(beginTxnResponse.getStatus());
        } catch (Exception e) {
            LOG.warn("beginTxn failed, exception:", e);
            throw new BeginTransactionException("beginTxn failed, errMsg:" + e.getMessage());
        }

        if (beginTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            switch (beginTxnResponse.getStatus().getCode()) {
                case TXN_DUPLICATED_REQ:
                    throw new DuplicatedRequestException(DebugUtil.printId(requestId),
                            beginTxnResponse.getDupTxnId(), beginTxnResponse.getStatus().getMsg());
                case TXN_LABEL_ALREADY_USED:
                    throw new LabelAlreadyUsedException(beginTxnResponse.getStatus().getMsg(), false,
                            beginTxnResponse.getTxnStatus());
                default:
                    if (MetricRepo.isInit) {
                        MetricRepo.COUNTER_TXN_REJECT.increase(1L);
                    }
                    throw new BeginTransactionException(beginTxnResponse.getStatus().getMsg());
            }
        }

        long txnId = beginTxnResponse.getTxnId();
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_TXN_BEGIN.increase(1L);
        }
        return txnId;
    }

    @Override
    public void preCommitTransaction2PC(Database db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        LOG.info("try to precommit transaction: {}", transactionId);
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }

        PrecommitTxnRequest.Builder builder = PrecommitTxnRequest.newBuilder();
        builder.setDbId(db.getId());
        builder.setTxnId(transactionId);

        if (txnCommitAttachment != null) {
            if (txnCommitAttachment instanceof LoadJobFinalOperation) {
                LoadJobFinalOperation loadJobFinalOperation = (LoadJobFinalOperation) txnCommitAttachment;
                builder.setCommitAttachment(TxnUtil
                        .loadJobFinalOperationToPb(loadJobFinalOperation));
            } else {
                throw new UserException("Invalid txnCommitAttachment");
            }
        }

        builder.setPrecommitTimeoutMs(timeoutMillis);

        final PrecommitTxnRequest precommitTxnRequest = builder.build();
        PrecommitTxnResponse precommitTxnResponse = null;
        try {
            LOG.info("precommitTxnRequest: {}", precommitTxnRequest);
            precommitTxnResponse = MetaServiceProxy
                    .getInstance().precommitTxn(precommitTxnRequest);
            LOG.info("precommitTxnResponse: {}", precommitTxnResponse);
        } catch (RpcException e) {
            throw new UserException(e.getMessage());
        }

        if (precommitTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            throw new UserException(precommitTxnResponse.getStatus().getMsg());
        }
    }

    @Deprecated
    @Override
    public void commitTransactionWithoutLock(long dbId, List<Table> tableList,
            long transactionId, List<TabletCommitInfo> tabletCommitInfos)
            throws UserException {
        commitTransactionWithoutLock(dbId, tableList, transactionId, tabletCommitInfos, null);
    }

    @Override
    public void commitTransactionWithoutLock(long dbId, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        List<OlapTable> mowTableList = getMowTableList(tableList, tabletCommitInfos);
        try {
            LOG.info("try to commit transaction, transactionId: {}, tableIds: {}", transactionId,
                    tableList.stream().map(Table::getId).collect(Collectors.toList()));
            Map<Long, List<TCalcDeleteBitmapPartitionInfo>> backendToPartitionInfos = null;
            if (!mowTableList.isEmpty()) {
                if (!checkTransactionStateBeforeCommit(dbId, transactionId)) {
                    return;
                }
                DeleteBitmapUpdateLockContext lockContext = new DeleteBitmapUpdateLockContext(transactionId);
                getDeleteBitmapUpdateLock(transactionId, mowTableList, tabletCommitInfos, lockContext);
                if (lockContext.getBackendToPartitionTablets().isEmpty()) {
                    throw new UserException(
                            "The partition info is empty, table may be dropped, txnid=" + transactionId);
                }
                backendToPartitionInfos = getCalcDeleteBitmapInfo(lockContext, null);
            }
            commitTransactionWithoutLock(dbId, tableList, transactionId, tabletCommitInfos, txnCommitAttachment, false,
                    mowTableList, backendToPartitionInfos);
        } catch (Exception e) {
            if (!mowTableList.isEmpty()) {
                LOG.warn("commit txn {} failed, release delete bitmap lock, catch exception {}", transactionId,
                        e.getMessage());
                removeDeleteBitmapUpdateLock(mowTableList, transactionId);
            }
            throw e;
        }
    }

    private boolean checkTransactionStateBeforeCommit(long dbId, long transactionId)
            throws TransactionCommitFailedException {
        // if this txn has been calculated by previously task but commit rpc is timeout,
        // be will send another commit request to fe, so if txn is committed or visible,
        // no need to calculate delete bitmap again, just return ok to be to finish this commit.
        TransactionState transactionState = Env.getCurrentGlobalTransactionMgr()
                .getTransactionState(dbId, transactionId);
        if (transactionState == null) {
            throw new TransactionCommitFailedException("txn does not exist: " + transactionId);
        }
        if (null != transactionState.getTransactionStatus()) {
            if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
                throw new TransactionCommitFailedException("transaction [" + transactionId
                        + "] is already aborted. abort reason: " + transactionState.getReason());
            } else if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED
                    || transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                LOG.info("txn={}, status={} not need to calculate delete bitmap again, just return ",
                        transactionId,
                        transactionState.getTransactionStatus().toString());
                return false;
            } else if (transactionState.getTransactionStatus() == TransactionStatus.PREPARE) {
                LOG.info("txn={}, status={} need to calculate delete bitmap", transactionId,
                        transactionState.getTransactionStatus().toString());
                return true;
            } else {
                throw new TransactionCommitFailedException("transaction [" + transactionId
                        + "] status is: " + transactionState.getTransactionStatus().toString());
            }
        } else {
            throw new TransactionCommitFailedException("transaction [" + transactionId
                    + "] status is null ");
        }
    }

    /**
     * Post process of commitTxn
     * 1. update some stats
     * 2. produce event for further processes like async MV
     * @param commitTxnResponse commit txn call response from meta-service
     */
    public void afterCommitTxnResp(CommitTxnResponse commitTxnResponse) {
        // ========================================
        // update some table stats
        // ========================================
        long dbId = commitTxnResponse.getTxnInfo().getDbId();
        long txnId = commitTxnResponse.getTxnInfo().getTxnId();
        // 1. update rowCountfor AnalysisManager
        Map<Long, Long> updatedRows = new HashMap<>();
        for (TableStatsPB tableStats : commitTxnResponse.getTableStatsList()) {
            LOG.info("Update RowCount for AnalysisManager. transactionId:{}, table_id:{}, updated_row_count:{}",
                    txnId, tableStats.getTableId(), tableStats.getUpdatedRowCount());
            updatedRows.put(tableStats.getTableId(), tableStats.getUpdatedRowCount());
        }
        Env env = Env.getCurrentEnv();
        env.getAnalysisManager().updateUpdatedRows(updatedRows);
        // 2. notify partition first load
        int totalPartitionNum = commitTxnResponse.getPartitionIdsList().size();
        // a map to record <tableId, [firstLoadPartitionIds]>
        Map<Long, List<Long>> tablePartitionMap = Maps.newHashMap();
        for (int idx = 0; idx < totalPartitionNum; ++idx) {
            long version = commitTxnResponse.getVersions(idx);
            long tableId = commitTxnResponse.getTableIds(idx);
            if (version == 2) {
                // inform AnalysisManager first load partitions
                tablePartitionMap.computeIfAbsent(tableId, k -> Lists.newArrayList());
                tablePartitionMap.get(tableId).add(commitTxnResponse.getPartitionIds(idx));
            }
            // 3. update CloudPartition
            OlapTable olapTable = (OlapTable) env.getInternalCatalog().getDb(dbId)
                    .flatMap(db -> db.getTable(tableId)).filter(t -> t.isManagedTable())
                    .orElse(null);
            if (olapTable == null) {
                continue;
            }
            CloudPartition partition = (CloudPartition) olapTable.getPartition(
                    commitTxnResponse.getPartitionIds(idx));
            if (partition == null) {
                continue;
            }
            if (version == 2) {
                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)
                    .stream().forEach(i -> i.setRowCountReported(false));
            }
            partition.setCachedVisibleVersion(version, commitTxnResponse.getVersionUpdateTimeMs());
            LOG.info("Update Partition. transactionId:{}, table_id:{}, partition_id:{}, version:{}, update time:{}",
                    txnId, tableId, partition.getId(), version, commitTxnResponse.getVersionUpdateTimeMs());
        }
        env.getAnalysisManager().setNewPartitionLoaded(
                tablePartitionMap.keySet().stream().collect(Collectors.toList()));
        // tablePartitionMap to string
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Long, List<Long>> entry : tablePartitionMap.entrySet()) {
            sb.append(entry.getKey()).append(":[");
            for (Long partitionId : entry.getValue()) {
                sb.append(partitionId).append(",");
            }
            sb.append("];");
        }
        if (sb.length() > 0) {
            LOG.info("notify partition first load. {}", sb);
        }

        // ========================================
        // produce event
        // ========================================
        List<Long> tableList = commitTxnResponse.getTxnInfo().getTableIdsList()
                .stream().distinct().collect(Collectors.toList());
        // Here, we only wait for the EventProcessor to finish processing the event,
        // but regardless of the success or failure of the result,
        // it does not affect the logic of transaction
        try {
            for (Long tableId : tableList) {
                Env.getCurrentEnv().getEventProcessor().processEvent(
                    new DataChangeEvent(InternalCatalog.INTERNAL_CATALOG_ID, dbId, tableId));
            }
        } catch (Throwable t) {
            // According to normal logic, no exceptions will be thrown,
            // but in order to avoid bugs affecting the original logic, all exceptions are caught
            LOG.warn("produceEvent failed, db {}, tables {} ", dbId, tableList, t);
        }
    }

    private Set<Long> getBaseTabletsFromTables(List<Table> tableList, List<TabletCommitInfo> tabletCommitInfos)
            throws MetaNotFoundException {
        Set<Long> baseTabletIds = Sets.newHashSet();
        if (tabletCommitInfos == null || tabletCommitInfos.isEmpty()) {
            return baseTabletIds;
        }
        for (Table table : tableList) {
            OlapTable olapTable = (OlapTable) table;
            try {
                olapTable.readLock();
                olapTable.getPartitions().stream()
                        .map(Partition::getBaseIndex)
                        .map(MaterializedIndex::getTablets)
                        .flatMap(Collection::stream)
                        .map(Tablet::getId)
                        .forEach(baseTabletIds::add);
            } finally {
                olapTable.readUnlock();
            }
        }
        Set<Long> tabletIds = tabletCommitInfos.stream().map(TabletCommitInfo::getTabletId).collect(Collectors.toSet());
        baseTabletIds.retainAll(tabletIds);
        LOG.debug("baseTabletIds: {}", baseTabletIds);

        return baseTabletIds;
    }

    private void commitTransactionWithoutLock(long dbId, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment, boolean is2PC,
            List<OlapTable> mowTableList, Map<Long, List<TCalcDeleteBitmapPartitionInfo>> backendToPartitionInfos)
            throws UserException {
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException(
                    "disable_load_job is set to true, all load jobs are not allowed");
        }

        if (!mowTableList.isEmpty()) {
            List<Long> mowTableIds = mowTableList.stream().map(Table::getId).collect(Collectors.toList());
            sendCalcDeleteBitmaptask(dbId, transactionId, backendToPartitionInfos, mowTableIds,
                    Config.calculate_delete_bitmap_task_timeout_seconds);
        }

        CommitTxnRequest.Builder builder = CommitTxnRequest.newBuilder();
        builder.setDbId(dbId)
                .setTxnId(transactionId)
                .setIs2Pc(is2PC)
                .setCloudUniqueId(Config.cloud_unique_id)
                .addAllBaseTabletIds(getBaseTabletsFromTables(tableList, tabletCommitInfos))
                .setEnableTxnLazyCommit(Config.enable_cloud_txn_lazy_commit);
        for (OlapTable olapTable : mowTableList) {
            builder.addMowTableIds(olapTable.getId());
        }

        if (txnCommitAttachment != null) {
            if (txnCommitAttachment instanceof LoadJobFinalOperation) {
                LoadJobFinalOperation loadJobFinalOperation = (LoadJobFinalOperation) txnCommitAttachment;
                builder.setCommitAttachment(TxnUtil
                        .loadJobFinalOperationToPb(loadJobFinalOperation));
            } else if (txnCommitAttachment instanceof RLTaskTxnCommitAttachment) {
                RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment = (RLTaskTxnCommitAttachment) txnCommitAttachment;
                TxnStateChangeCallback cb = callbackFactory.getCallback(rlTaskTxnCommitAttachment.getJobId());
                if (cb != null) {
                    // use a temporary transaction state to do before commit check,
                    // what actually works is the transactionId
                    TransactionState tmpTxnState = new TransactionState();
                    tmpTxnState.setTransactionId(transactionId);
                    cb.beforeCommitted(tmpTxnState);
                }
                builder.setCommitAttachment(TxnUtil
                        .rlTaskTxnCommitAttachmentToPb(rlTaskTxnCommitAttachment));
            } else {
                throw new UserException("invalid txnCommitAttachment");
            }
        }

        final CommitTxnRequest commitTxnRequest = builder.build();
        executeCommitTxnRequest(commitTxnRequest, transactionId, is2PC, txnCommitAttachment);
    }

    private void executeCommitTxnRequest(CommitTxnRequest commitTxnRequest, long transactionId, boolean is2PC,
            TxnCommitAttachment txnCommitAttachment) throws UserException {
        if (DebugPointUtil.isEnable("FE.mow.commit.exception")) {
            LOG.info("debug point FE.mow.commit.exception, throw e");
            throw new UserException(InternalErrorCode.INTERNAL_ERR, "debug point FE.mow.commit.exception");
        }
        if (DebugPointUtil.isEnable("CloudGlobalTransactionMgr.executeCommitTxnRequest.block")) {
            LOG.info("debug point: block at CloudGlobalTransactionMgr.executeCommitTxnRequest.block");
            while (DebugPointUtil.isEnable("CloudGlobalTransactionMgr.executeCommitTxnRequest.block")) {
                try {
                    Thread.sleep(800);
                } catch (InterruptedException e) {
                    LOG.info("error ", e);
                }
            }
            LOG.info("debug point: leave CloudGlobalTransactionMgr.executeCommitTxnRequest.block");
        }
        boolean txnOperated = false;
        TransactionState txnState = null;
        TxnStateChangeCallback cb = null;
        long callbackId = 0L;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        try {
            txnState = commitTxn(commitTxnRequest, transactionId, is2PC);
            txnOperated = true;
            if (DebugPointUtil.isEnable("CloudGlobalTransactionMgr.commitTransaction.timeout")) {
                throw new UserException(InternalErrorCode.DELETE_BITMAP_LOCK_ERR,
                        "test delete bitmap update lock timeout, transactionId:" + transactionId);
            }
        } finally {
            if (txnCommitAttachment != null && txnCommitAttachment instanceof RLTaskTxnCommitAttachment) {
                RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment = (RLTaskTxnCommitAttachment) txnCommitAttachment;
                callbackId = rlTaskTxnCommitAttachment.getJobId();
            } else if (txnState != null) {
                callbackId = txnState.getCallbackId();
            }

            cb = callbackFactory.getCallback(callbackId);
            if (cb != null) {
                LOG.info("commitTxn, run txn callback, transactionId:{} callbackId:{}, txnState:{}",
                        transactionId, callbackId, txnState);
                cb.afterCommitted(txnState, txnOperated);
                // do not exectue afterVisible if commit txn fail in cloud mode
                if (txnOperated) {
                    cb.afterVisible(txnState, txnOperated);
                }
            }
            long costTime = stopWatch.getTime();
            if (MetricRepo.isInit) {
                MetricRepo.HISTO_COMMIT_TO_MS_LATENCY.update(costTime);
            }
            if (commitCostTimeStatisticMap.containsKey(transactionId)) {
                commitCostTimeStatisticMap.get(transactionId).setCommitToMsCostTimeMs(costTime);
            }
        }
    }

    private TransactionState commitTxn(CommitTxnRequest commitTxnRequest, long transactionId, boolean is2PC)
            throws UserException {
        CommitTxnResponse commitTxnResponse = null;
        TransactionState txnState = null;
        int retryTime = 0;

        try {
            while (retryTime < Config.metaServiceRpcRetryTimes()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("retryTime:{}, commitTxnRequest:{}", retryTime, commitTxnRequest);
                }
                commitTxnResponse = MetaServiceProxy.getInstance().commitTxn(commitTxnRequest);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("retryTime:{}, commitTxnResponse:{}", retryTime, commitTxnResponse);
                }
                if (commitTxnResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                // sleep random [20, 200] ms, avoid txn conflict
                LOG.info("commitTxn KV_TXN_CONFLICT, transactionId:{}, retryTime:{}", transactionId, retryTime);
                backoff();
                retryTime++;
            }

            Preconditions.checkNotNull(commitTxnResponse);
            Preconditions.checkNotNull(commitTxnResponse.getStatus());
        } catch (Exception e) {
            LOG.warn("commitTxn failed, transactionId:{}, exception:", transactionId, e);
            throw new UserException("commitTxn() failed, errMsg:" + e.getMessage());
        }

        if (is2PC && (commitTxnResponse.getStatus().getCode() == MetaServiceCode.TXN_ALREADY_VISIBLE
                || commitTxnResponse.getStatus().getCode() == MetaServiceCode.TXN_ALREADY_ABORTED)) {
            throw new UserException(commitTxnResponse.getStatus().getMsg());
        }
        if (commitTxnResponse.getStatus().getCode() != MetaServiceCode.OK
                && commitTxnResponse.getStatus().getCode() != MetaServiceCode.TXN_ALREADY_VISIBLE) {
            LOG.warn("commitTxn failed, transactionId:{}, retryTime:{}, commitTxnResponse:{}",
                    transactionId, retryTime, commitTxnResponse);
            if (commitTxnResponse.getStatus().getCode() == MetaServiceCode.LOCK_EXPIRED) {
                // DELETE_BITMAP_LOCK_ERR will be retried on be
                throw new UserException(InternalErrorCode.DELETE_BITMAP_LOCK_ERR,
                        "delete bitmap update lock expired, transactionId:" + transactionId);
            }
            StringBuilder internalMsgBuilder =
                    new StringBuilder("commitTxn failed, transactionId:");
            internalMsgBuilder.append(transactionId);
            internalMsgBuilder.append(", code:");
            internalMsgBuilder.append(commitTxnResponse.getStatus().getCode());
            internalMsgBuilder.append(", msg:");
            internalMsgBuilder.append(commitTxnResponse.getStatus().getMsg());
            throw new UserException("internal error, " + internalMsgBuilder.toString());
        }

        txnState = TxnUtil.transactionStateFromPb(commitTxnResponse.getTxnInfo());
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_TXN_SUCCESS.increase(1L);
            MetricRepo.HISTO_TXN_EXEC_LATENCY.update(txnState.getCommitTime() - txnState.getPrepareTime());
        }
        afterCommitTxnResp(commitTxnResponse);
        return txnState;
    }

    // return mow tables with contains tablet commit info
    private List<OlapTable> getMowTableList(List<Table> tableList, List<TabletCommitInfo> tabletCommitInfos) {
        if (tabletCommitInfos == null || tabletCommitInfos.isEmpty()) {
            return Lists.newArrayList();
        }
        List<OlapTable> mowTableList = new ArrayList<>();
        TabletInvertedIndex tabletInvertedIndex = Env.getCurrentEnv().getTabletInvertedIndex();
        for (Table table : tableList) {
            if (!(table instanceof OlapTable)) {
                continue;
            }
            OlapTable olapTable = (OlapTable) table;
            if (!olapTable.getEnableUniqueKeyMergeOnWrite()) {
                continue;
            }
            boolean hasTabletCommitInfo = tabletCommitInfos.stream().anyMatch(ci -> {
                TabletMeta tabletMeta = tabletInvertedIndex.getTabletMeta(ci.getTabletId());
                return tabletMeta != null && tabletMeta.getTableId() == olapTable.getId();
            });
            if (hasTabletCommitInfo) {
                mowTableList.add(olapTable);
            }
        }
        return mowTableList;
    }

    private Map<Long, List<Long>> getPartitionSubTxnIds(List<SubTransactionState> subTransactionStates,
            Map<Long, List<Long>> tableToTabletList, Map<Long, TabletMeta> tabletToTabletMeta) {
        if (subTransactionStates == null) {
            return null;
        }
        Map<Long, List<Long>> partitionToSubTxnIds = Maps.newHashMap();
        for (SubTransactionState subTransactionState : subTransactionStates) {
            if (!tableToTabletList.containsKey(subTransactionState.getTable().getId())) {
                // skip non mow table
                continue;
            }
            for (TTabletCommitInfo ci : subTransactionState.getTabletCommitInfos()) {
                TabletMeta tabletMeta = tabletToTabletMeta.get(ci.getTabletId());
                long partitionId = tabletMeta.getPartitionId();
                List<Long> subTxnIds = partitionToSubTxnIds.computeIfAbsent(partitionId, k -> Lists.newArrayList());
                if (!subTxnIds.contains(subTransactionState.getSubTransactionId())) {
                    subTxnIds.add(subTransactionState.getSubTransactionId());
                }
            }
        }
        return partitionToSubTxnIds;
    }

    private void getPartitionInfo(List<OlapTable> tableList,
            List<TabletCommitInfo> tabletCommitInfos,
            DeleteBitmapUpdateLockContext lockContext) throws MetaNotFoundException {
        Map<Long, OlapTable> tableMap = Maps.newHashMap();
        for (OlapTable olapTable : tableList) {
            tableMap.put(olapTable.getId(), olapTable);
        }

        List<Long> tabletIds = tabletCommitInfos.stream()
                .map(TabletCommitInfo::getTabletId).collect(Collectors.toList());
        TabletInvertedIndex tabletInvertedIndex = Env.getCurrentEnv().getTabletInvertedIndex();
        List<TabletMeta> tabletMetaList = tabletInvertedIndex.getTabletMetaList(tabletIds);
        for (int i = 0; i < tabletMetaList.size(); i++) {
            long tabletId = tabletIds.get(i);
            if (lockContext.getTabletToTabletMeta().containsKey(tabletId)) {
                continue;
            }
            TabletMeta tabletMeta = tabletMetaList.get(i);
            long tableId = tabletMeta.getTableId();
            if (!tableMap.containsKey(tableId)) {
                continue;
            }

            lockContext.getTabletToTabletMeta().put(tabletId, tabletMeta);

            List<Long> tableTabletIds = lockContext.getTableToTabletList()
                    .computeIfAbsent(tableId, k -> Lists.newArrayList());
            if (!tableTabletIds.contains(tabletId)) {
                tableTabletIds.add(tabletId);
            }

            long partitionId = tabletMeta.getPartitionId();
            long backendId = tabletCommitInfos.get(i).getBackendId();

            if (!lockContext.getTableToPartitions().containsKey(tableId)) {
                lockContext.getTableToPartitions().put(tableId, Sets.newHashSet());
            }
            lockContext.getTableToPartitions().get(tableId).add(partitionId);

            if (!lockContext.getBackendToPartitionTablets().containsKey(backendId)) {
                lockContext.getBackendToPartitionTablets().put(backendId, Maps.newHashMap());
            }
            Map<Long, Set<Long>> partitionToTablets = lockContext.getBackendToPartitionTablets().get(backendId);
            if (!partitionToTablets.containsKey(partitionId)) {
                partitionToTablets.put(partitionId, Sets.newHashSet());
            }
            partitionToTablets.get(partitionId).add(tabletId);
            Partition partition = tableMap.get(tableId).getPartition(partitionId);
            if (partition == null) {
                throw new MetaNotFoundException("partition " + partitionId + " does not exist");
            }
            lockContext.getPartitions().putIfAbsent(partitionId, partition);
        }
        if (!tableList.isEmpty() && !tabletCommitInfos.isEmpty() && lockContext.getTableToTabletList().isEmpty()) {
            String tableListDebugStr = tableList.stream().map(table -> String.valueOf(table.getId()))
                    .collect(Collectors.joining(", "));
            String tabletMetaDebugStr = tabletMetaList.stream().map(TabletMeta::toString)
                    .collect(Collectors.joining("\n"));
            LOG.warn(
                    "getPartitionInfo for lock_id: {} failed, LockContext.TableToTabletList is empty,"
                            + " this should never happen. tableList: {}, tabletIds: {}, tabletMetaList: {}",
                    lockContext.getLockId(), tableListDebugStr, tabletIds.toString(), tabletMetaDebugStr);
        }
    }

    private Map<Long, Long> getPartitionVersions(Map<Long, Partition> partitionMap) {
        Map<Long, Long> partitionToVersions = Maps.newHashMap();
        partitionMap.forEach((key, value) -> {
            long visibleVersion = value.getVisibleVersion();
            long newVersion = visibleVersion <= 0 ? 2 : visibleVersion + 1;
            partitionToVersions.put(key, newVersion);
        });
        return partitionToVersions;
    }

    private Map<Long, List<TCalcDeleteBitmapPartitionInfo>> getCalcDeleteBitmapInfo(
            DeleteBitmapUpdateLockContext lockContext, Map<Long, List<Long>> partitionToSubTxnIds) {
        Map<Long, List<TCalcDeleteBitmapPartitionInfo>> backendToPartitionInfos = Maps.newHashMap();
        Map<Long, Long> partitionVersions = getPartitionVersions(lockContext.getPartitions());
        for (Map.Entry<Long, Map<Long, Set<Long>>> entry : lockContext.getBackendToPartitionTablets().entrySet()) {
            List<TCalcDeleteBitmapPartitionInfo> partitionInfos = Lists.newArrayList();
            for (Map.Entry<Long, Set<Long>> partitionToTablets : entry.getValue().entrySet()) {
                Long partitionId = partitionToTablets.getKey();
                Set<Long> tabletList = partitionToTablets.getValue();
                TCalcDeleteBitmapPartitionInfo partitionInfo = new TCalcDeleteBitmapPartitionInfo(partitionId,
                        partitionVersions.get(partitionId),
                        Lists.newArrayList(tabletList));
                if (!lockContext.getBaseCompactionCnts().isEmpty()
                        && !lockContext.getCumulativeCompactionCnts().isEmpty()
                        && !lockContext.getCumulativePoints().isEmpty()) {
                    boolean hasTabletStats = !lockContext.getTabletStates().isEmpty();

                    List<Long> reqBaseCompactionCnts = Lists.newArrayList();
                    List<Long> reqCumulativeCompactionCnts = Lists.newArrayList();
                    List<Long> reqCumulativePoints = Lists.newArrayList();
                    List<Long> reqTabletStates = Lists.newArrayList();
                    for (long tabletId : tabletList) {
                        reqBaseCompactionCnts.add(lockContext.getBaseCompactionCnts().get(tabletId));
                        reqCumulativeCompactionCnts.add(lockContext.getCumulativeCompactionCnts().get(tabletId));
                        reqCumulativePoints.add(lockContext.getCumulativePoints().get(tabletId));
                        if (hasTabletStats) {
                            reqTabletStates.add(lockContext.getTabletStates().get(tabletId));
                        }
                    }
                    partitionInfo.setBaseCompactionCnts(reqBaseCompactionCnts);
                    partitionInfo.setCumulativeCompactionCnts(reqCumulativeCompactionCnts);
                    partitionInfo.setCumulativePoints(reqCumulativePoints);
                    if (hasTabletStats) {
                        partitionInfo.setTabletStates(reqTabletStates);
                    }
                    if (partitionToSubTxnIds != null) {
                        List<Long> subTxnIds = partitionToSubTxnIds.get(partitionId);
                        if (subTxnIds != null && !subTxnIds.isEmpty()) {
                            partitionInfo.setSubTxnIds(subTxnIds);
                            LOG.debug("partitionId: {}, subTxnIds: {}", partitionId, subTxnIds);
                        }
                    }
                }
                partitionInfos.add(partitionInfo);
            }
            backendToPartitionInfos.put(entry.getKey(), partitionInfos);
        }
        return backendToPartitionInfos;
    }

    private void getDeleteBitmapUpdateLock(long transactionId, List<OlapTable> mowTableList,
            List<TabletCommitInfo> tabletCommitInfos, DeleteBitmapUpdateLockContext lockContext)
            throws UserException {
        if (DebugPointUtil.isEnable("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.sleep")) {
            DebugPoint debugPoint = DebugPointUtil.getDebugPoint(
                    "CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.sleep");
            int t = debugPoint.param("sleep_time", 8);
            try {
                Thread.sleep(t * 1000);
            } catch (InterruptedException e) {
                LOG.info("error ", e);
            }
        }
        if (DebugPointUtil.isEnable("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")) {
            LOG.info("debug point: block at CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait");
            DebugPoint debugPoint = DebugPointUtil.getDebugPoint(
                    "CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait");
            String token = debugPoint.param("token", "invalid_token");
            while (DebugPointUtil.isEnable("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")) {
                DebugPoint blockDebugPoint = DebugPointUtil.getDebugPoint(
                        "CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block");
                String passToken = blockDebugPoint.param("pass_token", "");
                if (token.equals(passToken)) {
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOG.info("error ", e);
                }
            }
            LOG.info("debug point: leave CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait");
        }
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        int totalRetryTime = 0;
        String retryMsg = "";
        boolean res = false;
        long startTime = System.currentTimeMillis();
        try {
            getPartitionInfo(mowTableList, tabletCommitInfos, lockContext);
            for (Map.Entry<Long, Set<Long>> entry : lockContext.getTableToPartitions().entrySet()) {
                GetDeleteBitmapUpdateLockRequest.Builder builder = GetDeleteBitmapUpdateLockRequest.newBuilder();
                long tableId = entry.getKey();
                builder.setTableId(tableId).setLockId(transactionId).setInitiator(-1)
                        .setExpiration(Config.delete_bitmap_lock_expiration_seconds).setRequireCompactionStats(true);
                List<Long> tabletList = lockContext.getTableToTabletList().get(tableId);
                for (Long tabletId : tabletList) {
                    TabletMeta tabletMeta = lockContext.getTabletToTabletMeta().get(tabletId);
                    TabletIndexPB.Builder tabletIndexBuilder = TabletIndexPB.newBuilder();
                    tabletIndexBuilder.setDbId(tabletMeta.getDbId());
                    tabletIndexBuilder.setTableId(tabletMeta.getTableId());
                    tabletIndexBuilder.setIndexId(tabletMeta.getIndexId());
                    tabletIndexBuilder.setPartitionId(tabletMeta.getPartitionId());
                    tabletIndexBuilder.setTabletId(tabletId);
                    builder.addTabletIndexes(tabletIndexBuilder);
                }
                GetDeleteBitmapUpdateLockResponse response = null;

                int retryTime = 0;
                while (retryTime++ < Config.metaServiceRpcRetryTimes()) {
                    long waitTime = System.currentTimeMillis() - startTime;
                    boolean enableUrgentLock = Config.enable_mow_load_force_take_ms_lock;
                    long remainedTime = Config.mow_load_force_take_ms_lock_threshold_ms - waitTime;
                    boolean urgent = enableUrgentLock && remainedTime < 0;
                    if (urgent) {
                        LOG.info("try to get delete bitmap lock with urgent flag, tableId={}, txnId={}",
                                tableId, transactionId);
                    }
                    builder.setUrgent(urgent);
                    GetDeleteBitmapUpdateLockRequest request = builder.build();
                    try {
                        response = MetaServiceProxy.getInstance().getDeleteBitmapUpdateLock(request);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("get delete bitmap lock, transactionId={}, Request: {}, Response: {}",
                                    transactionId,
                                    request, response);
                        }
                        if (DebugPointUtil.isEnable("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.conflict")) {
                            DebugPoint debugPoint = DebugPointUtil.getDebugPoint(
                                    "CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.conflict");
                            double percent = debugPoint.param("percent", 0.4);
                            long timestamp = System.currentTimeMillis();
                            Random random = new Random(timestamp);
                            if (Math.abs(random.nextInt()) % 100 < 100 * percent) {
                                LOG.info("set kv txn conflict for test");
                                GetDeleteBitmapUpdateLockResponse.Builder getLockResponseBuilder
                                        = GetDeleteBitmapUpdateLockResponse.newBuilder();
                                getLockResponseBuilder.setStatus(MetaServiceResponseStatus.newBuilder()
                                        .setCode(MetaServiceCode.KV_TXN_CONFLICT_RETRY_EXCEEDED_MAX_TIMES)
                                        .setMsg("kv txn conflict"));
                                response = getLockResponseBuilder.build();
                            }
                        }
                        if (response.getStatus().getCode() != MetaServiceCode.LOCK_CONFLICT
                                && response.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                            break;
                        }
                    } catch (Exception e) {
                        LOG.warn("ignore get delete bitmap lock exception, transactionId={}, retryTime={}, tableIds={}",
                                transactionId,
                                retryTime, mowTableList.stream().map(Table::getId).collect(Collectors.toList()), e);
                    }
                    retryMsg = response.toString();
                    if (DebugPointUtil.isEnable("FE.mow.check.lock.release")
                            && response.getStatus().getCode() == MetaServiceCode.LOCK_CONFLICT) {
                        throw new UserException(InternalErrorCode.INTERNAL_ERR,
                                "check delete bitmap lock release fail,response is " + response
                                        + ", tableList=(" + StringUtils.join(mowTableList, ",") + ")");
                    }

                    // don't sleep if it's urgent
                    if (!urgent) {
                        long base = Config.mow_get_ms_lock_retry_backoff_base;
                        long interval = Config.mow_get_ms_lock_retry_backoff_interval;
                        if (Config.enable_mow_load_force_take_ms_lock
                                && interval > Config.mow_load_force_take_ms_lock_threshold_ms) {
                            interval = Config.mow_load_force_take_ms_lock_threshold_ms;
                        }
                        long randomMillis = base + (long) (Math.random() * interval);
                        if (enableUrgentLock && randomMillis > remainedTime) {
                            randomMillis = remainedTime;
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("randomMillis:{}", randomMillis);
                        }
                        try {
                            Thread.sleep(randomMillis);
                        } catch (InterruptedException e) {
                            LOG.info("InterruptedException: ", e);
                        }
                    }
                }
                Preconditions.checkNotNull(response);
                Preconditions.checkNotNull(response.getStatus());
                if (DebugPointUtil.isEnable("FE.mow.get_delete_bitmap_lock.fail")) {
                    throw new UserException(InternalErrorCode.DELETE_BITMAP_LOCK_ERR,
                            "test get_delete_bitmap_lock fail");
                }
                if (response.getStatus().getCode() != MetaServiceCode.OK) {
                    LOG.warn("get delete bitmap lock failed, transactionId={}, for {} times, response:{}",
                            transactionId,
                            retryTime, response);
                    if (response.getStatus().getCode() == MetaServiceCode.LOCK_CONFLICT
                            || response.getStatus().getCode() == MetaServiceCode.KV_TXN_CONFLICT
                            || response.getStatus().getCode()
                            == MetaServiceCode.KV_TXN_CONFLICT_RETRY_EXCEEDED_MAX_TIMES) {
                        // DELETE_BITMAP_LOCK_ERR will be retried on be
                        throw new UserException(InternalErrorCode.DELETE_BITMAP_LOCK_ERR,
                                "Failed to get delete bitmap lock due to conflict");
                    }
                    throw new UserException(
                            "Failed to get delete bitmap lock, msg: " + response.getStatus().getMsg() + ", code: "
                                    + response.getStatus().getCode());
                }

                // record tablet's latest compaction stats from meta service and send them to BEs
                // to let BEs eliminate unnecessary sync_rowsets() calls if possible
                List<Long> respBaseCompactionCnts = response.getBaseCompactionCntsList();
                List<Long> respCumulativeCompactionCnts = response.getCumulativeCompactionCntsList();
                List<Long> respCumulativePoints = response.getCumulativePointsList();
                List<Long> respTabletStates = response.getTabletStatesList();
                int size1 = respBaseCompactionCnts.size();
                int size2 = respCumulativeCompactionCnts.size();
                int size3 = respCumulativePoints.size();
                int size4 = respTabletStates.size();
                if (size1 != tabletList.size() || size2 != tabletList.size() || size3 != tabletList.size()
                        || (size4 > 0 && size4 != tabletList.size())) {
                    throw new UserException("The size of returned compaction cnts can't match the size of tabletList, "
                            + "tabletList.size()=" + tabletList.size() + ", respBaseCompactionCnts.size()=" + size1
                            + ", respCumulativeCompactionCnts.size()=" + size2 + ", respCumulativePoints.size()="
                            + size3
                            + ", respTabletStates.size()=" + size4);
                }
                for (int i = 0; i < tabletList.size(); i++) {
                    long tabletId = tabletList.get(i);
                    lockContext.getBaseCompactionCnts().put(tabletId, respBaseCompactionCnts.get(i));
                    lockContext.getCumulativeCompactionCnts().put(tabletId, respCumulativeCompactionCnts.get(i));
                    lockContext.getCumulativePoints().put(tabletId, respCumulativePoints.get(i));
                    if (size4 > 0) {
                        lockContext.getTabletStates().put(tabletId, respTabletStates.get(i));
                    }
                }
                totalRetryTime += retryTime;
            }
            res = true;
        } finally {
            stopWatch.stop();
            long costTime = stopWatch.getTime();
            if (MetricRepo.isInit) {
                MetricRepo.HISTO_GET_DELETE_BITMAP_UPDATE_LOCK_LATENCY.update(costTime);
            }
            if (commitCostTimeStatisticMap.containsKey(transactionId)) {
                commitCostTimeStatisticMap.get(transactionId).setWaitDeleteBitmapLockCostTimeMs(costTime);
            }
            String status = res ? "successfully" : "fail";
            if (costTime > 1000) {
                LOG.info("get delete bitmap lock {} . txnId: {}. totalRetryTime: {}. "
                                + "tableSize: {}. cost: {} ms. tableIds: {}. retryMsg: {}.", status,
                        transactionId, totalRetryTime, lockContext.getTableToPartitions().size(), costTime,
                        mowTableList.stream().map(Table::getId).collect(Collectors.toList()), retryMsg);
            }
        }
    }

    private void removeDeleteBitmapUpdateLock(List<OlapTable> tableList, long transactionId) {
        for (OlapTable table : tableList) {
            RemoveDeleteBitmapUpdateLockRequest.Builder builder = RemoveDeleteBitmapUpdateLockRequest.newBuilder();
            builder.setTableId(table.getId())
                    .setLockId(transactionId)
                    .setInitiator(-1);
            final RemoveDeleteBitmapUpdateLockRequest request = builder.build();
            RemoveDeleteBitmapUpdateLockResponse response = null;
            try {
                response = MetaServiceProxy.getInstance().removeDeleteBitmapUpdateLock(request);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("remove delete bitmap lock, transactionId={}, Request: {}, Response: {}",
                            transactionId, request, response);
                }
                Preconditions.checkNotNull(response);
                Preconditions.checkNotNull(response.getStatus());
                if (response.getStatus().getCode() != MetaServiceCode.OK) {
                    LOG.warn("remove delete bitmap lock failed, transactionId={}, response:{}",
                            transactionId, response);
                }
            } catch (Exception e) {
                LOG.warn("ignore get delete bitmap lock exception, transactionId={}, exception={}",
                        transactionId, e);
            }
        }
    }

    private void sendCalcDeleteBitmaptask(long dbId, long transactionId,
            Map<Long, List<TCalcDeleteBitmapPartitionInfo>> backendToPartitionInfos,
            List<Long> mowTableIds,
            long calculateDeleteBitmapTaskTimeoutSeconds) throws UserException {
        if (backendToPartitionInfos == null) {
            throw new UserException("failed to send calculate delete bitmap task to be,transactionId=" + transactionId
                    + ",but backendToPartitionInfos is null");
        }
        if (mowTableIds == null) {
            throw new UserException("failed to send calculate delete bitmap task to be, transactionId=" + transactionId
                    + ", because mowTableIds is null");
        }
        if (backendToPartitionInfos.isEmpty()) {
            return;
        }
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        boolean res = false;
        try {
            int totalTaskNum = backendToPartitionInfos.size();
            MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<Long, Long>(
                    totalTaskNum);
            AgentBatchTask batchTask = new AgentBatchTask();

            long signature = getTxnLastSignature(dbId, transactionId);
            if (signature == -1) {
                // use txn_id as signature for every txn in first time
                signature = transactionId;
            } else {
                // If there exists other interleaved txns on the same table before,
                // we can not accept the response of previous calc delete bitmap task which is created
                // by the current transaction before because the delete bitmap written in those tasks
                // maybe be removed in MS.
                // So we change the signature to avoid to accept the response of previous calc delete bitmap task
                boolean hasOtherInterleavedTxnBefore = mowTableIds.stream()
                        .anyMatch(tableId -> {
                            long lastTxnId = getTableLastTxnId(dbId, tableId);
                            return lastTxnId != -1 && lastTxnId != transactionId;
                        });
                if (hasOtherInterleavedTxnBefore) {
                    signature = UUID.randomUUID().getLeastSignificantBits();
                }
            }
            if (DebugPointUtil.isEnable("sendCalcDbmtask.change_signature")) {
                signature = UUID.randomUUID().getLeastSignificantBits();
            }
            setTxnLastSignature(dbId, transactionId, signature);
            for (long tableId : mowTableIds) {
                setTableLastTxnId(dbId, tableId, transactionId);
            }

            for (Map.Entry<Long, List<TCalcDeleteBitmapPartitionInfo>> entry : backendToPartitionInfos.entrySet()) {
                CalcDeleteBitmapTask task = new CalcDeleteBitmapTask(entry.getKey(),
                        transactionId,
                        dbId,
                        entry.getValue(),
                        signature,
                        countDownLatch);
                countDownLatch.addMark(entry.getKey(), transactionId);
                // add to AgentTaskQueue for handling finish report.
                // not check return value, because the add will success
                AgentTaskQueue.addTask(task);
                batchTask.addTask(task);
                LOG.info("send calculate delete bitmap task to be {}, txn_id {}, signature {}, partitionInfos={}",
                        entry.getKey(), transactionId, signature, entry.getValue());
            }
            AgentTaskExecutor.submit(batchTask);

            boolean ok;
            try {
                ok = countDownLatch.await(calculateDeleteBitmapTaskTimeoutSeconds, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
                ok = false;
            }

            if (!ok || !countDownLatch.getStatus().ok()) {
                String errMsg = "Failed to calculate delete bitmap.";
                // clear tasks
                AgentTaskQueue.removeBatchTask(batchTask, TTaskType.CALCULATE_DELETE_BITMAP);

                if (!countDownLatch.getStatus().ok()) {
                    errMsg += countDownLatch.getStatus().getErrorMsg();
                    if (countDownLatch.getStatus().getErrorCode() != TStatusCode.DELETE_BITMAP_LOCK_ERROR) {
                        throw new UserException(errMsg);
                    }
                } else {
                    errMsg += " Timeout.";
                    List<Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
                    // only show at most 3 results
                    List<Entry<Long, Long>> subList = unfinishedMarks.subList(0,
                            Math.min(unfinishedMarks.size(), 3));
                    if (!subList.isEmpty()) {
                        errMsg += " Unfinished mark: " + Joiner.on(", ").join(subList);
                    }
                }
                LOG.warn(errMsg);
                // DELETE_BITMAP_LOCK_ERR will be retried on be
                throw new UserException(InternalErrorCode.DELETE_BITMAP_LOCK_ERR, errMsg);
            } else {
                // Sometimes BE calc delete bitmap succeed, but FE wait timeout for some unknown reasons,
                // FE will retry the calculation on BE, this debug point simulates such situation.
                debugCalcDeleteBitmapRandomTimeout();
            }
            res = true;
        } finally {
            stopWatch.stop();
            long costTime = stopWatch.getTime();
            if (MetricRepo.isInit) {
                MetricRepo.HISTO_CALCULATE_DELETE_BITMAP_LATENCY.update(costTime);
            }
            if (commitCostTimeStatisticMap.containsKey(transactionId)) {
                commitCostTimeStatisticMap.get(transactionId).setCalculateDeleteBitmapCostTimeMs(costTime);
            }
            String status = res ? "successfully" : "fail";
            if (costTime > 1000) {
                LOG.info("calc delete bitmap task {}. txns: {}. time cost: {} ms.",
                        status, transactionId, stopWatch.getTime());
            }
        }
    }

    private void debugCalcDeleteBitmapRandomTimeout() throws UserException {
        DebugPoint debugPoint = DebugPointUtil.getDebugPoint(
                "CloudGlobalTransactionMgr.calc_delete_bitmap_random_timeout");
        if (debugPoint == null) {
            return;
        }

        double percent = debugPoint.param("percent", 0.5);
        if (new SecureRandom().nextInt() % 100 < 100 * percent) {
            throw new UserException(InternalErrorCode.DELETE_BITMAP_LOCK_ERR,
                    "DebugPoint: Failed to calculate delete bitmap: Timeout.");
        }
    }

    @Override
    public boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList, long transactionId,
                                               List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis)
            throws UserException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        int retryTimes = 0;
        boolean res = false;
        try {
            while (true) {
                try {
                    res = commitAndPublishTransaction(db, tableList, transactionId, tabletCommitInfos, timeoutMillis,
                            null);
                    break;
                } catch (UserException e) {
                    LOG.warn("failed to commit txn, txnId={},retryTimes={},exception={}",
                            transactionId, retryTimes, e);
                    // only mow table will catch DELETE_BITMAP_LOCK_ERR and need to retry
                    if (e.getErrorCode() == InternalErrorCode.DELETE_BITMAP_LOCK_ERR) {
                        retryTimes++;
                        if (retryTimes >= Config.mow_calculate_delete_bitmap_retry_times) {
                            // should throw exception after running out of retry times
                            throw e;
                        }
                    } else {
                        throw e;
                    }
                }
            }
        } finally {
            stopWatch.stop();
            long commitAndPublishTime = stopWatch.getTime();
            LOG.info("commitAndPublishTransaction txn {} cost {} ms", transactionId, commitAndPublishTime);
            if (MetricRepo.isInit) {
                MetricRepo.HISTO_COMMIT_AND_PUBLISH_LATENCY.update(commitAndPublishTime);
            }
            clearTxnLastSignature(db.getId(), transactionId);
        }
        return res;
    }

    @Override
    public boolean commitAndPublishTransaction(DatabaseIf db, long transactionId,
            List<SubTransactionState> subTransactionStates, long timeoutMillis) throws UserException {
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException(
                    "disable_load_job is set to true, all load jobs are not allowed");
        }
        LOG.info("try to commit transaction, txnId: {}, subTxnStates: {}", transactionId, subTransactionStates);

        Preconditions.checkState(db instanceof Database);
        List<Long> tableIdList = subTransactionStates.stream().map(s -> s.getTable().getId()).distinct()
                .collect(Collectors.toList());
        List<Table> tableList = ((Database) db).getTablesOnIdOrderOrThrowException(tableIdList);
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        beforeCommitTransaction(tableList, transactionId, timeoutMillis);
        List<TabletCommitInfo> tabletCommitInfos = subTransactionStates.stream().map(
                        SubTransactionState::getTabletCommitInfos).flatMap(Collection::stream)
                .map(c -> new TabletCommitInfo(c.getTabletId(), c.getBackendId())).collect(Collectors.toList());
        List<OlapTable> mowTableList = getMowTableList(tableList, tabletCommitInfos);
        try {
            Map<Long, List<TCalcDeleteBitmapPartitionInfo>> backendToPartitionInfos = null;
            if (!mowTableList.isEmpty()) {
                if (!checkTransactionStateBeforeCommit(db.getId(), transactionId)) {
                    return true;
                }
                DeleteBitmapUpdateLockContext lockContext = new DeleteBitmapUpdateLockContext(transactionId);
                getDeleteBitmapUpdateLock(transactionId, mowTableList, tabletCommitInfos, lockContext);
                if (lockContext.getBackendToPartitionTablets().isEmpty()) {
                    throw new UserException(
                            "The partition info is empty, table may be dropped, txnid=" + transactionId);
                }
                Map<Long, List<Long>> partitionToSubTxnIds = getPartitionSubTxnIds(subTransactionStates,
                        lockContext.getTableToTabletList(),
                        lockContext.getTabletToTabletMeta());
                backendToPartitionInfos = getCalcDeleteBitmapInfo(
                        lockContext, partitionToSubTxnIds);
            }
            commitTransactionWithSubTxns(db.getId(), tableList, transactionId, subTransactionStates, mowTableList,
                    backendToPartitionInfos);
        } catch (Exception e) {
            if (!mowTableList.isEmpty()) {
                LOG.warn("commit txn {} failed, release delete bitmap lock, catch exception {}", transactionId,
                        e.getMessage());
                removeDeleteBitmapUpdateLock(mowTableList, transactionId);
            }
            throw e;
        } finally {
            stopWatch.stop();
            String detailMsg = "";
            long costTimeMs = stopWatch.getTime();
            if (costTimeMs > 1000) {
                if (commitCostTimeStatisticMap.containsKey(transactionId)) {
                    StringBuilder sb = new StringBuilder();
                    CommitCostTimeStatistic statistic = commitCostTimeStatisticMap.get(transactionId);
                    sb.append("get commit lock cost ").append(statistic.getWaitCommitLockCostTimeMs())
                            .append(" ms, get delete bitmap lock cost ")
                            .append(statistic.getWaitDeleteBitmapLockCostTimeMs())
                            .append(" ms, calculate delete bitmap cost ")
                            .append(statistic.getCalculateDeleteBitmapCostTimeMs()).append(" ms, commit to ms cost ")
                            .append(statistic.getCommitToMsCostTimeMs()).append(" ms");
                    detailMsg = sb.toString();
                }
                LOG.info(
                        "commit transaction {} cost {} ms, detail={}, tableIds={}",
                        transactionId, costTimeMs, detailMsg,
                        tableList.stream().map(Table::getId).collect(Collectors.toList()));
            }
            afterCommitTransaction(tableList, transactionId);
        }
        return true;
    }

    private void commitTransactionWithSubTxns(long dbId, List<Table> tableList, long transactionId,
            List<SubTransactionState> subTransactionStates, List<OlapTable> mowTableList,
            Map<Long, List<TCalcDeleteBitmapPartitionInfo>> backendToPartitionInfos) throws UserException {
        if (!mowTableList.isEmpty()) {
            List<Long> mowTableIds = mowTableList.stream().map(Table::getId).collect(Collectors.toList());
            sendCalcDeleteBitmaptask(dbId, transactionId, backendToPartitionInfos, mowTableIds,
                    Config.calculate_delete_bitmap_task_timeout_seconds_for_transaction_load);
        }

        cleanSubTransactions(transactionId);
        CommitTxnRequest.Builder builder = CommitTxnRequest.newBuilder();
        builder.setDbId(dbId)
                .setTxnId(transactionId)
                .setIs2Pc(false)
                .setCloudUniqueId(Config.cloud_unique_id)
                .setIsTxnLoad(true)
                .setEnableTxnLazyCommit(Config.enable_cloud_txn_lazy_commit);
        for (OlapTable olapTable : mowTableList) {
            builder.addMowTableIds(olapTable.getId());
        }
        // add sub txn infos
        for (SubTransactionState subTransactionState : subTransactionStates) {
            builder.addSubTxnInfos(SubTxnInfo.newBuilder().setSubTxnId(subTransactionState.getSubTransactionId())
                    .setTableId(subTransactionState.getTable().getId())
                    .addAllBaseTabletIds(
                            getBaseTabletsFromTables(Lists.newArrayList(subTransactionState.getTable()),
                                    subTransactionState.getTabletCommitInfos().stream()
                                            .map(c -> new TabletCommitInfo(c.getTabletId(), c.getBackendId()))
                                            .collect(Collectors.toList())))
                    .build());
        }

        final CommitTxnRequest commitTxnRequest = builder.build();
        executeCommitTxnRequest(commitTxnRequest, transactionId, false, null);
    }

    private List<Table> getTablesNeedCommitLock(List<Table> tableList) {
        if (Config.enable_commit_lock_for_all_tables) {
            // If enabled, lock all tables
            return tableList.stream()
                    .sorted(Comparator.comparingLong(Table::getId))
                    .collect(Collectors.toList());
        } else {
            // If disabled, only lock MOW tables
            return tableList.stream()
                    .filter(table -> table instanceof OlapTable && ((OlapTable) table).getEnableUniqueKeyMergeOnWrite())
                    .sorted(Comparator.comparingLong(Table::getId))
                    .collect(Collectors.toList());
        }
    }

    private void beforeCommitTransaction(List<Table> tableList, long transactionId, long timeoutMillis)
            throws UserException {
        for (int i = 0; i < tableList.size(); i++) {
            long tableId = tableList.get(i).getId();
            LOG.info("start commit txn=" + transactionId + ",table=" + tableId + ",timeoutMillis=" + timeoutMillis);
        }
        for (Map.Entry<Long, AtomicInteger> entry : waitToCommitTxnCountMap.entrySet()) {
            if (entry.getValue().get() > 5) {
                LOG.info("now table {} commitAndPublishTransaction queue is {}", entry.getKey(),
                        entry.getValue().get());
            }
        }
        if (!commitCostTimeStatisticMap.containsKey(transactionId)) {
            commitCostTimeStatisticMap.put(transactionId, new CommitCostTimeStatistic());
        }

        List<Table> tablesToLock = getTablesNeedCommitLock(tableList);
        StopWatch stopWatch = null;
        if (!tablesToLock.isEmpty()) {
            stopWatch = new StopWatch();
            stopWatch.start();
        }
        increaseWaitingLockCount(tablesToLock);
        if (!tablesToLock.isEmpty() && DebugPointUtil.isEnable("FE.mow.check.lock.release")) {
            for (int i = 0; i < tablesToLock.size(); i++) {
                if (tablesToLock.get(i).getCommitLockOwner() != null) {
                    throw new UserException(InternalErrorCode.INTERNAL_ERR,
                            "check lock release fail,lock owner is " + tablesToLock.get(i).getCommitLockOwner()
                                    + ", tableList=(" + StringUtils.join(tablesToLock, ",") + ")");
                }
            }
        }
        if (DebugPointUtil.isEnable("CloudGlobalTransactionMgr.tryCommitLock.timeout")) {
            DebugPoint debugPoint = DebugPointUtil.getDebugPoint(
                    "CloudGlobalTransactionMgr.tryCommitLock.timeout");
            int t = debugPoint.param("sleep_time", 5);
            try {
                Thread.sleep(t * 1000);
            } catch (InterruptedException e) {
                LOG.info("error ", e);
            }
            throw new UserException(InternalErrorCode.DELETE_BITMAP_LOCK_ERR,
                    "test get table cloud commit lock timeout, tableList=("
                            + StringUtils.join(tablesToLock, ",") + ")");
        }
        if (!MetaLockUtils.tryCommitLockTables(tablesToLock, timeoutMillis, TimeUnit.MILLISECONDS)) {
            decreaseWaitingLockCount(tablesToLock);
            // DELETE_BITMAP_LOCK_ERR will be retried on be
            throw new UserException(InternalErrorCode.DELETE_BITMAP_LOCK_ERR,
                    "get table cloud commit lock timeout, tableList=("
                            + StringUtils.join(tablesToLock, ",") + ")");
        }
        if (stopWatch != null) {
            stopWatch.stop();
            long costTimeMs = stopWatch.getTime();
            if (MetricRepo.isInit) {
                MetricRepo.HISTO_GET_COMMIT_LOCK_LATENCY.update(costTimeMs);
            }
            if (commitCostTimeStatisticMap.containsKey(transactionId)) {
                commitCostTimeStatisticMap.get(transactionId).setWaitCommitLockCostTimeMs(costTimeMs);
            }
            if (costTimeMs > 1000) {
                LOG.warn("get table cloud commit lock, tableList=(" + StringUtils.join(tablesToLock, ",") + ")"
                        + ", transactionId=" + transactionId + ", cost=" + costTimeMs + " ms");
            }
        }
    }

    private void afterCommitTransaction(List<Table> tableList, Long transactionId) {
        if (commitCostTimeStatisticMap.containsKey(transactionId)) {
            commitCostTimeStatisticMap.remove(transactionId);
        }
        List<Table> tablesToUnlock = getTablesNeedCommitLock(tableList);
        decreaseWaitingLockCount(tablesToUnlock);
        MetaLockUtils.commitUnlockTables(tablesToUnlock);
    }

    @Override
    public void commitTransaction(DatabaseIf db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        // There is no publish in cloud mode
        commitAndPublishTransaction(
                db, tableList, transactionId, tabletCommitInfos, timeoutMillis, txnCommitAttachment);
    }

    @Override
    public boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList, long transactionId,
                                               List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis,
                                               TxnCommitAttachment txnCommitAttachment) throws UserException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        beforeCommitTransaction(tableList, transactionId, timeoutMillis);
        try {
            commitTransactionWithoutLock(db.getId(), tableList, transactionId, tabletCommitInfos, txnCommitAttachment);
        } finally {
            stopWatch.stop();
            long costTimeMs = stopWatch.getTime();
            if (costTimeMs > 1000) {
                String detailMsg = "";
                if (commitCostTimeStatisticMap.containsKey(transactionId)) {
                    StringBuilder sb = new StringBuilder();
                    CommitCostTimeStatistic statistic = commitCostTimeStatisticMap.get(transactionId);
                    sb.append("get commit lock cost ").append(statistic.getWaitCommitLockCostTimeMs())
                            .append(" ms, get delete bitmap lock cost ")
                            .append(statistic.getWaitDeleteBitmapLockCostTimeMs())
                            .append(" ms, calculate delete bitmap cost ")
                            .append(statistic.getCalculateDeleteBitmapCostTimeMs()).append(" ms, commit to ms cost ")
                            .append(statistic.getCommitToMsCostTimeMs()).append(" ms");
                    detailMsg = sb.toString();
                }
                LOG.info(
                        "commit transaction {} cost {} ms, detail={}, tableIds={}",
                        transactionId, costTimeMs, detailMsg,
                        tableList.stream().map(Table::getId).collect(Collectors.toList()));
            }
            afterCommitTransaction(tableList, transactionId);
        }
        return true;
    }

    @Override
    public void commitTransaction2PC(Database db, List<Table> tableList, long transactionId, long timeoutMillis)
            throws UserException {
        List<OlapTable> mowTableList = getMowTableList(tableList, null);
        if (!mowTableList.isEmpty()) {
            if (!checkTransactionStateBeforeCommit(db.getId(), transactionId)) {
                return;
            }
        }
        commitTransactionWithoutLock(db.getId(), tableList, transactionId, null, null, true, mowTableList, null);
    }

    @Override
    public void abortTransaction(Long dbId, Long transactionId, String reason) throws UserException {
        cleanSubTransactions(transactionId);
        abortTransaction(dbId, transactionId, reason, null, null);
    }

    @Override
    public void abortTransaction(Long dbId, Long transactionId, String reason,
            TxnCommitAttachment txnCommitAttachment, List<Table> tableList) throws UserException {
        if (txnCommitAttachment != null) {
            if (txnCommitAttachment instanceof RLTaskTxnCommitAttachment) {
                RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment = (RLTaskTxnCommitAttachment) txnCommitAttachment;
                TxnStateChangeCallback cb = callbackFactory.getCallback(rlTaskTxnCommitAttachment.getJobId());
                if (cb != null) {
                    // use a temporary transaction state to do before commit check,
                    // what actually works is the transactionId
                    TransactionState tmpTxnState = new TransactionState();
                    tmpTxnState.setTransactionId(transactionId);
                    cb.beforeAborted(tmpTxnState);
                }
            }
        }

        AbortTxnResponse abortTxnResponse = null;
        try {
            abortTxnResponse = abortTransactionImpl(dbId, transactionId, reason, null);
        } finally {
            handleAfterAbort(abortTxnResponse, txnCommitAttachment, transactionId);
        }
    }

    private AbortTxnResponse abortTransactionImpl(Long dbId, Long transactionId, String reason,
            TxnCommitAttachment txnCommitAttachment) throws UserException {
        LOG.info("try to abort transaction, dbId:{}, transactionId:{}, reason: {}", dbId, transactionId, reason);

        AbortTxnRequest.Builder builder = AbortTxnRequest.newBuilder();
        if (reason != null && reason.length() > 1024) {
            reason = reason.substring(0, 1024) + " ... (reason is truncated, check fe.log with txnId for details)";
        }
        builder.setDbId(dbId);
        builder.setTxnId(transactionId);
        builder.setReason(reason);
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final AbortTxnRequest abortTxnRequest = builder.build();
        AbortTxnResponse abortTxnResponse = null;
        int retryTime = 0;
        try {
            while (retryTime < Config.metaServiceRpcRetryTimes()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("retryTime:{}, abortTxnRequest:{}", retryTime, abortTxnRequest);
                }
                abortTxnResponse = MetaServiceProxy
                        .getInstance().abortTxn(abortTxnRequest);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("retryTime:{}, abortTxnResponse:{}", retryTime, abortTxnResponse);
                }
                if (abortTxnResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                // sleep random [20, 200] ms, avoid txn conflict
                LOG.info("abortTxn KV_TXN_CONFLICT, transactionId:{}, retryTime:{}", transactionId, retryTime);
                backoff();
                retryTime++;
            }
            Preconditions.checkNotNull(abortTxnResponse);
            Preconditions.checkNotNull(abortTxnResponse.getStatus());
        } catch (RpcException e) {
            LOG.warn("abortTxn failed, transactionId:{}, Exception", transactionId, e);
            throw new UserException("abortTxn failed, errMsg:" + e.getMessage());
        }
        afterAbortTxnResp(abortTxnResponse, String.valueOf(transactionId), txnCommitAttachment);
        return abortTxnResponse;
    }

    private void handleAfterAbort(AbortTxnResponse abortTxnResponse, TxnCommitAttachment txnCommitAttachment,
                                long transactionId) throws UserException {
        TransactionState txnState = new TransactionState();
        boolean txnOperated = false;
        long callbackId = 0L;
        TxnStateChangeCallback cb = null;
        String abortReason = "";

        if (abortTxnResponse != null) {
            txnState = TxnUtil.transactionStateFromPb(abortTxnResponse.getTxnInfo());
            txnOperated = abortTxnResponse.getStatus().getCode() == MetaServiceCode.OK;
            callbackId = txnState.getCallbackId();
            abortReason = txnState.getReason();
        }
        if (txnCommitAttachment != null && txnCommitAttachment instanceof RLTaskTxnCommitAttachment) {
            RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment = (RLTaskTxnCommitAttachment) txnCommitAttachment;
            callbackId = rlTaskTxnCommitAttachment.getJobId();
            txnState.setTransactionId(transactionId);
        }

        cb = callbackFactory.getCallback(callbackId);
        if (cb != null) {
            LOG.info("run txn callback, txnId:{} callbackId:{}, txnState:{}",
                    transactionId, callbackId, txnState);
            cb.afterAborted(txnState, txnOperated, abortReason);
        }
    }

    private void afterAbortTxnResp(AbortTxnResponse abortTxnResponse, String txnIdOrLabel,
            TxnCommitAttachment txnCommitAttachment) throws UserException {
        if (abortTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            LOG.warn("abortTxn failed, transaction:{}, response:{}", txnIdOrLabel, abortTxnResponse);
            switch (abortTxnResponse.getStatus().getCode()) {
                case TXN_ID_NOT_FOUND:
                case TXN_LABEL_NOT_FOUND:
                case TXN_INVALID_STATUS:
                    throw new TransactionNotFoundException("transaction [" + txnIdOrLabel + "] not found");
                case TXN_ALREADY_ABORTED:
                    throw new TransactionNotFoundException("transaction [" + txnIdOrLabel + "] is already aborted");
                case TXN_ALREADY_VISIBLE:
                    throw new UserException(
                            "transaction [" + txnIdOrLabel + "] is already visible, " + ", could not abort");
                default:
                    throw new UserException(abortTxnResponse.getStatus().getMsg());
            }
        }

        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_TXN_FAILED.increase(1L);
        }
    }

    @Override
    public void abortTransaction(Long dbId, String label, String reason) throws UserException {
        LOG.info("try to abort transaction, dbId:{}, label:{}", dbId, label);

        AbortTxnRequest.Builder builder = AbortTxnRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setLabel(label);
        builder.setReason(reason);
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final AbortTxnRequest abortTxnRequest = builder.build();
        AbortTxnResponse abortTxnResponse = null;
        int retryTime = 0;

        try {
            while (retryTime < Config.metaServiceRpcRetryTimes()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("retyTime:{}, abortTxnRequest:{}", retryTime, abortTxnRequest);
                }
                abortTxnResponse = MetaServiceProxy
                        .getInstance().abortTxn(abortTxnRequest);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("retryTime:{}, abortTxnResponse:{}", retryTime, abortTxnResponse);
                }
                if (abortTxnResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }

                // sleep random [20, 200] ms, avoid txn conflict
                LOG.info("abortTxn KV_TXN_CONFLICT, dbId:{}, label:{}, retryTime:{}", dbId, label, retryTime);
                backoff();
                retryTime++;
            }
            Preconditions.checkNotNull(abortTxnResponse);
            Preconditions.checkNotNull(abortTxnResponse.getStatus());
        } catch (Exception e) {
            LOG.warn("abortTxn failed, label:{}, exception:", label, e);
            throw new UserException("abortTxn failed, errMsg:" + e.getMessage());
        }
        afterAbortTxnResp(abortTxnResponse, label, null);
    }

    @Override
    public void abortTransaction2PC(Long dbId, long transactionId, List<Table> tableList) throws UserException {
        LOG.info("try to abortTransaction2PC, dbId:{}, transactionId:{}", dbId, transactionId);
        abortTransaction(dbId, transactionId, "User Abort", null, null);
        LOG.info("abortTransaction2PC successfully, dbId:{}, transactionId:{}", dbId, transactionId);
    }

    @Override
    public List<TransactionState> getReadyToPublishTransactions() {
        //do nothing for CloudGlobalTransactionMgr
        return new ArrayList<TransactionState>();
    }

    @Override
    public boolean existCommittedTxns(Long dbId, Long tableId, Long partitionId) {
        //do nothing for CloudGlobalTransactionMgr
        return false;
    }

    @Override
    public void finishTransaction(long dbId, long transactionId, Map<Long, Long> partitionVisibleVersions,
            Map<Long, Set<Long>> backendPartitions) throws UserException {
        throw new UserException("Disallow to call finishTransaction()");
    }

    public List<TransactionState> getUnFinishedPreviousLoad(long endTransactionId, long dbId, List<Long> tableIdList)
            throws UserException {
        LOG.info("getUnFinishedPreviousLoad(), endTransactionId:{}, dbId:{}, tableIdList:{}",
                endTransactionId, dbId, tableIdList);

        if (endTransactionId <= 0) {
            throw new UserException("Invaid endTransactionId:" + endTransactionId);
        }
        CheckTxnConflictRequest.Builder builder = CheckTxnConflictRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setEndTxnId(endTransactionId);
        builder.addAllTableIds(tableIdList);
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final CheckTxnConflictRequest checkTxnConflictRequest = builder.build();
        CheckTxnConflictResponse checkTxnConflictResponse = null;
        try {
            LOG.info("CheckTxnConflictRequest:{}", checkTxnConflictRequest);
            checkTxnConflictResponse = MetaServiceProxy
                .getInstance().checkTxnConflict(checkTxnConflictRequest);
            LOG.info("CheckTxnConflictResponse: {}", checkTxnConflictResponse);
        } catch (RpcException e) {
            throw new UserException(e.getMessage());
        }

        if (checkTxnConflictResponse.getStatus().getCode() != MetaServiceCode.OK) {
            throw new UserException(checkTxnConflictResponse.getStatus().getMsg());
        }
        List<TxnInfoPB> conflictTxnInfoPbs = checkTxnConflictResponse.getConflictTxnsList();
        List<TransactionState> conflictTxns = new ArrayList<>();
        for (TxnInfoPB infoPb : conflictTxnInfoPbs) {
            conflictTxns.add(TxnUtil.transactionStateFromPb(infoPb));
        }
        return conflictTxns;
    }

    @Override
    public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, List<Long> tableIdList)
            throws AnalysisException {
        LOG.info("isPreviousTransactionsFinished(), endTransactionId:{}, dbId:{}, tableIdList:{}",
                endTransactionId, dbId, tableIdList);

        if (endTransactionId <= 0) {
            throw new AnalysisException("Invaid endTransactionId:" + endTransactionId);
        }
        CheckTxnConflictRequest.Builder builder = CheckTxnConflictRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setEndTxnId(endTransactionId);
        builder.addAllTableIds(tableIdList);
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final CheckTxnConflictRequest checkTxnConflictRequest = builder.build();
        CheckTxnConflictResponse checkTxnConflictResponse = null;
        try {
            LOG.info("CheckTxnConflictRequest:{}", checkTxnConflictRequest);
            checkTxnConflictResponse = MetaServiceProxy
                    .getInstance().checkTxnConflict(checkTxnConflictRequest);
            LOG.info("CheckTxnConflictResponse: {}", checkTxnConflictResponse);
        } catch (RpcException e) {
            throw new AnalysisException(e.getMessage());
        }

        if (checkTxnConflictResponse.getStatus().getCode() != MetaServiceCode.OK) {
            throw new AnalysisException(checkTxnConflictResponse.getStatus().getMsg());
        }
        return checkTxnConflictResponse.getFinished();
    }

    public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, long tableId,
                                                  long partitionId) throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    public boolean isPreviousNonTimeoutTxnFinished(long endTransactionId, long dbId, List<Long> tableIdList)
            throws AnalysisException {
        LOG.info("isPreviousNonTimeoutTxnFinished(), endTransactionId:{}, dbId:{}, tableIdList:{}",
                endTransactionId, dbId, tableIdList);

        if (endTransactionId <= 0) {
            throw new AnalysisException("Invaid endTransactionId:" + endTransactionId);
        }
        CheckTxnConflictRequest.Builder builder = CheckTxnConflictRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setEndTxnId(endTransactionId);
        builder.addAllTableIds(tableIdList);
        builder.setCloudUniqueId(Config.cloud_unique_id);
        builder.setIgnoreTimeoutTxn(true);

        final CheckTxnConflictRequest checkTxnConflictRequest = builder.build();
        CheckTxnConflictResponse checkTxnConflictResponse = null;
        try {
            LOG.info("CheckTxnConflictRequest:{}", checkTxnConflictRequest);
            checkTxnConflictResponse = MetaServiceProxy
                    .getInstance().checkTxnConflict(checkTxnConflictRequest);
            LOG.info("CheckTxnConflictResponse: {}", checkTxnConflictResponse);
        } catch (RpcException e) {
            throw new AnalysisException(e.getMessage());
        }

        if (checkTxnConflictResponse.getStatus().getCode() != MetaServiceCode.OK) {
            throw new AnalysisException(checkTxnConflictResponse.getStatus().getMsg());
        }
        return checkTxnConflictResponse.getFinished();
    }

    @Override
    public void removeExpiredAndTimeoutTxns() {
        // do nothing in cloud mode
    }

    public void cleanLabel(Long dbId, String label, boolean isReplay) throws Exception {
        LOG.info("try to cleanLabel dbId: {}, label:{}", dbId, label);
        CleanTxnLabelRequest.Builder builder = CleanTxnLabelRequest.newBuilder();
        builder.setDbId(dbId).setCloudUniqueId(Config.cloud_unique_id);

        if (!Strings.isNullOrEmpty(label)) {
            builder.addLabels(label);
        }

        final CleanTxnLabelRequest cleanTxnLabelRequest = builder.build();
        CleanTxnLabelResponse cleanTxnLabelResponse = null;
        int retryTime = 0;

        try {
            // 5 times retry is enough for clean label
            while (retryTime < 5) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("retryTime:{}, cleanTxnLabel:{}", retryTime, cleanTxnLabelRequest);
                }
                cleanTxnLabelResponse = MetaServiceProxy.getInstance().cleanTxnLabel(cleanTxnLabelRequest);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("retryTime:{}, cleanTxnLabel:{}", retryTime, cleanTxnLabelResponse);
                }
                if (cleanTxnLabelResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                // sleep random [20, 200] ms, avoid txn conflict
                LOG.info("cleanTxnLabel KV_TXN_CONFLICT, dbId:{}, label:{}, retryTime:{}", dbId, label, retryTime);
                backoff();
                retryTime++;
            }

            Preconditions.checkNotNull(cleanTxnLabelResponse);
            Preconditions.checkNotNull(cleanTxnLabelResponse.getStatus());
        } catch (Exception e) {
            LOG.warn("cleanTxnLabel failed, dbId:{}, exception:", dbId, e);
            throw new UserException("cleanTxnLabel failed, errMsg:" + e.getMessage());
        }

        if (cleanTxnLabelResponse.getStatus().getCode() != MetaServiceCode.OK) {
            LOG.warn("cleanTxnLabel failed, dbId:{} label:{} retryTime:{} cleanTxnLabelResponse:{}",
                    dbId, label, retryTime, cleanTxnLabelResponse);
            throw new UserException("cleanTxnLabel failed, errMsg:" + cleanTxnLabelResponse.getStatus().getMsg());
        }
        return;
    }

    @Override
    public void updateMultiTableRunningTransactionTableIds(Long dbId, Long transactionId, List<Long> tableIds)
            throws UserException {
        //throw new UserException(NOT_SUPPORTED_MSG);
    }

    @Override
    public TWaitingTxnStatusResult getWaitingTxnStatus(TWaitingTxnStatusRequest request)
            throws AnalysisException, TimeoutException {
        long dbId = request.getDbId();
        int commitTimeoutSec = Config.commit_timeout_second;
        for (int i = 0; i < commitTimeoutSec; ++i) {
            Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbId);
            TWaitingTxnStatusResult statusResult = new TWaitingTxnStatusResult();
            statusResult.status = new TStatus();
            TransactionStatus txnStatus = null;
            if (request.isSetTxnId()) {
                long txnId = request.getTxnId();
                TransactionState txnState = Env.getCurrentGlobalTransactionMgr()
                        .getTransactionState(dbId, txnId);
                if (txnState == null) {
                    throw new AnalysisException("txn does not exist: " + txnId);
                }
                txnStatus = txnState.getTransactionStatus();
                if (!txnState.getReason().trim().isEmpty()) {
                    statusResult.status.setErrorMsgsIsSet(true);
                    statusResult.status.addToErrorMsgs(txnState.getReason());
                }
            } else {
                txnStatus = getLabelState(dbId, request.getLabel());
            }
            if (txnStatus == TransactionStatus.UNKNOWN || txnStatus.isFinalStatus()) {
                statusResult.setTxnStatusId(txnStatus.value());
                return statusResult;
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                LOG.info("commit sleep exception.", e);
            }
        }
        throw new TimeoutException("Operation is timeout");
    }

    @Override
    public void updateDatabaseUsedQuotaData(long dbId, long usedQuotaDataBytes) throws AnalysisException {
        // do nothing in cloud mode
    }

    @Override
    public void abortTxnWhenCoordinateBeRestart(long coordinateBeId, String coordinateHost, long beStartTime) {
        AbortTxnWithCoordinatorRequest.Builder builder = AbortTxnWithCoordinatorRequest.newBuilder();
        builder.setIp(coordinateHost);
        builder.setId(coordinateBeId);
        builder.setStartTime(beStartTime);
        final AbortTxnWithCoordinatorRequest request = builder.build();
        AbortTxnWithCoordinatorResponse response = null;
        try {
            response = MetaServiceProxy
                .getInstance().abortTxnWithCoordinator(request);
            LOG.info("AbortTxnWithCoordinatorResponse: {}", response);
        } catch (RpcException e) {
            LOG.warn("Abort txn on coordinate BE {} failed, msg={}", coordinateHost, e.getMessage());
        }
    }

    @Override
    public void abortTxnWhenCoordinateBeDown(long coordinateBeId, String coordinateHost, int limit) {
        // do nothing in cloud mode
    }

    @Override
    public TransactionStatus getLabelState(long dbId, String label) throws AnalysisException {
        GetTxnRequest.Builder builder = GetTxnRequest.newBuilder();
        builder.setDbId(dbId).setCloudUniqueId(Config.cloud_unique_id).setLabel(label);
        final GetTxnRequest getTxnRequest = builder.build();
        GetTxnResponse getTxnResponse = null;
        int retryTime = 0;

        try {
            while (retryTime < 3) {
                getTxnResponse = MetaServiceProxy.getInstance().getTxn(getTxnRequest);
                if (getTxnResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                LOG.info("getLabelState KV_TXN_CONFLICT, dbId:{}, label:{}, retryTime:{}", dbId, label, retryTime);
                backoff();
                retryTime++;
                continue;
            }

            Preconditions.checkNotNull(getTxnResponse);
            Preconditions.checkNotNull(getTxnResponse.getStatus());
        } catch (Exception e) {
            LOG.warn("getLabelState failed, dbId:{}, exception:", dbId, e);
            throw new AnalysisException("getLabelState failed, errMsg:" + e.getMessage());
        }

        if (getTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            LOG.warn("getLabelState failed, dbId:{} label:{} retryTime:{} getTxnResponse:{}",
                    dbId, label, retryTime, getTxnResponse);
            throw new AnalysisException("getLabelState failed, errMsg:" + getTxnResponse.getStatus().getMsg());
        }

        return TxnUtil.transactionStateFromPb(getTxnResponse.getTxnInfo()).getTransactionStatus();
    }

    @Override
    public Long getTransactionId(Long dbId, String label)  {
        try {
            TransactionStatus labelState = getLabelState(dbId, label);
            List<TransactionStatus> statusList = Lists.newArrayList(labelState);
            return getTransactionIdByLabel(dbId, label, statusList);
        } catch (UserException e) {
            LOG.warn("Get transaction id by label " + label + " failed", e);
            return null;
        }
    }

    @Override
    public TransactionState getTransactionState(long dbId, long transactionId) {
        if (subTxnIdToTxnId.containsKey(transactionId)) {
            LOG.info("try to get transaction state, subTxnId:{}, transactionId:{}", transactionId,
                    subTxnIdToTxnId.get(transactionId));
            transactionId = subTxnIdToTxnId.get(transactionId);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("try to get transaction state, dbId:{}, transactionId:{}", dbId, transactionId);
        }
        GetTxnRequest.Builder builder = GetTxnRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setTxnId(transactionId);
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final GetTxnRequest getTxnRequest = builder.build();
        GetTxnResponse getTxnResponse = null;
        try {
            if (LOG.isDebugEnabled()) {
                LOG.info("getTxnRequest:{}", getTxnRequest);
            }
            getTxnResponse = MetaServiceProxy
                    .getInstance().getTxn(getTxnRequest);
            LOG.info("getTxnResponse: {}", getTxnResponse);
        } catch (RpcException e) {
            LOG.info("getTransactionState exception: {}", e.getMessage());
            return null;
        }

        if (getTxnResponse.getStatus().getCode() != MetaServiceCode.OK || !getTxnResponse.hasTxnInfo()) {
            LOG.info("getTransactionState exception: {}, {}", getTxnResponse.getStatus().getCode(),
                    getTxnResponse.getStatus().getMsg());
            return null;
        }
        return TxnUtil.transactionStateFromPb(getTxnResponse.getTxnInfo());
    }

    @Override
    public Long getTransactionIdByLabel(Long dbId, String label, List<TransactionStatus> statusList)
            throws UserException {
        LOG.info("try to get transaction id by label, dbId:{}, label:{}", dbId, label);
        GetTxnIdRequest.Builder builder = GetTxnIdRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setLabel(label);
        builder.setCloudUniqueId(Config.cloud_unique_id);
        for (TransactionStatus status : statusList) {
            if (status == TransactionStatus.PREPARE) {
                builder.addTxnStatus(TxnStatusPB.TXN_STATUS_PREPARED);
            } else if (status == TransactionStatus.PRECOMMITTED) {
                builder.addTxnStatus(TxnStatusPB.TXN_STATUS_PRECOMMITTED);
            } else if (status == TransactionStatus.COMMITTED) {
                builder.addTxnStatus(TxnStatusPB.TXN_STATUS_COMMITTED);
            }
        }

        final GetTxnIdRequest getTxnIdRequest = builder.build();
        GetTxnIdResponse getTxnIdResponse = null;
        try {
            LOG.info("getTxnRequest:{}", getTxnIdRequest);
            getTxnIdResponse = MetaServiceProxy
                    .getInstance().getTxnId(getTxnIdRequest);
            LOG.info("getTxnIdReponse: {}", getTxnIdResponse);
        } catch (RpcException e) {
            LOG.info("getTransactionId exception: {}", e.getMessage());
            throw new TransactionNotFoundException("transaction not found, label=" + label);
        }

        if (getTxnIdResponse.getStatus().getCode() != MetaServiceCode.OK) {
            LOG.info("getTransactionState exception: {}, {}", getTxnIdResponse.getStatus().getCode(),
                    getTxnIdResponse.getStatus().getMsg());
            throw new TransactionNotFoundException("transaction not found, label=" + label);
        }
        return getTxnIdResponse.getTxnId();
    }

    @Override
    public List<TransactionState> getPreCommittedTxnList(Long dbId) throws AnalysisException {
        // todo
        return new ArrayList<TransactionState>();
    }

    @Override
    public int getTransactionNum() {
        return 0;
    }

    @Override
    public Long getNextTransactionId() throws UserException {
        GetCurrentMaxTxnRequest.Builder builder = GetCurrentMaxTxnRequest.newBuilder();
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final GetCurrentMaxTxnRequest getCurrentMaxTxnRequest = builder.build();
        GetCurrentMaxTxnResponse getCurrentMaxTxnResponse = null;
        try {
            LOG.info("GetCurrentMaxTxnRequest:{}", getCurrentMaxTxnRequest);
            getCurrentMaxTxnResponse = MetaServiceProxy
                    .getInstance().getCurrentMaxTxnId(getCurrentMaxTxnRequest);
            LOG.info("GetCurrentMaxTxnResponse: {}", getCurrentMaxTxnResponse);
        } catch (RpcException e) {
            LOG.warn("getNextTransactionId() RpcException: {}", e.getMessage());
            throw new UserException("getNextTransactionId() RpcException: " + e.getMessage());
        }

        if (getCurrentMaxTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            LOG.info("getNextTransactionId() failed, code: {}, msg: {}",
                    getCurrentMaxTxnResponse.getStatus().getCode(), getCurrentMaxTxnResponse.getStatus().getMsg());
            throw new UserException("getNextTransactionId() failed, msg:"
                    + getCurrentMaxTxnResponse.getStatus().getMsg());
        }
        return getCurrentMaxTxnResponse.getCurrentMaxTxnId();
    }

    @Override
    public int getRunningTxnNums(Long dbId) throws AnalysisException {
        return 0;
    }

    @Override
    public long getTxnNumByStatus(TransactionStatus status) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getAllRunningTxnNum() {
        return 0;
    }

    @Override
    public long getAllPublishTxnNum() {
        return 0;
    }

    /**
     * backoff policy implement by sleep random ms in [20ms, 200ms]
     */
    private void backoff() {
        int randomMillis = 20 + (int) (Math.random() * (200 - 20));
        try {
            Thread.sleep(randomMillis);
        } catch (InterruptedException e) {
            LOG.info("InterruptedException: ", e);
        }
    }

    @Override
    public TransactionIdGenerator getTransactionIDGenerator() throws Exception {
        throw new Exception(NOT_SUPPORTED_MSG);
    }

    @Override
    public List<List<String>> getDbInfo() throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public List<List<String>> getDbTransStateInfo(Long dbId) throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public List<List<String>> getDbTransInfo(Long dbId, boolean running, int limit) throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public Map<Long, List<Long>> getDbRunningTransInfo(long dbId) throws AnalysisException {
        return Maps.newHashMap();
    }

    @Override
    public List<List<String>> getDbTransInfoByStatus(Long dbId, TransactionStatus status) throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public List<List<String>> getDbTransInfoByLabelMatch(long dbId, String label) throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public List<List<String>> getSingleTranInfo(long dbId, long txnId) throws AnalysisException {
        List<List<String>> infos = new ArrayList<List<String>>();
        TransactionState txnState = this.getTransactionState(dbId, txnId);
        if (txnState == null) {
            throw new AnalysisException("transaction with id " + txnId + " does not exist");
        }
        TransactionUtil.checkAuth(dbId, txnState);
        infos.add(TransactionUtil.getTxnStateInfo(txnState, Lists.newArrayList()));
        return infos;
    }

    @Override
    public List<List<Comparable>> getTableTransInfo(long dbId, long txnId) throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public List<List<Comparable>> getPartitionTransInfo(long dbId, long tid, long tableId)
            throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new IOException(NOT_SUPPORTED_MSG);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new IOException(NOT_SUPPORTED_MSG);
    }

    @Override
    public void replayUpsertTransactionState(TransactionState transactionState) throws Exception {
        throw new Exception(NOT_SUPPORTED_MSG);
    }

    @Deprecated
    public void replayDeleteTransactionState(TransactionState transactionState) throws Exception {
        throw new Exception(NOT_SUPPORTED_MSG);
    }

    @Override
    public void replayBatchRemoveTransactions(BatchRemoveTransactionsOperation operation) throws Exception {
        throw new Exception(NOT_SUPPORTED_MSG);
    }

    @Override
    public void replayBatchRemoveTransactionV2(BatchRemoveTransactionsOperationV2 operation)
            throws Exception {
        throw new Exception(NOT_SUPPORTED_MSG);
    }

    @Override
    public void addSubTransaction(long dbId, long transactionId, long subTransactionId) {
        subTxnIdToTxnId.put(subTransactionId, transactionId);
    }

    @Override
    public void removeSubTransaction(long dbId, long subTransactionId) {
        subTxnIdToTxnId.remove(subTransactionId);
    }

    private void cleanSubTransactions(long transactionId) {
        Iterator<Entry<Long, Long>> iterator = subTxnIdToTxnId.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Long, Long> entry = iterator.next();
            if (entry.getValue() == transactionId) {
                iterator.remove();
            }
        }
    }

    public Pair<Long, TransactionState> beginSubTxn(long txnId, long dbId, Set<Long> tableIds, String label,
            long subTxnNum) throws UserException {
        LOG.info("try to begin sub transaction, txnId: {}, dbId: {}, tableIds: {}, label: {}, subTxnNum: {}", txnId,
                dbId, tableIds, label, subTxnNum);
        BeginSubTxnRequest request = BeginSubTxnRequest.newBuilder().setCloudUniqueId(Config.cloud_unique_id)
                .setTxnId(txnId).setDbId(dbId).addAllTableIds(tableIds).setLabel(label).setSubTxnNum(subTxnNum).build();
        BeginSubTxnResponse response = null;
        int retryTime = 0;
        try {
            while (retryTime < Config.metaServiceRpcRetryTimes()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("retryTime:{}, beginSubTxnRequest:{}", retryTime, request);
                }
                response = MetaServiceProxy.getInstance().beginSubTxn(request);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("retryTime:{}, beginSubTxnResponse:{}", retryTime, response);
                }

                if (response.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                LOG.info("beginSubTxn KV_TXN_CONFLICT, retryTime:{}", retryTime);
                backoff();
                retryTime++;
            }

            Preconditions.checkNotNull(response);
            Preconditions.checkNotNull(response.getStatus());
        } catch (Exception e) {
            LOG.warn("beginSubTxn failed, exception:", e);
            throw new UserException("beginSubTxn failed, errMsg:" + e.getMessage());
        }

        if (response.getStatus().getCode() != MetaServiceCode.OK) {
            throw new UserException(response.getStatus().getMsg());
        }
        return Pair.of(response.hasSubTxnId() ? response.getSubTxnId() : 0,
                TxnUtil.transactionStateFromPb(response.getTxnInfo()));
    }

    public TransactionState abortSubTxn(long txnId, long subTxnId, long dbId, Set<Long> tableIds, long subTxnNum)
            throws UserException {
        LOG.info("try to abort sub transaction, txnId: {}, subTxnId: {}, dbId: {}, tableIds: {}, subTxnNum: {}", txnId,
                subTxnId, dbId, tableIds, subTxnNum);
        AbortSubTxnRequest request = AbortSubTxnRequest.newBuilder().setCloudUniqueId(Config.cloud_unique_id)
                .setTxnId(txnId).setSubTxnId(subTxnId).setDbId(dbId).addAllTableIds(tableIds).setSubTxnNum(subTxnId)
                .build();
        AbortSubTxnResponse response = null;
        int retryTime = 0;
        try {
            while (retryTime < Config.metaServiceRpcRetryTimes()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("retryTime:{}, abortSubTxnRequest:{}", retryTime, request);
                }
                response = MetaServiceProxy.getInstance().abortSubTxn(request);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("retryTime:{}, abortSubTxnResponse:{}", retryTime, response);
                }

                if (response.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                LOG.info("abortSubTxn KV_TXN_CONFLICT, retryTime:{}", retryTime);
                backoff();
                retryTime++;
            }

            Preconditions.checkNotNull(response);
            Preconditions.checkNotNull(response.getStatus());
        } catch (Exception e) {
            LOG.warn("abortSubTxn failed, exception:", e);
            throw new UserException("abortSubTxn failed, errMsg:" + e.getMessage());
        }

        if (response.getStatus().getCode() != MetaServiceCode.OK) {
            throw new UserException(response.getStatus().getMsg());
        }
        return TxnUtil.transactionStateFromPb(response.getTxnInfo());
    }

    private void increaseWaitingLockCount(List<Table> tableList) {
        for (int i = 0; i < tableList.size(); i++) {
            long tableId = tableList.get(i).getId();
            if (waitToCommitTxnCountMap.containsKey(tableId)) {
                waitToCommitTxnCountMap.get(tableId).addAndGet(1);
            } else {
                waitToCommitTxnCountMap.put(tableId, new AtomicInteger());
                waitToCommitTxnCountMap.get(tableId).addAndGet(1);
            }
        }
    }

    @Override
    public int getQueueLength() {
        int count = 0;
        for (Map.Entry<Long, AtomicInteger> entry : waitToCommitTxnCountMap.entrySet()) {
            count += entry.getValue().get();
        }
        return count;
    }

    private void decreaseWaitingLockCount(List<Table> tableList) {
        for (int i = 0; i < tableList.size(); i++) {
            long tableId = tableList.get(i).getId();
            waitToCommitTxnCountMap.get(tableId).decrementAndGet();
        }
    }

    public long getTableLastTxnId(long dbId, long tableId) {
        Map<Long, Long> tabletIdToTxnId = lastTxnIdMap.get(dbId);
        if (tabletIdToTxnId == null) {
            return -1;
        }
        return tabletIdToTxnId.getOrDefault(tableId, -1L);
    }

    public void setTableLastTxnId(long dbId, long tableId, long txnId) {
        lastTxnIdMap.compute(dbId, (k, v) -> {
            if (v == null) {
                v = Maps.newConcurrentMap();
            }
            LOG.debug("setTableLastTxnId dbId: {}, tableId: {}, txnId: {}", dbId, tableId, txnId);
            v.put(tableId, txnId);
            return v;
        });
    }

    public void clearTableLastTxnId(long dbId, long tableId) {
        lastTxnIdMap.computeIfPresent(dbId, (k, v) -> {
            v.remove(tableId);
            return v.isEmpty() ? null : v;
        });
    }

    public long getTxnLastSignature(long dbId, long txnId) {
        Map<Long, Long> txnIdToLastSignature = txnLastSignatureMap.get(dbId);
        if (txnIdToLastSignature == null) {
            return -1;
        }
        return txnIdToLastSignature.getOrDefault(txnId, -1L);
    }

    public void setTxnLastSignature(long dbId, long txnId, long signature) {
        txnLastSignatureMap.compute(dbId, (k, v) -> {
            if (v == null) {
                v = Maps.newConcurrentMap();
            }
            LOG.debug("setTxnLastSignature dbId: {}, txnId: {}, signature: {}", dbId, txnId, signature);
            v.put(txnId, signature);
            return v;
        });
    }

    public void clearTxnLastSignature(long dbId, long txnId) {
        txnLastSignatureMap.computeIfPresent(dbId, (k, v) -> {
            v.remove(txnId);
            return v.isEmpty() ? null : v;
        });
    }
}
