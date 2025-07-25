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

package org.apache.doris.catalog;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.cloud.catalog.CloudEnvFactory;
import org.apache.doris.common.Config;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.loadv2.BrokerLoadJob;
import org.apache.doris.load.loadv2.LoadJobScheduler;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.plans.distribute.DistributePlanner;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.NereidsCoordinator;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.GlobalTransactionMgrIface;

import org.apache.thrift.TException;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.UUID;

// EnvFactory is responsed for create none-cloud object.
// CloudEnvFactory is responsed for create cloud object.

public class EnvFactory {

    public EnvFactory() {}

    private static class SingletonHolder {
        private static final EnvFactory INSTANCE =
                Config.isCloudMode() ? new CloudEnvFactory() : new EnvFactory();
    }

    public static EnvFactory getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public Env createEnv(boolean isCheckpointCatalog) {
        return new Env(isCheckpointCatalog);
    }

    public InternalCatalog createInternalCatalog() {
        return new InternalCatalog();
    }

    public SystemInfoService createSystemInfoService() {
        return new SystemInfoService();
    }

    public Type getPartitionClass() {
        return Partition.class;
    }

    public Partition createPartition() {
        return new Partition();
    }

    public Type getTabletClass() {
        return Tablet.class;
    }

    public Tablet createTablet() {
        return new Tablet();
    }

    public Tablet createTablet(long tabletId) {
        return new Tablet(tabletId);
    }

    public Replica createReplica() {
        return new Replica();
    }

    public Replica createReplica(Replica.ReplicaContext context) {
        return new Replica(context);
    }

    public ReplicaAllocation createDefReplicaAllocation() {
        return new ReplicaAllocation((short) 3);
    }

    public PropertyAnalyzer createPropertyAnalyzer() {
        return new PropertyAnalyzer();
    }

    public DynamicPartitionProperty createDynamicPartitionProperty(Map<String, String> properties) {
        return new DynamicPartitionProperty(properties);
    }

    public GlobalTransactionMgrIface createGlobalTransactionMgr(Env env) {
        return new GlobalTransactionMgr(env);
    }

    public BrokerLoadJob createBrokerLoadJob(long dbId, String label, BrokerDesc brokerDesc, OriginStatement originStmt,
            UserIdentity userInfo) throws MetaNotFoundException {
        return new BrokerLoadJob(dbId, label, brokerDesc, originStmt, userInfo);
    }

    public BrokerLoadJob createBrokerLoadJob() {
        return new BrokerLoadJob();
    }

    public Coordinator createCoordinator(ConnectContext context, Planner planner,
                                         StatsErrorEstimator statsErrorEstimator) {
        if (planner instanceof NereidsPlanner && SessionVariable.canUseNereidsDistributePlanner()) {
            return new NereidsCoordinator(context, (NereidsPlanner) planner, statsErrorEstimator);
        }
        return new Coordinator(context, planner, statsErrorEstimator);
    }

    // Used for broker load task/export task/update coordinator
    public Coordinator createCoordinator(Long jobId, TUniqueId queryId, DescriptorTable descTable,
                                         List<PlanFragment> fragments, List<ScanNode> scanNodes,
                                         String timezone, boolean loadZeroTolerance, boolean enableProfile) {
        if (SessionVariable.canUseNereidsDistributePlanner()) {
            if (queryId == null) {
                UUID taskId = UUID.randomUUID();
                queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            }
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext == null) {
                connectContext = new ConnectContext();
            }
            if (connectContext.getLoadId() == null) {
                connectContext.setLoadId(queryId);
            }
            if (connectContext.getEnv() == null) {
                connectContext.setEnv(Env.getCurrentEnv());
            }
            StatementContext statementContext = connectContext.getStatementContext();
            if (statementContext == null) {
                statementContext = new StatementContext(connectContext, new OriginStatement("", 0));
            }
            DistributePlanner distributePlanner = new DistributePlanner(
                    statementContext, fragments, false, true);
            FragmentIdMapping<DistributedPlan> distributedPlans = distributePlanner.plan();

            return new NereidsCoordinator(
                    jobId, queryId, descTable, fragments, distributedPlans.valueList(),
                    scanNodes, timezone, loadZeroTolerance, enableProfile
            );
        }
        return new Coordinator(
                jobId, queryId, descTable, fragments, scanNodes, timezone, loadZeroTolerance, enableProfile
        );
    }

    public GroupCommitPlanner createGroupCommitPlanner(Database db, OlapTable table, List<String> targetColumnNames,
            TUniqueId queryId, String groupCommit) throws UserException, TException {
        return  new GroupCommitPlanner(db, table, targetColumnNames, queryId, groupCommit);
    }

    public RoutineLoadManager createRoutineLoadManager() {
        return new RoutineLoadManager();
    }

    public LoadManager createLoadManager(LoadJobScheduler loadJobScheduler) {
        return new LoadManager(loadJobScheduler);
    }

    public MasterDaemon createTabletStatMgr() {
        return new TabletStatMgr();
    }
}
