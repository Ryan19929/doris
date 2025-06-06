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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Physical odbc scan for odbc table.
 */
public class PhysicalOdbcScan extends PhysicalCatalogRelation {
    /**
     * Constructor for PhysicalOdbcScan.
     */
    public PhysicalOdbcScan(RelationId id, TableIf table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties) {
        this(id, table, qualifier, groupExpression, logicalProperties,
                null, null, ImmutableList.of());
    }

    /**
     * Constructor for PhysicalOdbcScan.
     */
    public PhysicalOdbcScan(RelationId id, TableIf table, List<String> qualifier,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties, Statistics statistics,
            Collection<Slot> operativeSlots) {
        super(id, PlanType.PHYSICAL_ODBC_SCAN, table, qualifier, groupExpression,
                logicalProperties, physicalProperties, statistics, operativeSlots);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalOdbcScan",
            "qualified", Utils.qualifiedName(qualifier, table.getName()),
            "output", getOutput(),
            "stats", statistics
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalOdbcScan(this, context);
    }

    @Override
    public PhysicalOdbcScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalOdbcScan(relationId, table, qualifier, groupExpression, getLogicalProperties());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalOdbcScan(relationId, table, qualifier, groupExpression, logicalProperties.get());
    }

    @Override
    public PhysicalOdbcScan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                                                           Statistics statistics) {
        return new PhysicalOdbcScan(relationId, table, qualifier, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, operativeSlots);
    }
}
