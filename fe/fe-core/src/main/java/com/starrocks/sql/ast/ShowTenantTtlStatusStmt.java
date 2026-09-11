// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

/** SHOW TENANT TTL STATUS for one physical OLAP table, optionally resolving one tenant. */
public final class ShowTenantTtlStatusStmt extends ShowStmt {
    public static final ImmutableList<String> BASE_TITLE_NAMES = ImmutableList.of(
            "PhysicalTable", "Enabled", "BindingState", "DictionaryName", "DictionaryId",
            "TableKey", "DefaultDays", "DictionaryLastSuccessTxnId", "DictionaryLastSuccessTime",
            "SnapshotTxnId", "SnapshotTime", "LastSnapshotAttemptTxnId", "LastSnapshotAttemptTime",
            "TableKeyMatch", "TableDefaultMatch", "TenantColumnId", "TenantColumnUniqueId",
            "TimeColumnId", "TimeColumnUniqueId", "PartitionExpressionType",
            "CompactionRetentionTimeZone", "ProvablePhysicalPartitions", "UnprovablePhysicalPartitions",
            "SchedulerState", "PendingRewritePartitions", "RunningReplicaTasks",
            "CompletedPhysicalPartitions", "NextExpiryTime", "IgnoredZeroRows", "ErrorMessage");
    public static final ImmutableList<String> TENANT_TITLE_NAMES = ImmutableList.of(
            "Tenant", "EffectiveRetentionDays", "ResolutionType");

    private final TableName tableName;
    private final String tenant;

    public ShowTenantTtlStatusStmt(TableName tableName, String tenant, NodePosition pos) {
        super(pos);
        this.tableName = tableName;
        this.tenant = tenant;
    }

    public TableName getTableName() {
        return tableName;
    }

    public boolean hasTenant() {
        return tenant != null;
    }

    public String getTenant() {
        return tenant;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowTenantTtlStatusStatement(this, context);
    }
}
