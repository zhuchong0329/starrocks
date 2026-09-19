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

package com.starrocks.qe;

import com.starrocks.http.HttpConnectContext;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.RuntimeFilterBuildNode;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.planner.ScanNode;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ExecPlan;

/** Eligibility only: never rewrites the plan or disables an optimization to enable tolerance. */
public final class QueryCorruptionPolicy {
    private QueryCorruptionPolicy() {
    }

    // Invoked only by the user-query execution entry point. Internal coordinators must not opt in.
    public static boolean isEligible(boolean configured, boolean internal, ConnectContext context,
                                     StatementBase statement, ExecPlan plan, boolean httpSupported) {
        if (!configured || internal || !(statement instanceof QueryStatement) || statement.isExplain()
                || ((QueryStatement) statement).hasOutFileClause() || plan == null || plan.isShortCircuit()
                || context.isArrowFlightSql() || !context.getSessionVariable().isEnablePipelineEngine()) {
            return false;
        }
        if (context instanceof HttpConnectContext
                && (!httpSupported || ((HttpConnectContext) context).isOnlyOutputResultRaw())) {
            return false;
        }
        if (plan.getScanNodes().isEmpty() || plan.getFragments().isEmpty()
                || !(plan.getFragments().get(0).getSink() instanceof ResultSink)) {
            return false;
        }
        for (ScanNode scan : plan.getScanNodes()) {
            if (!(scan instanceof OlapScanNode) || scan.isRunningAsConnectorOperator()
                    || ((OlapScanNode) scan).getOlapTable().isCloudNativeTableOrMaterializedView()) {
                return false;
            }
        }
        for (PlanFragment fragment : plan.getFragments()) {
            if (!fragment.canUsePipeline() || hasRemoteRuntimeFilter(fragment.getPlanRoot())) {
                return false;
            }
        }
        return true;
    }

    // Remote filters can affect another input before its data/EOS carries the diagnostic.
    // Keep these plans strict until their control channel is supported; never turn filters off.
    private static boolean hasRemoteRuntimeFilter(PlanNode node) {
        if (node instanceof RuntimeFilterBuildNode) {
            for (RuntimeFilterDescription filter : ((RuntimeFilterBuildNode) node).getBuildRuntimeFilters()) {
                if (filter.isHasRemoteTargets()) {
                    return true;
                }
            }
        }
        if (!(node instanceof ExchangeNode)) {
            for (PlanNode child : node.getChildren()) {
                if (hasRemoteRuntimeFilter(child)) {
                    return true;
                }
            }
        }
        return false;
    }
}
