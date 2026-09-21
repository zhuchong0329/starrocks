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
import com.starrocks.planner.ResultSink;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ExecPlan;

/** Constant-time opt-in only; supported BE readers decide whether to tolerate corruption. */
public final class QueryCorruptionPolicy {
    private QueryCorruptionPolicy() {
    }

    // Invoked only by the user-query execution entry point. Internal coordinators must not opt in.
    public static boolean isEligible(boolean configured, boolean internal, ConnectContext context,
                                     StatementBase statement, ExecPlan plan, boolean isOutfileQuery) {
        // The caller has already computed isOutfileQuery for the existing query execution flow.
        if (!configured || internal || !(statement instanceof QueryStatement) || isOutfileQuery
                || plan == null || plan.isShortCircuit() || context.isArrowFlightSql()) {
            return false;
        }
        if (context instanceof HttpConnectContext
                && ((HttpConnectContext) context).isOnlyOutputResultRaw()) {
            return false;
        }
        if (plan.getScanNodes().isEmpty() || plan.getFragments().isEmpty()
                || !(plan.getFragments().get(0).getSink() instanceof ResultSink)) {
            return false;
        }
        // Do not inspect scan types, Pipeline support or runtime filters. Unsupported readers
        // retain their errors; diagnostics from tolerated corruption are best-effort only.
        return true;
    }
}
