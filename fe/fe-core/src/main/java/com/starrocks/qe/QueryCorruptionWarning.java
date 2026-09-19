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

import com.starrocks.proto.PQueryStatistics;
import com.starrocks.sql.ast.ShowWarningStmt;
import com.starrocks.sql.ast.StatementBase;

import java.util.List;

/** One bounded, session-local diagnostic. It never changes result columns or execution scheduling. */
public final class QueryCorruptionWarning {
    // Product warning code; unused in the 4.0.11 ErrorCode registry.
    public static final int CODE = 9000;
    public static final String NAME = "SR_QUERY_PARTIAL_RESULT";

    private QueryCorruptionWarning() {
    }

    public static void beginStatement(ConnectContext context, StatementBase statement, boolean internal) {
        if (!internal && !(statement instanceof ShowWarningStmt)) {
            context.clearQueryCorruptionWarning();
        }
    }

    public static void record(ConnectContext context, PQueryStatistics statistics) {
        if (context.getState().isQueryCorruptionToleranceEnabled() && !context.getState().isError()
                && statistics != null && Boolean.TRUE.equals(statistics.queryCorruptionDetected)) {
            context.setQueryCorruptionWarning(NAME
                    + ": result may be incomplete or inaccurate because independent local OLAP scans"
                    + " encountered file corruption; query_id=" + context.getQueryId());
        }
    }

    public static List<List<String>> rows(ConnectContext context, long offset, long limit) {
        String message = context.getQueryCorruptionWarning();
        if (message == null || offset > 0 || limit == 0) {
            return List.of();
        }
        return List.of(List.of("Warning", Integer.toString(CODE), message));
    }
}
