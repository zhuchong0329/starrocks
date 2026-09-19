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

import com.starrocks.analysis.LimitElement;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.ShowWarningStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

class QueryCorruptionWarningTest {
    private ConnectContext context() {
        ConnectContext context = mock(ConnectContext.class, CALLS_REAL_METHODS);
        context.state = new QueryState();
        context.setQueryId(new UUID(1, 2));
        return context;
    }

    private PQueryStatistics damaged() {
        PQueryStatistics statistics = new PQueryStatistics();
        statistics.queryCorruptionDetected = true;
        return statistics;
    }

    @Test
    void onlyEnabledSuccessfulExecutionRecordsOneWarning() {
        ConnectContext context = context();
        QueryCorruptionWarning.record(context, damaged());
        assertNull(context.getQueryCorruptionWarning());
        context.getState().setQueryCorruptionToleranceEnabled(true);
        QueryCorruptionWarning.record(context, null);
        QueryCorruptionWarning.record(context, new PQueryStatistics());
        assertNull(context.getQueryCorruptionWarning());
        QueryCorruptionWarning.record(context, damaged());
        QueryCorruptionWarning.record(context, damaged());
        assertEquals(1, context.getState().getWarningRows());
        assertTrue(context.getQueryCorruptionWarning().contains(QueryCorruptionWarning.NAME));
        assertTrue(context.getQueryCorruptionWarning().contains(context.getQueryId().toString()));
    }

    @Test
    void resetThenShowPreservesSnapshotNextQueryClearsIt() {
        ConnectContext context = context();
        context.getState().setQueryCorruptionToleranceEnabled(true);
        QueryCorruptionWarning.record(context, damaged());
        context.getState().reset();
        assertFalse(context.getState().isQueryCorruptionToleranceEnabled());
        assertEquals(0, context.getState().getWarningRows());
        ShowWarningStmt show = new ShowWarningStmt(null, NodePosition.ZERO);
        QueryCorruptionWarning.beginStatement(context, show, false);
        assertEquals(1, QueryCorruptionWarning.rows(context, 0, -1).size());
        QueryCorruptionWarning.beginStatement(context, mock(QueryStatement.class), true);
        assertEquals(1, QueryCorruptionWarning.rows(context, 0, -1).size());
        QueryCorruptionWarning.beginStatement(context, mock(QueryStatement.class), false);
        assertTrue(QueryCorruptionWarning.rows(context, 0, -1).isEmpty());
    }

    @Test
    void zeroOutputIsPartialAndSessionsAreIsolated() {
        ConnectContext first = context();
        ConnectContext second = context();
        first.getState().setQueryCorruptionToleranceEnabled(true);
        PQueryStatistics statistics = damaged();
        statistics.returnedRows = 0L;
        QueryCorruptionWarning.record(first, statistics);
        assertEquals(1, first.getState().getWarningRows());
        assertNull(second.getQueryCorruptionWarning());
    }

    @Test
    void strictErrorIsNotReplacedByWarning() {
        ConnectContext context = context();
        context.getState().setQueryCorruptionToleranceEnabled(true);
        context.getState().setError("strict failure");
        QueryCorruptionWarning.record(context, damaged());
        assertTrue(context.getState().isError());
        assertNull(context.getQueryCorruptionWarning());
    }

    @Test
    void showWarningsHonorsLimitAndOffset() {
        ConnectContext context = context();
        context.setQueryCorruptionWarning("bounded warning");
        ShowWarningStmt show = new ShowWarningStmt(new LimitElement(1, 10), NodePosition.ZERO);
        assertEquals(1, show.getOffset());
        assertTrue(QueryCorruptionWarning.rows(context, show.getOffset(), show.getLimitNum()).isEmpty());
        assertTrue(QueryCorruptionWarning.rows(context, 0, 0).isEmpty());
        assertEquals("9000", QueryCorruptionWarning.rows(context, 0, 1).get(0).get(1));
        assertEquals(1, ShowExecutor.ShowExecutorVisitor.getInstance()
                .visitShowWarningStatement(new ShowWarningStmt(null, NodePosition.ZERO), context).getResultRows().size());
    }

    @Test
    void forwardedDiagnosticIsBoundedAndSetsPacketCount() {
        ConnectContext context = context();
        context.setQueryCorruptionWarning("x".repeat(2000));
        assertEquals(1024, context.getQueryCorruptionWarning().length());
        assertEquals(1, context.getState().getWarningRows());
        context.setQueryCorruptionWarning(null);
        assertEquals(1024, context.getQueryCorruptionWarning().length());
    }

    @Test
    void showErrorsDoesNotExposeWarningOrEraseSnapshot() {
        ConnectContext context = context();
        context.setQueryCorruptionWarning("partial result");
        for (String sql : List.of("show errors", "show errors limit 1")) {
            ShowWarningStmt errors = (ShowWarningStmt) SqlParser.parseSingleStatement(sql, 0);
            assertTrue(errors.isErrorsOnly());
            QueryCorruptionWarning.beginStatement(context, errors, false);
            assertTrue(ShowExecutor.ShowExecutorVisitor.getInstance()
                    .visitShowWarningStatement(errors, context).getResultRows().isEmpty());
        }
        ShowWarningStmt warnings = (ShowWarningStmt) SqlParser.parseSingleStatement("show warnings", 0);
        assertFalse(warnings.isErrorsOnly());
        assertEquals(1, ShowExecutor.ShowExecutorVisitor.getInstance()
                .visitShowWarningStatement(warnings, context).getResultRows().size());
    }
}
