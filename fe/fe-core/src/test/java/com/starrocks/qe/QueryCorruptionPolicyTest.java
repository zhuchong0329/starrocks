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

import com.starrocks.catalog.OlapTable;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.planner.HashJoinNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.planner.ScanNode;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TQueryOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class QueryCorruptionPolicyTest {
    private ConnectContext context;
    private QueryStatement statement;
    private ExecPlan plan;
    private SessionVariable session;
    private PlanFragment fragment;
    private OlapScanNode scan;
    private OlapTable table;

    @BeforeEach
    void setUp() {
        context = mock(ConnectContext.class);
        statement = mock(QueryStatement.class);
        plan = mock(ExecPlan.class);
        session = mock(SessionVariable.class);
        fragment = mock(PlanFragment.class);
        scan = mock(OlapScanNode.class);
        table = mock(OlapTable.class);
        when(context.getSessionVariable()).thenReturn(session);
        when(session.isEnablePipelineEngine()).thenReturn(true);
        when(plan.getScanNodes()).thenReturn(List.of(scan));
        when(plan.getFragments()).thenReturn(new ArrayList<>(List.of(fragment)));
        when(scan.getOlapTable()).thenReturn(table);
        when(fragment.getSink()).thenReturn(mock(ResultSink.class));
        when(fragment.canUsePipeline()).thenReturn(true);
        when(fragment.getPlanRoot()).thenReturn(mock(PlanNode.class));
    }

    private boolean eligible() {
        return QueryCorruptionPolicy.isEligible(true, false, context, statement, plan, false);
    }

    @Test
    void supportedSelectAndWireDefault() {
        assertTrue(eligible());
        TQueryOptions options = new TQueryOptions();
        assertFalse(options.isEnable_query_corruption_tolerance());
        assertFalse(options.isSetEnable_query_corruption_tolerance());
        options.setEnable_query_corruption_tolerance(true);
        assertTrue(options.deepCopy().isEnable_query_corruption_tolerance());
    }

    @Test
    void disabledDoesNotInspectPlan() {
        assertFalse(QueryCorruptionPolicy.isEligible(false, false, context, statement, plan, false));
        verifyNoInteractions(context, statement, plan, fragment, scan, table);
    }

    @Test
    void internalAndWritesAreStrict() {
        assertFalse(QueryCorruptionPolicy.isEligible(true, true, context, statement, plan, false));
        assertFalse(QueryCorruptionPolicy.isEligible(true, false, context, mock(StatementBase.class), plan, false));
    }

    @Test
    void explainIncludingAnalyzeMayOptInButOutfileIsStrict() {
        when(statement.isExplain()).thenReturn(true);
        for (StatementBase.ExplainLevel level : StatementBase.ExplainLevel.values()) {
            when(statement.getExplainLevel()).thenReturn(level);
            assertTrue(eligible());
        }
        assertFalse(QueryCorruptionPolicy.isEligible(true, false, context, statement, plan, true));
        // Reuse the caller's existing OUTFILE boolean; do not inspect the AST again.
        verify(statement, never()).hasOutFileClause();
        verify(statement, never()).isExplain();
        verify(statement, never()).getExplainLevel();
    }

    @Test
    void shortCircuitRemainsStrictButPipelineSupportIsNotChecked() {
        when(plan.isShortCircuit()).thenReturn(true);
        assertFalse(eligible());
        when(plan.isShortCircuit()).thenReturn(false);
        when(fragment.canUsePipeline()).thenReturn(false);
        when(session.isEnablePipelineEngine()).thenReturn(false);
        assertTrue(eligible());
        verify(fragment, never()).canUsePipeline();
        verifyNoInteractions(session);
    }

    @Test
    void lakeConnectorAndMixedSourcesMayCarryTheOptionWithoutNewReaderSupport() {
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        assertTrue(eligible());
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(false);
        when(scan.isRunningAsConnectorOperator()).thenReturn(true);
        assertTrue(eligible());
        when(scan.isRunningAsConnectorOperator()).thenReturn(false);
        when(plan.getScanNodes()).thenReturn(List.of(scan, mock(ScanNode.class)));
        assertTrue(eligible());
        when(plan.getScanNodes()).thenReturn(List.of(mock(ScanNode.class)));
        assertTrue(eligible());
        verifyNoInteractions(scan, table);
    }

    @Test
    void noStorageScanOrResultSinkIsStrict() {
        when(plan.getScanNodes()).thenReturn(List.of());
        assertFalse(eligible());
        when(plan.getScanNodes()).thenReturn(List.of(scan));
        when(fragment.getSink()).thenReturn(null);
        assertFalse(eligible());
    }

    @Test
    void remoteRuntimeFilterDoesNotPreventOptInOrGetInspected() {
        HashJoinNode join = mock(HashJoinNode.class);
        RuntimeFilterDescription filter = mock(RuntimeFilterDescription.class);
        when(join.getBuildRuntimeFilters()).thenReturn(List.of(filter));
        when(fragment.getPlanRoot()).thenReturn(join);
        assertTrue(eligible());
        when(filter.isHasRemoteTargets()).thenReturn(true);
        assertTrue(eligible());
        verify(fragment, never()).getPlanRoot();
        verifyNoInteractions(join, filter);
    }

    @Test
    void standardHttpMayOptInAndRawIsStrict() {
        HttpConnectContext http = mock(HttpConnectContext.class);
        assertTrue(QueryCorruptionPolicy.isEligible(true, false, http, statement, plan, false));
        when(plan.getScanNodes()).thenReturn(List.of(mock(ScanNode.class)));
        assertTrue(QueryCorruptionPolicy.isEligible(true, false, http, statement, plan, false));
        when(http.isOnlyOutputResultRaw()).thenReturn(true);
        assertFalse(QueryCorruptionPolicy.isEligible(true, false, http, statement, plan, false));
    }

    @Test
    void eligibilityDoesNotWalkScansOrFragmentTrees() {
        when(plan.getScanNodes()).thenReturn(new AbstractList<ScanNode>() {
            @Override
            public ScanNode get(int index) {
                throw new AssertionError("eligibility must not inspect scan nodes");
            }

            @Override
            public int size() {
                return 100_000;
            }
        });
        PlanFragment other = mock(PlanFragment.class);
        when(plan.getFragments()).thenReturn(new ArrayList<>(List.of(fragment, other)));
        when(fragment.getPlanRoot()).thenThrow(new AssertionError("must not inspect the plan tree"));
        when(fragment.canUsePipeline()).thenThrow(new AssertionError("must not recompute Pipeline support"));
        assertTrue(eligible());
        verifyNoInteractions(other, session, scan, table);
    }

    @Test
    void missingPlanOrFragmentsRemainStrict() {
        assertFalse(QueryCorruptionPolicy.isEligible(true, false, context, statement, null, false));
        when(plan.getFragments()).thenReturn(new ArrayList<>());
        assertFalse(eligible());
    }

    @Test
    void arrowIsStrict() {
        when(context.isArrowFlightSql()).thenReturn(true);
        assertFalse(eligible());
    }
}
