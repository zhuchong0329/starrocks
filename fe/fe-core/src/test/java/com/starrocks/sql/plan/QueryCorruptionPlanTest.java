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

package com.starrocks.sql.plan;

import com.starrocks.qe.QueryCorruptionPolicy;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Uses actual optimized plans, not mocked scan nodes or mocked filter metadata. */
class QueryCorruptionPlanTest extends PlanTestBase {
    private Runnable restoreSession;

    @BeforeEach
    void prepareSession() {
        SessionVariable session = connectContext.getSessionVariable();
        boolean pipeline = session.isEnablePipelineEngine();
        boolean cache = session.isEnableQueryCache();
        boolean shortCircuit = session.isEnableShortCircuit();
        boolean remoteFilter = session.getEnableGlobalRuntimeFilter();
        long filterMin = session.getGlobalRuntimeFilterProbeMinSize();
        double cteRatio = session.getCboCTERuseRatio();
        restoreSession = () -> {
            session.setEnablePipelineEngine(pipeline);
            session.setEnableQueryCache(cache);
            session.setEnableShortCircuit(shortCircuit);
            session.setEnableGlobalRuntimeFilter(remoteFilter);
            session.setGlobalRuntimeFilterProbeMinSize(filterMin);
            session.setCboCTERuseRatio(cteRatio);
        };
        session.setEnablePipelineEngine(true);
    }

    @AfterEach
    void restoreSession() {
        restoreSession.run();
    }

    private void checkUnchanged(String sql, boolean expected) throws Exception {
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        ExecPlan plan = getExecPlan(sql);
        String before = plan.getExplainString(TExplainLevel.VERBOSE);
        assertFalse(QueryCorruptionPolicy.isEligible(false, false, connectContext, statement, plan, true));
        assertEquals(expected, QueryCorruptionPolicy.isEligible(true, false, connectContext, statement, plan, true), before);
        assertEquals(before, plan.getExplainString(TExplainLevel.VERBOSE));
    }

    @Test
    void scansLimitsAggregationAndCacheKeepTheirExistingPlans() throws Exception {
        for (boolean cache : new boolean[] {false, true}) {
            connectContext.getSessionVariable().setEnableQueryCache(cache);
            for (String sql : List.of("select * from t0 limit 10", "select * from t0 where v2 > 5",
                    "select * from t0 order by v1 desc limit 1000",
                    "select v1, sum(v2) from t0 group by v1")) {
                checkUnchanged(sql, true);
                assertEquals(cache, connectContext.getSessionVariable().isEnableQueryCache());
            }
        }
    }

    @Test
    void realRemoteRuntimeFilterIsExcludedWithoutRemovingTheFilter() throws Exception {
        SessionVariable session = connectContext.getSessionVariable();
        session.setEnableGlobalRuntimeFilter(true);
        session.setGlobalRuntimeFilterProbeMinSize(0);
        String sql = "select * from t0 vt1 join [bucket] t0 vt2 on vt1.v1 = vt2.v1 "
                + "join [broadcast] t1 vt3 on vt1.v1 = vt3.v4 "
                + "join [colocate] t0 vt4 on vt1.v1 = vt4.v1";
        assertTrue(getExecPlan(sql).getExplainString(TExplainLevel.VERBOSE).contains("remote = true"));
        checkUnchanged(sql, false);
        assertTrue(session.getEnableGlobalRuntimeFilter());
    }

    @Test
    void alreadyFilterFreeJoinAndReusedCteAreEligible() throws Exception {
        connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(false);
        checkUnchanged("select * from t0 left join [broadcast] t1 on t0.v1 = t1.v4", true);
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        String cte = "with x as (select * from t0) select * from x union all select * from x";
        assertTrue(getExecPlan(cte).getExplainString(TExplainLevel.NORMAL).contains("MultiCastDataSinks"));
        checkUnchanged(cte, true);
    }

    @Test
    void realShortCircuitPlanRemainsStrictAndUnchanged() throws Exception {
        connectContext.getSessionVariable().setEnableShortCircuit(true);
        String sql = "select * from tprimary1 where pk1 = 20";
        assertTrue(getExecPlan(sql).isShortCircuit());
        checkUnchanged(sql, false);
        assertTrue(connectContext.getSessionVariable().isEnableShortCircuit());
    }
}
