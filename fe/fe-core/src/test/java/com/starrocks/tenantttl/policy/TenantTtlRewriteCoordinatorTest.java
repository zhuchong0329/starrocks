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

package com.starrocks.tenantttl.policy;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Dictionary;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Replica;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.TenantTtlCompactionTask;
import com.starrocks.tenantttl.scheduler.TenantTtlPartitionProgress;
import com.starrocks.tenantttl.scheduler.TenantTtlRewriteCoordinator;
import com.starrocks.tenantttl.scheduler.TenantTtlScheduleDecision;
import com.starrocks.tenantttl.scheduler.TenantTtlScheduler;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTenantTtlCompactionResult;
import com.starrocks.thrift.TTenantTtlTaskCode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TenantTtlRewriteCoordinatorTest {
    private static final String DB_NAME = "tenant_ttl_coordinator_test";
    private static final String TABLE_NAME = "event_log";
    private static final String DICTIONARY_NAME = "tenant_ttl_coordinator_dict";
    private static final String TABLE_KEY = "business.coordinator_log";
    private static final long DICTIONARY_ID = 96001L;
    private static final long SNAPSHOT_TXN_ID = 92L;

    private static GlobalStateMgr state;
    private static Database db;
    private static OlapTable table;
    private static TenantTtlPolicySnapshotManager originalSnapshotManager;
    private static FixedSnapshotManager fixedSnapshotManager;
    private static long evaluationTime;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        state = GlobalStateMgr.getCurrentState();
        StarRocksAssert starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        starRocksAssert.withTable("CREATE TABLE " + DB_NAME + ".tenant_ttl_policy (\n" +
                "tenant VARCHAR(128) NOT NULL, table_name VARCHAR(256) NOT NULL, retention_days INT NOT NULL\n" +
                ") PRIMARY KEY(tenant, table_name) DISTRIBUTED BY HASH(tenant) BUCKETS 1 " +
                "PROPERTIES('replication_num' = '1')");
        state.getDictionaryMgr().addDictionary(new Dictionary(
                DICTIONARY_ID, DICTIONARY_NAME, "tenant_ttl_policy",
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME,
                ImmutableList.of("tenant", "table_name"), ImmutableList.of("retention_days"), new HashMap<>()));

        evaluationTime = System.currentTimeMillis() / 1000L;
        long firstUpper = evaluationTime - 60L * 86400L;
        long retryUpper = evaluationTime - 40L * 86400L;
        starRocksAssert.withTable("CREATE TABLE " + DB_NAME + "." + TABLE_NAME + " (\n" +
                "tenant VARCHAR(128), recordTimestamp BIGINT NOT NULL\n" +
                ") DUPLICATE KEY(tenant, recordTimestamp) PARTITION BY RANGE(recordTimestamp) (\n" +
                "PARTITION p_first VALUES LESS THAN ('" + firstUpper + "'),\n" +
                "PARTITION p_retry VALUES LESS THAN ('" + retryUpper + "'))\n" +
                "DISTRIBUTED BY HASH(tenant) BUCKETS 2 PROPERTIES('replication_num'='1',\n" +
                "'compaction_retention_condition'=\"dictionary_ttl('" + DICTIONARY_NAME + "', '" +
                TABLE_KEY + "', 180)\")");
        db = state.getLocalMetastore().getDb(DB_NAME);
        table = (OlapTable) db.getTable(TABLE_NAME);

        originalSnapshotManager = state.getTenantTtlPolicySnapshotManager();
        fixedSnapshotManager = new FixedSnapshotManager(snapshot());
        Deencapsulation.setField(state, "tenantTtlPolicySnapshotManager", fixedSnapshotManager);
    }

    @AfterAll
    public static void afterClass() {
        if (state != null && originalSnapshotManager != null) {
            Deencapsulation.setField(state, "tenantTtlPolicySnapshotManager", originalSnapshotManager);
        }
    }

    @Test
    @Order(1)
    public void testAllTabletReplicasCompleteBeforeProgressAdvances() {
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_first", 97000);
        Assertions.assertEquals(2, plan.getPartitionPlan().getReplicaTasks().size());
        MutableClock clock = new MutableClock(1_000_000L);
        RecordingSubmitter submitter = new RecordingSubmitter();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(submitter, clock, () -> 0.5);

        coordinator.reconcile(state, Collections.singletonList(plan));
        coordinator.dispatchNext(state);
        TenantTtlCompactionTask first = coordinator.getActiveTask().orElseThrow(AssertionError::new);
        Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.ACCEPTED,
                first.finish(result(first, TTenantTtlTaskCode.NOOP_VERIFIED,
                        first.getObservedMaxVersion() + 5)));
        coordinator.reconcile(state, Collections.singletonList(plan));
        Assertions.assertFalse(state.getTenantTtlPartitionProgressManager().get(progressKey(plan)).isPresent());

        coordinator.dispatchNext(state);
        TenantTtlCompactionTask second = coordinator.getActiveTask().orElseThrow(AssertionError::new);
        Assertions.assertNotEquals(first.getSignature(), second.getSignature());
        Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.ACCEPTED,
                second.finish(result(second, TTenantTtlTaskCode.SUCCESS,
                        second.getObservedMaxVersion() + 3)));
        coordinator.reconcile(state, Collections.singletonList(plan));

        TenantTtlPartitionProgress progress = state.getTenantTtlPartitionProgressManager().get(progressKey(plan))
                .orElseThrow(AssertionError::new);
        Assertions.assertEquals(plan.getPartitionPlan().getObservedVisibleVersion() + 3,
                progress.getProcessedThroughVersion());
        Assertions.assertEquals(2, submitter.tasks.size());
        Assertions.assertFalse(coordinator.getActiveTask().isPresent());
    }

    @Test
    @Order(2)
    public void testDefiniteRetryReusesTaskIdAndReleasesGlobalSlot() {
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 98000);
        MutableClock clock = new MutableClock(2_000_000L);
        RecordingSubmitter submitter = new RecordingSubmitter();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(submitter, clock, () -> 0.5);

        coordinator.reconcile(state, Collections.singletonList(plan));
        coordinator.dispatchNext(state);
        TenantTtlCompactionTask firstAttempt = coordinator.getActiveTask().orElseThrow(AssertionError::new);
        long taskId = firstAttempt.getSignature();
        Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.ACCEPTED,
                firstAttempt.finish(result(firstAttempt, TTenantTtlTaskCode.TABLET_BUSY, -1)));
        coordinator.reconcile(state, Collections.singletonList(plan));

        Assertions.assertFalse(coordinator.hasDestructiveExecutionInFlight());
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.RETRY_BACKOFF,
                coordinator.getExecutionStatuses().get(0).getState());
        coordinator.dispatchNext(state);
        Assertions.assertEquals(1, submitter.tasks.size());
        clock.advance(10_000L);
        coordinator.dispatchNext(state);
        TenantTtlCompactionTask secondAttempt = coordinator.getActiveTask().orElseThrow(AssertionError::new);
        Assertions.assertSame(firstAttempt, secondAttempt);
        Assertions.assertEquals(taskId, secondAttempt.getSignature());
        Assertions.assertEquals(2, submitter.tasks.size());
        coordinator.resetForLeadershipLoss();
    }

    @Test
    @Order(3)
    public void testAlreadyRunningKeepsExclusiveSlotUntilSameRequestConverges() {
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 99000);
        MutableClock clock = new MutableClock(3_000_000L);
        RecordingSubmitter submitter = new RecordingSubmitter();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(submitter, clock, () -> 0.5);

        coordinator.reconcile(state, Collections.singletonList(plan));
        coordinator.dispatchNext(state);
        TenantTtlCompactionTask firstAttempt = coordinator.getActiveTask().orElseThrow(AssertionError::new);
        Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.ACCEPTED,
                firstAttempt.finish(result(firstAttempt, TTenantTtlTaskCode.TTL_ALREADY_RUNNING, -1)));
        coordinator.reconcile(state, Collections.singletonList(plan));

        Assertions.assertTrue(coordinator.hasDestructiveExecutionInFlight());
        coordinator.dispatchNext(state);
        Assertions.assertEquals(1, submitter.tasks.size());
        clock.advance(10_000L);
        coordinator.dispatchNext(state);
        Assertions.assertEquals(firstAttempt.getSignature(),
                coordinator.getActiveTask().orElseThrow(AssertionError::new).getSignature());
        Assertions.assertEquals(2, submitter.tasks.size());
        coordinator.resetForLeadershipLoss();
    }

    @Test
    @Order(4)
    public void testUnavailableReplicaWaitsWithoutTakingGlobalSlot() {
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 100000);
        TenantTtlEvaluationContext.ReplicaTaskSpec spec = plan.getPartitionPlan().getReplicaTasks().get(0);
        LocalTablet tablet = (LocalTablet) table.getPhysicalPartition(plan.getPartitionPlan().getPhysicalPartitionId())
                .getBaseIndex().getTablet(spec.getTabletId());
        Replica replica = tablet.getReplicaById(spec.getReplicaId());
        MutableClock clock = new MutableClock(4_000_000L);
        RecordingSubmitter submitter = new RecordingSubmitter();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(submitter, clock, () -> 0.5);

        replica.setBad(true);
        try {
            coordinator.reconcile(state, Collections.singletonList(plan));
            coordinator.dispatchNext(state);
            Assertions.assertFalse(coordinator.getActiveTask().isPresent());
            Assertions.assertFalse(coordinator.hasDestructiveExecutionInFlight());
            Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.WAITING_REPLICA,
                    coordinator.getExecutionStatuses().get(0).getState());
            Assertions.assertTrue(submitter.tasks.isEmpty());
        } finally {
            replica.setBad(false);
        }
        clock.advance(10_000L);
        coordinator.dispatchNext(state);
        Assertions.assertTrue(coordinator.getActiveTask().isPresent());
        coordinator.resetForLeadershipLoss();
    }

    @Test
    @Order(5)
    public void testPermanentBusinessErrorBlocksWithoutAdvancingProgress() {
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 101000);
        RecordingSubmitter submitter = new RecordingSubmitter();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(
                submitter, new MutableClock(5_000_000L), () -> 0.5);

        coordinator.reconcile(state, Collections.singletonList(plan));
        coordinator.dispatchNext(state);
        TenantTtlCompactionTask task = coordinator.getActiveTask().orElseThrow(AssertionError::new);
        Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.ACCEPTED,
                task.finish(result(task, TTenantTtlTaskCode.DATA_INVARIANT_VIOLATION, -1)));
        coordinator.reconcile(state, Collections.singletonList(plan));

        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.BLOCKED,
                coordinator.getExecutionStatuses().get(0).getState());
        Assertions.assertFalse(coordinator.hasDestructiveExecutionInFlight());
        Assertions.assertFalse(state.getTenantTtlPartitionProgressManager().get(progressKey(plan)).isPresent());
        coordinator.dispatchNext(state);
        Assertions.assertEquals(1, submitter.tasks.size());
        coordinator.resetForLeadershipLoss();
    }

    @Test
    @Order(6)
    public void testLateSuccessForInvalidatedPlanIsFenced() {
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 102000);
        RecordingSubmitter submitter = new RecordingSubmitter();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(
                submitter, new MutableClock(6_000_000L), () -> 0.5);

        coordinator.reconcile(state, Collections.singletonList(plan));
        coordinator.dispatchNext(state);
        TenantTtlCompactionTask task = coordinator.getActiveTask().orElseThrow(AssertionError::new);
        coordinator.reconcile(state, Collections.emptyList());
        Assertions.assertTrue(coordinator.hasDestructiveExecutionInFlight());
        Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.ACCEPTED,
                task.finish(result(task, TTenantTtlTaskCode.NOOP_VERIFIED, task.getObservedMaxVersion())));
        coordinator.reconcile(state, Collections.emptyList());

        Assertions.assertFalse(state.getTenantTtlPartitionProgressManager().get(progressKey(plan)).isPresent());
        Assertions.assertTrue(coordinator.getExecutionStatuses().isEmpty());
        Assertions.assertFalse(coordinator.hasDestructiveExecutionInFlight());
    }

    @Test
    @Order(7)
    public void testResultClassificationIsExhaustive() {
        Map<TTenantTtlTaskCode, TenantTtlRewriteCoordinator.ResultDisposition> expected =
                new EnumMap<>(TTenantTtlTaskCode.class);
        expected.put(TTenantTtlTaskCode.SUCCESS, TenantTtlRewriteCoordinator.ResultDisposition.SUCCESS);
        expected.put(TTenantTtlTaskCode.NOOP_VERIFIED, TenantTtlRewriteCoordinator.ResultDisposition.SUCCESS);
        expected.put(TTenantTtlTaskCode.TABLET_BUSY,
                TenantTtlRewriteCoordinator.ResultDisposition.DEFINITE_RETRY);
        expected.put(TTenantTtlTaskCode.REPLICA_NOT_CAUGHT_UP,
                TenantTtlRewriteCoordinator.ResultDisposition.DEFINITE_RETRY);
        expected.put(TTenantTtlTaskCode.STALE_ROWSET,
                TenantTtlRewriteCoordinator.ResultDisposition.DEFINITE_RETRY);
        expected.put(TTenantTtlTaskCode.CANCELLED,
                TenantTtlRewriteCoordinator.ResultDisposition.DEFINITE_RETRY);
        expected.put(TTenantTtlTaskCode.TTL_ALREADY_RUNNING,
                TenantTtlRewriteCoordinator.ResultDisposition.UNKNOWN_RETRY);
        expected.put(TTenantTtlTaskCode.SCHEMA_CHANGED, TenantTtlRewriteCoordinator.ResultDisposition.REPLAN);
        expected.put(TTenantTtlTaskCode.TABLET_NOT_FOUND, TenantTtlRewriteCoordinator.ResultDisposition.REPLAN);
        expected.put(TTenantTtlTaskCode.INVALID_ARGUMENT, TenantTtlRewriteCoordinator.ResultDisposition.BLOCKED);
        expected.put(TTenantTtlTaskCode.NOT_SUPPORTED, TenantTtlRewriteCoordinator.ResultDisposition.BLOCKED);
        expected.put(TTenantTtlTaskCode.DATA_INVARIANT_VIOLATION,
                TenantTtlRewriteCoordinator.ResultDisposition.BLOCKED);
        expected.put(TTenantTtlTaskCode.INTERNAL_ERROR, TenantTtlRewriteCoordinator.ResultDisposition.BLOCKED);

        Assertions.assertEquals(TTenantTtlTaskCode.values().length, expected.size());
        for (TTenantTtlTaskCode code : TTenantTtlTaskCode.values()) {
            Assertions.assertEquals(expected.get(code), TenantTtlRewriteCoordinator.classifyResult(code),
                    code.name());
        }
    }

    @Test
    @Order(8)
    public void testSoftTimeoutKeepsExclusiveSlotAndRetriesImmutableRequest() {
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 103000);
        MutableClock clock = new MutableClock(7_000_000L);
        RecordingSubmitter submitter = new RecordingSubmitter();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(submitter, clock, () -> 0.5);
        int previousTimeout = Config.tenant_ttl_agent_task_soft_timeout_seconds;
        try {
            Config.tenant_ttl_agent_task_soft_timeout_seconds = 1;
            coordinator.reconcile(state, Collections.singletonList(plan));
            coordinator.dispatchNext(state);
            TenantTtlCompactionTask firstAttempt = coordinator.getActiveTask().orElseThrow(AssertionError::new);
            clock.advance(1_000L);
            coordinator.reconcile(state, Collections.singletonList(plan));
            Assertions.assertFalse(coordinator.getActiveTask().isPresent());
            Assertions.assertTrue(coordinator.hasDestructiveExecutionInFlight());
            Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.UNKNOWN_RETRY,
                    coordinator.getExecutionStatuses().get(0).getState());

            clock.advance(10_000L);
            coordinator.dispatchNext(state);
            Assertions.assertSame(firstAttempt,
                    coordinator.getActiveTask().orElseThrow(AssertionError::new));
            Assertions.assertEquals(2, submitter.tasks.size());
        } finally {
            Config.tenant_ttl_agent_task_soft_timeout_seconds = previousTimeout;
            coordinator.resetForLeadershipLoss();
        }
    }

    @Test
    @Order(9)
    public void testLeaderLocalStateIsDiscardedAndNewPlanUsesNewTaskId() {
        TenantTtlScheduler.PendingRewritePlan oldPlan = pendingPlan("p_retry", 104000);
        RecordingSubmitter oldSubmitter = new RecordingSubmitter();
        TenantTtlRewriteCoordinator oldCoordinator = new TenantTtlRewriteCoordinator(
                oldSubmitter, new MutableClock(8_000_000L), () -> 0.5);
        oldCoordinator.reconcile(state, Collections.singletonList(oldPlan));
        oldCoordinator.dispatchNext(state);
        long oldTaskId = oldCoordinator.getActiveTask().orElseThrow(AssertionError::new).getSignature();
        oldCoordinator.resetForLeadershipLoss();
        Assertions.assertTrue(oldCoordinator.getExecutionStatuses().isEmpty());

        TenantTtlScheduler.PendingRewritePlan newPlan = pendingPlan("p_retry", 105000);
        RecordingSubmitter newSubmitter = new RecordingSubmitter();
        TenantTtlRewriteCoordinator newCoordinator = new TenantTtlRewriteCoordinator(
                newSubmitter, new MutableClock(9_000_000L), () -> 0.5);
        newCoordinator.reconcile(state, Collections.singletonList(newPlan));
        newCoordinator.dispatchNext(state);
        Assertions.assertNotEquals(oldTaskId,
                newCoordinator.getActiveTask().orElseThrow(AssertionError::new).getSignature());
        newCoordinator.resetForLeadershipLoss();
    }

    private static TenantTtlScheduler.PendingRewritePlan pendingPlan(String partitionName, long firstTaskId) {
        AtomicLong ids = new AtomicLong(firstTaskId);
        TenantTtlEvaluationContext.CaptureResult capture = TenantTtlEvaluationContext.capture(
                state, db.getId(), table.getId(), () -> evaluationTime, ids::incrementAndGet);
        Assertions.assertTrue(capture.isSuccess(), capture.getDetail());
        TenantTtlEvaluationContext context = capture.getContext();
        long physicalId = table.getPartition(partitionName).getDefaultPhysicalPartition().getId();
        TenantTtlEvaluationContext.PartitionPlan partitionPlan = context.getPartitionPlans().stream()
                .filter(plan -> plan.getPhysicalPartitionId() == physicalId)
                .findFirst().orElseThrow(AssertionError::new);
        Assertions.assertEquals(TenantTtlPolicyPlanner.PlanType.ROWSET_REWRITE, partitionPlan.getType());
        TenantTtlScheduleDecision.Decision decision =
                TenantTtlScheduleDecision.decide(context, partitionPlan, null, false);
        return new TenantTtlScheduler.PendingRewritePlan(context, partitionPlan, decision);
    }

    private static TenantTtlPartitionProgress.ProgressKey progressKey(TenantTtlScheduler.PendingRewritePlan plan) {
        return new TenantTtlPartitionProgress.ProgressKey(plan.getContext().getDbId(), plan.getContext().getTableId(),
                plan.getPartitionPlan().getPhysicalPartitionId());
    }

    private static TTenantTtlCompactionResult result(TenantTtlCompactionTask task, TTenantTtlTaskCode code,
                                                      long processedVersion) {
        boolean success = code == TTenantTtlTaskCode.SUCCESS || code == TTenantTtlTaskCode.NOOP_VERIFIED;
        TTenantTtlCompactionResult result = new TTenantTtlCompactionResult();
        result.setCode(code);
        result.setDetail_status(new TStatus(success ? TStatusCode.OK : TStatusCode.RUNTIME_ERROR));
        result.setRetryable(!success);
        result.setTask_id(task.getSignature());
        result.setTablet_id(task.getTabletId());
        result.setPartition_id(task.getPartitionId());
        result.setSnapshot_end_version(processedVersion);
        result.setProcessed_through_version(processedVersion);
        result.setCoverage_digest(success ? "verified" : "");
        result.setRowsets(new ArrayList<>());
        result.setScanned_rows(0);
        result.setKept_rows(0);
        result.setDeleted_rows(0);
        result.setTenant_rows_read(0);
        result.setRows_pruned_by_segment_zonemap(0);
        result.setRows_pruned_by_page_zonemap(0);
        result.setLinked_bytes(0);
        result.setRewritten_bytes(0);
        return result;
    }

    private static TenantTtlPolicySnapshot snapshot() {
        Map<TenantTtlByteKey, Integer> overrides = new HashMap<>();
        overrides.put(TenantTtlByteKey.utf8("tenant_a"), 30);
        Map<TenantTtlByteKey, TablePolicy> policies = new HashMap<>();
        policies.put(TenantTtlByteKey.utf8(TABLE_KEY), new TablePolicy(null, overrides));
        return new TenantTtlPolicySnapshot(DICTIONARY_ID, DICTIONARY_NAME, SNAPSHOT_TXN_ID, Instant.EPOCH,
                0, 0, 0, policies);
    }

    private static final class FixedSnapshotManager extends TenantTtlPolicySnapshotManager {
        private final TenantTtlPolicySnapshot snapshot;

        private FixedSnapshotManager(TenantTtlPolicySnapshot snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public synchronized Optional<TenantTtlPolicySnapshot> getCurrentSnapshot(long dictionaryId) {
            return dictionaryId == snapshot.getDictionaryId() ? Optional.of(snapshot) : Optional.empty();
        }
    }

    private static final class RecordingSubmitter implements TenantTtlRewriteCoordinator.TaskSubmitter {
        private final List<TenantTtlCompactionTask> tasks = new ArrayList<>();

        @Override
        public void submit(TenantTtlCompactionTask task) {
            tasks.add(task);
        }
    }

    private static final class MutableClock implements LongSupplier {
        private long now;

        private MutableClock(long now) {
            this.now = now;
        }

        private void advance(long millis) {
            now += millis;
        }

        @Override
        public long getAsLong() {
            return now;
        }
    }
}
