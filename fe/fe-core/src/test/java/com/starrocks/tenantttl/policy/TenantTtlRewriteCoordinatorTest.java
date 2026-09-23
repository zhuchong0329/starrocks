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
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.TenantTtlCompactionTask;
import com.starrocks.tenantttl.scheduler.TenantTtlPartitionProgress;
import com.starrocks.tenantttl.scheduler.TenantTtlPartitionProgressManager;
import com.starrocks.tenantttl.scheduler.TenantTtlRewriteCoordinator;
import com.starrocks.tenantttl.scheduler.TenantTtlScheduleDecision;
import com.starrocks.tenantttl.scheduler.TenantTtlScheduler;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTenantTtlCompactionResult;
import com.starrocks.thrift.TTenantTtlTaskCode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

    private int originalAttempts;
    private int originalTimeout;

    @org.junit.jupiter.api.BeforeEach
    public void resetProgress() {
        originalAttempts = Config.tenant_ttl_agent_task_max_attempts;
        originalTimeout = Config.tenant_ttl_agent_task_soft_timeout_seconds;
        state.getTenantTtlPartitionProgressManager().removeTable(db.getId(), table.getId());
    }

    @org.junit.jupiter.api.AfterEach
    public void restoreConfig() {
        Config.tenant_ttl_agent_task_max_attempts = originalAttempts;
        Config.tenant_ttl_agent_task_soft_timeout_seconds = originalTimeout;
        fixedSnapshotManager.snapshot = snapshot();
    }

    private static TenantTtlRewriteCoordinator.ExecutionStatus run(TenantTtlRewriteCoordinator coordinator,
                                                                   TenantTtlScheduler.PendingRewritePlan plan) {
        return coordinator.executePartition(state, plan, () -> true, status -> { });
    }

    @Test
    public void testAllTabletReplicasCompleteInOneCallAndUseMinimumVersion() {
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_first", 97000);
        AtomicLong clock = new AtomicLong();
        List<TenantTtlCompactionTask> sent = new ArrayList<>();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
            Assertions.assertFalse(state.getTenantTtlPartitionProgressManager().get(progressKey(plan)).isPresent());
            sent.add(task);
            task.finish(result(task, TTenantTtlTaskCode.SUCCESS,
                    task.getObservedMaxVersion() + (sent.size() == 1 ? 5 : 3)));
        }, clock::get, clock::addAndGet);
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.COMPLETED, run(coordinator, plan).getState());
        Assertions.assertEquals(2, sent.size());
        Assertions.assertNotEquals(sent.get(0).getSignature(), sent.get(1).getSignature());
        Assertions.assertEquals(plan.getPartitionPlan().getObservedVisibleVersion() + 3,
                state.getTenantTtlPartitionProgressManager().get(progressKey(plan)).orElseThrow().getProcessedThroughVersion());
        Assertions.assertFalse(coordinator.getActiveTask().isPresent());
    }

    @Test
    public void testThirtiethAttemptCanSucceedWithoutChangingRequest() {
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 98000);
        AtomicLong clock = new AtomicLong();
        List<TenantTtlCompactionTask> sent = new ArrayList<>();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
            sent.add(task);
            boolean success = sent.size() >= 30;
            task.finish(result(task, success ? TTenantTtlTaskCode.NOOP_VERIFIED : TTenantTtlTaskCode.TABLET_BUSY,
                    success ? task.getObservedMaxVersion() : -1));
        }, clock::get, clock::addAndGet);
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.COMPLETED, run(coordinator, plan).getState());
        Assertions.assertEquals(31, sent.size()); // Thirty for the first Replica, one for the second.
        Assertions.assertEquals(290_000L, clock.get());
        for (int index = 1; index < 30; index++) {
            Assertions.assertEquals(sent.get(0).toThrift(), sent.get(index).toThrift());
        }
    }

    @Test
    public void testAttemptExhaustionContinuesOtherReplicas() {
        Config.tenant_ttl_agent_task_max_attempts = 3;
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 99000);
        AtomicLong clock = new AtomicLong();
        List<TenantTtlCompactionTask> sent = new ArrayList<>();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
            sent.add(task);
            task.finish(result(task, TTenantTtlTaskCode.TABLET_BUSY, -1));
        }, clock::get, clock::addAndGet);
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.ATTEMPTS_EXHAUSTED,
                run(coordinator, plan).getState());
        Assertions.assertEquals(6, sent.size());
        Assertions.assertEquals(40_000L, clock.get());
        Assertions.assertFalse(state.getTenantTtlPartitionProgressManager().get(progressKey(plan)).isPresent());
    }

    @Test
    public void testTimeoutDoesNotRetryOrKeepAnExclusiveSlot() {
        Config.tenant_ttl_agent_task_soft_timeout_seconds = 2;
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 100000);
        AtomicLong clock = new AtomicLong();
        List<TenantTtlCompactionTask> sent = new ArrayList<>();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
            sent.add(task);
            if (sent.size() > 1) {
                task.finish(result(task, TTenantTtlTaskCode.SUCCESS, task.getObservedMaxVersion()));
            }
        }, clock::get, clock::addAndGet);
        TenantTtlRewriteCoordinator.ExecutionStatus status = run(coordinator, plan);
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.TIMED_OUT, status.getState());
        Assertions.assertEquals(1, status.getSuccessfulReplicas());
        Assertions.assertEquals(2, sent.size());
        Assertions.assertEquals(2000L, clock.get());
        Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.CLOSED,
                sent.get(0).finish(result(sent.get(0), TTenantTtlTaskCode.SUCCESS, sent.get(0).getObservedMaxVersion())));
        Assertions.assertFalse(state.getTenantTtlPartitionProgressManager().get(progressKey(plan)).isPresent());
    }

    @Test
    public void testCumulativeDeadlineDoesNotGrantAnotherHour() {
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 101000);
        AtomicLong clock = new AtomicLong();
        List<TenantTtlCompactionTask> sent = new ArrayList<>();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
            sent.add(task);
            if (sent.size() == 1) {
                clock.addAndGet(3_500_000L);
                task.finish(result(task, TTenantTtlTaskCode.STALE_ROWSET, -1));
            } else if (sent.size() > 2) {
                task.finish(result(task, TTenantTtlTaskCode.SUCCESS, task.getObservedMaxVersion()));
            }
        }, clock::get, clock::addAndGet);
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.TIMED_OUT, run(coordinator, plan).getState());
        Assertions.assertEquals(3, sent.size());
        Assertions.assertEquals(sent.get(0).toThrift(), sent.get(1).toThrift());
        Assertions.assertEquals(3_600_000L, clock.get());
    }

    @Test
    public void testUnavailablePreflightConsumesAttemptsWithoutSending() {
        Config.tenant_ttl_agent_task_max_attempts = 3;
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 102000);
        TenantTtlEvaluationContext.ReplicaTaskSpec spec = plan.getPartitionPlan().getReplicaTasks().get(0);
        LocalTablet tablet = (LocalTablet) table.getPhysicalPartition(plan.getPartitionPlan().getPhysicalPartitionId())
                .getBaseIndex().getTablet(spec.getTabletId());
        Replica replica = tablet.getReplicaById(spec.getReplicaId());
        AtomicLong clock = new AtomicLong();
        List<TenantTtlCompactionTask> sent = new ArrayList<>();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
            sent.add(task);
            task.finish(result(task, TTenantTtlTaskCode.SUCCESS, task.getObservedMaxVersion()));
        }, clock::get, clock::addAndGet);
        replica.setBad(true);
        try {
            Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.ATTEMPTS_EXHAUSTED,
                    run(coordinator, plan).getState());
            Assertions.assertEquals(1, sent.size());
            Assertions.assertEquals(20_000L, clock.get());
        } finally {
            replica.setBad(false);
        }
    }

    @Test
    public void testAlreadyRunningWaitsForRealCompletionWithoutBusinessRetry() {
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 103000);
        AtomicLong clock = new AtomicLong();
        List<TenantTtlCompactionTask> sent = new ArrayList<>();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
            sent.add(task);
            if (sent.size() == 1) {
                Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.STILL_RUNNING,
                        task.finish(result(task, TTenantTtlTaskCode.TTL_ALREADY_RUNNING, -1)));
            } else {
                task.finish(result(task, TTenantTtlTaskCode.SUCCESS, task.getObservedMaxVersion()));
            }
        }, clock::get, millis -> {
            clock.addAndGet(millis);
            TenantTtlCompactionTask task = sent.get(0);
            task.finish(result(task, TTenantTtlTaskCode.SUCCESS, task.getObservedMaxVersion()));
        });
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.COMPLETED, run(coordinator, plan).getState());
        Assertions.assertEquals(2, sent.size());
        Assertions.assertEquals(1000L, clock.get());
    }

    @Test
    public void testUncertainSubmissionWaitsUntilDeadline() {
        Config.tenant_ttl_agent_task_soft_timeout_seconds = 2;
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 104000);
        AtomicLong clock = new AtomicLong();
        AtomicLong sends = new AtomicLong();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
            sends.incrementAndGet();
            throw new IllegalStateException("response lost");
        }, clock::get, clock::addAndGet);
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.TIMED_OUT, run(coordinator, plan).getState());
        Assertions.assertEquals(2, sends.get());
        Assertions.assertEquals(4000L, clock.get());
    }

    @Test
    public void testBlockedPlanDoesNotRetryWithNewTaskIds() {
        for (TTenantTtlTaskCode code : List.of(TTenantTtlTaskCode.INVALID_ARGUMENT, TTenantTtlTaskCode.NOT_SUPPORTED,
                TTenantTtlTaskCode.DATA_INVARIANT_VIOLATION, TTenantTtlTaskCode.INTERNAL_ERROR)) {
            AtomicLong clock = new AtomicLong();
            AtomicLong sends = new AtomicLong();
            TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
                sends.incrementAndGet();
                task.finish(result(task, code, -1));
            }, clock::get, clock::addAndGet);
            Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.BLOCKED,
                    run(coordinator, pendingPlan("p_retry", 105000)).getState());
            clock.addAndGet(600_000L);
            Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.BLOCKED,
                    run(coordinator, pendingPlan("p_retry", 106000)).getState());
            Assertions.assertEquals(1, sends.get(), code.name());
            coordinator.resetForLeadershipLoss();
            run(coordinator, pendingPlan("p_retry", 107000));
            Assertions.assertEquals(2, sends.get(), code.name());
        }
    }

    @Test
    public void testLeadershipLossClosesAttemptAndStopsRemainingTasks() {
        AtomicLong clock = new AtomicLong();
        List<TenantTtlCompactionTask> sent = new ArrayList<>();
        TenantTtlRewriteCoordinator[] holder = new TenantTtlRewriteCoordinator[1];
        holder[0] = new TenantTtlRewriteCoordinator(sent::add, clock::get, millis -> {
            clock.addAndGet(millis);
            holder[0].resetForLeadershipLoss();
        });
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.REPLAN_REQUIRED,
                run(holder[0], pendingPlan("p_retry", 108000)).getState());
        Assertions.assertEquals(1, sent.size());
        Assertions.assertFalse(holder[0].getActiveTask().isPresent());
        Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.CLOSED,
                sent.get(0).finish(result(sent.get(0), TTenantTtlTaskCode.SUCCESS, sent.get(0).getObservedMaxVersion())));
    }

    @Test
    public void testPartialSuccessIsRecheckedNextRoundWithNewIds() {
        Config.tenant_ttl_agent_task_max_attempts = 1;
        AtomicLong clock = new AtomicLong();
        List<TenantTtlCompactionTask> sent = new ArrayList<>();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
            sent.add(task);
            boolean fail = sent.size() == 2;
            task.finish(result(task, fail ? TTenantTtlTaskCode.TABLET_BUSY : TTenantTtlTaskCode.NOOP_VERIFIED,
                    fail ? -1 : task.getObservedMaxVersion()));
        }, clock::get, clock::addAndGet);
        TenantTtlScheduler.PendingRewritePlan first = pendingPlan("p_retry", 109000);
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.ATTEMPTS_EXHAUSTED,
                run(coordinator, first).getState());
        Assertions.assertFalse(state.getTenantTtlPartitionProgressManager().get(progressKey(first)).isPresent());
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.COMPLETED,
                run(coordinator, pendingPlan("p_retry", 110000)).getState());
        Assertions.assertEquals(4, sent.size());
        Assertions.assertNotEquals(sent.get(0).getSignature(), sent.get(2).getSignature());
    }

    @Test
    public void testSchemaChangedRequiresReplanWithoutRetry() {
        AtomicLong clock = new AtomicLong();
        AtomicLong sends = new AtomicLong();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
            sends.incrementAndGet();
            task.finish(result(task, TTenantTtlTaskCode.SCHEMA_CHANGED, -1));
        }, clock::get, clock::addAndGet);
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.REPLAN_REQUIRED,
                run(coordinator, pendingPlan("p_retry", 111000)).getState());
        Assertions.assertEquals(1, sends.get());
    }

    @Test
    public void testEveryBusinessCodeIsClassified() {
        for (TTenantTtlTaskCode code : TTenantTtlTaskCode.values()) {
            Assertions.assertNotNull(TenantTtlRewriteCoordinator.classifyResult(code));
        }
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ResultDisposition.WAIT_FOR_RESULT,
                TenantTtlRewriteCoordinator.classifyResult(TTenantTtlTaskCode.TTL_ALREADY_RUNNING));
    }

    @Test
    public void testCatalogReadLockCoversInitialProgressPublication() {
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 112000);
        AtomicLong publications = new AtomicLong();
        new MockUp<TenantTtlPartitionProgressManager>() {
            @Mock
            public TenantTtlPartitionProgressManager.AdvanceResult compareAndAdvanceCompletedPlan(
                    Invocation invocation, TenantTtlPartitionProgress expected, TenantTtlPartitionProgress candidate)
                    throws Exception {
                if (candidate.key().equals(progressKey(plan))) {
                    Assertions.assertNull(expected);
                    FutureTask<Boolean> conflictingDdl = new FutureTask<>(() -> {
                        Locker locker = new Locker();
                        boolean acquired = locker.tryLockDatabase(db.getId(), LockType.WRITE, 50, TimeUnit.MILLISECONDS);
                        if (acquired) {
                            locker.unLockDatabase(db.getId(), LockType.WRITE);
                        }
                        return acquired;
                    });
                    Thread thread = new Thread(conflictingDdl, "tenant-ttl-progress-ddl-race");
                    thread.start();
                    Assertions.assertFalse(conflictingDdl.get(5, TimeUnit.SECONDS),
                            "Catalog DROP/rebind must not pass between validation and progress journal");
                    publications.incrementAndGet();
                }
                return invocation.proceed(expected, candidate);
            }
        };
        AtomicLong clock = new AtomicLong();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task ->
                task.finish(result(task, TTenantTtlTaskCode.SUCCESS, task.getObservedMaxVersion())),
                clock::get, clock::addAndGet);
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.COMPLETED, run(coordinator, plan).getState());
        Assertions.assertEquals(1, publications.get());
    }

    @Test
    public void testSamePolicyNewTransactionDoesNotInvalidateFrozenRequest() {
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 113000);
        AtomicLong clock = new AtomicLong();
        AtomicLong sends = new AtomicLong();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
            sends.incrementAndGet();
            fixedSnapshotManager.snapshot = snapshot(SNAPSHOT_TXN_ID + 1, 30);
            Assertions.assertEquals(SNAPSHOT_TXN_ID, task.toThrift().getPolicy_watermark().getDictionary_txn_id());
            task.finish(result(task, TTenantTtlTaskCode.SUCCESS, task.getObservedMaxVersion()));
        }, clock::get, clock::addAndGet);
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.COMPLETED, run(coordinator, plan).getState());
        Assertions.assertEquals(2, sends.get());
        Assertions.assertEquals(SNAPSHOT_TXN_ID, state.getTenantTtlPartitionProgressManager()
                .get(progressKey(plan)).orElseThrow().getLastSuccessSnapshotTxnId());
    }

    @Test
    public void testPolicyChangeAfterLastSuccessCannotPublishOldProgress() {
        TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 114000);
        AtomicLong clock = new AtomicLong();
        AtomicLong sends = new AtomicLong();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
            if (sends.incrementAndGet() == plan.getPartitionPlan().getReplicaTasks().size()) {
                fixedSnapshotManager.snapshot = snapshot(SNAPSHOT_TXN_ID + 1, 45);
            }
            task.finish(result(task, TTenantTtlTaskCode.SUCCESS, task.getObservedMaxVersion()));
        }, clock::get, clock::addAndGet);
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.REPLAN_REQUIRED,
                run(coordinator, plan).getState());
        Assertions.assertEquals(2, sends.get());
        Assertions.assertFalse(state.getTenantTtlPartitionProgressManager().get(progressKey(plan)).isPresent());
    }

    @Test
    public void testBlockedRecoveryUsesSemanticsNotTransactionOrClock() {
        for (TTenantTtlTaskCode code : List.of(TTenantTtlTaskCode.INVALID_ARGUMENT, TTenantTtlTaskCode.NOT_SUPPORTED,
                TTenantTtlTaskCode.DATA_INVARIANT_VIOLATION, TTenantTtlTaskCode.INTERNAL_ERROR)) {
            fixedSnapshotManager.snapshot = snapshot();
            AtomicLong clock = new AtomicLong();
            AtomicLong sends = new AtomicLong();
            TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
                sends.incrementAndGet();
                task.finish(result(task, code, -1));
            }, clock::get, clock::addAndGet);
            run(coordinator, pendingPlan("p_retry", 115000));
            fixedSnapshotManager.snapshot = snapshot(SNAPSHOT_TXN_ID + 1, 30);
            long originalEvaluationTime = evaluationTime;
            try {
                evaluationTime += 60;
                run(coordinator, pendingPlan("p_retry", 116000));
            } finally {
                evaluationTime = originalEvaluationTime;
            }
            Assertions.assertEquals(1, sends.get(), code.name());
            Assertions.assertTrue(coordinator.getExecutionStatuses().isEmpty());
            fixedSnapshotManager.snapshot = snapshot(SNAPSHOT_TXN_ID + 2, 31);
            run(coordinator, pendingPlan("p_retry", 117000));
            Assertions.assertEquals(2, sends.get(), code.name());
            coordinator.discardOrphanedBlocks(state, Collections.emptySet());
            run(coordinator, pendingPlan("p_retry", 118000));
            Assertions.assertEquals(3, sends.get(), code.name());
        }
    }

    @Test
    public void testBlockedRecoveryRequiresTrustedBackendRestart() {
        long backendId = pendingPlan("p_retry", 119000).getPartitionPlan().getReplicaTasks().get(0).getBackendId();
        ComputeNode backend = state.getNodeMgr().getClusterInfo().getBackendOrComputeNode(backendId);
        long originalStart = backend.getLastStartTime();
        try {
            for (TTenantTtlTaskCode code : List.of(TTenantTtlTaskCode.INVALID_ARGUMENT, TTenantTtlTaskCode.NOT_SUPPORTED,
                    TTenantTtlTaskCode.DATA_INVARIANT_VIOLATION, TTenantTtlTaskCode.INTERNAL_ERROR)) {
                backend.setLastStartTime(100);
                AtomicLong clock = new AtomicLong();
                AtomicLong sends = new AtomicLong();
                TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
                    sends.incrementAndGet();
                    task.finish(result(task, code, -1));
                }, clock::get, clock::addAndGet);
                run(coordinator, pendingPlan("p_retry", 120000));
                backend.setLastStartTime(0);
                run(coordinator, pendingPlan("p_retry", 121000));
                Assertions.assertEquals(1, sends.get(), "unknown start is not recovery");
                backend.setLastStartTime(200);
                run(coordinator, pendingPlan("p_retry", 122000));
                Assertions.assertEquals(code == TTenantTtlTaskCode.INVALID_ARGUMENT ? 1 : 2, sends.get(), code.name());
            }
        } finally {
            backend.setLastStartTime(originalStart);
        }
    }

    @Test
    public void testDataVersionOnlyUnlocksDataAndInternalErrors() {
        PhysicalPartition physical = table.getPartition("p_retry").getDefaultPhysicalPartition();
        long originalVersion = physical.getVisibleVersion();
        long originalVersionTime = physical.getVisibleVersionTime();
        List<Replica> replicas = new ArrayList<>();
        Map<Replica, Long> versions = new HashMap<>();
        physical.getBaseIndex().getTablets().forEach(tablet ->
                replicas.addAll(((LocalTablet) tablet).getImmutableReplicas()));
        replicas.forEach(replica -> versions.put(replica, replica.getVersion()));
        try {
            for (TTenantTtlTaskCode code : List.of(TTenantTtlTaskCode.INVALID_ARGUMENT, TTenantTtlTaskCode.NOT_SUPPORTED,
                    TTenantTtlTaskCode.DATA_INVARIANT_VIOLATION, TTenantTtlTaskCode.INTERNAL_ERROR)) {
                physical.setVisibleVersion(originalVersion, originalVersionTime);
                AtomicLong clock = new AtomicLong();
                AtomicLong sends = new AtomicLong();
                TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
                    sends.incrementAndGet();
                    task.finish(result(task, code, -1));
                }, clock::get, clock::addAndGet);
                run(coordinator, pendingPlan("p_retry", 123000));
                physical.setVisibleVersion(originalVersion + 1, originalVersionTime + 1);
                replicas.forEach(replica -> Deencapsulation.setField(replica, "version", originalVersion + 1));
                run(coordinator, pendingPlan("p_retry", 124000));
                boolean dataError = code == TTenantTtlTaskCode.DATA_INVARIANT_VIOLATION ||
                        code == TTenantTtlTaskCode.INTERNAL_ERROR;
                Assertions.assertEquals(dataError ? 2 : 1, sends.get(), code.name());
            }
        } finally {
            physical.setVisibleVersion(originalVersion, originalVersionTime);
            versions.forEach((replica, version) -> Deencapsulation.setField(replica, "version", version));
        }
    }

    @Test
    public void testDynamicBudgetChangesOnlyAffectTheNextReplica() {
        Config.tenant_ttl_agent_task_max_attempts = 2;
        Config.tenant_ttl_agent_task_soft_timeout_seconds = 30;
        AtomicLong clock = new AtomicLong();
        AtomicLong sends = new AtomicLong();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
            long index = sends.incrementAndGet();
            Config.tenant_ttl_agent_task_max_attempts = 1;
            Config.tenant_ttl_agent_task_soft_timeout_seconds = 1;
            if (index <= 2) {
                task.finish(result(task, index == 1 ? TTenantTtlTaskCode.TABLET_BUSY : TTenantTtlTaskCode.SUCCESS,
                        index == 1 ? -1 : task.getObservedMaxVersion()));
            }
        }, clock::get, clock::addAndGet);
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.TIMED_OUT,
                run(coordinator, pendingPlan("p_retry", 125000)).getState());
        Assertions.assertEquals(3, sends.get());
        Assertions.assertEquals(11_000L, clock.get());
    }

    @Test
    public void testJournalFailureDoesNotBecomeCompleteProgress() {
        AtomicLong publications = new AtomicLong();
        new MockUp<TenantTtlPartitionProgressManager>() {
            @Mock
            public TenantTtlPartitionProgressManager.AdvanceResult compareAndAdvanceCompletedPlan(
                    Invocation invocation, TenantTtlPartitionProgress expected, TenantTtlPartitionProgress candidate) {
                if (publications.incrementAndGet() == 1) {
                    throw new IllegalStateException("injected journal failure");
                }
                return invocation.proceed(expected, candidate);
            }
        };
        AtomicLong clock = new AtomicLong();
        AtomicLong sends = new AtomicLong();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
            sends.incrementAndGet();
            task.finish(result(task, TTenantTtlTaskCode.SUCCESS, task.getObservedMaxVersion()));
        }, clock::get, clock::addAndGet);
        TenantTtlScheduler.PendingRewritePlan first = pendingPlan("p_retry", 126000);
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.FAILED, run(coordinator, first).getState());
        Assertions.assertFalse(state.getTenantTtlPartitionProgressManager().get(progressKey(first)).isPresent());
        Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.COMPLETED,
                run(coordinator, pendingPlan("p_retry", 127000)).getState());
        Assertions.assertEquals(4, sends.get());
    }

    @Test
    public void testEveryCatalogReplicaIsRequiredNotAQuorum() throws Exception {
        UtFrameUtils.addMockBackend(12002);
        UtFrameUtils.addMockBackend(12003);
        PhysicalPartition physical = table.getPartition("p_retry").getDefaultPhysicalPartition();
        LocalTablet tablet = (LocalTablet) physical.getBaseIndex().getTablets().get(0);
        Replica second = new Replica(130002, 12002, Replica.ReplicaState.NORMAL, physical.getVisibleVersion(), 0);
        Replica third = new Replica(130003, 12003, Replica.ReplicaState.NORMAL, physical.getVisibleVersion(), 0);
        tablet.addReplica(second, true);
        tablet.addReplica(third, true);
        Config.tenant_ttl_agent_task_max_attempts = 1;
        try {
            AtomicLong clock = new AtomicLong();
            AtomicLong sends = new AtomicLong();
            TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
                sends.incrementAndGet();
                boolean fail = task.getReplicaId() == third.getId();
                task.finish(result(task, fail ? TTenantTtlTaskCode.TABLET_BUSY : TTenantTtlTaskCode.SUCCESS,
                        fail ? -1 : task.getObservedMaxVersion()));
            }, clock::get, clock::addAndGet);
            TenantTtlScheduler.PendingRewritePlan plan = pendingPlan("p_retry", 128000);
            TenantTtlRewriteCoordinator.ExecutionStatus status = run(coordinator, plan);
            Assertions.assertEquals(TenantTtlRewriteCoordinator.ExecutionState.ATTEMPTS_EXHAUSTED, status.getState());
            Assertions.assertEquals(4, sends.get()); // Three on the first Tablet, one on the second.
            Assertions.assertEquals(3, status.getSuccessfulReplicas());
            Assertions.assertFalse(state.getTenantTtlPartitionProgressManager().get(progressKey(plan)).isPresent());
        } finally {
            tablet.deleteReplica(second);
            tablet.deleteReplica(third);
            state.getNodeMgr().getClusterInfo().dropBackend(12002L);
            state.getNodeMgr().getClusterInfo().dropBackend(12003L);
        }
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

    static TTenantTtlCompactionResult result(TenantTtlCompactionTask task, TTenantTtlTaskCode code,
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
        return snapshot(SNAPSHOT_TXN_ID, 30);
    }

    private static TenantTtlPolicySnapshot snapshot(long txnId, int days) {
        Map<TenantTtlByteKey, Integer> overrides = new HashMap<>();
        overrides.put(TenantTtlByteKey.utf8("tenant_a"), days);
        Map<TenantTtlByteKey, TablePolicy> policies = new HashMap<>();
        policies.put(TenantTtlByteKey.utf8(TABLE_KEY), new TablePolicy(null, overrides));
        return new TenantTtlPolicySnapshot(DICTIONARY_ID, DICTIONARY_NAME, txnId, Instant.EPOCH,
                0, 0, 0, policies);
    }

    private static final class FixedSnapshotManager extends TenantTtlPolicySnapshotManager {
        private TenantTtlPolicySnapshot snapshot;

        private FixedSnapshotManager(TenantTtlPolicySnapshot snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public synchronized Optional<TenantTtlPolicySnapshot> getCurrentSnapshot(long dictionaryId) {
            return dictionaryId == snapshot.getDictionaryId() ? Optional.of(snapshot) : Optional.empty();
        }
    }

}
