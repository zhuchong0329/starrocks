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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TenantTtlDictionaryBinding;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.tenantttl.scheduler.TenantTtlPartitionProgress;
import com.starrocks.tenantttl.scheduler.TenantTtlScheduleDecision;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class TenantTtlEvaluationContextTest {
    private static final String DB_NAME = "tenant_ttl_evaluation_test";
    private static final String TABLE_NAME = "event_log";
    private static final String DICTIONARY_NAME = "tenant_ttl_evaluation_dict";
    private static final String TABLE_KEY = "business.http_log";
    private static final long DICTIONARY_ID = 92001L;
    private static final long SNAPSHOT_TXN_ID = 73L;
    private static final long EVALUATION_TIME = 100L + 30L * 86400L;

    private static Database db;
    private static OlapTable table;
    private static TenantTtlPolicySnapshot snapshot;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        StarRocksAssert starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        starRocksAssert.withTable("CREATE TABLE " + DB_NAME + ".tenant_ttl_policy (\n" +
                "tenant VARCHAR(128) NOT NULL,\n" +
                "table_name VARCHAR(256) NOT NULL,\n" +
                "retention_days INT NOT NULL\n" +
                ") PRIMARY KEY(tenant, table_name)\n" +
                "DISTRIBUTED BY HASH(tenant) BUCKETS 1\n" +
                "PROPERTIES('replication_num' = '1')");
        GlobalStateMgr.getCurrentState().getDictionaryMgr().addDictionary(new Dictionary(
                DICTIONARY_ID, DICTIONARY_NAME, "tenant_ttl_policy",
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME,
                ImmutableList.of("tenant", "table_name"), ImmutableList.of("retention_days"), new HashMap<>()));
        starRocksAssert.withTable("CREATE TABLE " + DB_NAME + "." + TABLE_NAME + " (\n" +
                "tenant VARCHAR(128) NULL,\n" +
                "recordTimestamp BIGINT NOT NULL,\n" +
                "payload VARCHAR(64) NULL\n" +
                ") DUPLICATE KEY(tenant, recordTimestamp)\n" +
                "PARTITION BY RANGE(recordTimestamp) (\n" +
                "PARTITION p0 VALUES LESS THAN ('100'),\n" +
                "PARTITION p1 VALUES LESS THAN ('1000'))\n" +
                "DISTRIBUTED BY HASH(tenant) BUCKETS 2\n" +
                "PROPERTIES('replication_num' = '1',\n" +
                "'compaction_retention_condition' = \"dictionary_ttl('" + DICTIONARY_NAME +
                "', '" + TABLE_KEY + "', 180)\")");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        table = (OlapTable) db.getTable(TABLE_NAME);
        snapshot = snapshot(SNAPSHOT_TXN_ID, 30, 365);
    }

    @Test
    public void testCaptureFreezesOneSnapshotTimeAndReplicaTaskInputs() {
        AtomicLong taskIds = new AtomicLong(5000);
        TenantTtlEvaluationContext.CaptureResult result = TenantTtlEvaluationContext.captureLocked(
                db, table, snapshot, () -> EVALUATION_TIME, taskIds::incrementAndGet);
        Assertions.assertTrue(result.isSuccess(), result.getDetail());
        TenantTtlEvaluationContext context = result.getContext();
        Assertions.assertSame(snapshot, context.getSnapshot());
        Assertions.assertEquals(SNAPSHOT_TXN_ID, context.getSnapshotTxnId());
        Assertions.assertEquals(EVALUATION_TIME, context.getEvaluationTimeEpochSeconds());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> context.getPartitionPlans().clear());

        List<TenantTtlEvaluationContext.PartitionPlan> rewritePlans = new ArrayList<>();
        int noops = 0;
        for (TenantTtlEvaluationContext.PartitionPlan plan : context.getPartitionPlans()) {
            if (plan.getType() == TenantTtlPolicyPlanner.PlanType.ROWSET_REWRITE) {
                rewritePlans.add(plan);
            } else if (plan.getType() == TenantTtlPolicyPlanner.PlanType.FE_NOOP) {
                noops++;
            }
        }
        Assertions.assertEquals(1, rewritePlans.size());
        Assertions.assertEquals(1, noops);

        TenantTtlEvaluationContext.PartitionPlan rewrite = rewritePlans.get(0);
        Assertions.assertEquals(100L, rewrite.getPartitionUpperEpochSecond());
        Assertions.assertEquals(TenantTtlPolicyPlanner.FilterMode.DELETE_LIST,
                rewrite.getPolicyPlan().getFilterMode());
        Assertions.assertEquals(Collections.singletonList(TenantTtlByteKey.utf8("tenant_a")),
                rewrite.getPolicyPlan().getTenants());
        Assertions.assertFalse(rewrite.getReplicaTasks().isEmpty());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> rewrite.getReplicaTasks().clear());

        Set<Long> uniqueTaskIds = new HashSet<>();
        Set<String> uniqueRequestFingerprints = new HashSet<>();
        int expectedReplicaTasks = replicaCount(rewrite.getPhysicalPartitionId());
        Assertions.assertEquals(expectedReplicaTasks, rewrite.getReplicaTasks().size());
        for (TenantTtlEvaluationContext.ReplicaTaskSpec task : rewrite.getReplicaTasks()) {
            Assertions.assertTrue(uniqueTaskIds.add(task.getTaskId()));
            Assertions.assertTrue(uniqueRequestFingerprints.add(task.getRequestFingerprint()));
            Assertions.assertEquals(table.getBaseIndexId(), task.getIndexId());
            Assertions.assertEquals(DICTIONARY_ID, task.getDictionaryId());
            Assertions.assertEquals(SNAPSHOT_TXN_ID, task.getSnapshotTxnId());
            Assertions.assertEquals(EVALUATION_TIME, task.getEvaluationTimeEpochSeconds());
            Assertions.assertEquals(context.getTableBindingFingerprint(), task.getTableBindingFingerprint());
            Assertions.assertEquals(rewrite.getBoundaryFingerprint(), task.getPartitionBoundaryFingerprint());
            Assertions.assertEquals(rewrite.getTopologyFingerprint(), task.getReplicaTopologyFingerprint());
        }

        TenantTtlPolicySnapshot replacement = snapshot(SNAPSHOT_TXN_ID + 1, 10, 20);
        Assertions.assertNotEquals(replacement.getSemanticFingerprint(), snapshot.getSemanticFingerprint());
        Assertions.assertSame(snapshot, context.getSnapshot());
        Assertions.assertEquals(SNAPSHOT_TXN_ID,
                context.getPartitionPlans().stream()
                        .flatMap(plan -> plan.getReplicaTasks().stream())
                        .findFirst().orElseThrow(AssertionError::new).getSnapshotTxnId());
    }

    @Test
    public void testStableSemanticFingerprintsAndNewPhysicalTaskIdentity() {
        TenantTtlEvaluationContext first = TenantTtlEvaluationContext.captureLocked(
                db, table, snapshot, () -> EVALUATION_TIME, new AtomicLong(6000)::incrementAndGet).getContext();
        TenantTtlEvaluationContext second = TenantTtlEvaluationContext.captureLocked(
                db, table, snapshot, () -> EVALUATION_TIME, new AtomicLong(7000)::incrementAndGet).getContext();
        Assertions.assertEquals(first.getTableBindingFingerprint(), second.getTableBindingFingerprint());

        TenantTtlEvaluationContext.PartitionPlan firstRewrite = rewritePlan(first);
        TenantTtlEvaluationContext.PartitionPlan secondRewrite = rewritePlan(second);
        Assertions.assertEquals(firstRewrite.getBoundaryFingerprint(), secondRewrite.getBoundaryFingerprint());
        Assertions.assertEquals(firstRewrite.getTopologyFingerprint(), secondRewrite.getTopologyFingerprint());
        Assertions.assertEquals(firstRewrite.getPolicyPlan().getPlanFingerprint(),
                secondRewrite.getPolicyPlan().getPlanFingerprint());
        Assertions.assertNotEquals(firstRewrite.getReplicaTasks().get(0).getTaskId(),
                secondRewrite.getReplicaTasks().get(0).getTaskId());
        Assertions.assertNotEquals(firstRewrite.getReplicaTasks().get(0).getRequestFingerprint(),
                secondRewrite.getReplicaTasks().get(0).getRequestFingerprint());

        TenantTtlEvaluationContext newerTxn = TenantTtlEvaluationContext.captureLocked(
                db, table, snapshot(SNAPSHOT_TXN_ID + 1, 30, 365), () -> EVALUATION_TIME,
                new AtomicLong(6000)::incrementAndGet).getContext();
        TenantTtlEvaluationContext.PartitionPlan newerTxnRewrite = rewritePlan(newerTxn);
        Assertions.assertEquals(firstRewrite.getPolicyPlan().getTablePolicyFingerprint(),
                newerTxnRewrite.getPolicyPlan().getTablePolicyFingerprint());
        Assertions.assertNotEquals(firstRewrite.getReplicaTasks().get(0).getRequestFingerprint(),
                newerTxnRewrite.getReplicaTasks().get(0).getRequestFingerprint());

        TenantTtlEvaluationContext changedPolicy = TenantTtlEvaluationContext.captureLocked(
                db, table, snapshot(SNAPSHOT_TXN_ID, 29, 365), () -> EVALUATION_TIME,
                new AtomicLong(6000)::incrementAndGet).getContext();
        Assertions.assertNotEquals(firstRewrite.getPolicyPlan().getTablePolicyFingerprint(),
                rewritePlan(changedPolicy).getPolicyPlan().getTablePolicyFingerprint());
    }

    @Test
    public void testMultiplePhysicalPlansAllocateUniqueTasksWithOneWatermark() {
        long catchUpTime = 1000L + 30L * 86400L;
        TenantTtlEvaluationContext context = TenantTtlEvaluationContext.captureLocked(
                db, table, snapshot, () -> catchUpTime, new AtomicLong(10000)::incrementAndGet).getContext();
        Set<Long> physicalPartitions = new HashSet<>();
        Set<Long> taskIds = new HashSet<>();
        for (TenantTtlEvaluationContext.PartitionPlan plan : context.getPartitionPlans()) {
            Assertions.assertEquals(TenantTtlPolicyPlanner.PlanType.ROWSET_REWRITE, plan.getType());
            physicalPartitions.add(plan.getPhysicalPartitionId());
            for (TenantTtlEvaluationContext.ReplicaTaskSpec task : plan.getReplicaTasks()) {
                Assertions.assertTrue(taskIds.add(task.getTaskId()));
                Assertions.assertEquals(SNAPSHOT_TXN_ID, task.getSnapshotTxnId());
                Assertions.assertEquals(catchUpTime, task.getEvaluationTimeEpochSeconds());
                Assertions.assertEquals(table.getBaseIndexId(), task.getIndexId());
            }
        }
        Assertions.assertEquals(2, physicalPartitions.size());
        Assertions.assertEquals(4, taskIds.size());
    }

    @Test
    public void testTopologyFingerprintIsOrderIndependentAndIdentitySensitive() {
        TenantTtlEvaluationContext.ReplicaTopologyEntry first =
                new TenantTtlEvaluationContext.ReplicaTopologyEntry(1, 2, 3, 4, 5);
        TenantTtlEvaluationContext.ReplicaTopologyEntry second =
                new TenantTtlEvaluationContext.ReplicaTopologyEntry(1, 2, 6, 7, 8);
        Assertions.assertEquals(
                TenantTtlEvaluationContext.replicaTopologyFingerprint(ImmutableList.of(first, second)),
                TenantTtlEvaluationContext.replicaTopologyFingerprint(ImmutableList.of(second, first)));
        TenantTtlEvaluationContext.ReplicaTopologyEntry changed =
                new TenantTtlEvaluationContext.ReplicaTopologyEntry(1, 2, 6, 7, 9);
        Assertions.assertNotEquals(
                TenantTtlEvaluationContext.replicaTopologyFingerprint(ImmutableList.of(first, second)),
                TenantTtlEvaluationContext.replicaTopologyFingerprint(ImmutableList.of(first, changed)));
    }

    @Test
    public void testPublicationRevalidationRejectsConcurrentBindingAlter() {
        TenantTtlEvaluationContext context = TenantTtlEvaluationContext.captureLocked(
                db, table, snapshot, () -> EVALUATION_TIME, new AtomicLong(8000)::incrementAndGet).getContext();
        Assertions.assertTrue(context.validateForPublication(GlobalStateMgr.getCurrentState()));

        TableProperty property = table.getTableProperty();
        TenantTtlDictionaryBinding original = property.getTenantTtlDictionaryBinding();
        TenantTtlDictionaryBinding changed = new TenantTtlDictionaryBinding(
                original.getDictionaryId(), original.getDictionaryName(), original.getTableKey(),
                original.getDefaultDays() + 1);
        setDictionaryBinding(changed);
        try {
            Assertions.assertFalse(context.validateForPublication(GlobalStateMgr.getCurrentState()));
        } finally {
            setDictionaryBinding(original);
        }
        Assertions.assertTrue(context.validateForPublication(GlobalStateMgr.getCurrentState()));
    }

    @Test
    public void testInvalidRoundInputsFailClosedBeforeTaskCreation() {
        TenantTtlEvaluationContext.CaptureResult result = TenantTtlEvaluationContext.captureLocked(
                db, table, snapshot, () -> 0, new AtomicLong(9000)::incrementAndGet);
        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(TenantTtlEvaluationContext.CaptureFailure.INVALID_EVALUATION_TIME,
                result.getFailure());

        result = TenantTtlEvaluationContext.captureLocked(db, table,
                snapshot(0, 30, 365), () -> EVALUATION_TIME, new AtomicLong(9000)::incrementAndGet);
        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(TenantTtlEvaluationContext.CaptureFailure.SNAPSHOT_UNAVAILABLE,
                result.getFailure());

        result = TenantTtlEvaluationContext.captureLocked(db, table, snapshot,
                () -> EVALUATION_TIME, () -> 0);
        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(TenantTtlEvaluationContext.CaptureFailure.BINDING_INVALID, result.getFailure());
        Assertions.assertTrue(result.getDetail().contains("task ID"));
    }

    @Test
    public void testScheduleDecisionCatchesUpOnceAndDetectsSemanticOrDataChanges() {
        TenantTtlEvaluationContext atExpiry = TenantTtlEvaluationContext.captureLocked(
                db, table, snapshot, () -> EVALUATION_TIME, new AtomicLong(12000)::incrementAndGet).getContext();
        TenantTtlEvaluationContext.PartitionPlan rewrite = rewritePlan(atExpiry);
        TenantTtlScheduleDecision.Decision initial = TenantTtlScheduleDecision.decide(
                atExpiry, rewrite, null, false);
        Assertions.assertTrue(initial.hasReason(TenantTtlScheduleDecision.TriggerReason.INITIAL_CATCH_UP));
        Assertions.assertTrue(initial.hasReason(TenantTtlScheduleDecision.TriggerReason.EXPIRY_EVENT_DUE));
        Assertions.assertTrue(initial.hasReason(TenantTtlScheduleDecision.TriggerReason.DATA_VERSION_ADVANCED));

        TenantTtlPartitionProgress completed = progress(atExpiry, rewrite,
                rewrite.getPolicyPlan().getCompletedExpiryCursorEpochSeconds(), rewrite.getObservedVisibleVersion());
        TenantTtlEvaluationContext nextDay = TenantTtlEvaluationContext.captureLocked(
                db, table, snapshot, () -> EVALUATION_TIME + 86400L,
                new AtomicLong(13000)::incrementAndGet).getContext();
        TenantTtlEvaluationContext.PartitionPlan nextDayRewrite = rewritePlan(nextDay);
        TenantTtlScheduleDecision.Decision unchanged = TenantTtlScheduleDecision.decide(
                nextDay, nextDayRewrite, completed, false);
        Assertions.assertFalse(unchanged.shouldEvaluate());

        TenantTtlPartitionProgress behindData = progress(atExpiry, rewrite,
                rewrite.getPolicyPlan().getCompletedExpiryCursorEpochSeconds(),
                Math.max(0, rewrite.getObservedVisibleVersion() - 1));
        TenantTtlScheduleDecision.Decision dataAdvanced = TenantTtlScheduleDecision.decide(
                nextDay, nextDayRewrite, behindData, false);
        Assertions.assertEquals(TenantTtlScheduleDecision.TriggerReason.DATA_VERSION_ADVANCED,
                dataAdvanced.getPrimaryReason());

        TenantTtlEvaluationContext changedPolicy = TenantTtlEvaluationContext.captureLocked(
                db, table, snapshot(SNAPSHOT_TXN_ID + 1, 29, 365), () -> EVALUATION_TIME + 86400L,
                new AtomicLong(14000)::incrementAndGet).getContext();
        TenantTtlScheduleDecision.Decision policyChanged = TenantTtlScheduleDecision.decide(
                changedPolicy, rewritePlan(changedPolicy), completed, false);
        Assertions.assertTrue(policyChanged.hasReason(TenantTtlScheduleDecision.TriggerReason.POLICY_CHANGED));
        Assertions.assertEquals(TenantTtlScheduleDecision.TriggerReason.POLICY_CHANGED,
                policyChanged.getPrimaryReason());

        TenantTtlEvaluationContext.PartitionPlan noop = atExpiry.getPartitionPlans().stream()
                .filter(plan -> plan.getType() == TenantTtlPolicyPlanner.PlanType.FE_NOOP)
                .findFirst().orElseThrow(AssertionError::new);
        TenantTtlScheduleDecision.Decision noopInitial = TenantTtlScheduleDecision.decide(
                atExpiry, noop, null, false);
        Assertions.assertTrue(noopInitial.hasReason(TenantTtlScheduleDecision.TriggerReason.INITIAL_CATCH_UP));
        Assertions.assertFalse(noopInitial.hasReason(TenantTtlScheduleDecision.TriggerReason.DATA_VERSION_ADVANCED));
    }

    private static TenantTtlEvaluationContext.PartitionPlan rewritePlan(TenantTtlEvaluationContext context) {
        return context.getPartitionPlans().stream()
                .filter(plan -> plan.getType() == TenantTtlPolicyPlanner.PlanType.ROWSET_REWRITE)
                .findFirst().orElseThrow(AssertionError::new);
    }

    private static TenantTtlPartitionProgress progress(TenantTtlEvaluationContext context,
                                                       TenantTtlEvaluationContext.PartitionPlan plan,
                                                       long completedCursor, long processedThroughVersion) {
        return new TenantTtlPartitionProgress(context.getDbId(), context.getTableId(),
                plan.getPhysicalPartitionId(), context.getTableBindingFingerprint(),
                plan.getPolicyPlan().getTablePolicyFingerprint(), plan.getBoundaryFingerprint(), completedCursor,
                processedThroughVersion, context.getSnapshotTxnId(), context.getEvaluationTimeEpochSeconds());
    }

    private static int replicaCount(long physicalPartitionId) {
        PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
        MaterializedIndex index = physicalPartition.getBaseIndex();
        int count = 0;
        for (Tablet tablet : index.getTablets()) {
            for (Replica ignored : ((LocalTablet) tablet).getImmutableReplicas()) {
                count++;
            }
        }
        return count;
    }

    private static void setDictionaryBinding(TenantTtlDictionaryBinding binding) {
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            table.getTableProperty().setTenantTtlDictionaryBinding(binding);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    private static TenantTtlPolicySnapshot snapshot(long txnId, int tenantADays, int tenantBDays) {
        Map<TenantTtlByteKey, Integer> overrides = new HashMap<>();
        overrides.put(TenantTtlByteKey.utf8("tenant_a"), tenantADays);
        overrides.put(TenantTtlByteKey.utf8("tenant_b"), tenantBDays);
        Map<TenantTtlByteKey, TablePolicy> policies = new HashMap<>();
        policies.put(TenantTtlByteKey.utf8(TABLE_KEY), new TablePolicy(null, overrides));
        return new TenantTtlPolicySnapshot(DICTIONARY_ID, DICTIONARY_NAME, txnId, Instant.EPOCH,
                0, 0, 0, policies);
    }
}
