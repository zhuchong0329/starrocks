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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TenantTtlDictionaryBinding;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.TenantTtlCompactionTask;
import com.starrocks.tenantttl.scheduler.TenantTtlPartitionProgress;
import com.starrocks.tenantttl.scheduler.TenantTtlRewriteCoordinator;
import com.starrocks.tenantttl.scheduler.TenantTtlScheduler;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TenantTtlSchedulerTest {
    private static final String DB_NAME = "tenant_ttl_scheduler_test";
    private static final String TABLE_NAME = "event_log";
    private static final String DICTIONARY_NAME = "tenant_ttl_scheduler_dict";
    private static final String TABLE_KEY = "business.scheduler_log";
    private static final long DICTIONARY_ID = 95001L;
    private static final long SNAPSHOT_TXN_ID = 91L;

    private static GlobalStateMgr state;
    private static Database db;
    private static OlapTable table;
    private static TenantTtlPolicySnapshotManager originalSnapshotManager;
    private static FixedSnapshotManager fixedSnapshotManager;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        state = GlobalStateMgr.getCurrentState();
        StarRocksAssert starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        starRocksAssert.withTable("CREATE TABLE " + DB_NAME + ".tenant_ttl_policy (\n" +
                "tenant VARCHAR(128) NOT NULL,\n" +
                "table_name VARCHAR(256) NOT NULL,\n" +
                "retention_days INT NOT NULL\n" +
                ") PRIMARY KEY(tenant, table_name)\n" +
                "DISTRIBUTED BY HASH(tenant) BUCKETS 1\n" +
                "PROPERTIES('replication_num' = '1')");
        state.getDictionaryMgr().addDictionary(new Dictionary(
                DICTIONARY_ID, DICTIONARY_NAME, "tenant_ttl_policy",
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME,
                ImmutableList.of("tenant", "table_name"), ImmutableList.of("retention_days"), new HashMap<>()));

        long now = System.currentTimeMillis() / 1000L;
        long dropUpper = now - 400L * 86400L;
        long rewriteUpper = now - 60L * 86400L;
        long noopUpper = now - 10L * 86400L;
        starRocksAssert.withTable("CREATE TABLE " + DB_NAME + "." + TABLE_NAME + " (\n" +
                "tenant VARCHAR(128) NULL,\n" +
                "recordTimestamp BIGINT NOT NULL,\n" +
                "payload VARCHAR(64) NULL\n" +
                ") DUPLICATE KEY(tenant, recordTimestamp)\n" +
                "PARTITION BY RANGE(recordTimestamp) (\n" +
                "PARTITION p_drop VALUES LESS THAN ('" + dropUpper + "'),\n" +
                "PARTITION p_rewrite VALUES LESS THAN ('" + rewriteUpper + "'),\n" +
                "PARTITION p_noop VALUES LESS THAN ('" + noopUpper + "'))\n" +
                "DISTRIBUTED BY HASH(tenant) BUCKETS 1\n" +
                "PROPERTIES('replication_num' = '1',\n" +
                "'compaction_retention_condition' = \"dictionary_ttl('" + DICTIONARY_NAME +
                "', '" + TABLE_KEY + "', 180)\")");
        db = state.getLocalMetastore().getDb(DB_NAME);
        table = (OlapTable) db.getTable(TABLE_NAME);

        originalSnapshotManager = state.getTenantTtlPolicySnapshotManager();
        fixedSnapshotManager = new FixedSnapshotManager(table.getId(), snapshot());
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
    public void testNoopRewriteAndCatalogDropAreSeparatedInOneCycle() {
        long noopPhysicalId = table.getPartition("p_noop").getDefaultPhysicalPartition().getId();
        long rewritePhysicalId = table.getPartition("p_rewrite").getDefaultPhysicalPartition().getId();
        long dropPhysicalId = table.getPartition("p_drop").getDefaultPhysicalPartition().getId();
        AtomicLong clock = new AtomicLong();
        TenantTtlScheduler scheduler = new TenantTtlScheduler(new TenantTtlRewriteCoordinator(
                task -> task.finish(TenantTtlRewriteCoordinatorTest.result(
                        task, TTenantTtlTaskCode.SUCCESS, task.getObservedMaxVersion())),
                clock::get, clock::addAndGet));

        scheduler.scheduleOnce(state);

        Assertions.assertNull(table.getPartition("p_drop"));
        Assertions.assertFalse(state.getTenantTtlPartitionProgressManager()
                .get(new TenantTtlPartitionProgress.ProgressKey(db.getId(), table.getId(), dropPhysicalId))
                .isPresent());
        TenantTtlPartitionProgress noopProgress = state.getTenantTtlPartitionProgressManager()
                .get(new TenantTtlPartitionProgress.ProgressKey(db.getId(), table.getId(), noopPhysicalId))
                .orElseThrow(AssertionError::new);
        Assertions.assertEquals(0, noopProgress.getProcessedThroughVersion());
        Assertions.assertEquals(SNAPSHOT_TXN_ID, noopProgress.getLastSuccessSnapshotTxnId());
        Assertions.assertEquals(TenantTtlScheduler.PartitionState.FE_NOOP,
                scheduler.getPartitionStatus(noopProgress.key()).orElseThrow(AssertionError::new).getState());
        Assertions.assertTrue(scheduler.getPendingRewritePlans().isEmpty());
        Assertions.assertTrue(state.getTenantTtlPartitionProgressManager().get(
                new TenantTtlPartitionProgress.ProgressKey(db.getId(), table.getId(), rewritePhysicalId)).isPresent());
        Assertions.assertFalse(scheduler.getNextExpiries().isEmpty());
    }

    @Test
    @Order(2)
    public void testDropRevalidationRejectsConcurrentBindingChange() {
        String secondTable = "event_log_revalidate";
        long now = System.currentTimeMillis() / 1000L;
        StarRocksAssert starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        starRocksAssert.useDatabase(DB_NAME);
        Assertions.assertDoesNotThrow(() -> starRocksAssert.withTable("CREATE TABLE " + DB_NAME + "." +
                secondTable + " (tenant VARCHAR(128), recordTimestamp BIGINT NOT NULL) " +
                "DUPLICATE KEY(tenant, recordTimestamp) " +
                "PARTITION BY RANGE(recordTimestamp) (PARTITION p_old VALUES LESS THAN ('" +
                (now - 400L * 86400L) + "')) DISTRIBUTED BY HASH(tenant) BUCKETS 1 " +
                "PROPERTIES('replication_num'='1', 'compaction_retention_condition'=\"dictionary_ttl('" +
                DICTIONARY_NAME + "', '" + TABLE_KEY + "', 180)\")"));
        OlapTable second = (OlapTable) db.getTable(secondTable);
        fixedSnapshotManager.tableRefs.clear();
        fixedSnapshotManager.addTable(second.getId());
        TenantTtlDictionaryBinding original = second.getTableProperty().getTenantTtlDictionaryBinding();
        // The injected evaluation clock runs after capture has saved its binding reference.
        // Change the live binding at this deterministic boundary, before Catalog DROP revalidation.
        TenantTtlScheduler scheduler = new TenantTtlScheduler(new TenantTtlRewriteCoordinator(), () -> {
            second.getTableProperty().setTenantTtlDictionaryBinding(new TenantTtlDictionaryBinding(
                    original.getDictionaryId(), original.getDictionaryName(), original.getTableKey(),
                    original.getDefaultDays() + 1));
            return now;
        });

        scheduler.scheduleOnce(state);
        Assertions.assertNotNull(second.getPartition("p_old"));

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            second.getTableProperty().setTenantTtlDictionaryBinding(original);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Test
    @Order(3)
    public void testOneRoundVisitsMoreThanLegacyTableLimit() {
        int previousLimit = Config.tenant_ttl_scheduler_max_tables_per_cycle;
        Config.tenant_ttl_scheduler_max_tables_per_cycle = 1;
        Set<TenantTtlPolicySnapshotManager.TableRef> previousRefs = new HashSet<>(fixedSnapshotManager.tableRefs);
        fixedSnapshotManager.tableRefs.clear();
        long firstMissingTable = Long.MAX_VALUE - 2000;
        for (int index = 0; index < 1001; index++) {
            fixedSnapshotManager.addTable(firstMissingTable + index);
        }
        try {
            TenantTtlScheduler scheduler = new TenantTtlScheduler();
            scheduler.scheduleOnce(state);
            for (int index = 0; index < 1001; index++) {
                Assertions.assertTrue(scheduler.getTableStatus(db.getId(), firstMissingTable + index).isPresent());
            }
        } finally {
            fixedSnapshotManager.tableRefs.clear();
            fixedSnapshotManager.tableRefs.addAll(previousRefs);
            Config.tenant_ttl_scheduler_max_tables_per_cycle = previousLimit;
        }
    }

    @Test
    @Order(4)
    public void testTwoTablesDrainAllTabletsAndNewBindingWaitsForNextRound() throws Exception {
        StarRocksAssert starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        starRocksAssert.useDatabase(DB_NAME);
        long now = System.currentTimeMillis() / 1000L;
        List<OlapTable> tables = new ArrayList<>();
        for (String name : List.of("round_a", "round_b", "round_new")) {
            starRocksAssert.withTable("CREATE TABLE " + DB_NAME + "." + name +
                    " (tenant VARCHAR(128), recordTimestamp BIGINT NOT NULL) DUPLICATE KEY(tenant, recordTimestamp) " +
                    "PARTITION BY RANGE(recordTimestamp) (PARTITION p_data VALUES LESS THAN ('" +
                    (now - 60L * 86400L) + "')) DISTRIBUTED BY HASH(tenant) BUCKETS 2 " +
                    "PROPERTIES('replication_num'='1', 'compaction_retention_condition'=\"dictionary_ttl('" +
                    DICTIONARY_NAME + "', '" + TABLE_KEY + "', 180)\")");
            tables.add((OlapTable) db.getTable(name));
        }
        int oldLimit = Config.tenant_ttl_scheduler_max_tables_per_cycle;
        Config.tenant_ttl_scheduler_max_tables_per_cycle = 1;
        Set<TenantTtlPolicySnapshotManager.TableRef> previousRefs = new HashSet<>(fixedSnapshotManager.tableRefs);
        fixedSnapshotManager.tableRefs.clear();
        fixedSnapshotManager.addTable(tables.get(0).getId());
        fixedSnapshotManager.addTable(tables.get(1).getId());
        AtomicLong clock = new AtomicLong();
        AtomicLong sends = new AtomicLong();
        TenantTtlScheduler scheduler = new TenantTtlScheduler(new TenantTtlRewriteCoordinator(task -> {
            sends.incrementAndGet();
            fixedSnapshotManager.addTable(tables.get(2).getId());
            task.finish(TenantTtlRewriteCoordinatorTest.result(task,
                    TTenantTtlTaskCode.NOOP_VERIFIED, task.getObservedMaxVersion()));
        }, clock::get, clock::addAndGet), () -> now);
        try {
            scheduler.scheduleOnce(state);
            Assertions.assertEquals(4, sends.get());
            Assertions.assertFalse(scheduler.getTableStatus(db.getId(), tables.get(2).getId()).isPresent());
            for (OlapTable completed : tables.subList(0, 2)) {
                Assertions.assertEquals(1,
                        state.getTenantTtlPartitionProgressManager().countTable(db.getId(), completed.getId()));
            }
            scheduler.scheduleOnce(state);
            Assertions.assertEquals(6, sends.get());
            scheduler.scheduleOnce(state);
            Assertions.assertEquals(6, sends.get(), "all three complete tables need no duplicate work");
        } finally {
            fixedSnapshotManager.tableRefs.clear();
            fixedSnapshotManager.tableRefs.addAll(previousRefs);
            Config.tenant_ttl_scheduler_max_tables_per_cycle = oldLimit;
        }
    }

    private static TenantTtlPolicySnapshot snapshot() {
        Map<TenantTtlByteKey, Integer> overrides = new HashMap<>();
        overrides.put(TenantTtlByteKey.utf8("tenant_a"), 30);
        overrides.put(TenantTtlByteKey.utf8("tenant_b"), 365);
        Map<TenantTtlByteKey, TablePolicy> policies = new HashMap<>();
        policies.put(TenantTtlByteKey.utf8(TABLE_KEY), new TablePolicy(null, overrides));
        return new TenantTtlPolicySnapshot(DICTIONARY_ID, DICTIONARY_NAME, SNAPSHOT_TXN_ID, Instant.EPOCH,
                0, 0, 0, policies);
    }

    @Test
    @Order(5)
    public void testRoleLossAndRegainInvalidatesOldRoundBeforeNextRound() {
        Set<TenantTtlPolicySnapshotManager.TableRef> saved = new HashSet<>(fixedSnapshotManager.tableRefs);
        fixedSnapshotManager.tableRefs.clear();
        fixedSnapshotManager.addTable(table.getId());
        state.getTenantTtlPartitionProgressManager().removeTable(db.getId(), table.getId());
        List<TenantTtlCompactionTask> sent = new ArrayList<>();
        AtomicLong clock = new AtomicLong();
        TenantTtlScheduler[] holder = new TenantTtlScheduler[1];
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(task -> {
            sent.add(task);
            if (sent.size() > 1) {
                task.finish(TenantTtlRewriteCoordinatorTest.result(task,
                        TTenantTtlTaskCode.SUCCESS, task.getObservedMaxVersion()));
            }
        }, clock::get, millis -> {
            clock.addAndGet(millis);
            holder[0].onLeadershipLost();
            holder[0].onLeadershipGained();
        });
        holder[0] = new TenantTtlScheduler(coordinator);
        try {
            holder[0].scheduleOnce(state);
            Assertions.assertEquals(1, sent.size());
            Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.CLOSED,
                    sent.get(0).finish(TenantTtlRewriteCoordinatorTest.result(sent.get(0),
                            TTenantTtlTaskCode.SUCCESS, sent.get(0).getObservedMaxVersion())));
            holder[0].scheduleOnce(state);
            Assertions.assertEquals(2, sent.size());
            Assertions.assertNotEquals(sent.get(0).getSignature(), sent.get(1).getSignature());
            holder[0].scheduleOnce(state);
            Assertions.assertEquals(2, sent.size());
        } finally {
            fixedSnapshotManager.tableRefs.clear();
            fixedSnapshotManager.tableRefs.addAll(saved);
        }
    }

    @Test
    @Order(6)
    public void testDaemonReadsDynamicDelayAfterCompletingWholeRound() {
        int savedInterval = Config.tenant_ttl_scheduler_interval_seconds;
        Set<TenantTtlPolicySnapshotManager.TableRef> saved = new HashSet<>(fixedSnapshotManager.tableRefs);
        fixedSnapshotManager.tableRefs.clear();
        fixedSnapshotManager.addTable(table.getId());
        state.getTenantTtlPartitionProgressManager().removeTable(db.getId(), table.getId());
        AtomicLong clock = new AtomicLong();
        AtomicLong sends = new AtomicLong();
        Config.tenant_ttl_scheduler_interval_seconds = 600;
        TenantTtlScheduler scheduler = new TenantTtlScheduler(new TenantTtlRewriteCoordinator(task -> {
            sends.incrementAndGet();
            Config.tenant_ttl_scheduler_interval_seconds = 17;
            task.finish(TenantTtlRewriteCoordinatorTest.result(task,
                    TTenantTtlTaskCode.SUCCESS, task.getObservedMaxVersion()));
        }, clock::get, clock::addAndGet));
        try {
            Assertions.assertEquals(600_000L, scheduler.getInterval());
            Deencapsulation.invoke(scheduler, "runAfterCatalogReady");
            Assertions.assertEquals(1, sends.get());
            Assertions.assertEquals(17_000L, scheduler.getInterval());
            Assertions.assertEquals(0, clock.get()); // No scheduler-period sleep between tasks.
        } finally {
            Config.tenant_ttl_scheduler_interval_seconds = savedInterval;
            fixedSnapshotManager.tableRefs.clear();
            fixedSnapshotManager.tableRefs.addAll(saved);
        }
    }

    private static final class FixedSnapshotManager extends TenantTtlPolicySnapshotManager {
        private final Set<TableRef> tableRefs = new HashSet<>();
        private final TenantTtlPolicySnapshot snapshot;

        private FixedSnapshotManager(long tableId, TenantTtlPolicySnapshot snapshot) {
            this.snapshot = snapshot;
            addTable(tableId);
        }

        private void addTable(long tableId) {
            tableRefs.add(new TableRef(db.getId(), tableId));
        }

        @Override
        public synchronized Set<TableRef> getAllReferencedTables() {
            return Collections.unmodifiableSet(new HashSet<>(tableRefs));
        }

        @Override
        public synchronized Optional<TenantTtlPolicySnapshot> getCurrentSnapshot(long dictionaryId) {
            return dictionaryId == snapshot.getDictionaryId() ? Optional.of(snapshot) : Optional.empty();
        }
    }
}
