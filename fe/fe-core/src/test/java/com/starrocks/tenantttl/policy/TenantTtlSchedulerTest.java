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
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.tenantttl.scheduler.TenantTtlPartitionProgress;
import com.starrocks.tenantttl.scheduler.TenantTtlScheduler;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
        TenantTtlScheduler scheduler = new TenantTtlScheduler();
        scheduler.setRewriteExecutionView(TenantTtlScheduler.RewriteExecutionView.NONE);

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
        Assertions.assertEquals(1, scheduler.getPendingRewritePlans().size());
        Assertions.assertEquals(rewritePhysicalId, scheduler.getPendingRewritePlans().get(0)
                .getPartitionPlan().getPhysicalPartitionId());
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
        fixedSnapshotManager.addTable(second.getId());
        TenantTtlDictionaryBinding original = second.getTableProperty().getTenantTtlDictionaryBinding();
        TenantTtlScheduler scheduler = new TenantTtlScheduler();
        scheduler.setRewriteExecutionView(new TenantTtlScheduler.RewriteExecutionView() {
            private boolean changed;

            @Override
            public boolean hasRewriteInFlight(long dbId, long tableId, long logicalPartitionId) {
                if (tableId == second.getId() && !changed) {
                    changed = true;
                    Locker locker = new Locker();
                    locker.lockDatabase(db.getId(), LockType.WRITE);
                    try {
                        second.getTableProperty().setTenantTtlDictionaryBinding(new TenantTtlDictionaryBinding(
                                original.getDictionaryId(), original.getDictionaryName(), original.getTableKey(),
                                original.getDefaultDays() + 1));
                    } finally {
                        locker.unLockDatabase(db.getId(), LockType.WRITE);
                    }
                }
                return false;
            }
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

    private static TenantTtlPolicySnapshot snapshot() {
        Map<TenantTtlByteKey, Integer> overrides = new HashMap<>();
        overrides.put(TenantTtlByteKey.utf8("tenant_a"), 30);
        overrides.put(TenantTtlByteKey.utf8("tenant_b"), 365);
        Map<TenantTtlByteKey, TablePolicy> policies = new HashMap<>();
        policies.put(TenantTtlByteKey.utf8(TABLE_KEY), new TablePolicy(null, overrides));
        return new TenantTtlPolicySnapshot(DICTIONARY_ID, DICTIONARY_NAME, SNAPSHOT_TXN_ID, Instant.EPOCH,
                0, 0, 0, policies);
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
