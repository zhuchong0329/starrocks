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

package com.starrocks.tenantttl.scheduler;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Dictionary;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Text;
import com.starrocks.persist.EditLogDeserializer;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.TenantTtlPartitionProgressBatchLog;
import com.starrocks.persist.TenantTtlPartitionProgressRemoveLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.tenantttl.scheduler.TenantTtlPartitionProgressManager.AdvanceResult;
import com.starrocks.tenantttl.scheduler.TenantTtlPartitionProgressManager.CompletedPlanUpdate;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class TenantTtlPartitionProgressManagerTest {
    private static final String DB_NAME = "tenant_ttl_progress_test";
    private static final String DICTIONARY_NAME = "tenant_ttl_progress_dict";
    private static final String TABLE_KEY = "business.http_log";
    private static StarRocksAssert starRocksAssert;
    private static Database db;
    private static OlapTable table;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        starRocksAssert.withTable("CREATE TABLE " + DB_NAME + ".tenant_ttl_policy (\n" +
                "tenant VARCHAR(128) NOT NULL, table_name VARCHAR(256) NOT NULL, retention_days INT NOT NULL\n" +
                ") PRIMARY KEY(tenant, table_name) DISTRIBUTED BY HASH(tenant) BUCKETS 1\n" +
                "PROPERTIES('replication_num' = '1')");
        GlobalStateMgr.getCurrentState().getDictionaryMgr().addDictionary(new Dictionary(
                93001L, DICTIONARY_NAME, "tenant_ttl_policy", InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                DB_NAME, ImmutableList.of("tenant", "table_name"), ImmutableList.of("retention_days"),
                new HashMap<>()));
        starRocksAssert.withTable(tableSql("event_log"));
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        table = (OlapTable) db.getTable("event_log");
    }

    @Test
    public void testCompareAndAdvanceFencesStalePlansAndRegressions() {
        RecordingJournal journal = new RecordingJournal();
        TenantTtlPartitionProgressManager manager = new TenantTtlPartitionProgressManager(journal);
        TenantTtlPartitionProgress first = progress(1, 10, 20, "binding-a", "policy-a", "boundary-a");
        Assertions.assertEquals(AdvanceResult.ADVANCED,
                manager.compareAndAdvanceCompletedPlan(null, first));
        Assertions.assertEquals(AdvanceResult.UNCHANGED,
                manager.compareAndAdvanceCompletedPlan(first, first));
        Assertions.assertEquals(1, journal.upserts.size());

        TenantTtlPartitionProgress advanced = progress(1, 11, 21, "binding-a", "policy-a", "boundary-a");
        Assertions.assertEquals(AdvanceResult.ADVANCED,
                manager.compareAndAdvanceCompletedPlan(first, advanced));
        TenantTtlPartitionProgress regressed = progress(1, 10, 22, "binding-a", "policy-a", "boundary-a");
        Assertions.assertEquals(AdvanceResult.REJECTED_REGRESSION,
                manager.compareAndAdvanceCompletedPlan(advanced, regressed));

        TenantTtlPartitionProgress changedPlan = progress(1, -100, 0,
                "binding-a", "policy-b", "boundary-a");
        Assertions.assertEquals(AdvanceResult.ADVANCED,
                manager.compareAndAdvanceCompletedPlan(advanced, changedPlan));
        Assertions.assertEquals(AdvanceResult.STALE_EXPECTATION,
                manager.compareAndAdvanceCompletedPlan(advanced,
                        progress(1, 30, 40, "binding-a", "policy-old-late", "boundary-a")));
        Assertions.assertEquals(changedPlan, manager.get(changedPlan.key()).orElseThrow(AssertionError::new));
    }

    @Test
    public void testBatchAdvanceIsAtomicAndJournalsOneRecord() {
        RecordingJournal journal = new RecordingJournal();
        TenantTtlPartitionProgressManager manager = new TenantTtlPartitionProgressManager(journal);
        TenantTtlPartitionProgress first = progress(1, 10, 20, "b", "p", "x");
        TenantTtlPartitionProgress second = progress(2, 10, 20, "b", "p", "y");
        Assertions.assertEquals(AdvanceResult.ADVANCED, manager.compareAndAdvanceCompletedPlans(ImmutableList.of(
                new CompletedPlanUpdate(null, second), new CompletedPlanUpdate(null, first))));
        Assertions.assertEquals(1, journal.upserts.size());
        Assertions.assertEquals(2, journal.upserts.get(0).getProgresses().size());
        Assertions.assertEquals(first, journal.upserts.get(0).getProgresses().get(0));
        Assertions.assertEquals(second, journal.upserts.get(0).getProgresses().get(1));

        TenantTtlPartitionProgress secondAdvanced = progress(2, 11, 21, "b", "p", "y");
        Assertions.assertEquals(AdvanceResult.STALE_EXPECTATION, manager.compareAndAdvanceCompletedPlans(
                ImmutableList.of(new CompletedPlanUpdate(first, progress(1, 11, 21, "b", "p", "x")),
                        new CompletedPlanUpdate(null, secondAdvanced))));
        Assertions.assertEquals(first, manager.get(first.key()).orElseThrow(AssertionError::new));
        Assertions.assertEquals(second, manager.get(second.key()).orElseThrow(AssertionError::new));
        Assertions.assertEquals(1, journal.upserts.size());

        Assertions.assertThrows(IllegalArgumentException.class, () -> manager.compareAndAdvanceCompletedPlans(
                ImmutableList.of(new CompletedPlanUpdate(first, first), new CompletedPlanUpdate(first, first))));
    }

    @Test
    public void testImageAndJournalRoundTripAreIdempotent() throws Exception {
        RecordingJournal journal = new RecordingJournal();
        TenantTtlPartitionProgressManager original = new TenantTtlPartitionProgressManager(journal);
        TenantTtlPartitionProgress first = progress(2, 20, 30, "b2", "p2", "x2");
        TenantTtlPartitionProgress second = progress(1, 10, 20, "b1", "p1", "x1");
        original.replayUpsert(new TenantTtlPartitionProgressBatchLog(ImmutableList.of(first, second)));

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        original.save(image.getImageWriter());
        TenantTtlPartitionProgressManager restored = new TenantTtlPartitionProgressManager(new RecordingJournal());
        SRMetaBlockReader reader = image.getMetaBlockReader();
        try {
            Assertions.assertEquals(SRMetaBlockID.TENANT_TTL_PARTITION_PROGRESS_MGR,
                    reader.getHeader().getSrMetaBlockID());
            restored.load(reader);
        } finally {
            reader.close();
        }
        Assertions.assertEquals(ImmutableList.of(second, first), restored.getAll());

        TenantTtlPartitionProgressBatchLog upsert = roundTrip(OperationType.OP_UPSERT_TENANT_TTL_PARTITION_PROGRESS,
                new TenantTtlPartitionProgressBatchLog(Collections.singletonList(first)),
                TenantTtlPartitionProgressBatchLog.class);
        restored.replayUpsert(upsert);
        restored.replayUpsert(upsert);
        Assertions.assertEquals(2, restored.size());

        TenantTtlPartitionProgressRemoveLog remove = roundTrip(
                OperationType.OP_REMOVE_TENANT_TTL_PARTITION_PROGRESS,
                new TenantTtlPartitionProgressRemoveLog(Collections.singletonList(first.key())),
                TenantTtlPartitionProgressRemoveLog.class);
        restored.replayRemove(remove);
        restored.replayRemove(remove);
        Assertions.assertFalse(restored.get(first.key()).isPresent());
    }

    @Test
    public void testLargeTableRemovalUsesOneSortedBatchLog() {
        RecordingJournal journal = new RecordingJournal();
        TenantTtlPartitionProgressManager manager = new TenantTtlPartitionProgressManager(journal);
        List<TenantTtlPartitionProgress> progresses = new ArrayList<>();
        for (int i = 0; i < 256; i++) {
            progresses.add(progress(1000 + i, 10, 20, "b", "p", "x"));
        }
        manager.replayUpsert(new TenantTtlPartitionProgressBatchLog(progresses));
        Assertions.assertEquals(256, manager.removeTable(1, 2));
        Assertions.assertEquals(1, journal.removes.size());
        Assertions.assertEquals(256, journal.removes.get(0).getKeys().size());
        Assertions.assertEquals(1000,
                journal.removes.get(0).getKeys().get(0).getPhysicalPartitionId());
        Assertions.assertEquals(1255,
                journal.removes.get(0).getKeys().get(255).getPhysicalPartitionId());
    }

    @Test
    public void testOrphanGcKeepsOnlyCurrentBoundPhysicalPartition() {
        RecordingJournal journal = new RecordingJournal();
        TenantTtlPartitionProgressManager manager = new TenantTtlPartitionProgressManager(journal);
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        TenantTtlPartitionProgress existing = progress(db.getId(), table.getId(), physicalPartitionId,
                10, 20, "b", "p", "x");
        TenantTtlPartitionProgress missingPartition = progress(db.getId(), table.getId(), Long.MAX_VALUE,
                10, 20, "b", "p", "missing");
        TenantTtlPartitionProgress missingDatabase = progress(Long.MAX_VALUE - 1, table.getId(), physicalPartitionId,
                10, 20, "b", "p", "missing-db");
        manager.replayUpsert(new TenantTtlPartitionProgressBatchLog(
                ImmutableList.of(existing, missingPartition, missingDatabase)));

        Assertions.assertEquals(2, manager.gcOrphanProgress(GlobalStateMgr.getCurrentState(), 100));
        Assertions.assertEquals(Collections.singletonList(existing), manager.getAll());
        Assertions.assertEquals(1, journal.removes.size());
        Assertions.assertEquals(2, journal.removes.get(0).getKeys().size());
    }

    @Test
    public void testCatalogDropHooksRemovePartitionAndTableProgress() throws Exception {
        starRocksAssert.withTable(tableSql("drop_hook"));
        OlapTable dropHook = (OlapTable) db.getTable("drop_hook");
        List<PhysicalPartition> partitions = new ArrayList<>(dropHook.getPhysicalPartitions());
        TenantTtlPartitionProgressManager globalManager =
                GlobalStateMgr.getCurrentState().getTenantTtlPartitionProgressManager();
        List<TenantTtlPartitionProgress> progresses = new ArrayList<>();
        for (PhysicalPartition partition : partitions) {
            progresses.add(progress(db.getId(), dropHook.getId(), partition.getId(),
                    10, 20, "b", "p", "x-" + partition.getId()));
        }
        globalManager.replayUpsert(new TenantTtlPartitionProgressBatchLog(progresses));

        starRocksAssert.alterTable("ALTER TABLE drop_hook DROP PARTITION p0 FORCE");
        Assertions.assertFalse(globalManager.get(progresses.get(0).key()).isPresent() &&
                globalManager.get(progresses.get(1).key()).isPresent());
        Assertions.assertEquals(1, progresses.stream().filter(progress -> globalManager.get(progress.key()).isPresent())
                .count());

        starRocksAssert.dropTable("drop_hook");
        for (TenantTtlPartitionProgress progress : progresses) {
            Assertions.assertFalse(globalManager.get(progress.key()).isPresent());
        }
    }

    private static String tableSql(String tableName) {
        return "CREATE TABLE " + DB_NAME + "." + tableName + " (\n" +
                "tenant VARCHAR(128) NULL, recordTimestamp BIGINT NOT NULL\n" +
                ") DUPLICATE KEY(tenant, recordTimestamp)\n" +
                "PARTITION BY RANGE(recordTimestamp) (PARTITION p0 VALUES LESS THAN ('100'), " +
                "PARTITION p1 VALUES LESS THAN ('1000'))\n" +
                "DISTRIBUTED BY HASH(tenant) BUCKETS 1\n" +
                "PROPERTIES('replication_num' = '1', 'compaction_retention_condition' = " +
                "\"dictionary_ttl('" + DICTIONARY_NAME + "', '" + TABLE_KEY + "', 180)\")";
    }

    private static TenantTtlPartitionProgress progress(long physicalPartitionId, long cursor, long version,
                                                        String binding, String policy, String boundary) {
        return progress(1, 2, physicalPartitionId, cursor, version, binding, policy, boundary);
    }

    private static TenantTtlPartitionProgress progress(long dbId, long tableId, long physicalPartitionId,
                                                        long cursor, long version, String binding,
                                                        String policy, String boundary) {
        return new TenantTtlPartitionProgress(dbId, tableId, physicalPartitionId, binding, policy, boundary,
                cursor, version, 100, 200);
    }

    private static <T> T roundTrip(short operationType, Object log, Class<T> type) throws Exception {
        DataOutputBuffer buffer = new DataOutputBuffer(128);
        Text.writeString(buffer, GsonUtils.GSON.toJson(log));
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(buffer.getData()));
        return type.cast(EditLogDeserializer.deserialize(operationType, input));
    }

    private static final class RecordingJournal implements TenantTtlPartitionProgressManager.JournalSink {
        private final List<TenantTtlPartitionProgressBatchLog> upserts = new ArrayList<>();
        private final List<TenantTtlPartitionProgressRemoveLog> removes = new ArrayList<>();

        @Override
        public void logUpsert(TenantTtlPartitionProgressBatchLog log) {
            upserts.add(log);
        }

        @Override
        public void logRemove(TenantTtlPartitionProgressRemoveLog log) {
            removes.add(log);
        }
    }
}
