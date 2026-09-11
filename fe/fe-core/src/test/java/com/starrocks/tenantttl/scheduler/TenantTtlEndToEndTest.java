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

import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Dictionary;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.proto.CompressionTypePB;
import com.starrocks.proto.PCompressedTenantTtlPolicyBatchPB;
import com.starrocks.proto.PDictionaryCacheExportOutcome;
import com.starrocks.proto.PExportDictionaryCacheResult;
import com.starrocks.proto.PTenantTtlPolicyBatchPB;
import com.starrocks.proto.PTenantTtlPolicyEntryPB;
import com.starrocks.proto.StatusPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.TenantTtlCompactionTask;
import com.starrocks.tenantttl.policy.TenantTtlPolicySnapshot;
import com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuilder;
import com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotManager;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTenantTtlCompactionReq;
import com.starrocks.thrift.TTenantTtlCompactionResult;
import com.starrocks.thrift.TTenantTtlFilterMode;
import com.starrocks.thrift.TTenantTtlTaskCode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.zip.CRC32C;

/** Exercises the complete FE path without daemon sleeps or a live Backend. */
public class TenantTtlEndToEndTest {
    private static final String DB_NAME = "tenant_ttl_e2e_test";
    private static final String TABLE_NAME = "event_log";
    private static final String POLICY_TABLE = "tenant_ttl_policy";
    private static final String DICTIONARY_NAME = "tenant_ttl_e2e_dict";
    private static final String TABLE_KEY = "business.e2e_log";
    private static final long DICTIONARY_ID = 99001L;
    private static final long SNAPSHOT_TXN_ID = 7001L;
    private static final long EVALUATION_TIME = 2_000_000_000L;
    private static final Codec<PTenantTtlPolicyBatchPB> POLICY_BATCH_CODEC =
            ProtobufProxy.create(PTenantTtlPolicyBatchPB.class);

    private static GlobalStateMgr state;
    private static Database db;
    private static OlapTable table;
    private static TenantTtlPolicySnapshotManager originalSnapshotManager;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        state = GlobalStateMgr.getCurrentState();
        StarRocksAssert starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        starRocksAssert.withTable("CREATE TABLE " + DB_NAME + "." + POLICY_TABLE + " (" +
                "tenant VARCHAR(128) NOT NULL, table_name VARCHAR(256) NOT NULL, retention_days INT NOT NULL" +
                ") PRIMARY KEY(tenant, table_name) DISTRIBUTED BY HASH(tenant) BUCKETS 1 " +
                "PROPERTIES('replication_num'='1')");
        state.getDictionaryMgr().addDictionary(new Dictionary(
                DICTIONARY_ID, DICTIONARY_NAME, POLICY_TABLE,
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME,
                ImmutableList.of("tenant", "table_name"), ImmutableList.of("retention_days"), new HashMap<>()));

        long dropUpper = EVALUATION_TIME - 400L * 86400L;
        long rewriteUpper = EVALUATION_TIME - 60L * 86400L;
        long noopUpper = EVALUATION_TIME - 10L * 86400L;
        starRocksAssert.withTable("CREATE TABLE " + DB_NAME + "." + TABLE_NAME + " (" +
                "tenant VARCHAR(128) NULL, recordTimestamp BIGINT NOT NULL, payload VARCHAR(64) NULL" +
                ") DUPLICATE KEY(tenant, recordTimestamp) PARTITION BY RANGE(recordTimestamp) (" +
                "PARTITION p_drop VALUES LESS THAN ('" + dropUpper + "')," +
                "PARTITION p_rewrite VALUES LESS THAN ('" + rewriteUpper + "')," +
                "PARTITION p_noop VALUES LESS THAN ('" + noopUpper + "')) " +
                "DISTRIBUTED BY HASH(tenant) BUCKETS 2 PROPERTIES('replication_num'='1'," +
                "'compaction_retention_condition'=\"dictionary_ttl('" + DICTIONARY_NAME + "', '" +
                TABLE_KEY + "', 180)\")");
        db = state.getLocalMetastore().getDb(DB_NAME);
        table = (OlapTable) db.getTable(TABLE_NAME);

        originalSnapshotManager = state.getTenantTtlPolicySnapshotManager();
        Deencapsulation.setField(state, "tenantTtlPolicySnapshotManager",
                new FixedSnapshotManager(table.getId(), snapshot()));
    }

    @AfterAll
    public static void afterClass() {
        if (state != null && originalSnapshotManager != null) {
            Deencapsulation.setField(state, "tenantTtlPolicySnapshotManager", originalSnapshotManager);
        }
    }

    @Test
    public void testBindingThroughSchedulingAndReplicaCompletion() {
        RecordingSubmitter submitter = new RecordingSubmitter();
        TenantTtlRewriteCoordinator coordinator = new TenantTtlRewriteCoordinator(
                submitter, () -> 1_000_000L, () -> 0.5);
        TenantTtlScheduler scheduler = new TenantTtlScheduler(coordinator, () -> EVALUATION_TIME);
        long rewritePhysicalId = table.getPartition("p_rewrite").getDefaultPhysicalPartition().getId();
        long noopPhysicalId = table.getPartition("p_noop").getDefaultPhysicalPartition().getId();

        scheduler.scheduleOnce(state);
        Assertions.assertNull(table.getPartition("p_drop"));
        Assertions.assertEquals(1, scheduler.getPendingRewritePlans().size());
        Assertions.assertTrue(submitter.tasks.isEmpty());
        Assertions.assertTrue(state.getTenantTtlPartitionProgressManager().get(
                new TenantTtlPartitionProgress.ProgressKey(db.getId(), table.getId(), noopPhysicalId)).isPresent());

        scheduler.scheduleOnce(state);
        TenantTtlCompactionTask first = coordinator.getActiveTask().orElseThrow(AssertionError::new);
        assertFrozenRequest(first);
        Assertions.assertEquals(1, submitter.tasks.size());
        Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.ACCEPTED,
                first.finish(success(first)));

        scheduler.scheduleOnce(state);
        TenantTtlCompactionTask second = coordinator.getActiveTask().orElseThrow(AssertionError::new);
        assertFrozenRequest(second);
        Assertions.assertNotEquals(first.getSignature(), second.getSignature());
        Assertions.assertEquals(2, submitter.tasks.size());
        Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.ACCEPTED,
                second.finish(success(second)));

        scheduler.scheduleOnce(state);
        TenantTtlPartitionProgress progress = state.getTenantTtlPartitionProgressManager().get(
                new TenantTtlPartitionProgress.ProgressKey(db.getId(), table.getId(), rewritePhysicalId))
                .orElseThrow(AssertionError::new);
        Assertions.assertEquals(SNAPSHOT_TXN_ID, progress.getLastSuccessSnapshotTxnId());
        Assertions.assertEquals(EVALUATION_TIME, progress.getLastSuccessEvaluationTimeEpochSeconds());
        Assertions.assertFalse(coordinator.getActiveTask().isPresent());

        scheduler.scheduleOnce(state);
        Assertions.assertTrue(scheduler.getPendingRewritePlans().isEmpty());
    }

    private static void assertFrozenRequest(TenantTtlCompactionTask task) {
        TTenantTtlCompactionReq request = task.toThrift();
        Assertions.assertEquals(TTenantTtlFilterMode.DELETE_LIST, request.getFilter().getMode());
        Assertions.assertEquals(1, request.getFilter().getTenantsSize());
        Assertions.assertEquals("tenant_a", utf8(request.getFilter().getTenants().get(0)));
        Assertions.assertEquals(DICTIONARY_ID, request.getPolicy_watermark().getDictionary_id());
        Assertions.assertEquals(SNAPSHOT_TXN_ID, request.getPolicy_watermark().getDictionary_txn_id());
        Assertions.assertEquals(EVALUATION_TIME,
                request.getPolicy_watermark().getEvaluation_time_epoch_seconds());
    }

    private static TTenantTtlCompactionResult success(TenantTtlCompactionTask task) {
        TTenantTtlCompactionResult result = new TTenantTtlCompactionResult();
        result.setCode(TTenantTtlTaskCode.NOOP_VERIFIED);
        result.setDetail_status(new TStatus(TStatusCode.OK));
        result.setRetryable(false);
        result.setTask_id(task.getSignature());
        result.setTablet_id(task.getTabletId());
        result.setPartition_id(task.getPartitionId());
        result.setSnapshot_end_version(task.getObservedMaxVersion());
        result.setProcessed_through_version(task.getObservedMaxVersion());
        result.setCoverage_digest("verified");
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

    private static TenantTtlPolicySnapshot snapshot() throws Exception {
        PTenantTtlPolicyEntryPB shortTtl = policyEntry("tenant_a", 30);
        PTenantTtlPolicyEntryPB longTtl = policyEntry("tenant_b", 365);
        PTenantTtlPolicyBatchPB batch = new PTenantTtlPolicyBatchPB();
        batch.entries = ImmutableList.of(shortTtl, longTtl);
        byte[] payload = POLICY_BATCH_CODEC.encode(batch);
        CRC32C crc = new CRC32C();
        crc.update(payload, 0, payload.length);

        PCompressedTenantTtlPolicyBatchPB compressed = new PCompressedTenantTtlPolicyBatchPB();
        compressed.sequence = 0;
        compressed.rowCount = 2L;
        compressed.compressionType = CompressionTypePB.NO_COMPRESSION;
        compressed.uncompressedSize = (long) payload.length;
        compressed.uncompressedCrc32c = (int) crc.getValue();
        compressed.payload = payload;

        PExportDictionaryCacheResult response = new PExportDictionaryCacheResult();
        response.status = new StatusPB();
        response.status.statusCode = 0;
        response.outcome = PDictionaryCacheExportOutcome.EXPORT_OK;
        response.protocolVersion = TenantTtlPolicySnapshotBuilder.PROTOCOL_VERSION;
        response.dictionaryId = DICTIONARY_ID;
        response.expectedTxnId = SNAPSHOT_TXN_ID;
        response.actualTxnId = SNAPSHOT_TXN_ID;
        response.complete = true;
        response.batches = Collections.singletonList(compressed);
        response.totalRowCount = 2L;
        response.batchCount = 1;
        response.totalUncompressedBytes = (long) payload.length;
        response.totalPayloadBytes = (long) payload.length;
        response.contentCrc32c = (int) crc.getValue();
        return new TenantTtlPolicySnapshotBuilder().build(DICTIONARY_ID, DICTIONARY_NAME, SNAPSHOT_TXN_ID,
                Instant.ofEpochSecond(EVALUATION_TIME), response);
    }

    private static PTenantTtlPolicyEntryPB policyEntry(String tenant, int retentionDays) {
        PTenantTtlPolicyEntryPB entry = new PTenantTtlPolicyEntryPB();
        entry.tenant = tenant.getBytes(StandardCharsets.UTF_8);
        entry.tableName = TABLE_KEY.getBytes(StandardCharsets.UTF_8);
        entry.retentionDays = retentionDays;
        return entry;
    }

    private static String utf8(ByteBuffer value) {
        ByteBuffer copy = value.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static final class RecordingSubmitter implements TenantTtlRewriteCoordinator.TaskSubmitter {
        private final List<TenantTtlCompactionTask> tasks = new ArrayList<>();

        @Override
        public void submit(TenantTtlCompactionTask task) {
            tasks.add(task);
        }
    }

    private static final class FixedSnapshotManager extends TenantTtlPolicySnapshotManager {
        private final Set<TableRef> tableRefs;
        private final TenantTtlPolicySnapshot snapshot;

        private FixedSnapshotManager(long tableId, TenantTtlPolicySnapshot snapshot) {
            this.tableRefs = Collections.unmodifiableSet(new HashSet<>(Collections.singletonList(
                    new TableRef(db.getId(), tableId))));
            this.snapshot = snapshot;
        }

        @Override
        public synchronized Set<TableRef> getAllReferencedTables() {
            return tableRefs;
        }

        @Override
        public synchronized Optional<TenantTtlPolicySnapshot> getCurrentSnapshot(long dictionaryId) {
            return dictionaryId == snapshot.getDictionaryId() ? Optional.of(snapshot) : Optional.empty();
        }
    }
}
