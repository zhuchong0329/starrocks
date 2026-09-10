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

import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.starrocks.proto.CompressionTypePB;
import com.starrocks.proto.PCompressedTenantTtlPolicyBatchPB;
import com.starrocks.proto.PDictionaryCacheExportOutcome;
import com.starrocks.proto.PExportDictionaryCacheResult;
import com.starrocks.proto.PTenantTtlPolicyBatchPB;
import com.starrocks.proto.PTenantTtlPolicyEntryPB;
import com.starrocks.proto.StatusPB;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.CRC32C;

import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.Classification.DETERMINISTIC;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.Classification.RETRYABLE;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_DUPLICATE_KEY;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_INVALID_ROW;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_LIMIT_EXCEEDED;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_RESPONSE_INTEGRITY;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_RPC_ERROR;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_SCHEMA_MISMATCH;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_UNSUPPORTED_PROTOCOL;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_VERSION_MISMATCH;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TenantTtlPolicySnapshotBuilderTest {
    private static final long DICTIONARY_ID = 101;
    private static final long TXN_ID = 202;
    private static final Instant SNAPSHOT_TIME = Instant.parse("2026-09-11T00:00:00Z");
    private static final Codec<PTenantTtlPolicyBatchPB> POLICY_BATCH_CODEC =
            ProtobufProxy.create(PTenantTtlPolicyBatchPB.class);

    @Test
    public void testBuildPoliciesWithStableRawByteFingerprint() throws Exception {
        byte[] binaryTenant = new byte[] {'a', 0, (byte) 0xff};
        List<PTenantTtlPolicyEntryPB> entries = new ArrayList<>();
        entries.add(entry("default", "business.http_log", 180));
        entries.add(entry(binaryTenant, bytes("business.http_log"), 30));
        entries.add(entry("租户", "business.http_log", 365));
        entries.add(entry("archive", "archive.http_log", 90));

        TenantTtlPolicySnapshotBuilder builder = builder(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
                Long.MAX_VALUE);
        TenantTtlPolicySnapshot first = builder.build(DICTIONARY_ID, "ttl_dict", TXN_ID, SNAPSHOT_TIME,
                response(Collections.singletonList(entries), true));
        Collections.reverse(entries);
        TenantTtlPolicySnapshot second = builder.build(DICTIONARY_ID, "ttl_dict", TXN_ID, SNAPSHOT_TIME,
                response(Collections.singletonList(entries), false));

        assertEquals(first.getSemanticFingerprint(), second.getSemanticFingerprint());
        TablePolicy policy = first.getTablePolicy("business.http_log").orElseThrow();
        assertEquals(OptionalInt.of(180), policy.getTableDefaultDays());
        assertEquals(OptionalInt.of(30), policy.getTenantOverride(TenantTtlByteKey.copyOf(binaryTenant)));
        assertEquals(OptionalInt.of(365), policy.getTenantOverride(TenantTtlByteKey.utf8("租户")));
        assertEquals(OptionalInt.empty(), policy.getTenantOverride(TenantTtlByteKey.utf8("missing")));
        assertEquals(2, policy.getTenantOverrides().size());
        assertEquals(1, first.getTablePolicy("archive.http_log").orElseThrow().getTenantOverrides().size());

        TenantTtlByteKey copiedKey = TenantTtlByteKey.copyOf(binaryTenant);
        binaryTenant[0] = 'z';
        assertArrayEquals(new byte[] {'a', 0, (byte) 0xff}, copiedKey.copyBytes());
        assertTrue(TenantTtlByteKey.copyOf(new byte[] {(byte) 0xff})
                .compareTo(TenantTtlByteKey.copyOf(new byte[] {0})) > 0);
        assertNotEquals(first.getTablePolicy("business.http_log").orElseThrow().getSemanticFingerprint(),
                first.getTablePolicy("archive.http_log").orElseThrow().getSemanticFingerprint());
    }

    @Test
    public void testZeroRowsAreFilteredAfterDuplicateValidation() throws Exception {
        PExportDictionaryCacheResult allZero = response(Collections.singletonList(List.of(
                entry("default", "business.http_log", 0),
                entry("tenant_a", "business.http_log", 0))), false);
        TenantTtlPolicySnapshot snapshot = defaultBuilder().build(
                DICTIONARY_ID, "ttl_dict", TXN_ID, SNAPSHOT_TIME, allZero);
        assertEquals(2, snapshot.getIgnoredZeroRetentionRows());
        assertTrue(snapshot.getTablePolicies().isEmpty());

        assertFailure(response(Collections.singletonList(List.of(
                        entry("tenant_a", "business.http_log", 0),
                        entry("tenant_a", "business.http_log", 30))), false),
                TENANT_TTL_POLICY_DUPLICATE_KEY, DETERMINISTIC);
        assertFailure(response(Collections.singletonList(List.of(
                        entry("tenant_a", "business.http_log", 30),
                        entry("tenant_a", "business.http_log", 30))), false),
                TENANT_TTL_POLICY_DUPLICATE_KEY, DETERMINISTIC);
    }

    @Test
    public void testInvalidRowsRejectTheWholeCandidate() throws Exception {
        assertFailure(response(Collections.singletonList(List.of(entry("tenant", "table.key", -1))), false),
                TENANT_TTL_POLICY_INVALID_ROW, DETERMINISTIC);
        assertFailure(response(Collections.singletonList(List.of(entry(new byte[0], bytes("table.key"), 1))), false),
                TENANT_TTL_POLICY_INVALID_ROW, DETERMINISTIC);
        assertFailure(response(Collections.singletonList(List.of(entry(bytes("tenant"), new byte[0], 1))), false),
                TENANT_TTL_POLICY_INVALID_ROW, DETERMINISTIC);
        PTenantTtlPolicyEntryPB missingDays = entry("tenant", "table.key", 1);
        missingDays.retentionDays = null;
        assertFailure(response(Collections.singletonList(List.of(missingDays)), false),
                TENANT_TTL_POLICY_INVALID_ROW, DETERMINISTIC);
    }

    @Test
    public void testEnvelopeOutcomesHaveStableClassifications() throws Exception {
        PExportDictionaryCacheResult response = validResponse();
        response.status.statusCode = 1;
        assertFailure(response, TENANT_TTL_POLICY_RPC_ERROR, RETRYABLE);

        response = validResponse();
        response.outcome = PDictionaryCacheExportOutcome.VERSION_MISMATCH;
        assertFailure(response, TENANT_TTL_POLICY_VERSION_MISMATCH, RETRYABLE);

        response = validResponse();
        response.outcome = PDictionaryCacheExportOutcome.SCHEMA_MISMATCH;
        assertFailure(response, TENANT_TTL_POLICY_SCHEMA_MISMATCH, DETERMINISTIC);

        response = validResponse();
        response.outcome = PDictionaryCacheExportOutcome.LIMIT_EXCEEDED;
        assertFailure(response, TENANT_TTL_POLICY_LIMIT_EXCEEDED, DETERMINISTIC);

        response = validResponse();
        response.protocolVersion = 2;
        assertFailure(response, TENANT_TTL_POLICY_UNSUPPORTED_PROTOCOL, DETERMINISTIC);
    }

    @Test
    public void testEveryIntegrityMismatchRejectsCandidate() throws Exception {
        PExportDictionaryCacheResult response = validResponse();
        response.dictionaryId++;
        assertIntegrityFailure(response);

        response = validResponse();
        response.complete = false;
        assertIntegrityFailure(response);

        response = validResponse();
        response.batches.get(0).sequence = 1;
        assertIntegrityFailure(response);

        response = validResponse();
        response.batches.get(0).rowCount++;
        assertIntegrityFailure(response);

        response = validResponse();
        response.batches.get(0).uncompressedSize++;
        assertIntegrityFailure(response);

        response = validResponse();
        response.batches.get(0).uncompressedCrc32c++;
        assertIntegrityFailure(response);

        response = validResponse();
        response.totalRowCount++;
        assertIntegrityFailure(response);

        response = validResponse();
        response.totalPayloadBytes++;
        assertIntegrityFailure(response);

        response = validResponse();
        response.contentCrc32c++;
        assertIntegrityFailure(response);

        response = validResponse();
        response.batches.get(0).compressionType = CompressionTypePB.ZSTD;
        assertIntegrityFailure(response);

        response = validResponse();
        response.batches.get(0).payload[0] ^= 0x7f;
        assertIntegrityFailure(response);
    }

    @Test
    public void testRowAndMemoryLimitsAreExact() throws Exception {
        PExportDictionaryCacheResult twoRows = response(Collections.singletonList(List.of(
                entry("tenant_a", "business.http_log", 30),
                entry("tenant_b", "business.http_log", 60))), true);
        assertFailure(builder(1, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE), twoRows,
                TENANT_TTL_POLICY_LIMIT_EXCEEDED, DETERMINISTIC);
        assertFailure(builder(Long.MAX_VALUE, 1, Long.MAX_VALUE, Long.MAX_VALUE), twoRows,
                TENANT_TTL_POLICY_LIMIT_EXCEEDED, DETERMINISTIC);
        assertFailure(builder(Long.MAX_VALUE, Long.MAX_VALUE, 1, Long.MAX_VALUE), twoRows,
                TENANT_TTL_POLICY_LIMIT_EXCEEDED, DETERMINISTIC);

        TenantTtlPolicySnapshot unrestricted = builder(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
                Long.MAX_VALUE).build(DICTIONARY_ID, "ttl_dict", TXN_ID, SNAPSHOT_TIME, twoRows);
        long exactPeak = unrestricted.getBuildPeakMemoryBytes();
        TenantTtlPolicySnapshot exact = builder(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, exactPeak)
                .build(DICTIONARY_ID, "ttl_dict", TXN_ID, SNAPSHOT_TIME, twoRows);
        assertEquals(exactPeak, exact.getBuildPeakMemoryBytes());
        assertFailure(builder(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, exactPeak - 1), twoRows,
                TENANT_TTL_POLICY_LIMIT_EXCEEDED, DETERMINISTIC);
    }

    @Test
    public void testOneHundredThousandRowsBoundary() throws Exception {
        List<List<PTenantTtlPolicyEntryPB>> batches = new ArrayList<>();
        for (int batch = 0; batch < 20; ++batch) {
            List<PTenantTtlPolicyEntryPB> entries = new ArrayList<>();
            for (int row = 0; row < 5000; ++row) {
                entries.add(entry("tenant-" + (batch * 5000 + row), "business.http_log", 30));
            }
            batches.add(entries);
        }
        PExportDictionaryCacheResult response = response(batches, true);
        TenantTtlPolicySnapshot snapshot = builder(100000, 64L * 1024 * 1024, 64L * 1024 * 1024,
                128L * 1024 * 1024).build(DICTIONARY_ID, "ttl_dict", TXN_ID, SNAPSHOT_TIME, response);
        assertEquals(100000,
                snapshot.getTablePolicy("business.http_log").orElseThrow().getTenantOverrides().size());
        assertFailure(builder(99999, 64L * 1024 * 1024, 64L * 1024 * 1024, 128L * 1024 * 1024), response,
                TENANT_TTL_POLICY_LIMIT_EXCEEDED, DETERMINISTIC);
    }

    @Test
    public void testPublishedMapsAreImmutableForConcurrentReaders() throws Exception {
        TenantTtlPolicySnapshot snapshot = defaultBuilder().build(
                DICTIONARY_ID, "ttl_dict", TXN_ID, SNAPSHOT_TIME, validResponse());
        assertThrows(UnsupportedOperationException.class,
                () -> snapshot.getTablePolicies().put(TenantTtlByteKey.utf8("x"),
                        new TablePolicy(null, Collections.emptyMap())));
        TablePolicy policy = snapshot.getTablePolicy("business.http_log").orElseThrow();
        assertThrows(UnsupportedOperationException.class,
                () -> policy.getTenantOverrides().put(TenantTtlByteKey.utf8("x"), 1));

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<Boolean>> reads = new ArrayList<>();
            for (int i = 0; i < 32; ++i) {
                reads.add(executor.submit(() -> {
                    for (int j = 0; j < 1000; ++j) {
                        if (snapshot.getTablePolicy("business.http_log").orElseThrow()
                                .getTenantOverride(TenantTtlByteKey.utf8("tenant_a")).orElseThrow() != 30) {
                            return false;
                        }
                    }
                    return true;
                }));
            }
            for (Future<Boolean> read : reads) {
                assertTrue(read.get());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static TenantTtlPolicySnapshotBuilder defaultBuilder() {
        return builder(100000, 64L * 1024 * 1024, 64L * 1024 * 1024, 128L * 1024 * 1024);
    }

    private static TenantTtlPolicySnapshotBuilder builder(long rows, long uncompressed, long response,
                                                           long memory) {
        return new TenantTtlPolicySnapshotBuilder(
                new TenantTtlPolicySnapshotBuilder.Limits(rows, uncompressed, response, memory));
    }

    private static PExportDictionaryCacheResult validResponse() throws Exception {
        return response(Collections.singletonList(List.of(
                entry("default", "business.http_log", 180),
                entry("tenant_a", "business.http_log", 30))), true);
    }

    private static PTenantTtlPolicyEntryPB entry(String tenant, String tableName, int retentionDays) {
        return entry(bytes(tenant), bytes(tableName), retentionDays);
    }

    private static PTenantTtlPolicyEntryPB entry(byte[] tenant, byte[] tableName, int retentionDays) {
        PTenantTtlPolicyEntryPB entry = new PTenantTtlPolicyEntryPB();
        entry.tenant = tenant;
        entry.tableName = tableName;
        entry.retentionDays = retentionDays;
        return entry;
    }

    private static PExportDictionaryCacheResult response(List<List<PTenantTtlPolicyEntryPB>> inputBatches,
                                                         boolean snappy) throws Exception {
        PExportDictionaryCacheResult response = new PExportDictionaryCacheResult();
        response.status = new StatusPB();
        response.status.statusCode = 0;
        response.outcome = PDictionaryCacheExportOutcome.EXPORT_OK;
        response.protocolVersion = TenantTtlPolicySnapshotBuilder.PROTOCOL_VERSION;
        response.dictionaryId = DICTIONARY_ID;
        response.expectedTxnId = TXN_ID;
        response.actualTxnId = TXN_ID;
        response.complete = true;
        response.batches = new ArrayList<>();
        long rows = 0;
        long uncompressedBytes = 0;
        long payloadBytes = 0;
        CRC32C contentCrc = new CRC32C();
        for (int i = 0; i < inputBatches.size(); ++i) {
            PTenantTtlPolicyBatchPB batch = new PTenantTtlPolicyBatchPB();
            batch.entries = inputBatches.get(i);
            byte[] uncompressed = POLICY_BATCH_CODEC.encode(batch);
            assertTrue(uncompressed.length <= TenantTtlPolicySnapshotBuilder.MAX_BATCH_UNCOMPRESSED_BYTES);
            CRC32C batchCrc = new CRC32C();
            batchCrc.update(uncompressed, 0, uncompressed.length);
            contentCrc.update(uncompressed, 0, uncompressed.length);

            PCompressedTenantTtlPolicyBatchPB compressed = new PCompressedTenantTtlPolicyBatchPB();
            compressed.sequence = i;
            compressed.rowCount = (long) inputBatches.get(i).size();
            compressed.compressionType = snappy ? CompressionTypePB.SNAPPY : CompressionTypePB.NO_COMPRESSION;
            compressed.uncompressedSize = (long) uncompressed.length;
            compressed.uncompressedCrc32c = (int) batchCrc.getValue();
            compressed.payload = snappy ? Snappy.compress(uncompressed) : uncompressed;
            response.batches.add(compressed);
            rows += compressed.rowCount;
            uncompressedBytes += compressed.uncompressedSize;
            payloadBytes += compressed.payload.length;
        }
        response.totalRowCount = rows;
        response.batchCount = response.batches.size();
        response.totalUncompressedBytes = uncompressedBytes;
        response.totalPayloadBytes = payloadBytes;
        response.contentCrc32c = (int) contentCrc.getValue();
        return response;
    }

    private static void assertIntegrityFailure(PExportDictionaryCacheResult response) {
        assertFailure(response, TENANT_TTL_POLICY_RESPONSE_INTEGRITY, RETRYABLE);
    }

    private static void assertFailure(PExportDictionaryCacheResult response,
                                      TenantTtlPolicySnapshotBuildException.ErrorCode code,
                                      TenantTtlPolicySnapshotBuildException.Classification classification) {
        assertFailure(defaultBuilder(), response, code, classification);
    }

    private static void assertFailure(TenantTtlPolicySnapshotBuilder builder,
                                      PExportDictionaryCacheResult response,
                                      TenantTtlPolicySnapshotBuildException.ErrorCode code,
                                      TenantTtlPolicySnapshotBuildException.Classification classification) {
        TenantTtlPolicySnapshotBuildException exception = assertThrows(
                TenantTtlPolicySnapshotBuildException.class,
                () -> builder.build(DICTIONARY_ID, "ttl_dict", TXN_ID, SNAPSHOT_TIME, response));
        assertEquals(code, exception.getErrorCode());
        assertEquals(classification, exception.getClassification());
        assertFalse(exception.getMessage().isEmpty());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
