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
import com.starrocks.common.Config;
import com.starrocks.proto.CompressionTypePB;
import com.starrocks.proto.PCompressedTenantTtlPolicyBatchPB;
import com.starrocks.proto.PExportDictionaryCacheResult;
import com.starrocks.proto.PTenantTtlPolicyBatchPB;
import com.starrocks.proto.PTenantTtlPolicyEntryPB;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.zip.CRC32C;

import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.Classification.DETERMINISTIC;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.Classification.RETRYABLE;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_CACHE_NOT_FOUND;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_DUPLICATE_KEY;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_EXPORT_INTERNAL_ERROR;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_INVALID_ROW;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_LIMIT_EXCEEDED;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_RESPONSE_INTEGRITY;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_RPC_ERROR;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_SCHEMA_MISMATCH;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_UNSUPPORTED_PROTOCOL;
import static com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotBuildException.ErrorCode.TENANT_TTL_POLICY_VERSION_MISMATCH;

/** Validates a complete Dictionary export and builds one immutable candidate snapshot. */
public final class TenantTtlPolicySnapshotBuilder {
    public static final int PROTOCOL_VERSION = 1;
    public static final long MAX_BATCH_UNCOMPRESSED_BYTES = 1024L * 1024;

    private static final long POLICY_KEY_OVERHEAD_BYTES = 80;
    private static final long ACTIVE_POLICY_ENTRY_OVERHEAD_BYTES = 48;
    private static final long TABLE_POLICY_OVERHEAD_BYTES = 128;
    private static final long FROZEN_MAP_ENTRY_OVERHEAD_BYTES = 48;
    private static final byte[] DEFAULT_TENANT = {'d', 'e', 'f', 'a', 'u', 'l', 't'};
    private static final TenantTtlByteKey DEFAULT_TENANT_KEY = TenantTtlByteKey.copyOf(DEFAULT_TENANT);
    private static final Codec<PTenantTtlPolicyBatchPB> POLICY_BATCH_CODEC =
            ProtobufProxy.create(PTenantTtlPolicyBatchPB.class);

    private final Limits limits;

    public TenantTtlPolicySnapshotBuilder() {
        this(new Limits(Config.tenant_ttl_policy_snapshot_export_max_rows,
                Config.tenant_ttl_policy_snapshot_export_max_uncompressed_bytes,
                Config.tenant_ttl_policy_snapshot_export_max_response_bytes,
                Config.tenant_ttl_policy_snapshot_builder_max_memory_bytes));
    }

    public TenantTtlPolicySnapshotBuilder(Limits limits) {
        this.limits = Objects.requireNonNull(limits, "limits is null");
    }

    public TenantTtlPolicySnapshot build(long dictionaryId, String dictionaryName, long expectedTxnId,
                                         Instant snapshotTime, PExportDictionaryCacheResult response)
            throws TenantTtlPolicySnapshotBuildException {
        Objects.requireNonNull(dictionaryName, "dictionaryName is null");
        Objects.requireNonNull(snapshotTime, "snapshotTime is null");
        Objects.requireNonNull(response, "response is null");
        validateEnvelope(dictionaryId, expectedTxnId, response);

        long totalRows = requiredNonNegative(response.totalRowCount, "total_row_count");
        int batchCount = requiredNonNegativeInt(response.batchCount, "batch_count");
        long totalUncompressedBytes = requiredNonNegative(response.totalUncompressedBytes,
                "total_uncompressed_bytes");
        long totalPayloadBytes = requiredNonNegative(response.totalPayloadBytes, "total_payload_bytes");
        if (totalRows > limits.maxRows || totalUncompressedBytes > limits.maxUncompressedBytes ||
                totalPayloadBytes > limits.maxResponseBytes) {
            throw failure(DETERMINISTIC, TENANT_TTL_POLICY_LIMIT_EXCEEDED,
                    "Tenant-TTL export exceeds the FE resource limits");
        }

        List<PCompressedTenantTtlPolicyBatchPB> batches = response.batches == null ?
                Collections.emptyList() : response.batches;
        if (batchCount != batches.size()) {
            throw integrity("batch_count does not match the number of batches");
        }

        MemoryTracker memory = new MemoryTracker(limits.maxEstimatedMemoryBytes);
        memory.reserve(totalPayloadBytes);
        Map<TenantTtlByteKey, MutableTablePolicy> mutableTables = new HashMap<>();
        Set<PolicyKey> seenKeys = new HashSet<>();
        long decodedRows = 0;
        long decodedUncompressedBytes = 0;
        long decodedPayloadBytes = 0;
        long ignoredZeroRows = 0;
        long activePolicyRows = 0;
        CRC32C contentCrc32c = new CRC32C();

        for (int i = 0; i < batches.size(); ++i) {
            PCompressedTenantTtlPolicyBatchPB compressedBatch = batches.get(i);
            if (compressedBatch == null) {
                throw integrity("batch " + i + " is null");
            }
            validateBatchEnvelope(i, compressedBatch);
            byte[] uncompressed = decompress(compressedBatch);
            long transientBytes = Math.multiplyExact((long) uncompressed.length, 2L);
            memory.reserve(transientBytes);
            try {
                verifyBatchBytes(compressedBatch, uncompressed);
                contentCrc32c.update(uncompressed, 0, uncompressed.length);
                PTenantTtlPolicyBatchPB policyBatch = decodeBatch(uncompressed, i);
                List<PTenantTtlPolicyEntryPB> entries = policyBatch.entries == null ?
                        Collections.emptyList() : policyBatch.entries;
                if (compressedBatch.rowCount != entries.size()) {
                    throw integrity("row_count mismatch in batch " + i);
                }
                for (int row = 0; row < entries.size(); ++row) {
                    PTenantTtlPolicyEntryPB entry = entries.get(row);
                    validateRow(entry, i, row);
                    TenantTtlByteKey tenant = TenantTtlByteKey.copyOf(entry.tenant);
                    TenantTtlByteKey tableName = TenantTtlByteKey.copyOf(entry.tableName);
                    PolicyKey policyKey = new PolicyKey(tenant, tableName);
                    if (!seenKeys.add(policyKey)) {
                        throw failure(DETERMINISTIC, TENANT_TTL_POLICY_DUPLICATE_KEY,
                                "duplicate (tenant, table_name) key in batch " + i + ", row " + row);
                    }
                    memory.reserve(POLICY_KEY_OVERHEAD_BYTES + tenant.size() + tableName.size());
                    if (entry.retentionDays == 0) {
                        ++ignoredZeroRows;
                        continue;
                    }
                    ++activePolicyRows;
                    memory.reserve(ACTIVE_POLICY_ENTRY_OVERHEAD_BYTES);
                    MutableTablePolicy table = mutableTables.get(tableName);
                    if (table == null) {
                        memory.reserve(TABLE_POLICY_OVERHEAD_BYTES);
                        table = new MutableTablePolicy();
                        mutableTables.put(tableName, table);
                    }
                    if (DEFAULT_TENANT_KEY.equals(tenant)) {
                        table.tableDefaultDays = entry.retentionDays;
                    } else {
                        table.tenantOverrides.put(tenant, entry.retentionDays);
                    }
                }
                decodedRows = addExact(decodedRows, entries.size(), "decoded row count overflow");
                decodedUncompressedBytes = addExact(decodedUncompressedBytes, uncompressed.length,
                        "decoded byte count overflow");
                decodedPayloadBytes = addExact(decodedPayloadBytes, compressedBatch.payload.length,
                        "payload byte count overflow");
            } finally {
                memory.release(transientBytes);
            }
        }

        if (decodedRows != totalRows || decodedUncompressedBytes != totalUncompressedBytes ||
                decodedPayloadBytes != totalPayloadBytes || response.contentCrc32c == null ||
                (int) contentCrc32c.getValue() != response.contentCrc32c) {
            throw integrity("top-level export totals or content CRC32C do not match decoded batches");
        }

        long freezeBytes = addExact(multiplyExact(activePolicyRows, FROZEN_MAP_ENTRY_OVERHEAD_BYTES,
                        "snapshot freeze memory estimate overflow"),
                multiplyExact(mutableTables.size(), TABLE_POLICY_OVERHEAD_BYTES,
                        "snapshot freeze memory estimate overflow"),
                "snapshot freeze memory estimate overflow");
        memory.reserve(freezeBytes);
        Map<TenantTtlByteKey, TablePolicy> tablePolicies = new HashMap<>();
        for (Map.Entry<TenantTtlByteKey, MutableTablePolicy> entry : mutableTables.entrySet()) {
            MutableTablePolicy mutable = entry.getValue();
            tablePolicies.put(entry.getKey(), new TablePolicy(mutable.tableDefaultDays, mutable.tenantOverrides));
        }
        long estimatedSnapshotMemory = freezeBytes;
        for (PolicyKey key : seenKeys) {
            estimatedSnapshotMemory = addExact(estimatedSnapshotMemory,
                    POLICY_KEY_OVERHEAD_BYTES + key.tenant.size() + key.tableName.size(),
                    "snapshot memory estimate overflow");
        }
        return new TenantTtlPolicySnapshot(dictionaryId, dictionaryName, expectedTxnId, snapshotTime,
                ignoredZeroRows, estimatedSnapshotMemory, memory.getPeakBytes(), tablePolicies);
    }

    private void validateEnvelope(long dictionaryId, long expectedTxnId, PExportDictionaryCacheResult response)
            throws TenantTtlPolicySnapshotBuildException {
        if (response.status == null || response.status.statusCode == null || response.status.statusCode != 0) {
            throw failure(RETRYABLE, TENANT_TTL_POLICY_RPC_ERROR, "Dictionary export returned a non-OK status");
        }
        if (response.outcome == null) {
            throw integrity("Dictionary export outcome is missing");
        }
        switch (response.outcome) {
            case VERSION_MISMATCH:
                throw failure(RETRYABLE, TENANT_TTL_POLICY_VERSION_MISMATCH,
                        "Dictionary Cache no longer has the expected transaction");
            case CACHE_NOT_FOUND:
                throw failure(RETRYABLE, TENANT_TTL_POLICY_CACHE_NOT_FOUND,
                        "Dictionary Cache is unavailable on the selected node");
            case SCHEMA_MISMATCH:
                throw failure(DETERMINISTIC, TENANT_TTL_POLICY_SCHEMA_MISMATCH,
                        "Dictionary Cache schema does not match the Tenant-TTL contract");
            case LIMIT_EXCEEDED:
                throw failure(DETERMINISTIC, TENANT_TTL_POLICY_LIMIT_EXCEEDED,
                        "Dictionary export exceeded a configured resource limit");
            case EXPORT_INTERNAL_ERROR:
                throw failure(RETRYABLE, TENANT_TTL_POLICY_EXPORT_INTERNAL_ERROR,
                        "Dictionary Cache export failed internally");
            case EXPORT_OK:
                break;
            default:
                throw integrity("unknown Dictionary export outcome");
        }
        if (response.protocolVersion == null || response.protocolVersion != PROTOCOL_VERSION) {
            throw failure(DETERMINISTIC, TENANT_TTL_POLICY_UNSUPPORTED_PROTOCOL,
                    "unsupported Tenant-TTL export protocol version: " + response.protocolVersion);
        }
        if (response.dictionaryId == null || response.dictionaryId != dictionaryId ||
                response.expectedTxnId == null || response.expectedTxnId != expectedTxnId ||
                response.actualTxnId == null || response.actualTxnId != expectedTxnId) {
            throw integrity("Dictionary id or transaction id does not match the requested snapshot");
        }
        if (!Boolean.TRUE.equals(response.complete)) {
            throw integrity("Dictionary export is incomplete");
        }
    }

    private void validateBatchEnvelope(int expectedSequence, PCompressedTenantTtlPolicyBatchPB batch)
            throws TenantTtlPolicySnapshotBuildException {
        if (batch.sequence == null || batch.sequence != expectedSequence || batch.rowCount == null ||
                batch.rowCount < 0 || batch.uncompressedSize == null || batch.uncompressedSize < 0 ||
                batch.uncompressedSize > MAX_BATCH_UNCOMPRESSED_BYTES || batch.uncompressedCrc32c == null ||
                batch.compressionType == null || batch.payload == null) {
            throw integrity("invalid envelope for batch " + expectedSequence);
        }
        if (batch.compressionType != CompressionTypePB.SNAPPY &&
                batch.compressionType != CompressionTypePB.NO_COMPRESSION) {
            throw integrity("unsupported compression type in batch " + expectedSequence);
        }
    }

    private byte[] decompress(PCompressedTenantTtlPolicyBatchPB batch)
            throws TenantTtlPolicySnapshotBuildException {
        try {
            if (batch.compressionType == CompressionTypePB.SNAPPY) {
                byte[] uncompressed = Snappy.uncompress(batch.payload);
                if (uncompressed.length != batch.uncompressedSize) {
                    throw integrity("Snappy output length does not match uncompressed_size");
                }
                return uncompressed;
            }
            return batch.payload.clone();
        } catch (IOException | RuntimeException e) {
            throw failure(RETRYABLE, TENANT_TTL_POLICY_RESPONSE_INTEGRITY,
                    "failed to decompress Tenant-TTL policy batch", e);
        }
    }

    private void verifyBatchBytes(PCompressedTenantTtlPolicyBatchPB batch, byte[] uncompressed)
            throws TenantTtlPolicySnapshotBuildException {
        if (uncompressed.length != batch.uncompressedSize) {
            throw integrity("batch payload length does not match uncompressed_size");
        }
        CRC32C crc32c = new CRC32C();
        crc32c.update(uncompressed, 0, uncompressed.length);
        if ((int) crc32c.getValue() != batch.uncompressedCrc32c) {
            throw integrity("batch CRC32C mismatch");
        }
    }

    private PTenantTtlPolicyBatchPB decodeBatch(byte[] uncompressed, int batchIndex)
            throws TenantTtlPolicySnapshotBuildException {
        try {
            PTenantTtlPolicyBatchPB batch = POLICY_BATCH_CODEC.decode(uncompressed);
            if (batch == null) {
                throw integrity("decoded batch " + batchIndex + " is null");
            }
            return batch;
        } catch (IOException | RuntimeException e) {
            throw failure(RETRYABLE, TENANT_TTL_POLICY_RESPONSE_INTEGRITY,
                    "failed to decode Tenant-TTL policy batch " + batchIndex, e);
        }
    }

    private void validateRow(PTenantTtlPolicyEntryPB entry, int batch, int row)
            throws TenantTtlPolicySnapshotBuildException {
        if (entry == null || entry.tenant == null || entry.tenant.length == 0 || entry.tableName == null ||
                entry.tableName.length == 0 || entry.retentionDays == null || entry.retentionDays < 0) {
            throw failure(DETERMINISTIC, TENANT_TTL_POLICY_INVALID_ROW,
                    "invalid Tenant-TTL policy row at batch " + batch + ", row " + row);
        }
    }

    private long requiredNonNegative(Long value, String field) throws TenantTtlPolicySnapshotBuildException {
        if (value == null || value < 0) {
            throw integrity(field + " is missing or negative");
        }
        return value;
    }

    private int requiredNonNegativeInt(Integer value, String field) throws TenantTtlPolicySnapshotBuildException {
        if (value == null || value < 0) {
            throw integrity(field + " is missing or negative");
        }
        return value;
    }

    private long addExact(long left, long right, String message) throws TenantTtlPolicySnapshotBuildException {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException e) {
            throw failure(DETERMINISTIC, TENANT_TTL_POLICY_LIMIT_EXCEEDED, message, e);
        }
    }

    private long multiplyExact(long left, long right, String message) throws TenantTtlPolicySnapshotBuildException {
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException e) {
            throw failure(DETERMINISTIC, TENANT_TTL_POLICY_LIMIT_EXCEEDED, message, e);
        }
    }

    private TenantTtlPolicySnapshotBuildException integrity(String message) {
        return failure(RETRYABLE, TENANT_TTL_POLICY_RESPONSE_INTEGRITY, message);
    }

    private TenantTtlPolicySnapshotBuildException failure(
            TenantTtlPolicySnapshotBuildException.Classification classification,
            TenantTtlPolicySnapshotBuildException.ErrorCode errorCode, String message) {
        return new TenantTtlPolicySnapshotBuildException(classification, errorCode, message);
    }

    private TenantTtlPolicySnapshotBuildException failure(
            TenantTtlPolicySnapshotBuildException.Classification classification,
            TenantTtlPolicySnapshotBuildException.ErrorCode errorCode, String message, Throwable cause) {
        return new TenantTtlPolicySnapshotBuildException(classification, errorCode, message, cause);
    }

    public static final class Limits {
        private final long maxRows;
        private final long maxUncompressedBytes;
        private final long maxResponseBytes;
        private final long maxEstimatedMemoryBytes;

        public Limits(long maxRows, long maxUncompressedBytes, long maxResponseBytes,
                      long maxEstimatedMemoryBytes) {
            if (maxRows <= 0 || maxUncompressedBytes <= 0 || maxResponseBytes <= 0 ||
                    maxEstimatedMemoryBytes <= 0) {
                throw new IllegalArgumentException("Tenant-TTL snapshot limits must be positive");
            }
            this.maxRows = maxRows;
            this.maxUncompressedBytes = maxUncompressedBytes;
            this.maxResponseBytes = maxResponseBytes;
            this.maxEstimatedMemoryBytes = maxEstimatedMemoryBytes;
        }
    }

    private static final class MutableTablePolicy {
        private Integer tableDefaultDays;
        private final Map<TenantTtlByteKey, Integer> tenantOverrides = new HashMap<>();
    }

    private static final class PolicyKey {
        private final TenantTtlByteKey tenant;
        private final TenantTtlByteKey tableName;

        private PolicyKey(TenantTtlByteKey tenant, TenantTtlByteKey tableName) {
            this.tenant = tenant;
            this.tableName = tableName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PolicyKey)) {
                return false;
            }
            PolicyKey policyKey = (PolicyKey) o;
            return tenant.equals(policyKey.tenant) && tableName.equals(policyKey.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tenant, tableName);
        }
    }

    private final class MemoryTracker {
        private final long limitBytes;
        private long currentBytes;
        private long peakBytes;

        private MemoryTracker(long limitBytes) {
            this.limitBytes = limitBytes;
        }

        private void reserve(long bytes) throws TenantTtlPolicySnapshotBuildException {
            try {
                currentBytes = Math.addExact(currentBytes, bytes);
            } catch (ArithmeticException e) {
                throw failure(DETERMINISTIC, TENANT_TTL_POLICY_LIMIT_EXCEEDED,
                        "Tenant-TTL Builder memory estimate overflow", e);
            }
            peakBytes = Math.max(peakBytes, currentBytes);
            if (currentBytes > limitBytes) {
                throw failure(DETERMINISTIC, TENANT_TTL_POLICY_LIMIT_EXCEEDED,
                        "Tenant-TTL Builder exceeds the FE memory limit");
            }
        }

        private void release(long bytes) {
            currentBytes -= bytes;
        }

        private long getPeakBytes() {
            return peakBytes;
        }
    }

}
