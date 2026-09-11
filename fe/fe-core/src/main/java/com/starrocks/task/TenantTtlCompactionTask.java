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

package com.starrocks.task;

import com.starrocks.tenantttl.policy.TenantTtlByteKey;
import com.starrocks.tenantttl.policy.TenantTtlEvaluationContext;
import com.starrocks.tenantttl.policy.TenantTtlPolicyPlanner;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TTenantTtlCompactionReq;
import com.starrocks.thrift.TTenantTtlCompactionResult;
import com.starrocks.thrift.TTenantTtlFilter;
import com.starrocks.thrift.TTenantTtlFilterMode;
import com.starrocks.thrift.TTenantTtlPolicyWatermark;
import com.starrocks.thrift.TTenantTtlRowsetAction;
import com.starrocks.thrift.TTenantTtlRowsetResult;
import com.starrocks.thrift.TTenantTtlSchemaExpectation;
import com.starrocks.thrift.TTenantTtlTaskCode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** One immutable Tenant-TTL Agent request for a specific Tablet replica. */
public final class TenantTtlCompactionTask extends AgentTask {
    private final int protocolVersion;
    private final long replicaId;
    private final int tenantColumnUniqueId;
    private final TenantTtlPolicyPlanner.FilterMode filterMode;
    private final List<TenantTtlByteKey> tenants;
    private final long dictionaryId;
    private final long snapshotTxnId;
    private final long evaluationTimeEpochSeconds;
    private final long schemaId;
    private final int schemaVersion;
    private final long observedMaxVersion;
    private final String tableBindingFingerprint;
    private final String tablePolicyFingerprint;
    private final String partitionBoundaryFingerprint;
    private final String replicaTopologyFingerprint;
    private final String requestFingerprint;

    private TTenantTtlCompactionResult result;

    public TenantTtlCompactionTask(TenantTtlEvaluationContext.ReplicaTaskSpec spec) {
        this(spec.getBackendId(), spec.getDbId(), spec.getTableId(), spec.getPhysicalPartitionId(),
                spec.getIndexId(), spec.getTabletId(), spec.getReplicaId(), spec.getTaskId(),
                spec.getTenantColumnUniqueId(), spec.getFilterMode(), spec.getTenants(),
                spec.getDictionaryId(), spec.getSnapshotTxnId(), spec.getEvaluationTimeEpochSeconds(),
                spec.getSchemaId(), spec.getSchemaVersion(), spec.getObservedMaxVersion(),
                spec.getTableBindingFingerprint(), spec.getTablePolicyFingerprint(),
                spec.getPartitionBoundaryFingerprint(), spec.getReplicaTopologyFingerprint(),
                spec.getRequestFingerprint());
    }

    TenantTtlCompactionTask(long backendId, long dbId, long tableId, long physicalPartitionId,
                            long indexId, long tabletId, long replicaId, long taskId,
                            int tenantColumnUniqueId, TenantTtlPolicyPlanner.FilterMode filterMode,
                            List<TenantTtlByteKey> tenants, long dictionaryId, long snapshotTxnId,
                            long evaluationTimeEpochSeconds, long schemaId, int schemaVersion,
                            long observedMaxVersion, String tableBindingFingerprint,
                            String tablePolicyFingerprint, String partitionBoundaryFingerprint,
                            String replicaTopologyFingerprint, String requestFingerprint) {
        super(null, backendId, TTaskType.TENANT_TTL_COMPACTION, dbId, tableId, physicalPartitionId,
                indexId, tabletId, taskId);
        this.protocolVersion = TenantTtlEvaluationContext.PROTOCOL_VERSION;
        this.replicaId = replicaId;
        this.tenantColumnUniqueId = tenantColumnUniqueId;
        this.filterMode = Objects.requireNonNull(filterMode, "filter mode is null");
        this.tenants = immutableTenants(tenants);
        this.dictionaryId = dictionaryId;
        this.snapshotTxnId = snapshotTxnId;
        this.evaluationTimeEpochSeconds = evaluationTimeEpochSeconds;
        this.schemaId = schemaId;
        this.schemaVersion = schemaVersion;
        this.observedMaxVersion = observedMaxVersion;
        this.tableBindingFingerprint = requireFingerprint(tableBindingFingerprint, "table binding");
        this.tablePolicyFingerprint = requireFingerprint(tablePolicyFingerprint, "table policy");
        this.partitionBoundaryFingerprint = requireFingerprint(partitionBoundaryFingerprint, "partition boundary");
        this.replicaTopologyFingerprint = requireFingerprint(replicaTopologyFingerprint, "replica topology");
        this.requestFingerprint = requireFingerprint(requestFingerprint, "request");
        validateRequest();
    }

    private static List<TenantTtlByteKey> immutableTenants(List<TenantTtlByteKey> tenants) {
        Objects.requireNonNull(tenants, "tenants are null");
        List<TenantTtlByteKey> copy = new ArrayList<>(tenants.size());
        TenantTtlByteKey previous = null;
        for (TenantTtlByteKey tenant : tenants) {
            TenantTtlByteKey current = TenantTtlByteKey.copyOf(
                    Objects.requireNonNull(tenant, "tenant is null").copyBytes());
            if (current.size() == 0) {
                throw new IllegalArgumentException("tenant must not be empty");
            }
            if (previous != null && previous.compareTo(current) >= 0) {
                throw new IllegalArgumentException("tenants must be strictly sorted and unique");
            }
            copy.add(current);
            previous = current;
        }
        return Collections.unmodifiableList(copy);
    }

    private static String requireFingerprint(String fingerprint, String name) {
        if (fingerprint == null || fingerprint.isEmpty()) {
            throw new IllegalArgumentException(name + " fingerprint is empty");
        }
        return fingerprint;
    }

    private void validateRequest() {
        if (protocolVersion <= 0 || backendId <= 0 || dbId <= 0 || tableId <= 0 || partitionId <= 0 ||
                indexId <= 0 || tabletId <= 0 || replicaId <= 0 || signature <= 0 || tenantColumnUniqueId < 0 ||
                dictionaryId <= 0 || snapshotTxnId <= 0 || evaluationTimeEpochSeconds <= 0 || schemaId <= 0 ||
                schemaVersion < 0 || observedMaxVersion < 0) {
            throw new IllegalArgumentException("invalid Tenant-TTL Agent request identity or watermark");
        }
        if (filterMode != TenantTtlPolicyPlanner.FilterMode.DELETE_LIST &&
                filterMode != TenantTtlPolicyPlanner.FilterMode.KEEP_LIST) {
            throw new IllegalArgumentException("Tenant-TTL Agent request requires DELETE_LIST or KEEP_LIST");
        }
        if (filterMode == TenantTtlPolicyPlanner.FilterMode.KEEP_LIST && tenants.isEmpty()) {
            throw new IllegalArgumentException("Tenant-TTL KEEP_LIST must not be empty");
        }
    }

    public TTenantTtlCompactionReq toThrift() {
        TTenantTtlFilter filter = new TTenantTtlFilter();
        filter.setMode(filterMode == TenantTtlPolicyPlanner.FilterMode.DELETE_LIST ?
                TTenantTtlFilterMode.DELETE_LIST : TTenantTtlFilterMode.KEEP_LIST);
        List<ByteBuffer> tenantBytes = new ArrayList<>(tenants.size());
        for (TenantTtlByteKey tenant : tenants) {
            tenantBytes.add(ByteBuffer.wrap(tenant.copyBytes()));
        }
        filter.setTenants(tenantBytes);

        TTenantTtlPolicyWatermark watermark = new TTenantTtlPolicyWatermark();
        watermark.setDictionary_id(dictionaryId);
        watermark.setDictionary_txn_id(snapshotTxnId);
        watermark.setEvaluation_time_epoch_seconds(evaluationTimeEpochSeconds);

        TTenantTtlSchemaExpectation schema = new TTenantTtlSchemaExpectation();
        schema.setSchema_id(schemaId);
        schema.setSchema_version(schemaVersion);

        TTenantTtlCompactionReq request = new TTenantTtlCompactionReq();
        request.setProtocol_version(protocolVersion);
        request.setTask_id(signature);
        request.setTablet_id(tabletId);
        request.setPartition_id(partitionId);
        request.setTenant_column_unique_id(tenantColumnUniqueId);
        request.setFilter(filter);
        request.setPolicy_watermark(watermark);
        request.setExpected_schema(schema);
        request.setFe_observed_max_version(observedMaxVersion);
        return request;
    }

    /** Accepts a complete business result only after its envelope and accounting have been verified. */
    public synchronized FinishResult finish(TTenantTtlCompactionResult candidate) {
        if (!isWellFormed(candidate)) {
            return FinishResult.MALFORMED_RESULT;
        }
        if (candidate.getTask_id() != signature || candidate.getTablet_id() != tabletId ||
                candidate.getPartition_id() != partitionId) {
            return FinishResult.IDENTITY_MISMATCH;
        }
        if ((candidate.getCode() == TTenantTtlTaskCode.SUCCESS ||
                candidate.getCode() == TTenantTtlTaskCode.NOOP_VERIFIED) &&
                (candidate.getProcessed_through_version() < observedMaxVersion ||
                        candidate.getSnapshot_end_version() < candidate.getProcessed_through_version())) {
            return FinishResult.MALFORMED_RESULT;
        }
        result = new TTenantTtlCompactionResult(candidate);
        setFinished(true);
        return FinishResult.ACCEPTED;
    }

    private static boolean isWellFormed(TTenantTtlCompactionResult candidate) {
        if (candidate == null || candidate.getCode() == null || candidate.getDetail_status() == null ||
                candidate.getDetail_status().getStatus_code() == null || candidate.getCoverage_digest() == null ||
                candidate.getRowsets() == null || candidate.getTask_id() <= 0 || candidate.getTablet_id() <= 0 ||
                candidate.getPartition_id() <= 0 || candidate.getSnapshot_end_version() < -1 ||
                candidate.getProcessed_through_version() < -1 || candidate.getScanned_rows() < 0 ||
                candidate.getKept_rows() < 0 || candidate.getDeleted_rows() < 0 ||
                candidate.getTenant_rows_read() < 0 || candidate.getRows_pruned_by_segment_zonemap() < 0 ||
                candidate.getRows_pruned_by_page_zonemap() < 0 || candidate.getLinked_bytes() < 0 ||
                candidate.getRewritten_bytes() < 0 ||
                candidate.getScanned_rows() != candidate.getKept_rows() + candidate.getDeleted_rows()) {
            return false;
        }
        for (TTenantTtlRowsetResult rowset : candidate.getRowsets()) {
            if (!isWellFormed(rowset)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isWellFormed(TTenantTtlRowsetResult rowset) {
        if (rowset == null || rowset.getAction() == null || rowset.getSource_rowset_id() == null ||
                rowset.getSource_rowset_id().isEmpty() || rowset.getSource_version_start() < 0 ||
                rowset.getSource_version_end() < rowset.getSource_version_start() || rowset.getSource_rows() < 0 ||
                rowset.getKept_rows() < 0 || rowset.getDeleted_rows() < 0 || rowset.getSource_segments() < 0 ||
                rowset.getLinked_segments() < 0 || rowset.getDropped_segments() < 0 ||
                rowset.getRewritten_segments() < 0 ||
                rowset.getSource_rows() != rowset.getKept_rows() + rowset.getDeleted_rows()) {
            return false;
        }
        boolean hasOutput = rowset.isSetOutput_rowset_id() && rowset.getOutput_rowset_id() != null &&
                !rowset.getOutput_rowset_id().isEmpty();
        return rowset.getAction() == TTenantTtlRowsetAction.VERIFIED_NO_CHANGE ? !hasOutput : hasOutput;
    }

    public synchronized Optional<TTenantTtlCompactionResult> getResult() {
        return result == null ? Optional.empty() : Optional.of(new TTenantTtlCompactionResult(result));
    }

    /** Reset only mutable delivery/result state; all request fields and the task ID remain immutable. */
    public synchronized void prepareForRetry() {
        result = null;
        isFinished = false;
        isFailed = false;
        failedTimes = 0;
        errorMsg = null;
    }

    public long getReplicaId() {
        return replicaId;
    }

    public long getObservedMaxVersion() {
        return observedMaxVersion;
    }

    public String getTableBindingFingerprint() {
        return tableBindingFingerprint;
    }

    public String getTablePolicyFingerprint() {
        return tablePolicyFingerprint;
    }

    public String getPartitionBoundaryFingerprint() {
        return partitionBoundaryFingerprint;
    }

    public String getReplicaTopologyFingerprint() {
        return replicaTopologyFingerprint;
    }

    public String getRequestFingerprint() {
        return requestFingerprint;
    }

    public enum FinishResult {
        ACCEPTED,
        IDENTITY_MISMATCH,
        MALFORMED_RESULT
    }
}
