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

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

/** Durable proof that one Physical Partition completed a Tenant-TTL plan. */
public final class TenantTtlPartitionProgress {
    public static final int CURRENT_FORMAT_VERSION = 1;

    @SerializedName("formatVersion")
    private int formatVersion = CURRENT_FORMAT_VERSION;
    @SerializedName("dbId")
    private long dbId;
    @SerializedName("tableId")
    private long tableId;
    @SerializedName("physicalPartitionId")
    private long physicalPartitionId;
    @SerializedName("tableBindingFingerprint")
    private String tableBindingFingerprint;
    @SerializedName("tablePolicyFingerprint")
    private String tablePolicyFingerprint;
    @SerializedName("partitionBoundaryFingerprint")
    private String partitionBoundaryFingerprint;
    @SerializedName("completedExpiryCursorEpochSeconds")
    private long completedExpiryCursorEpochSeconds;
    @SerializedName("processedThroughVersion")
    private long processedThroughVersion;
    @SerializedName("lastSuccessSnapshotTxnId")
    private long lastSuccessSnapshotTxnId;
    @SerializedName("lastSuccessEvaluationTimeEpochSeconds")
    private long lastSuccessEvaluationTimeEpochSeconds;

    private TenantTtlPartitionProgress() {
    }

    public TenantTtlPartitionProgress(long dbId, long tableId, long physicalPartitionId,
                                      String tableBindingFingerprint, String tablePolicyFingerprint,
                                      String partitionBoundaryFingerprint,
                                      long completedExpiryCursorEpochSeconds, long processedThroughVersion,
                                      long lastSuccessSnapshotTxnId,
                                      long lastSuccessEvaluationTimeEpochSeconds) {
        this(CURRENT_FORMAT_VERSION, dbId, tableId, physicalPartitionId, tableBindingFingerprint,
                tablePolicyFingerprint, partitionBoundaryFingerprint, completedExpiryCursorEpochSeconds,
                processedThroughVersion, lastSuccessSnapshotTxnId, lastSuccessEvaluationTimeEpochSeconds);
    }

    public TenantTtlPartitionProgress(int formatVersion, long dbId, long tableId, long physicalPartitionId,
                                      String tableBindingFingerprint, String tablePolicyFingerprint,
                                      String partitionBoundaryFingerprint,
                                      long completedExpiryCursorEpochSeconds, long processedThroughVersion,
                                      long lastSuccessSnapshotTxnId,
                                      long lastSuccessEvaluationTimeEpochSeconds) {
        this.formatVersion = formatVersion;
        this.dbId = dbId;
        this.tableId = tableId;
        this.physicalPartitionId = physicalPartitionId;
        this.tableBindingFingerprint = tableBindingFingerprint;
        this.tablePolicyFingerprint = tablePolicyFingerprint;
        this.partitionBoundaryFingerprint = partitionBoundaryFingerprint;
        this.completedExpiryCursorEpochSeconds = completedExpiryCursorEpochSeconds;
        this.processedThroughVersion = processedThroughVersion;
        this.lastSuccessSnapshotTxnId = lastSuccessSnapshotTxnId;
        this.lastSuccessEvaluationTimeEpochSeconds = lastSuccessEvaluationTimeEpochSeconds;
        validate();
    }

    public TenantTtlPartitionProgress(TenantTtlPartitionProgress other) {
        this(other.formatVersion, other.dbId, other.tableId, other.physicalPartitionId,
                other.tableBindingFingerprint, other.tablePolicyFingerprint, other.partitionBoundaryFingerprint,
                other.completedExpiryCursorEpochSeconds, other.processedThroughVersion,
                other.lastSuccessSnapshotTxnId, other.lastSuccessEvaluationTimeEpochSeconds);
    }

    public void validate() {
        if (formatVersion != CURRENT_FORMAT_VERSION) {
            throw new IllegalArgumentException("unsupported Tenant-TTL progress format version: " + formatVersion);
        }
        if (dbId <= 0 || tableId <= 0 || physicalPartitionId <= 0) {
            throw new IllegalArgumentException("Tenant-TTL progress IDs must be positive");
        }
        requireFingerprint(tableBindingFingerprint, "table binding");
        requireFingerprint(tablePolicyFingerprint, "table policy");
        requireFingerprint(partitionBoundaryFingerprint, "partition boundary");
        if (processedThroughVersion < 0) {
            throw new IllegalArgumentException("processed-through version must not be negative");
        }
        if (lastSuccessSnapshotTxnId <= 0 || lastSuccessEvaluationTimeEpochSeconds <= 0) {
            throw new IllegalArgumentException("successful snapshot transaction and evaluation time must be positive");
        }
    }

    private static void requireFingerprint(String fingerprint, String name) {
        if (fingerprint == null || fingerprint.isEmpty()) {
            throw new IllegalArgumentException(name + " fingerprint is empty");
        }
    }

    public ProgressKey key() {
        return new ProgressKey(dbId, tableId, physicalPartitionId);
    }

    public boolean hasSamePlanIdentity(TenantTtlPartitionProgress other) {
        return other != null && Objects.equals(tableBindingFingerprint, other.tableBindingFingerprint) &&
                Objects.equals(tablePolicyFingerprint, other.tablePolicyFingerprint) &&
                Objects.equals(partitionBoundaryFingerprint, other.partitionBoundaryFingerprint);
    }

    public int getFormatVersion() {
        return formatVersion;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPhysicalPartitionId() {
        return physicalPartitionId;
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

    public long getCompletedExpiryCursorEpochSeconds() {
        return completedExpiryCursorEpochSeconds;
    }

    public long getProcessedThroughVersion() {
        return processedThroughVersion;
    }

    public long getLastSuccessSnapshotTxnId() {
        return lastSuccessSnapshotTxnId;
    }

    public long getLastSuccessEvaluationTimeEpochSeconds() {
        return lastSuccessEvaluationTimeEpochSeconds;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof TenantTtlPartitionProgress)) {
            return false;
        }
        TenantTtlPartitionProgress that = (TenantTtlPartitionProgress) object;
        return formatVersion == that.formatVersion && dbId == that.dbId && tableId == that.tableId &&
                physicalPartitionId == that.physicalPartitionId &&
                completedExpiryCursorEpochSeconds == that.completedExpiryCursorEpochSeconds &&
                processedThroughVersion == that.processedThroughVersion &&
                lastSuccessSnapshotTxnId == that.lastSuccessSnapshotTxnId &&
                lastSuccessEvaluationTimeEpochSeconds == that.lastSuccessEvaluationTimeEpochSeconds &&
                Objects.equals(tableBindingFingerprint, that.tableBindingFingerprint) &&
                Objects.equals(tablePolicyFingerprint, that.tablePolicyFingerprint) &&
                Objects.equals(partitionBoundaryFingerprint, that.partitionBoundaryFingerprint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(formatVersion, dbId, tableId, physicalPartitionId, tableBindingFingerprint,
                tablePolicyFingerprint, partitionBoundaryFingerprint, completedExpiryCursorEpochSeconds,
                processedThroughVersion, lastSuccessSnapshotTxnId, lastSuccessEvaluationTimeEpochSeconds);
    }

    /** Durable map identity; logical partition IDs and names deliberately are not part of the key. */
    public static final class ProgressKey implements Comparable<ProgressKey> {
        @SerializedName("dbId")
        private long dbId;
        @SerializedName("tableId")
        private long tableId;
        @SerializedName("physicalPartitionId")
        private long physicalPartitionId;

        private ProgressKey() {
        }

        public ProgressKey(long dbId, long tableId, long physicalPartitionId) {
            if (dbId <= 0 || tableId <= 0 || physicalPartitionId <= 0) {
                throw new IllegalArgumentException("Tenant-TTL progress key IDs must be positive");
            }
            this.dbId = dbId;
            this.tableId = tableId;
            this.physicalPartitionId = physicalPartitionId;
        }

        public long getDbId() {
            return dbId;
        }

        public long getTableId() {
            return tableId;
        }

        public long getPhysicalPartitionId() {
            return physicalPartitionId;
        }

        @Override
        public int compareTo(ProgressKey other) {
            int result = Long.compare(dbId, other.dbId);
            if (result == 0) {
                result = Long.compare(tableId, other.tableId);
            }
            if (result == 0) {
                result = Long.compare(physicalPartitionId, other.physicalPartitionId);
            }
            return result;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof ProgressKey)) {
                return false;
            }
            ProgressKey that = (ProgressKey) object;
            return dbId == that.dbId && tableId == that.tableId &&
                    physicalPartitionId == that.physicalPartitionId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbId, tableId, physicalPartitionId);
        }
    }
}
