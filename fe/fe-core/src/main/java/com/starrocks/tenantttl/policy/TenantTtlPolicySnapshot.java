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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** A completely validated, immutable FE view of one committed Dictionary transaction. */
public final class TenantTtlPolicySnapshot {
    private static final String FINGERPRINT_DOMAIN = "tenant-ttl-policy-snapshot-v1";

    private final long dictionaryId;
    private final String dictionaryName;
    private final long dictionaryTxnId;
    private final Instant snapshotTime;
    private final long ignoredZeroRetentionRows;
    private final long estimatedMemoryBytes;
    private final long buildPeakMemoryBytes;
    private final Map<TenantTtlByteKey, TablePolicy> tablePolicies;
    private final byte[] semanticFingerprint;

    TenantTtlPolicySnapshot(long dictionaryId, String dictionaryName, long dictionaryTxnId, Instant snapshotTime,
                            long ignoredZeroRetentionRows, long estimatedMemoryBytes, long buildPeakMemoryBytes,
                            Map<TenantTtlByteKey, TablePolicy> tablePolicies) {
        this.dictionaryId = dictionaryId;
        this.dictionaryName = Objects.requireNonNull(dictionaryName, "dictionaryName is null");
        this.dictionaryTxnId = dictionaryTxnId;
        this.snapshotTime = Objects.requireNonNull(snapshotTime, "snapshotTime is null");
        this.ignoredZeroRetentionRows = ignoredZeroRetentionRows;
        this.estimatedMemoryBytes = estimatedMemoryBytes;
        this.buildPeakMemoryBytes = buildPeakMemoryBytes;
        this.tablePolicies = Collections.unmodifiableMap(new HashMap<>(tablePolicies));
        this.semanticFingerprint = computeSemanticFingerprint();
    }

    public long getDictionaryId() {
        return dictionaryId;
    }

    public String getDictionaryName() {
        return dictionaryName;
    }

    public long getDictionaryTxnId() {
        return dictionaryTxnId;
    }

    public Instant getSnapshotTime() {
        return snapshotTime;
    }

    public long getIgnoredZeroRetentionRows() {
        return ignoredZeroRetentionRows;
    }

    public long getEstimatedMemoryBytes() {
        return estimatedMemoryBytes;
    }

    public long getBuildPeakMemoryBytes() {
        return buildPeakMemoryBytes;
    }

    public Map<TenantTtlByteKey, TablePolicy> getTablePolicies() {
        return tablePolicies;
    }

    public Optional<TablePolicy> getTablePolicy(TenantTtlByteKey tableKey) {
        return Optional.ofNullable(tablePolicies.get(tableKey));
    }

    public Optional<TablePolicy> getTablePolicy(String tableKey) {
        return getTablePolicy(TenantTtlByteKey.utf8(tableKey));
    }

    public String getSemanticFingerprint() {
        return TenantTtlPolicyFingerprint.toHex(semanticFingerprint);
    }

    private byte[] computeSemanticFingerprint() {
        TenantTtlPolicyFingerprint fingerprint = new TenantTtlPolicyFingerprint(FINGERPRINT_DOMAIN);
        List<Map.Entry<TenantTtlByteKey, TablePolicy>> entries = new ArrayList<>(tablePolicies.entrySet());
        entries.sort(Map.Entry.comparingByKey());
        fingerprint.putInt(entries.size());
        for (Map.Entry<TenantTtlByteKey, TablePolicy> entry : entries) {
            entry.getKey().updateFingerprint(fingerprint);
            fingerprint.putBytes(entry.getValue().copySemanticFingerprintBytes());
        }
        return fingerprint.finish();
    }
}
