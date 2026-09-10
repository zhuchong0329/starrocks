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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/** Immutable policy rows for one logical table key. */
public final class TablePolicy {
    private static final String FINGERPRINT_DOMAIN = "tenant-ttl-table-policy-v1";

    private final Integer tableDefaultDays;
    private final Map<TenantTtlByteKey, Integer> tenantOverrides;
    private final byte[] semanticFingerprint;

    TablePolicy(Integer tableDefaultDays, Map<TenantTtlByteKey, Integer> tenantOverrides) {
        this.tableDefaultDays = tableDefaultDays;
        this.tenantOverrides = Collections.unmodifiableMap(new HashMap<>(tenantOverrides));
        this.semanticFingerprint = computeSemanticFingerprint();
    }

    public OptionalInt getTableDefaultDays() {
        return tableDefaultDays == null ? OptionalInt.empty() : OptionalInt.of(tableDefaultDays);
    }

    public Map<TenantTtlByteKey, Integer> getTenantOverrides() {
        return tenantOverrides;
    }

    public OptionalInt getTenantOverride(TenantTtlByteKey tenant) {
        Integer days = tenantOverrides.get(tenant);
        return days == null ? OptionalInt.empty() : OptionalInt.of(days);
    }

    public String getSemanticFingerprint() {
        return TenantTtlPolicyFingerprint.toHex(semanticFingerprint);
    }

    byte[] copySemanticFingerprintBytes() {
        return semanticFingerprint.clone();
    }

    private byte[] computeSemanticFingerprint() {
        TenantTtlPolicyFingerprint fingerprint = new TenantTtlPolicyFingerprint(FINGERPRINT_DOMAIN);
        if (tableDefaultDays == null) {
            fingerprint.putByte(0);
        } else {
            fingerprint.putByte(1);
            fingerprint.putInt(tableDefaultDays);
        }
        List<Map.Entry<TenantTtlByteKey, Integer>> entries = new ArrayList<>(tenantOverrides.entrySet());
        entries.sort(Map.Entry.comparingByKey());
        fingerprint.putInt(entries.size());
        for (Map.Entry<TenantTtlByteKey, Integer> entry : entries) {
            entry.getKey().updateFingerprint(fingerprint);
            fingerprint.putInt(entry.getValue());
        }
        return fingerprint.finish();
    }
}
