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

import com.starrocks.common.Config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/** Pure policy resolution and partition filter planning for one fixed snapshot and evaluation time. */
public final class TenantTtlPolicyPlanner {
    private static final long SECONDS_PER_DAY = 86400L;
    private static final long FILTER_FIXED_SERIALIZED_BYTES = 1L + Integer.BYTES;
    private static final String TABLE_POLICY_FINGERPRINT_DOMAIN = "tenant-ttl-effective-table-policy-v1";
    private static final String PLAN_FINGERPRINT_DOMAIN = "tenant-ttl-partition-plan-v1";

    private final long maxFilterTenants;
    private final long maxFilterSerializedBytes;

    public TenantTtlPolicyPlanner(long maxFilterTenants, long maxFilterSerializedBytes) {
        this.maxFilterTenants = maxFilterTenants;
        this.maxFilterSerializedBytes = maxFilterSerializedBytes;
    }

    public static TenantTtlPolicyPlanner fromConfig() {
        return new TenantTtlPolicyPlanner(Config.tenant_ttl_filter_max_tenants,
                Config.tenant_ttl_filter_max_serialized_bytes);
    }

    public Plan plan(TenantTtlPolicySnapshot snapshot, TenantTtlByteKey tableKey, int propertyDefaultDays,
                     long partitionUpperEpochSecond, long evaluationTimeEpochSecond) {
        if (snapshot == null) {
            return Plan.failClosed(TableKeyMatch.UNKNOWN, TableDefaultMatch.UNKNOWN,
                    ResolutionType.UNRESOLVED, propertyDefaultDays, propertyDefaultDays,
                    FailReason.SNAPSHOT_UNAVAILABLE, FilterMode.NONE, 0, 0, "", "");
        }
        if (tableKey == null || tableKey.size() == 0 || propertyDefaultDays <= 0) {
            return Plan.failClosed(TableKeyMatch.UNKNOWN, TableDefaultMatch.UNKNOWN,
                    ResolutionType.UNRESOLVED, propertyDefaultDays, propertyDefaultDays,
                    FailReason.INVALID_DEFAULT_OR_POLICY, FilterMode.NONE, 0, 0, "", "");
        }
        ResolvedTablePolicy resolved = resolveTablePolicy(snapshot, tableKey, propertyDefaultDays);
        if (!resolved.valid || maxFilterTenants <= 0 || maxFilterSerializedBytes <= 0) {
            return Plan.failClosed(resolved.tableKeyMatch, resolved.tableDefaultMatch, resolved.defaultResolutionType,
                    resolved.effectiveDefaultDays, resolved.nullRetentionDays,
                    FailReason.INVALID_DEFAULT_OR_POLICY, FilterMode.NONE, 0, 0,
                    resolved.tablePolicyFingerprint, "");
        }

        List<TenantTtlByteKey> expiredOverrides = new ArrayList<>();
        List<TenantTtlByteKey> retainedOverrides = new ArrayList<>();
        try {
            boolean defaultExpired = expired(partitionUpperEpochSecond, evaluationTimeEpochSecond,
                    resolved.effectiveDefaultDays);
            for (Map.Entry<TenantTtlByteKey, Integer> entry : resolved.sortedOverrides) {
                if (expired(partitionUpperEpochSecond, evaluationTimeEpochSecond, entry.getValue())) {
                    expiredOverrides.add(entry.getKey());
                } else {
                    retainedOverrides.add(entry.getKey());
                }
            }

            if (!defaultExpired && expiredOverrides.isEmpty()) {
                return createPlan(PlanType.FE_NOOP, FilterMode.NONE, Collections.emptyList(), resolved,
                        partitionUpperEpochSecond, evaluationTimeEpochSecond, 0);
            }
            if (defaultExpired && retainedOverrides.isEmpty()) {
                return createPlan(PlanType.DROP_LOGICAL_PARTITION, FilterMode.NONE, Collections.emptyList(), resolved,
                        partitionUpperEpochSecond, evaluationTimeEpochSecond, 0);
            }
            FilterMode mode = defaultExpired ? FilterMode.KEEP_LIST : FilterMode.DELETE_LIST;
            List<TenantTtlByteKey> tenants = defaultExpired ? retainedOverrides : expiredOverrides;
            long serializedBytes = serializedFilterBytes(tenants);
            if (tenants.size() > maxFilterTenants) {
                return Plan.failClosed(resolved.tableKeyMatch, resolved.tableDefaultMatch,
                        resolved.defaultResolutionType, resolved.effectiveDefaultDays, resolved.nullRetentionDays,
                        FailReason.FILTER_ROW_LIMIT, mode, tenants.size(), serializedBytes,
                        resolved.tablePolicyFingerprint,
                        planFingerprint(partitionUpperEpochSecond, evaluationTimeEpochSecond, mode, tenants,
                                resolved.tablePolicyFingerprint));
            }
            if (serializedBytes > maxFilterSerializedBytes) {
                return Plan.failClosed(resolved.tableKeyMatch, resolved.tableDefaultMatch,
                        resolved.defaultResolutionType, resolved.effectiveDefaultDays, resolved.nullRetentionDays,
                        FailReason.FILTER_BYTE_LIMIT, mode, tenants.size(), serializedBytes,
                        resolved.tablePolicyFingerprint,
                        planFingerprint(partitionUpperEpochSecond, evaluationTimeEpochSecond, mode, tenants,
                                resolved.tablePolicyFingerprint));
            }
            return createPlan(PlanType.ROWSET_REWRITE, mode, tenants, resolved,
                    partitionUpperEpochSecond, evaluationTimeEpochSecond, serializedBytes);
        } catch (ArithmeticException e) {
            return Plan.failClosed(resolved.tableKeyMatch, resolved.tableDefaultMatch, resolved.defaultResolutionType,
                    resolved.effectiveDefaultDays, resolved.nullRetentionDays, FailReason.EXACT_ARITHMETIC_OVERFLOW,
                    FilterMode.NONE, 0, 0, resolved.tablePolicyFingerprint, "");
        }
    }

    public Plan plan(TenantTtlPolicySnapshot snapshot, String tableKey, int propertyDefaultDays,
                     long partitionUpperEpochSecond, long evaluationTimeEpochSecond) {
        return plan(snapshot, tableKey == null ? null : TenantTtlByteKey.utf8(tableKey), propertyDefaultDays,
                partitionUpperEpochSecond, evaluationTimeEpochSecond);
    }

    public TenantResolution resolveTenant(TenantTtlPolicySnapshot snapshot, TenantTtlByteKey tableKey,
                                          TenantTtlByteKey tenant, int propertyDefaultDays) {
        if (snapshot == null || tableKey == null || tenant == null || propertyDefaultDays <= 0) {
            return new TenantResolution(ResolutionType.UNRESOLVED, 0);
        }
        Optional<TablePolicy> tablePolicy = snapshot.getTablePolicy(tableKey);
        if (tablePolicy.isPresent()) {
            OptionalInt override = tablePolicy.get().getTenantOverride(tenant);
            if (override.isPresent()) {
                return new TenantResolution(ResolutionType.TENANT_OVERRIDE, override.getAsInt());
            }
            OptionalInt tableDefault = tablePolicy.get().getTableDefaultDays();
            if (tableDefault.isPresent()) {
                return new TenantResolution(ResolutionType.TABLE_DEFAULT, tableDefault.getAsInt());
            }
        }
        return new TenantResolution(ResolutionType.PROPERTY_DEFAULT, propertyDefaultDays);
    }

    private Plan createPlan(PlanType type, FilterMode mode, List<TenantTtlByteKey> tenants,
                            ResolvedTablePolicy resolved, long partitionUpperEpochSecond,
                            long evaluationTimeEpochSecond, long serializedBytes) {
        return new Plan(type, mode, tenants, resolved.tableKeyMatch, resolved.tableDefaultMatch,
                resolved.defaultResolutionType, resolved.effectiveDefaultDays, resolved.nullRetentionDays,
                null, tenants.size(), serializedBytes, resolved.tablePolicyFingerprint,
                planFingerprint(partitionUpperEpochSecond, evaluationTimeEpochSecond, mode, tenants,
                        resolved.tablePolicyFingerprint));
    }

    private ResolvedTablePolicy resolveTablePolicy(TenantTtlPolicySnapshot snapshot, TenantTtlByteKey tableKey,
                                                   int propertyDefaultDays) {
        Optional<TablePolicy> optional = snapshot.getTablePolicy(tableKey);
        TablePolicy policy = optional.orElse(null);
        TableKeyMatch keyMatch = policy == null ? TableKeyMatch.NOT_FOUND_USE_DEFAULT : TableKeyMatch.MATCHED;
        TableDefaultMatch defaultMatch = policy != null && policy.getTableDefaultDays().isPresent() ?
                TableDefaultMatch.MATCHED : TableDefaultMatch.NOT_FOUND;
        int effectiveDefault = defaultMatch == TableDefaultMatch.MATCHED ?
                policy.getTableDefaultDays().getAsInt() : propertyDefaultDays;
        ResolutionType source = defaultMatch == TableDefaultMatch.MATCHED ?
                ResolutionType.TABLE_DEFAULT : ResolutionType.PROPERTY_DEFAULT;
        List<Map.Entry<TenantTtlByteKey, Integer>> overrides = policy == null ? new ArrayList<>() :
                new ArrayList<>(policy.getTenantOverrides().entrySet());
        overrides.sort(Map.Entry.comparingByKey());
        int nullRetentionDays = effectiveDefault;
        boolean valid = effectiveDefault > 0;
        for (Map.Entry<TenantTtlByteKey, Integer> entry : overrides) {
            if (entry.getKey() == null || entry.getKey().size() == 0 || entry.getValue() == null ||
                    entry.getValue() <= 0) {
                valid = false;
                continue;
            }
            nullRetentionDays = Math.max(nullRetentionDays, entry.getValue());
        }
        String fingerprint = valid ? tablePolicyFingerprint(tableKey, effectiveDefault, overrides) : "";
        return new ResolvedTablePolicy(keyMatch, defaultMatch, source, effectiveDefault, nullRetentionDays,
                overrides, valid, fingerprint);
    }

    private boolean expired(long partitionUpper, long evaluationTime, int retentionDays) {
        long retentionSeconds = Math.multiplyExact((long) retentionDays, SECONDS_PER_DAY);
        long cutoff = Math.subtractExact(evaluationTime, retentionSeconds);
        return partitionUpper <= cutoff;
    }

    private long serializedFilterBytes(List<TenantTtlByteKey> tenants) {
        long bytes = FILTER_FIXED_SERIALIZED_BYTES;
        for (TenantTtlByteKey tenant : tenants) {
            bytes = Math.addExact(bytes, Math.addExact(Integer.BYTES, (long) tenant.size()));
        }
        return bytes;
    }

    private String tablePolicyFingerprint(TenantTtlByteKey tableKey, int effectiveDefault,
                                          List<Map.Entry<TenantTtlByteKey, Integer>> overrides) {
        TenantTtlPolicyFingerprint fingerprint = new TenantTtlPolicyFingerprint(TABLE_POLICY_FINGERPRINT_DOMAIN);
        tableKey.updateFingerprint(fingerprint);
        fingerprint.putInt(effectiveDefault);
        fingerprint.putInt(overrides.size());
        for (Map.Entry<TenantTtlByteKey, Integer> entry : overrides) {
            entry.getKey().updateFingerprint(fingerprint);
            fingerprint.putInt(entry.getValue());
        }
        return TenantTtlPolicyFingerprint.toHex(fingerprint.finish());
    }

    private String planFingerprint(long partitionUpper, long evaluationTime, FilterMode mode,
                                   List<TenantTtlByteKey> tenants, String tablePolicyFingerprint) {
        TenantTtlPolicyFingerprint fingerprint = new TenantTtlPolicyFingerprint(PLAN_FINGERPRINT_DOMAIN);
        fingerprint.putLong(partitionUpper);
        fingerprint.putLong(evaluationTime);
        fingerprint.putInt(mode.ordinal());
        fingerprint.putBytes(tablePolicyFingerprint.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        fingerprint.putInt(tenants.size());
        for (TenantTtlByteKey tenant : tenants) {
            tenant.updateFingerprint(fingerprint);
        }
        return TenantTtlPolicyFingerprint.toHex(fingerprint.finish());
    }

    public enum PlanType {
        FE_NOOP,
        ROWSET_REWRITE,
        DROP_LOGICAL_PARTITION,
        FAIL_CLOSED
    }

    public enum FilterMode {
        NONE,
        DELETE_LIST,
        KEEP_LIST
    }

    public enum TableKeyMatch {
        MATCHED,
        NOT_FOUND_USE_DEFAULT,
        UNKNOWN
    }

    public enum TableDefaultMatch {
        MATCHED,
        NOT_FOUND,
        UNKNOWN
    }

    public enum ResolutionType {
        TENANT_OVERRIDE,
        TABLE_DEFAULT,
        PROPERTY_DEFAULT,
        UNRESOLVED
    }

    public enum FailReason {
        SNAPSHOT_UNAVAILABLE,
        INVALID_DEFAULT_OR_POLICY,
        EXACT_ARITHMETIC_OVERFLOW,
        FILTER_ROW_LIMIT,
        FILTER_BYTE_LIMIT
    }

    public static final class TenantResolution {
        private final ResolutionType type;
        private final int retentionDays;

        private TenantResolution(ResolutionType type, int retentionDays) {
            this.type = type;
            this.retentionDays = retentionDays;
        }

        public ResolutionType getType() {
            return type;
        }

        public int getRetentionDays() {
            return retentionDays;
        }
    }

    public static final class Plan {
        private final PlanType type;
        private final FilterMode filterMode;
        private final List<TenantTtlByteKey> tenants;
        private final TableKeyMatch tableKeyMatch;
        private final TableDefaultMatch tableDefaultMatch;
        private final ResolutionType defaultResolutionType;
        private final int effectiveDefaultDays;
        private final int nullRetentionDays;
        private final FailReason failReason;
        private final long requiredTenantCount;
        private final long requiredSerializedBytes;
        private final String tablePolicyFingerprint;
        private final String planFingerprint;

        private Plan(PlanType type, FilterMode filterMode, List<TenantTtlByteKey> tenants,
                     TableKeyMatch tableKeyMatch, TableDefaultMatch tableDefaultMatch,
                     ResolutionType defaultResolutionType, int effectiveDefaultDays, int nullRetentionDays,
                     FailReason failReason, long requiredTenantCount, long requiredSerializedBytes,
                     String tablePolicyFingerprint, String planFingerprint) {
            this.type = Objects.requireNonNull(type);
            this.filterMode = Objects.requireNonNull(filterMode);
            this.tenants = Collections.unmodifiableList(new ArrayList<>(tenants));
            this.tableKeyMatch = Objects.requireNonNull(tableKeyMatch);
            this.tableDefaultMatch = Objects.requireNonNull(tableDefaultMatch);
            this.defaultResolutionType = Objects.requireNonNull(defaultResolutionType);
            this.effectiveDefaultDays = effectiveDefaultDays;
            this.nullRetentionDays = nullRetentionDays;
            this.failReason = failReason;
            this.requiredTenantCount = requiredTenantCount;
            this.requiredSerializedBytes = requiredSerializedBytes;
            this.tablePolicyFingerprint = tablePolicyFingerprint;
            this.planFingerprint = planFingerprint;
        }

        private static Plan failClosed(TableKeyMatch keyMatch, TableDefaultMatch defaultMatch,
                                       ResolutionType defaultSource, int effectiveDefaultDays,
                                       int nullRetentionDays, FailReason reason, FilterMode requiredMode,
                                       long requiredTenantCount, long requiredSerializedBytes,
                                       String tablePolicyFingerprint, String planFingerprint) {
            return new Plan(PlanType.FAIL_CLOSED, requiredMode, Collections.emptyList(), keyMatch, defaultMatch,
                    defaultSource, effectiveDefaultDays, nullRetentionDays, reason, requiredTenantCount,
                    requiredSerializedBytes, tablePolicyFingerprint, planFingerprint);
        }

        public PlanType getType() {
            return type;
        }

        public FilterMode getFilterMode() {
            return filterMode;
        }

        public List<TenantTtlByteKey> getTenants() {
            return tenants;
        }

        public TableKeyMatch getTableKeyMatch() {
            return tableKeyMatch;
        }

        public TableDefaultMatch getTableDefaultMatch() {
            return tableDefaultMatch;
        }

        public ResolutionType getDefaultResolutionType() {
            return defaultResolutionType;
        }

        public int getEffectiveDefaultDays() {
            return effectiveDefaultDays;
        }

        public int getNullRetentionDays() {
            return nullRetentionDays;
        }

        public FailReason getFailReason() {
            return failReason;
        }

        public long getRequiredTenantCount() {
            return requiredTenantCount;
        }

        public long getRequiredSerializedBytes() {
            return requiredSerializedBytes;
        }

        public String getTablePolicyFingerprint() {
            return tablePolicyFingerprint;
        }

        public String getPlanFingerprint() {
            return planFingerprint;
        }
    }

    private static final class ResolvedTablePolicy {
        private final TableKeyMatch tableKeyMatch;
        private final TableDefaultMatch tableDefaultMatch;
        private final ResolutionType defaultResolutionType;
        private final int effectiveDefaultDays;
        private final int nullRetentionDays;
        private final List<Map.Entry<TenantTtlByteKey, Integer>> sortedOverrides;
        private final boolean valid;
        private final String tablePolicyFingerprint;

        private ResolvedTablePolicy(TableKeyMatch tableKeyMatch, TableDefaultMatch tableDefaultMatch,
                                    ResolutionType defaultResolutionType, int effectiveDefaultDays,
                                    int nullRetentionDays,
                                    List<Map.Entry<TenantTtlByteKey, Integer>> sortedOverrides,
                                    boolean valid, String tablePolicyFingerprint) {
            this.tableKeyMatch = tableKeyMatch;
            this.tableDefaultMatch = tableDefaultMatch;
            this.defaultResolutionType = defaultResolutionType;
            this.effectiveDefaultDays = effectiveDefaultDays;
            this.nullRetentionDays = nullRetentionDays;
            this.sortedOverrides = sortedOverrides;
            this.valid = valid;
            this.tablePolicyFingerprint = tablePolicyFingerprint;
        }
    }
}
