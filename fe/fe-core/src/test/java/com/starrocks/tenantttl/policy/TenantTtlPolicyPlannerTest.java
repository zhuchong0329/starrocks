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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public class TenantTtlPolicyPlannerTest {
    private static final long UPPER = 1000L;
    private static final String TABLE_KEY = "business.http_log";
    private static final TenantTtlPolicyPlanner PLANNER = new TenantTtlPolicyPlanner(1000, 1024 * 1024);

    @Test
    public void testTenantResolutionHierarchyAndMissingTableFallback() {
        Map<String, Integer> overrides = new HashMap<>();
        overrides.put("tenant_a", 30);
        TenantTtlPolicySnapshot snapshot = snapshot(11, 90, overrides);

        TenantTtlPolicyPlanner.TenantResolution resolution = PLANNER.resolveTenant(snapshot,
                TenantTtlByteKey.utf8(TABLE_KEY), TenantTtlByteKey.utf8("tenant_a"), 180);
        Assertions.assertEquals(TenantTtlPolicyPlanner.ResolutionType.TENANT_OVERRIDE, resolution.getType());
        Assertions.assertEquals(30, resolution.getRetentionDays());

        resolution = PLANNER.resolveTenant(snapshot, TenantTtlByteKey.utf8(TABLE_KEY),
                TenantTtlByteKey.utf8("tenant_x"), 180);
        Assertions.assertEquals(TenantTtlPolicyPlanner.ResolutionType.TABLE_DEFAULT, resolution.getType());
        Assertions.assertEquals(90, resolution.getRetentionDays());

        resolution = PLANNER.resolveTenant(snapshot, TenantTtlByteKey.utf8("business.missing"),
                TenantTtlByteKey.utf8("tenant_a"), 180);
        Assertions.assertEquals(TenantTtlPolicyPlanner.ResolutionType.PROPERTY_DEFAULT, resolution.getType());
        Assertions.assertEquals(180, resolution.getRetentionDays());

        resolution = PLANNER.resolveTenant(null, TenantTtlByteKey.utf8(TABLE_KEY),
                TenantTtlByteKey.utf8("tenant_a"), 180);
        Assertions.assertEquals(TenantTtlPolicyPlanner.ResolutionType.UNRESOLVED, resolution.getType());
    }

    @Test
    public void testDeleteNoopKeepAndDropMatrix() {
        Map<String, Integer> overrides = new HashMap<>();
        overrides.put("tenant_a", 30);
        overrides.put("tenant_b", 365);
        TenantTtlPolicySnapshot snapshot = snapshot(12, null, overrides);

        TenantTtlPolicyPlanner.Plan plan = PLANNER.plan(snapshot, TABLE_KEY, 180, UPPER, atDay(29));
        assertPlan(plan, TenantTtlPolicyPlanner.PlanType.FE_NOOP, TenantTtlPolicyPlanner.FilterMode.NONE);
        Assertions.assertEquals(0, plan.getCompletedExpiryCursorEpochSeconds());
        Assertions.assertEquals(atDay(30), plan.getNextExpiryEpochSeconds());

        plan = PLANNER.plan(snapshot, TABLE_KEY, 180, UPPER, atDay(30));
        assertPlan(plan, TenantTtlPolicyPlanner.PlanType.ROWSET_REWRITE,
                TenantTtlPolicyPlanner.FilterMode.DELETE_LIST);
        Assertions.assertEquals(Collections.singletonList("tenant_a"), strings(plan.getTenants()));
        Assertions.assertEquals(365, plan.getNullRetentionDays());
        Assertions.assertEquals(atDay(30), plan.getCompletedExpiryCursorEpochSeconds());
        Assertions.assertEquals(atDay(180), plan.getNextExpiryEpochSeconds());

        plan = PLANNER.plan(snapshot, TABLE_KEY, 180, UPPER, atDay(180));
        assertPlan(plan, TenantTtlPolicyPlanner.PlanType.ROWSET_REWRITE,
                TenantTtlPolicyPlanner.FilterMode.KEEP_LIST);
        Assertions.assertEquals(Collections.singletonList("tenant_b"), strings(plan.getTenants()));
        Assertions.assertEquals(atDay(180), plan.getCompletedExpiryCursorEpochSeconds());
        Assertions.assertEquals(atDay(365), plan.getNextExpiryEpochSeconds());

        plan = PLANNER.plan(snapshot, TABLE_KEY, 180, UPPER, atDay(365));
        assertPlan(plan, TenantTtlPolicyPlanner.PlanType.DROP_LOGICAL_PARTITION,
                TenantTtlPolicyPlanner.FilterMode.NONE);
        Assertions.assertEquals(atDay(365), plan.getCompletedExpiryCursorEpochSeconds());
        Assertions.assertEquals(0, plan.getNextExpiryEpochSeconds());
    }

    @Test
    public void testTableDefaultAndTableKeyMatchMetadata() {
        TenantTtlPolicySnapshot withDefault = snapshot(13, 60, Collections.emptyMap());
        TenantTtlPolicyPlanner.Plan plan = PLANNER.plan(withDefault, TABLE_KEY, 180, UPPER, atDay(59));
        Assertions.assertEquals(TenantTtlPolicyPlanner.TableKeyMatch.MATCHED, plan.getTableKeyMatch());
        Assertions.assertEquals(TenantTtlPolicyPlanner.TableDefaultMatch.MATCHED, plan.getTableDefaultMatch());
        Assertions.assertEquals(TenantTtlPolicyPlanner.ResolutionType.TABLE_DEFAULT,
                plan.getDefaultResolutionType());
        Assertions.assertEquals(60, plan.getEffectiveDefaultDays());

        plan = PLANNER.plan(withDefault, "business.not_configured", 180, UPPER, atDay(179));
        Assertions.assertEquals(TenantTtlPolicyPlanner.TableKeyMatch.NOT_FOUND_USE_DEFAULT,
                plan.getTableKeyMatch());
        Assertions.assertEquals(TenantTtlPolicyPlanner.TableDefaultMatch.NOT_FOUND,
                plan.getTableDefaultMatch());
        Assertions.assertEquals(TenantTtlPolicyPlanner.ResolutionType.PROPERTY_DEFAULT,
                plan.getDefaultResolutionType());
        Assertions.assertEquals(180, plan.getEffectiveDefaultDays());

        plan = PLANNER.plan(null, TABLE_KEY, 180, UPPER, atDay(365));
        Assertions.assertEquals(TenantTtlPolicyPlanner.PlanType.FAIL_CLOSED, plan.getType());
        Assertions.assertEquals(TenantTtlPolicyPlanner.FailReason.SNAPSHOT_UNAVAILABLE, plan.getFailReason());
        Assertions.assertEquals(TenantTtlPolicyPlanner.TableKeyMatch.UNKNOWN, plan.getTableKeyMatch());
    }

    @Test
    public void testExactBoundaryAndArithmeticOverflow() {
        TenantTtlPolicySnapshot snapshot = snapshot(14, null, Collections.emptyMap());
        TenantTtlPolicyPlanner.Plan before = PLANNER.plan(snapshot, TABLE_KEY, 30, UPPER, atDay(30) - 1);
        Assertions.assertEquals(TenantTtlPolicyPlanner.PlanType.FE_NOOP, before.getType());
        TenantTtlPolicyPlanner.Plan exact = PLANNER.plan(snapshot, TABLE_KEY, 30, UPPER, atDay(30));
        Assertions.assertEquals(TenantTtlPolicyPlanner.PlanType.DROP_LOGICAL_PARTITION, exact.getType());

        TenantTtlPolicyPlanner.Plan overflow = PLANNER.plan(snapshot, TABLE_KEY, Integer.MAX_VALUE,
                Long.MAX_VALUE, Long.MAX_VALUE);
        Assertions.assertEquals(TenantTtlPolicyPlanner.PlanType.FAIL_CLOSED, overflow.getType());
        Assertions.assertEquals(TenantTtlPolicyPlanner.FailReason.EXACT_ARITHMETIC_OVERFLOW,
                overflow.getFailReason());
    }

    @Test
    public void testFilterLimitsAreExactAndNeverChangePolarity() {
        Map<String, Integer> overrides = new HashMap<>();
        overrides.put("a", 30);
        overrides.put("bb", 30);
        TenantTtlPolicySnapshot snapshot = snapshot(15, null, overrides);
        long exactBytes = 1L + Integer.BYTES + Integer.BYTES + 1 + Integer.BYTES + 2;

        TenantTtlPolicyPlanner.Plan exact = new TenantTtlPolicyPlanner(2, exactBytes)
                .plan(snapshot, TABLE_KEY, 180, UPPER, atDay(30));
        assertPlan(exact, TenantTtlPolicyPlanner.PlanType.ROWSET_REWRITE,
                TenantTtlPolicyPlanner.FilterMode.DELETE_LIST);
        Assertions.assertEquals(exactBytes, exact.getRequiredSerializedBytes());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> exact.getTenants().clear());

        TenantTtlPolicyPlanner.Plan rowLimited = new TenantTtlPolicyPlanner(1, exactBytes)
                .plan(snapshot, TABLE_KEY, 180, UPPER, atDay(30));
        Assertions.assertEquals(TenantTtlPolicyPlanner.PlanType.FAIL_CLOSED, rowLimited.getType());
        Assertions.assertEquals(TenantTtlPolicyPlanner.FilterMode.DELETE_LIST, rowLimited.getFilterMode());
        Assertions.assertEquals(TenantTtlPolicyPlanner.FailReason.FILTER_ROW_LIMIT, rowLimited.getFailReason());
        Assertions.assertEquals(2, rowLimited.getRequiredTenantCount());
        Assertions.assertTrue(rowLimited.getTenants().isEmpty());

        TenantTtlPolicyPlanner.Plan byteLimited = new TenantTtlPolicyPlanner(2, exactBytes - 1)
                .plan(snapshot, TABLE_KEY, 180, UPPER, atDay(30));
        Assertions.assertEquals(TenantTtlPolicyPlanner.PlanType.FAIL_CLOSED, byteLimited.getType());
        Assertions.assertEquals(TenantTtlPolicyPlanner.FilterMode.DELETE_LIST, byteLimited.getFilterMode());
        Assertions.assertEquals(TenantTtlPolicyPlanner.FailReason.FILTER_BYTE_LIMIT, byteLimited.getFailReason());
        Assertions.assertTrue(byteLimited.getTenants().isEmpty());
    }

    @Test
    public void testStableFingerprintsAndRandomOracle() {
        Map<String, Integer> firstOrder = new HashMap<>();
        firstOrder.put("tenant_b", 60);
        firstOrder.put("tenant_a", 30);
        Map<String, Integer> secondOrder = new HashMap<>();
        secondOrder.put("tenant_a", 30);
        secondOrder.put("tenant_b", 60);
        TenantTtlPolicyPlanner.Plan first = PLANNER.plan(snapshot(16, null, firstOrder), TABLE_KEY, 180,
                UPPER, atDay(45));
        TenantTtlPolicyPlanner.Plan second = PLANNER.plan(snapshot(999, null, secondOrder), TABLE_KEY, 180,
                UPPER, atDay(45));
        Assertions.assertEquals(first.getTablePolicyFingerprint(), second.getTablePolicyFingerprint());
        Assertions.assertEquals(first.getPlanFingerprint(), second.getPlanFingerprint());

        Random random = new Random(20260911L);
        for (int iteration = 0; iteration < 200; iteration++) {
            int propertyDefault = 1 + random.nextInt(20);
            int ageDays = random.nextInt(25);
            Map<String, Integer> overrides = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                overrides.put("tenant_" + i, 1 + random.nextInt(20));
            }
            TenantTtlPolicyPlanner.Plan actual = PLANNER.plan(snapshot(1000 + iteration, null, overrides),
                    TABLE_KEY, propertyDefault, UPPER, atDay(ageDays));
            List<String> expired = overrides.entrySet().stream().filter(e -> e.getValue() <= ageDays)
                    .map(Map.Entry::getKey).sorted().collect(Collectors.toList());
            List<String> retained = overrides.entrySet().stream().filter(e -> e.getValue() > ageDays)
                    .map(Map.Entry::getKey).sorted().collect(Collectors.toList());
            if (propertyDefault > ageDays) {
                if (expired.isEmpty()) {
                    Assertions.assertEquals(TenantTtlPolicyPlanner.PlanType.FE_NOOP, actual.getType());
                } else {
                    assertPlan(actual, TenantTtlPolicyPlanner.PlanType.ROWSET_REWRITE,
                            TenantTtlPolicyPlanner.FilterMode.DELETE_LIST);
                    Assertions.assertEquals(expired, strings(actual.getTenants()));
                }
            } else if (retained.isEmpty()) {
                Assertions.assertEquals(TenantTtlPolicyPlanner.PlanType.DROP_LOGICAL_PARTITION, actual.getType());
            } else {
                assertPlan(actual, TenantTtlPolicyPlanner.PlanType.ROWSET_REWRITE,
                        TenantTtlPolicyPlanner.FilterMode.KEEP_LIST);
                Assertions.assertEquals(retained, strings(actual.getTenants()));
            }
        }
    }

    private static TenantTtlPolicySnapshot snapshot(long txnId, Integer tableDefault,
                                                     Map<String, Integer> stringOverrides) {
        Map<TenantTtlByteKey, Integer> overrides = new HashMap<>();
        for (Map.Entry<String, Integer> entry : stringOverrides.entrySet()) {
            overrides.put(TenantTtlByteKey.utf8(entry.getKey()), entry.getValue());
        }
        Map<TenantTtlByteKey, TablePolicy> policies = new HashMap<>();
        policies.put(TenantTtlByteKey.utf8(TABLE_KEY), new TablePolicy(tableDefault, overrides));
        return new TenantTtlPolicySnapshot(1, "dict", txnId, Instant.EPOCH, 0, 0, 0, policies);
    }

    private static long atDay(int days) {
        return UPPER + days * 86400L;
    }

    private static List<String> strings(List<TenantTtlByteKey> tenants) {
        List<String> result = new ArrayList<>();
        for (TenantTtlByteKey tenant : tenants) {
            result.add(new String(tenant.copyBytes(), java.nio.charset.StandardCharsets.UTF_8));
        }
        return result;
    }

    private static void assertPlan(TenantTtlPolicyPlanner.Plan plan, TenantTtlPolicyPlanner.PlanType type,
                                   TenantTtlPolicyPlanner.FilterMode mode) {
        Assertions.assertEquals(type, plan.getType());
        Assertions.assertEquals(mode, plan.getFilterMode());
        Assertions.assertNull(plan.getFailReason());
    }
}
