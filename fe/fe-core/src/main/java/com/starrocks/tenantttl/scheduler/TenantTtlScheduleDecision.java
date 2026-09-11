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

import com.starrocks.tenantttl.policy.TenantTtlEvaluationContext;
import com.starrocks.tenantttl.policy.TenantTtlPolicyPlanner;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/** Pure trigger classification for one immutable Physical Partition plan. */
public final class TenantTtlScheduleDecision {
    private TenantTtlScheduleDecision() {
    }

    public static Decision decide(TenantTtlEvaluationContext context,
                                  TenantTtlEvaluationContext.PartitionPlan plan,
                                  TenantTtlPartitionProgress progress,
                                  boolean retryPending) {
        Objects.requireNonNull(context, "evaluation context is null");
        Objects.requireNonNull(plan, "partition plan is null");
        if (plan.getType() == TenantTtlPolicyPlanner.PlanType.FAIL_CLOSED || plan.getPolicyPlan() == null) {
            return Decision.none();
        }

        EnumSet<TriggerReason> reasons = EnumSet.noneOf(TriggerReason.class);
        boolean sameBindingAndBoundary = progress != null &&
                context.getTableBindingFingerprint().equals(progress.getTableBindingFingerprint()) &&
                plan.getBoundaryFingerprint().equals(progress.getPartitionBoundaryFingerprint());
        boolean samePolicy = sameBindingAndBoundary &&
                plan.getPolicyPlan().getTablePolicyFingerprint().equals(progress.getTablePolicyFingerprint());

        if (retryPending) {
            reasons.add(TriggerReason.RETRY_PENDING);
        }
        if (progress == null || !sameBindingAndBoundary) {
            reasons.add(TriggerReason.INITIAL_CATCH_UP);
        } else if (!samePolicy) {
            reasons.add(TriggerReason.POLICY_CHANGED);
        }

        long completedCursor = samePolicy ? progress.getCompletedExpiryCursorEpochSeconds() : 0;
        if (plan.getPolicyPlan().getCompletedExpiryCursorEpochSeconds() > completedCursor) {
            reasons.add(TriggerReason.EXPIRY_EVENT_DUE);
        }

        long processedThrough = progress == null ? 0 : progress.getProcessedThroughVersion();
        if (plan.getType() != TenantTtlPolicyPlanner.PlanType.FE_NOOP &&
                plan.getObservedVisibleVersion() > processedThrough) {
            reasons.add(TriggerReason.DATA_VERSION_ADVANCED);
        }
        return reasons.isEmpty() ? Decision.none() : new Decision(reasons);
    }

    public enum TriggerReason {
        RETRY_PENDING,
        INITIAL_CATCH_UP,
        POLICY_CHANGED,
        EXPIRY_EVENT_DUE,
        DATA_VERSION_ADVANCED
    }

    public static final class Decision {
        private static final Decision NONE = new Decision(EnumSet.noneOf(TriggerReason.class));
        private final Set<TriggerReason> reasons;

        private Decision(EnumSet<TriggerReason> reasons) {
            this.reasons = Collections.unmodifiableSet(EnumSet.copyOf(reasons));
        }

        private static Decision none() {
            return NONE;
        }

        public boolean shouldEvaluate() {
            return !reasons.isEmpty();
        }

        public boolean hasReason(TriggerReason reason) {
            return reasons.contains(reason);
        }

        public Set<TriggerReason> getReasons() {
            return reasons;
        }

        public TriggerReason getPrimaryReason() {
            if (reasons.isEmpty()) {
                return null;
            }
            for (TriggerReason reason : TriggerReason.values()) {
                if (reasons.contains(reason)) {
                    return reason;
                }
            }
            throw new IllegalStateException("Tenant-TTL trigger set is non-empty without a known reason");
        }
    }
}
