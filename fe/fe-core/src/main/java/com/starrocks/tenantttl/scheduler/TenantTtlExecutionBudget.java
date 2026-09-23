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

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/** One Replica's round-local budget, including preflight checks, delivery and retry delays. */
final class TenantTtlExecutionBudget {
    static final long POLL_MILLIS = 1_000L;
    static final long RETRY_MILLIS = 10_000L;

    private final int maxAttempts;
    private final long timeoutMillis;
    private final LongSupplier monotonicMillis;
    private final Waiter waiter;
    private final long startedAtMillis;
    private int attempts;

    TenantTtlExecutionBudget(int maxAttempts, int timeoutSeconds, LongSupplier monotonicMillis, Waiter waiter) {
        this.maxAttempts = Math.max(1, maxAttempts);
        this.timeoutMillis = Math.multiplyExact((long) Math.max(1, timeoutSeconds), 1_000L);
        this.monotonicMillis = Objects.requireNonNull(monotonicMillis, "monotonic clock is null");
        this.waiter = Objects.requireNonNull(waiter, "waiter is null");
        this.startedAtMillis = monotonicMillis.getAsLong();
    }

    /** Call before preflight, not only after an RPC was submitted. */
    boolean tryStartAttempt() {
        if (attempts >= maxAttempts || remainingMillis() == 0) {
            return false;
        }
        attempts++;
        return true;
    }

    int getAttempts() {
        return attempts;
    }

    boolean hasAttemptsRemaining() {
        return attempts < maxAttempts;
    }

    long remainingMillis() {
        long elapsed = Math.max(0, monotonicMillis.getAsLong() - startedAtMillis);
        return elapsed >= timeoutMillis ? 0 : timeoutMillis - elapsed;
    }

    /** Sleep in short slices; role loss and the cumulative deadline also interrupt retry delays. */
    boolean await(long millis, BooleanSupplier stillCurrent) throws InterruptedException {
        long waitStarted = monotonicMillis.getAsLong();
        while (stillCurrent.getAsBoolean()) {
            long remaining = remainingMillis();
            if (remaining == 0) {
                return false;
            }
            long waitRemaining = millis - Math.max(0, monotonicMillis.getAsLong() - waitStarted);
            if (waitRemaining <= 0) {
                return true;
            }
            waiter.sleep(Math.min(POLL_MILLIS, Math.min(remaining, waitRemaining)));
        }
        return false;
    }

    @FunctionalInterface
    interface Waiter {
        void sleep(long millis) throws InterruptedException;
    }
}
