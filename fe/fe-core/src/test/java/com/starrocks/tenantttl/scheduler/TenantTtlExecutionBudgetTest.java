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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class TenantTtlExecutionBudgetTest {
    @Test
    void testThirtyAttemptsIncludeFirstAndPreflightFailures() throws Exception {
        AtomicLong clock = new AtomicLong();
        TenantTtlExecutionBudget budget = new TenantTtlExecutionBudget(30, 3600, clock::get, clock::addAndGet);
        for (int attempt = 1; attempt <= 30; attempt++) {
            Assertions.assertTrue(budget.tryStartAttempt());
            Assertions.assertEquals(attempt, budget.getAttempts());
            if (attempt < 30) {
                Assertions.assertTrue(budget.await(TenantTtlExecutionBudget.RETRY_MILLIS, () -> true));
            }
        }
        Assertions.assertFalse(budget.hasAttemptsRemaining());
        Assertions.assertFalse(budget.tryStartAttempt());
        Assertions.assertEquals(290_000L, clock.get());
    }

    @Test
    void testRetryKeepsOnlyNinetySecondsAfterSlowFailure() throws Exception {
        AtomicLong clock = new AtomicLong();
        TenantTtlExecutionBudget budget = new TenantTtlExecutionBudget(30, 3600, clock::get, clock::addAndGet);
        Assertions.assertTrue(budget.tryStartAttempt());
        clock.addAndGet(3_500_000L);
        Assertions.assertTrue(budget.await(10_000L, () -> true));
        Assertions.assertTrue(budget.tryStartAttempt());
        Assertions.assertEquals(90_000L, budget.remainingMillis());
        Assertions.assertFalse(budget.await(90_000L, () -> true));
        Assertions.assertFalse(budget.tryStartAttempt());
        Assertions.assertEquals(2, budget.getAttempts());
    }

    @Test
    void testWaitSlicesStopAtDeadlineWithoutExtendingIt() throws Exception {
        AtomicLong clock = new AtomicLong();
        List<Long> slices = new ArrayList<>();
        TenantTtlExecutionBudget budget = new TenantTtlExecutionBudget(30, 2, clock::get, millis -> {
            slices.add(millis);
            clock.addAndGet(millis);
        });
        Assertions.assertTrue(budget.tryStartAttempt());
        clock.addAndGet(500);
        Assertions.assertFalse(budget.await(10_000L, () -> true));
        Assertions.assertEquals(List.of(1000L, 500L), slices);
        Assertions.assertEquals(2000L, clock.get());
    }

    @Test
    void testLeadershipLossStopsRetryDelay() throws Exception {
        AtomicLong clock = new AtomicLong();
        AtomicBoolean leader = new AtomicBoolean(true);
        TenantTtlExecutionBudget budget = new TenantTtlExecutionBudget(30, 3600, clock::get, millis -> {
            clock.addAndGet(millis);
            leader.set(false);
        });
        Assertions.assertFalse(budget.await(10_000L, leader::get));
        Assertions.assertEquals(1000L, clock.get());
    }

    @Test
    void testPositiveClampAndIntMaxConversion() {
        AtomicLong clock = new AtomicLong(-1000L);
        TenantTtlExecutionBudget minimum = new TenantTtlExecutionBudget(0, -1, clock::get, clock::addAndGet);
        Assertions.assertTrue(minimum.tryStartAttempt());
        Assertions.assertFalse(minimum.tryStartAttempt());
        Assertions.assertEquals(1000L, minimum.remainingMillis());
        TenantTtlExecutionBudget maximum = new TenantTtlExecutionBudget(
                Integer.MAX_VALUE, Integer.MAX_VALUE, clock::get, clock::addAndGet);
        Assertions.assertEquals(2_147_483_647_000L, maximum.remainingMillis());
    }

    @Test
    void testWaitInterruptionPropagates() {
        TenantTtlExecutionBudget budget = new TenantTtlExecutionBudget(30, 3600, () -> 0, millis -> {
            throw new InterruptedException("role change");
        });
        Assertions.assertThrows(InterruptedException.class, () -> budget.await(10_000L, () -> true));
    }
}
