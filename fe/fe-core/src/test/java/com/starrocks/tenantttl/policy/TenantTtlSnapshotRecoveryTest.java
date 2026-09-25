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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TenantTtlSnapshotRecoveryTest {
    @Test
    public void testExplicitMissingAndBothNetworkThresholds() {
        TenantTtlSnapshotRecovery recovery = new TenantTtlSnapshotRecovery();
        recovery.recordSweep(7, 0, 1, 3, 0, 0);
        assertTrue(recovery.needed);
        recovery.resetEvidence(7);
        recovery.recordSweep(7, 0, 60_000, 1, 2, 0);
        recovery.recordSweep(7, 60_000, 70_000, 1, 2, 0);
        assertFalse(recovery.needed);
        recovery.recordSweep(7, 70_000, 80_000, 1, 2, 0);
        assertTrue(recovery.needed);
        recovery.resetEvidence(7);
        for (int i = 0; i < 3; i++) {
            recovery.recordSweep(7, i * 1000, (i + 1) * 1000, 0, 3, 0);
        }
        assertFalse(recovery.needed);
        recovery.recordSweep(7, 3000, 60_000, 0, 3, 0);
        assertTrue(recovery.needed);
    }

    @Test
    public void testOtherErrorsEmptySweepAndTargetChangesBreakEvidence() {
        TenantTtlSnapshotRecovery recovery = new TenantTtlSnapshotRecovery();
        recovery.recordSweep(7, 0, 60_000, 1, 2, 0);
        recovery.recordSweep(7, 60_000, 70_000, 1, 2, 0);
        recovery.recordSweep(7, 70_000, 80_000, 1, 1, 1);
        assertEquals(0, recovery.uncertainSweeps);
        assertFalse(recovery.needed);
        recovery.recordSweep(7, 80_000, 90_000, 1, 2, 0);
        recovery.recordSweep(8, 90_000, 100_000, 1, 2, 0);
        assertEquals(1, recovery.uncertainSweeps);
        assertEquals(90_000, recovery.uncertainSince);
        recovery.recordSweep(8, 100_000, 110_000, 0, 0, 0);
        assertEquals(0, recovery.uncertainSweeps);
    }

    @Test
    public void testCooldownStartsAtCompletionAndTracksActualRefresh() {
        TenantTtlSnapshotRecovery recovery = new TenantTtlSnapshotRecovery();
        recovery.waitingRefreshId = 2;
        assertEquals(0, recovery.cooldownRemaining(500_000, 300));
        assertFalse(recovery.finishRefresh(1, false, 500_000));
        assertEquals(2, recovery.waitingRefreshId);
        assertTrue(recovery.finishRefresh(2, false, 500_000));
        assertEquals(1000, recovery.cooldownRemaining(799_000, 300));
        assertEquals(0, recovery.cooldownRemaining(800_000, 300));
        assertEquals(300_000, recovery.cooldownRemaining(500_000, 0));
        assertEquals(300_000, recovery.cooldownRemaining(500_000, -1));
        assertEquals(100_000, recovery.cooldownRemaining(500_000, 100));
        assertEquals(Long.MAX_VALUE, recovery.cooldownRemaining(500_000, Long.MAX_VALUE));
        assertFalse(recovery.finishRefresh(2, false, 900_000));
        assertEquals(500_000, recovery.refreshFinishedAt);
        assertTrue(recovery.finishRefresh(3, true, 900_000));
        assertEquals("SUCCEEDED", recovery.lastRefreshResult);
    }
}
