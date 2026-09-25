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

/** Leader-local recovery evidence. Access is serialized by the snapshot manager monitor. */
final class TenantTtlSnapshotRecovery {
    static final int MIN_UNCERTAIN_SWEEPS = 3;
    static final long MIN_UNCERTAIN_MILLIS = 60_000;
    static final long DEFAULT_COOLDOWN_SECONDS = 300;

    long evidenceTxn = -1;
    int uncertainSweeps;
    long uncertainSince;
    boolean needed;
    long waitingRefreshId;
    long lastCompletedRefreshId;
    boolean cooling;
    long refreshFinishedAt;
    String lastRefreshResult = "NONE";
    String phase = "PROBING";
    int visited;
    int candidates;

    void resetEvidence(long txnId) {
        evidenceTxn = txnId;
        uncertainSweeps = 0;
        uncertainSince = 0;
        needed = false;
    }

    void recordSweep(long txnId, long startedAt, long now, int missing, int uncertain, int other) {
        if (evidenceTxn != txnId) {
            resetEvidence(txnId);
        }
        if (other > 0 || missing + uncertain == 0) {
            resetEvidence(txnId);
        } else if (uncertain == 0) {
            resetEvidence(txnId);
            needed = true;
        } else {
            if (uncertainSweeps == 0) {
                uncertainSince = startedAt;
            }
            uncertainSweeps++;
            needed = uncertainSweeps >= MIN_UNCERTAIN_SWEEPS && now - uncertainSince >= MIN_UNCERTAIN_MILLIS;
        }
    }

    boolean finishRefresh(long refreshId, boolean success, long now) {
        if (refreshId <= lastCompletedRefreshId || (waitingRefreshId > 0 && refreshId < waitingRefreshId)) {
            return false;
        }
        lastCompletedRefreshId = refreshId;
        waitingRefreshId = 0;
        cooling = true;
        refreshFinishedAt = now;
        lastRefreshResult = success ? "SUCCEEDED" : "FAILED";
        resetEvidence(evidenceTxn);
        return true;
    }

    long cooldownRemaining(long now, long configuredSeconds) {
        long seconds = configuredSeconds > 0 ? configuredSeconds : DEFAULT_COOLDOWN_SECONDS;
        long millis = seconds > Long.MAX_VALUE / 1000 ? Long.MAX_VALUE : seconds * 1000;
        return cooling ? Math.max(0, millis - Math.max(0, now - refreshFinishedAt)) : 0;
    }
}
