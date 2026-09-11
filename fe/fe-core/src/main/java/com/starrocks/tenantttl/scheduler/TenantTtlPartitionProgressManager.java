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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.TenantTtlPartitionProgressBatchLog;
import com.starrocks.persist.TenantTtlPartitionProgressRemoveLog;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.tenantttl.scheduler.TenantTtlPartitionProgress.ProgressKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Owns the durable, completed-plan facts used to rebuild Tenant-TTL scheduling after failover. */
public final class TenantTtlPartitionProgressManager {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<ProgressKey, TenantTtlPartitionProgress> progresses = new HashMap<>();
    private final JournalSink journalSink;

    public TenantTtlPartitionProgressManager() {
        this(new JournalSink() {
            @Override
            public void logUpsert(TenantTtlPartitionProgressBatchLog log) {
                EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
                if (editLog == null) {
                    throw new IllegalStateException("EditLog is unavailable");
                }
                editLog.logUpsertTenantTtlPartitionProgress(log);
            }

            @Override
            public void logRemove(TenantTtlPartitionProgressRemoveLog log) {
                EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
                if (editLog == null) {
                    throw new IllegalStateException("EditLog is unavailable");
                }
                editLog.logRemoveTenantTtlPartitionProgress(log);
            }
        });
    }

    TenantTtlPartitionProgressManager(JournalSink journalSink) {
        this.journalSink = Objects.requireNonNull(journalSink, "journal sink is null");
    }

    /**
     * Atomically journals and publishes a completed plan if the caller still owns the observed state.
     * The expected value is null only when the caller observed no progress for this key.
     */
    public AdvanceResult compareAndAdvanceCompletedPlan(TenantTtlPartitionProgress expected,
                                                        TenantTtlPartitionProgress candidate) {
        return compareAndAdvanceCompletedPlans(
                Collections.singletonList(new CompletedPlanUpdate(expected, candidate)));
    }

    /** Atomically journals a batch; one stale or regressing entry rejects the entire batch. */
    public AdvanceResult compareAndAdvanceCompletedPlans(Collection<CompletedPlanUpdate> updates) {
        Objects.requireNonNull(updates, "completed plan updates are null");
        if (updates.isEmpty()) {
            return AdvanceResult.UNCHANGED;
        }
        List<CompletedPlanUpdate> copiedUpdates = new ArrayList<>(updates.size());
        Set<ProgressKey> uniqueKeys = new HashSet<>();
        for (CompletedPlanUpdate update : updates) {
            CompletedPlanUpdate copied = new CompletedPlanUpdate(update.expected, update.candidate);
            if (!uniqueKeys.add(copied.candidate.key())) {
                throw new IllegalArgumentException("duplicate Tenant-TTL progress key in completion batch");
            }
            copiedUpdates.add(copied);
        }
        copiedUpdates.sort((left, right) -> left.candidate.key().compareTo(right.candidate.key()));
        lock.writeLock().lock();
        try {
            List<TenantTtlPartitionProgress> changed = new ArrayList<>();
            for (CompletedPlanUpdate update : copiedUpdates) {
                TenantTtlPartitionProgress current = progresses.get(update.candidate.key());
                if (!Objects.equals(current, update.expected)) {
                    return AdvanceResult.STALE_EXPECTATION;
                }
                if (update.candidate.equals(current)) {
                    continue;
                }
                if (current != null && current.hasSamePlanIdentity(update.candidate) &&
                        isAnyWatermarkRegressed(current, update.candidate)) {
                    return AdvanceResult.REJECTED_REGRESSION;
                }
                changed.add(update.candidate);
            }
            if (changed.isEmpty()) {
                return AdvanceResult.UNCHANGED;
            }
            journalSink.logUpsert(new TenantTtlPartitionProgressBatchLog(changed));
            for (TenantTtlPartitionProgress progress : changed) {
                progresses.put(progress.key(), progress);
            }
            return AdvanceResult.ADVANCED;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static boolean isAnyWatermarkRegressed(TenantTtlPartitionProgress current,
                                                    TenantTtlPartitionProgress candidate) {
        return candidate.getCompletedExpiryCursorEpochSeconds() <
                current.getCompletedExpiryCursorEpochSeconds() ||
                candidate.getProcessedThroughVersion() < current.getProcessedThroughVersion() ||
                candidate.getLastSuccessSnapshotTxnId() < current.getLastSuccessSnapshotTxnId() ||
                candidate.getLastSuccessEvaluationTimeEpochSeconds() <
                        current.getLastSuccessEvaluationTimeEpochSeconds();
    }

    public Optional<TenantTtlPartitionProgress> get(ProgressKey key) {
        lock.readLock().lock();
        try {
            TenantTtlPartitionProgress progress = progresses.get(key);
            return progress == null ? Optional.empty() :
                    Optional.of(new TenantTtlPartitionProgress(progress));
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<TenantTtlPartitionProgress> getAll() {
        lock.readLock().lock();
        try {
            List<TenantTtlPartitionProgress> result = new ArrayList<>(progresses.size());
            for (TenantTtlPartitionProgress progress : progresses.values()) {
                result.add(new TenantTtlPartitionProgress(progress));
            }
            result.sort((left, right) -> left.key().compareTo(right.key()));
            return Collections.unmodifiableList(result);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return progresses.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public int countTable(long dbId, long tableId) {
        lock.readLock().lock();
        try {
            int count = 0;
            for (ProgressKey key : progresses.keySet()) {
                if (key.getDbId() == dbId && key.getTableId() == tableId) {
                    count++;
                }
            }
            return count;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int removePhysicalPartitions(long dbId, long tableId, Collection<Long> physicalPartitionIds) {
        List<ProgressKey> keys = new ArrayList<>(physicalPartitionIds.size());
        for (Long physicalPartitionId : physicalPartitionIds) {
            if (physicalPartitionId != null) {
                keys.add(new ProgressKey(dbId, tableId, physicalPartitionId));
            }
        }
        return removeKeys(keys, false);
    }

    public int removeTable(long dbId, long tableId) {
        return removeMatching(key -> key.getDbId() == dbId && key.getTableId() == tableId, false);
    }

    public int replayRemoveTable(long dbId, long tableId) {
        return removeMatching(key -> key.getDbId() == dbId && key.getTableId() == tableId, true);
    }

    public int removeDatabase(long dbId) {
        return removeMatching(key -> key.getDbId() == dbId, false);
    }

    public int replayRemoveDatabase(long dbId) {
        return removeMatching(key -> key.getDbId() == dbId, true);
    }

    private int removeMatching(KeyPredicate predicate, boolean replay) {
        lock.writeLock().lock();
        try {
            List<ProgressKey> keys = new ArrayList<>();
            for (ProgressKey key : progresses.keySet()) {
                if (predicate.test(key)) {
                    keys.add(key);
                }
            }
            return removeKeysLocked(keys, replay);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int removeKeys(Collection<ProgressKey> keys) {
        return removeKeys(keys, false);
    }

    private int removeKeys(Collection<ProgressKey> keys, boolean replay) {
        lock.writeLock().lock();
        try {
            return removeKeysLocked(keys, replay);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private int removeKeysLocked(Collection<ProgressKey> requestedKeys, boolean replay) {
        List<ProgressKey> existing = new ArrayList<>();
        Set<ProgressKey> seen = new HashSet<>();
        for (ProgressKey key : requestedKeys) {
            if (key != null && seen.add(key) && progresses.containsKey(key)) {
                existing.add(key);
            }
        }
        existing.sort(ProgressKey::compareTo);
        if (existing.isEmpty()) {
            return 0;
        }
        if (!replay) {
            journalSink.logRemove(new TenantTtlPartitionProgressRemoveLog(existing));
        }
        for (ProgressKey key : existing) {
            progresses.remove(key);
        }
        return existing.size();
    }

    public void replayUpsert(TenantTtlPartitionProgressBatchLog log) {
        if (log == null) {
            return;
        }
        lock.writeLock().lock();
        try {
            for (TenantTtlPartitionProgress progress : log.getProgresses()) {
                progress.validate();
                progresses.put(progress.key(), new TenantTtlPartitionProgress(progress));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replayRemove(TenantTtlPartitionProgressRemoveLog log) {
        if (log != null) {
            removeKeys(log.getKeys(), true);
        }
    }

    /** Removes at most maxRemovals records whose database, table, binding, or Physical Partition disappeared. */
    public int gcOrphanProgress(GlobalStateMgr state, int maxRemovals) {
        Objects.requireNonNull(state, "state is null");
        if (maxRemovals <= 0) {
            throw new IllegalArgumentException("max removals must be positive");
        }
        List<ProgressKey> candidates = new ArrayList<>();
        lock.readLock().lock();
        try {
            candidates.addAll(progresses.keySet());
        } finally {
            lock.readLock().unlock();
        }
        candidates.sort(ProgressKey::compareTo);

        List<ProgressKey> orphans = new ArrayList<>();
        long lockedDbId = -1;
        Database lockedDb = null;
        Locker catalogLocker = new Locker();
        try {
            for (ProgressKey key : candidates) {
                if (orphans.size() >= maxRemovals) {
                    break;
                }
                if (key.getDbId() != lockedDbId) {
                    if (lockedDb != null) {
                        catalogLocker.unLockDatabase(lockedDbId, LockType.READ);
                    }
                    lockedDbId = key.getDbId();
                    lockedDb = state.getLocalMetastore().getDb(lockedDbId);
                    if (lockedDb != null) {
                        catalogLocker.lockDatabase(lockedDbId, LockType.READ);
                    }
                }
                if (isOrphan(lockedDb, key)) {
                    orphans.add(key);
                }
            }
        } finally {
            if (lockedDb != null) {
                catalogLocker.unLockDatabase(lockedDbId, LockType.READ);
            }
        }
        return removeKeys(orphans, false);
    }

    private static boolean isOrphan(Database db, ProgressKey key) {
        if (db == null) {
            return true;
        }
        Table table = db.getTable(key.getTableId());
        if (!(table instanceof OlapTable)) {
            return true;
        }
        OlapTable olapTable = (OlapTable) table;
        return olapTable.getTableProperty() == null ||
                olapTable.getTableProperty().getTenantTtlDictionaryBinding() == null ||
                olapTable.getPhysicalPartition(key.getPhysicalPartitionId()) == null;
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        List<TenantTtlPartitionProgress> snapshot = getAll();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(
                SRMetaBlockID.TENANT_TTL_PARTITION_PROGRESS_MGR, snapshot.size() + 1);
        writer.writeInt(snapshot.size());
        for (TenantTtlPartitionProgress progress : snapshot) {
            writer.writeJson(progress);
        }
        writer.close();
    }

    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        Map<ProgressKey, TenantTtlPartitionProgress> loaded = new HashMap<>();
        reader.readCollection(TenantTtlPartitionProgress.class, progress -> {
            progress.validate();
            TenantTtlPartitionProgress previous = loaded.put(progress.key(), progress);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate Tenant-TTL progress key in image");
            }
        });
        lock.writeLock().lock();
        try {
            progresses.clear();
            for (TenantTtlPartitionProgress progress : loaded.values()) {
                progresses.put(progress.key(), new TenantTtlPartitionProgress(progress));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public enum AdvanceResult {
        ADVANCED,
        UNCHANGED,
        STALE_EXPECTATION,
        REJECTED_REGRESSION
    }

    public static final class CompletedPlanUpdate {
        private final TenantTtlPartitionProgress expected;
        private final TenantTtlPartitionProgress candidate;

        public CompletedPlanUpdate(TenantTtlPartitionProgress expected,
                                   TenantTtlPartitionProgress candidate) {
            this.candidate = new TenantTtlPartitionProgress(
                    Objects.requireNonNull(candidate, "candidate progress is null"));
            if (expected != null && !expected.key().equals(candidate.key())) {
                throw new IllegalArgumentException("expected and candidate progress keys differ");
            }
            this.expected = expected == null ? null : new TenantTtlPartitionProgress(expected);
        }
    }

    interface JournalSink {
        void logUpsert(TenantTtlPartitionProgressBatchLog log);

        void logRemove(TenantTtlPartitionProgressRemoveLog log);
    }

    private interface KeyPredicate {
        boolean test(ProgressKey key);
    }
}
