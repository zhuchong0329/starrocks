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
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TenantTtlDictionaryBinding;
import com.starrocks.catalog.TenantTtlTableBinding;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.tenantttl.TenantTtlPartitionBoundResolver;
import com.starrocks.tenantttl.policy.TenantTtlByteKey;
import com.starrocks.tenantttl.policy.TenantTtlEvaluationContext;
import com.starrocks.tenantttl.policy.TenantTtlPolicyPlanner;
import com.starrocks.tenantttl.policy.TenantTtlPolicySnapshot;
import com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotManager;
import com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotManager.TableRef;
import com.starrocks.tenantttl.scheduler.TenantTtlPartitionProgress.ProgressKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/** Leader-only planner and FE/Catalog execution half of Tenant-TTL scheduling. */
public final class TenantTtlScheduler extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(TenantTtlScheduler.class);
    private static final long MIN_INTERVAL_MS = 1000L;

    private final Map<ProgressKey, PartitionRuntimeStatus> partitionStatuses = new ConcurrentHashMap<>();
    private final Map<TableRef, TableRuntimeStatus> tableStatuses = new ConcurrentHashMap<>();
    private final AtomicBoolean catalogDropRunning = new AtomicBoolean();
    private final TenantTtlRewriteCoordinator rewriteCoordinator;
    private final LongSupplier evaluationTimeEpochSecondsSupplier;
    private volatile RewriteExecutionView rewriteExecutionView;
    private volatile List<PendingRewritePlan> pendingRewritePlans = Collections.emptyList();
    private volatile List<NextExpiry> nextExpiries = Collections.emptyList();
    private volatile boolean leadershipEnabled = true;
    private int tableScanOffset;

    public TenantTtlScheduler() {
        this(new TenantTtlRewriteCoordinator(), () -> System.currentTimeMillis() / 1000L);
    }

    TenantTtlScheduler(TenantTtlRewriteCoordinator rewriteCoordinator) {
        this(rewriteCoordinator, () -> System.currentTimeMillis() / 1000L);
    }

    TenantTtlScheduler(TenantTtlRewriteCoordinator rewriteCoordinator,
                       LongSupplier evaluationTimeEpochSecondsSupplier) {
        super("tenant-ttl-scheduler", configuredIntervalMs());
        this.rewriteCoordinator = Objects.requireNonNull(rewriteCoordinator, "rewrite coordinator is null");
        this.evaluationTimeEpochSecondsSupplier = Objects.requireNonNull(
                evaluationTimeEpochSecondsSupplier, "evaluation clock is null");
        this.rewriteExecutionView = rewriteCoordinator;
    }

    @Override
    protected void runAfterCatalogReady() {
        setInterval(configuredIntervalMs());
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        if (!leadershipEnabled || !state.isLeader()) {
            rewriteCoordinator.resetForLeadershipLoss();
            return;
        }
        scheduleOnce(state);
    }

    public synchronized void scheduleOnce(GlobalStateMgr state) {
        Objects.requireNonNull(state, "global state is null");
        if (!leadershipEnabled || !state.isLeader()) {
            rewriteCoordinator.resetForLeadershipLoss();
            return;
        }
        TenantTtlPolicySnapshotManager snapshotManager = state.getTenantTtlPolicySnapshotManager();
        TenantTtlPartitionProgressManager progressManager = state.getTenantTtlPartitionProgressManager();
        List<TableRef> tableRefs = new ArrayList<>(snapshotManager.getAllReferencedTables());
        tableRefs.sort(Comparator.comparingLong(TableRef::getDbId).thenComparingLong(TableRef::getTableId));
        int tableLimit = Math.max(1, Config.tenant_ttl_scheduler_max_tables_per_cycle);
        if (tableRefs.size() > tableLimit) {
            List<TableRef> selected = new ArrayList<>(tableLimit);
            int start = Math.floorMod(tableScanOffset, tableRefs.size());
            for (int i = 0; i < tableLimit; ++i) {
                selected.add(tableRefs.get((start + i) % tableRefs.size()));
            }
            tableScanOffset = (start + tableLimit) % tableRefs.size();
            tableRefs = selected;
        } else {
            tableScanOffset = 0;
        }

        List<PendingRewritePlan> rewrites = new ArrayList<>();
        List<CatalogDropCandidate> drops = new ArrayList<>();
        PriorityQueue<NextExpiry> expiryQueue = new PriorityQueue<>(NextExpiry.ORDER);
        for (TableRef tableRef : tableRefs) {
            TenantTtlEvaluationContext.CaptureResult capture = TenantTtlEvaluationContext.capture(
                    state, tableRef.getDbId(), tableRef.getTableId(),
                    evaluationTimeEpochSecondsSupplier, state::getNextId);
            if (!capture.isSuccess()) {
                tableStatuses.put(tableRef, TableRuntimeStatus.captureFailure(capture));
                continue;
            }
            TenantTtlEvaluationContext context = capture.getContext();
            tableStatuses.put(tableRef, TableRuntimeStatus.active(context));
            evaluateTable(context, progressManager, rewrites, drops, expiryQueue);
        }

        rewrites.sort(PendingRewritePlan.ORDER);
        pendingRewritePlans = Collections.unmodifiableList(new ArrayList<>(rewrites));
        List<NextExpiry> sortedExpiries = new ArrayList<>(expiryQueue.size());
        while (!expiryQueue.isEmpty()) {
            sortedExpiries.add(expiryQueue.remove());
        }
        nextExpiries = Collections.unmodifiableList(sortedExpiries);

        drops.sort(CatalogDropCandidate.ORDER);
        boolean coordinatorEnabled = rewriteExecutionView == rewriteCoordinator;
        if (coordinatorEnabled) {
            rewriteCoordinator.reconcile(state, pendingRewritePlans);
        }
        if (!drops.isEmpty()) {
            executeFirstCatalogDrop(state, drops.get(0));
        } else if (coordinatorEnabled) {
            rewriteCoordinator.dispatchNext(state);
        }
        if (coordinatorEnabled) {
            applyCoordinatorStatuses(rewriteCoordinator.getExecutionStatuses());
        }
    }

    private void applyCoordinatorStatuses(List<TenantTtlRewriteCoordinator.ExecutionStatus> statuses) {
        for (TenantTtlRewriteCoordinator.ExecutionStatus status : statuses) {
            PartitionRuntimeStatus previous = partitionStatuses.get(status.getKey());
            PartitionState state;
            switch (status.getState()) {
                case RUNNING:
                    state = PartitionState.REWRITE_RUNNING;
                    break;
                case WAITING_REPLICA:
                    state = PartitionState.WAITING_REPLICA;
                    break;
                case RETRY_BACKOFF:
                case UNKNOWN_RETRY:
                    state = PartitionState.RETRY_PENDING;
                    break;
                case BLOCKED:
                    state = PartitionState.BLOCKED;
                    break;
                case REPLAN_REQUIRED:
                    state = PartitionState.REPLAN_REQUIRED;
                    break;
                default:
                    state = PartitionState.WAITING_REWRITE;
                    break;
            }
            partitionStatuses.put(status.getKey(), new PartitionRuntimeStatus(state,
                    previous == null ? null : previous.getTriggerReason(),
                    previous == null ? 0 : previous.getSnapshotTxnId(),
                    previous == null ? 0 : previous.getEvaluationTimeEpochSeconds(),
                    previous == null ? 0 : previous.getNextExpiryEpochSeconds(),
                    status.getDetail(), System.currentTimeMillis()));
        }
    }

    private void evaluateTable(TenantTtlEvaluationContext context,
                               TenantTtlPartitionProgressManager progressManager,
                               List<PendingRewritePlan> rewrites,
                               List<CatalogDropCandidate> drops,
                               PriorityQueue<NextExpiry> expiryQueue) {
        Map<Long, List<TenantTtlEvaluationContext.PartitionPlan>> plansByLogicalPartition =
                new LinkedHashMap<>();
        Map<Long, TenantTtlScheduleDecision.Decision> decisions = new HashMap<>();
        for (TenantTtlEvaluationContext.PartitionPlan plan : context.getPartitionPlans()) {
            plansByLogicalPartition.computeIfAbsent(plan.getLogicalPartitionId(), ignored -> new ArrayList<>())
                    .add(plan);
            ProgressKey key = key(context, plan);
            TenantTtlPartitionProgress progress = progressManager.get(key).orElse(null);
            TenantTtlScheduleDecision.Decision decision = TenantTtlScheduleDecision.decide(
                    context, plan, progress, rewriteExecutionView.isRetryPending(key));
            decisions.put(plan.getPhysicalPartitionId(), decision);
            if (plan.getPolicyPlan() != null && plan.getPolicyPlan().getNextExpiryEpochSeconds() > 0) {
                expiryQueue.add(new NextExpiry(key, plan.getPolicyPlan().getNextExpiryEpochSeconds()));
            }
        }

        for (List<TenantTtlEvaluationContext.PartitionPlan> logicalPlans : plansByLogicalPartition.values()) {
            logicalPlans.sort(Comparator.comparingLong(TenantTtlEvaluationContext.PartitionPlan::getPhysicalPartitionId));
            boolean allDrop = !logicalPlans.isEmpty() && logicalPlans.stream().allMatch(
                    plan -> plan.getType() == TenantTtlPolicyPlanner.PlanType.DROP_LOGICAL_PARTITION);
            boolean anyDropDue = allDrop && logicalPlans.stream().anyMatch(
                    plan -> decisions.get(plan.getPhysicalPartitionId()).shouldEvaluate());
            if (allDrop && anyDropDue) {
                CatalogDropCandidate candidate = new CatalogDropCandidate(context, logicalPlans);
                drops.add(candidate);
                for (TenantTtlEvaluationContext.PartitionPlan plan : logicalPlans) {
                    updateStatus(context, plan, PartitionState.WAITING_CATALOG_DROP,
                            decisions.get(plan.getPhysicalPartitionId()), "");
                }
                continue;
            }

            for (TenantTtlEvaluationContext.PartitionPlan plan : logicalPlans) {
                TenantTtlScheduleDecision.Decision decision = decisions.get(plan.getPhysicalPartitionId());
                switch (plan.getType()) {
                    case FAIL_CLOSED:
                        updateStatus(context, plan, PartitionState.FAIL_CLOSED, decision,
                                plan.getFailureDetail());
                        break;
                    case FE_NOOP:
                        if (decision.shouldEvaluate()) {
                            completeFeNoop(context, plan, progressManager, decision);
                        } else {
                            updateStatus(context, plan, PartitionState.IDLE, decision, "");
                        }
                        break;
                    case ROWSET_REWRITE:
                        if (decision.shouldEvaluate()) {
                            rewrites.add(new PendingRewritePlan(context, plan, decision));
                            updateStatus(context, plan, PartitionState.WAITING_REWRITE, decision, "");
                        } else {
                            updateStatus(context, plan, PartitionState.IDLE, decision, "");
                        }
                        break;
                    case DROP_LOGICAL_PARTITION:
                        updateStatus(context, plan, PartitionState.WAITING_LOGICAL_PARTITION, decision,
                                "not all Physical Partitions in the logical partition are droppable");
                        break;
                    default:
                        throw new IllegalStateException("unknown Tenant-TTL plan type: " + plan.getType());
                }
            }
        }
    }

    private void completeFeNoop(TenantTtlEvaluationContext context,
                                TenantTtlEvaluationContext.PartitionPlan plan,
                                TenantTtlPartitionProgressManager progressManager,
                                TenantTtlScheduleDecision.Decision decision) {
        ProgressKey key = key(context, plan);
        TenantTtlPartitionProgress expected = progressManager.get(key).orElse(null);
        long processedThrough = expected == null ? 0 : expected.getProcessedThroughVersion();
        long completedCursor = plan.getPolicyPlan().getCompletedExpiryCursorEpochSeconds();
        if (expected != null && expected.hasSamePlanIdentity(new TenantTtlPartitionProgress(
                context.getDbId(), context.getTableId(), plan.getPhysicalPartitionId(),
                context.getTableBindingFingerprint(), plan.getPolicyPlan().getTablePolicyFingerprint(),
                plan.getBoundaryFingerprint(), completedCursor, processedThrough,
                context.getSnapshotTxnId(), context.getEvaluationTimeEpochSeconds()))) {
            completedCursor = Math.max(completedCursor, expected.getCompletedExpiryCursorEpochSeconds());
        }
        TenantTtlPartitionProgress candidate = new TenantTtlPartitionProgress(
                context.getDbId(), context.getTableId(), plan.getPhysicalPartitionId(),
                context.getTableBindingFingerprint(), plan.getPolicyPlan().getTablePolicyFingerprint(),
                plan.getBoundaryFingerprint(), completedCursor, processedThrough,
                context.getSnapshotTxnId(), context.getEvaluationTimeEpochSeconds());
        TenantTtlPartitionProgressManager.AdvanceResult result =
                progressManager.compareAndAdvanceCompletedPlan(expected, candidate);
        if (result == TenantTtlPartitionProgressManager.AdvanceResult.ADVANCED ||
                result == TenantTtlPartitionProgressManager.AdvanceResult.UNCHANGED) {
            updateStatus(context, plan, PartitionState.FE_NOOP, decision, "");
        } else {
            updateStatus(context, plan, PartitionState.RETRY_PENDING, decision,
                    "FE NOOP progress publication lost a concurrent metadata race: " + result);
        }
    }

    private void executeFirstCatalogDrop(GlobalStateMgr state, CatalogDropCandidate candidate) {
        if (rewriteExecutionView.hasDestructiveExecutionInFlight() ||
                rewriteExecutionView.hasRewriteInFlight(candidate.context.getDbId(), candidate.context.getTableId(),
                        candidate.logicalPartitionId) || !catalogDropRunning.compareAndSet(false, true)) {
            markDrop(candidate, PartitionState.DROP_SKIPPED_REWRITE_IN_FLIGHT,
                    "a Tenant-TTL destructive execution is already in flight");
            return;
        }
        try {
            CatalogDropResult result = dropLogicalPartition(state, candidate);
            if (result.success) {
                for (TenantTtlEvaluationContext.PartitionPlan plan : candidate.physicalPlans) {
                    partitionStatuses.remove(key(candidate.context, plan));
                }
            } else {
                markDrop(candidate, result.retryable ? PartitionState.RETRY_PENDING : PartitionState.FAIL_CLOSED,
                        result.detail);
            }
        } finally {
            catalogDropRunning.set(false);
        }
    }

    private CatalogDropResult dropLogicalPartition(GlobalStateMgr state, CatalogDropCandidate candidate) {
        Database db = state.getLocalMetastore().getDb(candidate.context.getDbId());
        if (db == null) {
            return CatalogDropResult.retry("database disappeared before Tenant-TTL Catalog drop");
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            Table current = db.getTable(candidate.context.getTableId());
            if (!(current instanceof OlapTable)) {
                return CatalogDropResult.retry("table disappeared before Tenant-TTL Catalog drop");
            }
            OlapTable table = (OlapTable) current;
            if (table.getState() != OlapTable.OlapTableState.NORMAL) {
                return CatalogDropResult.retry("table is not in NORMAL state");
            }
            if (!candidate.context.validateTableBindingLocked(db, table)) {
                return CatalogDropResult.retry("table binding or schema changed before Catalog drop");
            }
            Partition partition = table.getPartition(candidate.logicalPartitionId);
            if (partition == null || table.isTempPartition(partition.getId()) ||
                    !candidate.logicalPartitionName.equals(partition.getName()) ||
                    table.getPartition(candidate.logicalPartitionName) != partition) {
                return CatalogDropResult.retry("logical partition identity changed before Catalog drop");
            }
            List<Long> currentPhysicalIds = new ArrayList<>();
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                currentPhysicalIds.add(physicalPartition.getId());
            }
            currentPhysicalIds.sort(Long::compare);
            if (!currentPhysicalIds.equals(candidate.physicalPartitionIds)) {
                return CatalogDropResult.retry("Physical Partition membership changed before Catalog drop");
            }
            if (rewriteExecutionView.hasRewriteInFlight(candidate.context.getDbId(), candidate.context.getTableId(),
                    candidate.logicalPartitionId)) {
                return CatalogDropResult.retry("a Rowset Rewrite became in flight before Catalog drop");
            }
            CatalogDropResult validation = validateDropPoliciesLocked(state, db, table, candidate);
            if (!validation.success) {
                return validation;
            }
            DropPartitionClause dropClause =
                    new DropPartitionClause(false, candidate.logicalPartitionName, false, true);
            // LocalMetastore consumes the analyzer-resolved names. Tenant-TTL constructs the clause
            // internally, so resolve the already revalidated logical partition explicitly.
            dropClause.setResolvedPartitionNames(Collections.singletonList(candidate.logicalPartitionName));
            state.getLocalMetastore().dropPartition(db, table, dropClause);
            LOG.info("Tenant-TTL dropped logical partition. dbId={}, tableId={}, partitionId={}, partitionName={}",
                    db.getId(), table.getId(), partition.getId(), partition.getName());
            return CatalogDropResult.success();
        } catch (DdlException | RuntimeException e) {
            LOG.warn("Tenant-TTL Catalog drop failed. dbId={}, tableId={}, partitionId={}",
                    candidate.context.getDbId(), candidate.context.getTableId(), candidate.logicalPartitionId, e);
            return CatalogDropResult.retry(rootMessage(e));
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    private CatalogDropResult validateDropPoliciesLocked(GlobalStateMgr state, Database db, OlapTable table,
                                                           CatalogDropCandidate candidate) {
        TableProperty property = table.getTableProperty();
        TenantTtlDictionaryBinding dictionaryBinding = property == null ? null :
                property.getTenantTtlDictionaryBinding();
        TenantTtlTableBinding tableBinding = property == null ? null : property.getTenantTtlTableBinding();
        if (dictionaryBinding == null || tableBinding == null) {
            return CatalogDropResult.retry("Tenant-TTL binding disappeared before Catalog drop");
        }
        Optional<TenantTtlPolicySnapshot> currentSnapshot = state.getTenantTtlPolicySnapshotManager()
                .getCurrentSnapshot(dictionaryBinding.getDictionaryId());
        if (!currentSnapshot.isPresent()) {
            return CatalogDropResult.retry("current Tenant-TTL policy snapshot is unavailable");
        }
        TenantTtlPolicyPlanner planner = TenantTtlPolicyPlanner.fromConfig();
        for (TenantTtlEvaluationContext.PartitionPlan oldPlan : candidate.physicalPlans) {
            PhysicalPartition physicalPartition = table.getPhysicalPartition(oldPlan.getPhysicalPartitionId());
            if (physicalPartition == null || physicalPartition.getParentId() != candidate.logicalPartitionId) {
                return CatalogDropResult.retry("Physical Partition identity changed before Catalog drop");
            }
            TenantTtlPartitionBoundResolver.Resolution boundary =
                    TenantTtlPartitionBoundResolver.resolve(table, physicalPartition, tableBinding);
            String boundaryFingerprint = TenantTtlEvaluationContext.partitionBoundaryFingerprint(
                    candidate.logicalPartitionId, physicalPartition.getId(), boundary);
            if (!boundary.isProvable() || !oldPlan.getBoundaryFingerprint().equals(boundaryFingerprint)) {
                return CatalogDropResult.retry("Physical Partition boundary changed before Catalog drop");
            }
            TenantTtlPolicyPlanner.Plan currentPlan = planner.plan(currentSnapshot.get(),
                    TenantTtlByteKey.utf8(dictionaryBinding.getTableKey()), dictionaryBinding.getDefaultDays(),
                    boundary.getPartitionUpperEpochSecond(), candidate.context.getEvaluationTimeEpochSeconds());
            if (currentPlan.getType() != TenantTtlPolicyPlanner.PlanType.DROP_LOGICAL_PARTITION ||
                    oldPlan.getPolicyPlan() == null ||
                    !oldPlan.getPolicyPlan().getTablePolicyFingerprint()
                            .equals(currentPlan.getTablePolicyFingerprint())) {
                return CatalogDropResult.retry("Tenant-TTL policy changed before Catalog drop");
            }
        }
        return CatalogDropResult.success();
    }

    private void markDrop(CatalogDropCandidate candidate, PartitionState state, String detail) {
        for (TenantTtlEvaluationContext.PartitionPlan plan : candidate.physicalPlans) {
            updateStatus(candidate.context, plan, state,
                    TenantTtlScheduleDecision.decide(candidate.context, plan, null, true), detail);
        }
    }

    private void updateStatus(TenantTtlEvaluationContext context,
                              TenantTtlEvaluationContext.PartitionPlan plan,
                              PartitionState state,
                              TenantTtlScheduleDecision.Decision decision,
                              String detail) {
        long nextExpiry = plan.getPolicyPlan() == null ? 0 : plan.getPolicyPlan().getNextExpiryEpochSeconds();
        partitionStatuses.put(key(context, plan), new PartitionRuntimeStatus(state,
                decision == null ? null : decision.getPrimaryReason(), context.getSnapshotTxnId(),
                context.getEvaluationTimeEpochSeconds(), nextExpiry, detail, System.currentTimeMillis()));
    }

    private static ProgressKey key(TenantTtlEvaluationContext context,
                                   TenantTtlEvaluationContext.PartitionPlan plan) {
        return new ProgressKey(context.getDbId(), context.getTableId(), plan.getPhysicalPartitionId());
    }

    private static long configuredIntervalMs() {
        return Math.max(MIN_INTERVAL_MS,
                Math.multiplyExact((long) Math.max(1, Config.tenant_ttl_scheduler_interval_seconds), 1000L));
    }

    private static String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        String message = current.getMessage();
        return message == null || message.isEmpty() ? current.getClass().getSimpleName() : message;
    }

    public List<PendingRewritePlan> getPendingRewritePlans() {
        return pendingRewritePlans;
    }

    public List<NextExpiry> getNextExpiries() {
        return nextExpiries;
    }

    public Optional<PartitionRuntimeStatus> getPartitionStatus(ProgressKey key) {
        return Optional.ofNullable(partitionStatuses.get(key));
    }

    /** Returns a stable copy for bounded SHOW aggregation; tenant filter payloads are never exposed. */
    public List<PartitionRuntimeStatus> getPartitionStatuses(long dbId, long tableId) {
        List<Map.Entry<ProgressKey, PartitionRuntimeStatus>> entries = new ArrayList<>();
        for (Map.Entry<ProgressKey, PartitionRuntimeStatus> entry : partitionStatuses.entrySet()) {
            if (entry.getKey().getDbId() == dbId && entry.getKey().getTableId() == tableId) {
                entries.add(entry);
            }
        }
        entries.sort(Map.Entry.comparingByKey());
        List<PartitionRuntimeStatus> result = new ArrayList<>(entries.size());
        for (Map.Entry<ProgressKey, PartitionRuntimeStatus> entry : entries) {
            result.add(entry.getValue());
        }
        return Collections.unmodifiableList(result);
    }

    public Optional<TableRuntimeStatus> getTableStatus(long dbId, long tableId) {
        return Optional.ofNullable(tableStatuses.get(new TableRef(dbId, tableId)));
    }

    public void setRewriteExecutionView(RewriteExecutionView rewriteExecutionView) {
        this.rewriteExecutionView = rewriteExecutionView == null ? RewriteExecutionView.NONE : rewriteExecutionView;
    }

    public TenantTtlRewriteCoordinator getRewriteCoordinator() {
        return rewriteCoordinator;
    }

    /** Called synchronously from the FE role transition before this former Leader serves as a Follower. */
    public synchronized void onLeadershipLost() {
        leadershipEnabled = false;
        rewriteCoordinator.resetForLeadershipLoss();
    }

    public synchronized void onLeadershipGained() {
        rewriteCoordinator.resetForLeadershipLoss();
        leadershipEnabled = true;
    }

    public interface RewriteExecutionView {
        RewriteExecutionView NONE = new RewriteExecutionView() {
        };

        default boolean isRetryPending(ProgressKey key) {
            return false;
        }

        default boolean hasRewriteInFlight(long dbId, long tableId, long logicalPartitionId) {
            return false;
        }

        default boolean hasDestructiveExecutionInFlight() {
            return false;
        }
    }

    public enum PartitionState {
        IDLE,
        FE_NOOP,
        WAITING_REWRITE,
        REWRITE_RUNNING,
        WAITING_REPLICA,
        WAITING_LOGICAL_PARTITION,
        WAITING_CATALOG_DROP,
        DROP_SKIPPED_REWRITE_IN_FLIGHT,
        RETRY_PENDING,
        REPLAN_REQUIRED,
        BLOCKED,
        FAIL_CLOSED
    }

    public static final class PartitionRuntimeStatus {
        private final PartitionState state;
        private final TenantTtlScheduleDecision.TriggerReason triggerReason;
        private final long snapshotTxnId;
        private final long evaluationTimeEpochSeconds;
        private final long nextExpiryEpochSeconds;
        private final String detail;
        private final long updateTimeMillis;

        private PartitionRuntimeStatus(PartitionState state,
                                       TenantTtlScheduleDecision.TriggerReason triggerReason,
                                       long snapshotTxnId, long evaluationTimeEpochSeconds,
                                       long nextExpiryEpochSeconds, String detail, long updateTimeMillis) {
            this.state = state;
            this.triggerReason = triggerReason;
            this.snapshotTxnId = snapshotTxnId;
            this.evaluationTimeEpochSeconds = evaluationTimeEpochSeconds;
            this.nextExpiryEpochSeconds = nextExpiryEpochSeconds;
            this.detail = detail == null ? "" : detail;
            this.updateTimeMillis = updateTimeMillis;
        }

        public PartitionState getState() {
            return state;
        }

        public TenantTtlScheduleDecision.TriggerReason getTriggerReason() {
            return triggerReason;
        }

        public long getSnapshotTxnId() {
            return snapshotTxnId;
        }

        public long getEvaluationTimeEpochSeconds() {
            return evaluationTimeEpochSeconds;
        }

        public long getNextExpiryEpochSeconds() {
            return nextExpiryEpochSeconds;
        }

        public String getDetail() {
            return detail;
        }

        public long getUpdateTimeMillis() {
            return updateTimeMillis;
        }
    }

    public static final class TableRuntimeStatus {
        private final TenantTtlEvaluationContext.CaptureFailure captureFailure;
        private final String detail;
        private final long snapshotTxnId;
        private final long evaluationTimeEpochSeconds;
        private final long updateTimeMillis;

        private TableRuntimeStatus(TenantTtlEvaluationContext.CaptureFailure captureFailure, String detail,
                                   long snapshotTxnId, long evaluationTimeEpochSeconds, long updateTimeMillis) {
            this.captureFailure = captureFailure;
            this.detail = detail == null ? "" : detail;
            this.snapshotTxnId = snapshotTxnId;
            this.evaluationTimeEpochSeconds = evaluationTimeEpochSeconds;
            this.updateTimeMillis = updateTimeMillis;
        }

        private static TableRuntimeStatus captureFailure(TenantTtlEvaluationContext.CaptureResult capture) {
            return new TableRuntimeStatus(capture.getFailure(), capture.getDetail(), 0, 0,
                    System.currentTimeMillis());
        }

        private static TableRuntimeStatus active(TenantTtlEvaluationContext context) {
            return new TableRuntimeStatus(null, "", context.getSnapshotTxnId(),
                    context.getEvaluationTimeEpochSeconds(), System.currentTimeMillis());
        }

        public boolean isActive() {
            return captureFailure == null;
        }

        public TenantTtlEvaluationContext.CaptureFailure getCaptureFailure() {
            return captureFailure;
        }

        public String getDetail() {
            return detail;
        }

        public long getSnapshotTxnId() {
            return snapshotTxnId;
        }

        public long getEvaluationTimeEpochSeconds() {
            return evaluationTimeEpochSeconds;
        }

        public long getUpdateTimeMillis() {
            return updateTimeMillis;
        }
    }

    public static final class PendingRewritePlan {
        private static final Comparator<PendingRewritePlan> ORDER = Comparator
                .comparingLong((PendingRewritePlan plan) -> plan.context.getDbId())
                .thenComparingLong(plan -> plan.context.getTableId())
                .thenComparingLong(plan -> plan.partitionPlan.getPhysicalPartitionId());

        private final TenantTtlEvaluationContext context;
        private final TenantTtlEvaluationContext.PartitionPlan partitionPlan;
        private final TenantTtlScheduleDecision.Decision decision;

        public PendingRewritePlan(TenantTtlEvaluationContext context,
                                  TenantTtlEvaluationContext.PartitionPlan partitionPlan,
                                  TenantTtlScheduleDecision.Decision decision) {
            this.context = context;
            this.partitionPlan = partitionPlan;
            this.decision = decision;
        }

        public TenantTtlEvaluationContext getContext() {
            return context;
        }

        public TenantTtlEvaluationContext.PartitionPlan getPartitionPlan() {
            return partitionPlan;
        }

        public TenantTtlScheduleDecision.Decision getDecision() {
            return decision;
        }
    }

    public static final class NextExpiry {
        private static final Comparator<NextExpiry> ORDER = Comparator
                .comparingLong(NextExpiry::getExpiryEpochSeconds)
                .thenComparing(next -> next.key);

        private final ProgressKey key;
        private final long expiryEpochSeconds;

        private NextExpiry(ProgressKey key, long expiryEpochSeconds) {
            this.key = key;
            this.expiryEpochSeconds = expiryEpochSeconds;
        }

        public ProgressKey getKey() {
            return key;
        }

        public long getExpiryEpochSeconds() {
            return expiryEpochSeconds;
        }
    }

    private static final class CatalogDropCandidate {
        private static final Comparator<CatalogDropCandidate> ORDER = Comparator
                .comparingLong((CatalogDropCandidate candidate) -> candidate.context.getDbId())
                .thenComparingLong(candidate -> candidate.context.getTableId())
                .thenComparingLong(candidate -> candidate.logicalPartitionId);

        private final TenantTtlEvaluationContext context;
        private final long logicalPartitionId;
        private final String logicalPartitionName;
        private final List<TenantTtlEvaluationContext.PartitionPlan> physicalPlans;
        private final List<Long> physicalPartitionIds;

        private CatalogDropCandidate(TenantTtlEvaluationContext context,
                                     List<TenantTtlEvaluationContext.PartitionPlan> physicalPlans) {
            this.context = context;
            this.logicalPartitionId = physicalPlans.get(0).getLogicalPartitionId();
            this.logicalPartitionName = physicalPlans.get(0).getLogicalPartitionName();
            this.physicalPlans = Collections.unmodifiableList(new ArrayList<>(physicalPlans));
            List<Long> physicalIds = new ArrayList<>();
            for (TenantTtlEvaluationContext.PartitionPlan plan : physicalPlans) {
                if (plan.getLogicalPartitionId() != logicalPartitionId ||
                        !logicalPartitionName.equals(plan.getLogicalPartitionName())) {
                    throw new IllegalArgumentException("Catalog drop candidate spans logical partitions");
                }
                physicalIds.add(plan.getPhysicalPartitionId());
            }
            physicalIds.sort(Long::compare);
            this.physicalPartitionIds = Collections.unmodifiableList(physicalIds);
        }
    }

    private static final class CatalogDropResult {
        private final boolean success;
        private final boolean retryable;
        private final String detail;

        private CatalogDropResult(boolean success, boolean retryable, String detail) {
            this.success = success;
            this.retryable = retryable;
            this.detail = detail;
        }

        private static CatalogDropResult success() {
            return new CatalogDropResult(true, false, "");
        }

        private static CatalogDropResult retry(String detail) {
            return new CatalogDropResult(false, true, detail);
        }
    }
}
