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
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TenantTtlDictionaryBinding;
import com.starrocks.catalog.TenantTtlTableBinding;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.TenantTtlCompactionTask;
import com.starrocks.tenantttl.TenantTtlPartitionBoundResolver;
import com.starrocks.tenantttl.policy.TenantTtlByteKey;
import com.starrocks.tenantttl.policy.TenantTtlEvaluationContext;
import com.starrocks.tenantttl.policy.TenantTtlEvaluationContext.PartitionPlan;
import com.starrocks.tenantttl.policy.TenantTtlEvaluationContext.ReplicaTaskSpec;
import com.starrocks.tenantttl.policy.TenantTtlEvaluationContext.ReplicaTopologyEntry;
import com.starrocks.tenantttl.policy.TenantTtlPolicyPlanner;
import com.starrocks.tenantttl.policy.TenantTtlPolicySnapshot;
import com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotManager.TableRef;
import com.starrocks.tenantttl.scheduler.TenantTtlPartitionProgress.ProgressKey;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TTenantTtlCompactionResult;
import com.starrocks.thrift.TTenantTtlTaskCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** Sequential, round-local execution. Only completed progress and lightweight blocking facts survive a round. */
public final class TenantTtlRewriteCoordinator {
    private static final Logger LOG = LogManager.getLogger(TenantTtlRewriteCoordinator.class);
    private final Object lifecycleLock = new Object();
    private final Map<ProgressKey, BlockedPlan> blockedPlans = new ConcurrentHashMap<>();
    private final TaskSubmitter taskSubmitter;
    private final LongSupplier monotonicMillis;
    private final TenantTtlExecutionBudget.Waiter waiter;
    private volatile long generation;
    private volatile TenantTtlCompactionTask activeTask;
    private volatile ExecutionStatus activeStatus;

    public TenantTtlRewriteCoordinator() {
        this(new AgentTaskSubmitter(), () -> System.nanoTime() / 1_000_000L, Thread::sleep);
    }

    public TenantTtlRewriteCoordinator(TaskSubmitter taskSubmitter, LongSupplier monotonicMillis,
                                      Waiter waiter) {
        this.taskSubmitter = Objects.requireNonNull(taskSubmitter, "task submitter is null");
        this.monotonicMillis = Objects.requireNonNull(monotonicMillis, "monotonic clock is null");
        Objects.requireNonNull(waiter, "waiter is null");
        this.waiter = waiter::sleep;
    }

    /** Caller owns the one scheduler worker; no Catalog or lifecycle lock is held during waits. */
    public ExecutionStatus executePartition(GlobalStateMgr state, TenantTtlScheduler.PendingRewritePlan pending,
                                            BooleanSupplier leader, Consumer<ExecutionStatus> observer) {
        long epoch = generation;
        BooleanSupplier current = () -> generation == epoch && state.isLeader() && leader.getAsBoolean();
        ProgressKey key = key(pending);
        PartitionPlan plan = pending.getPartitionPlan();
        TenantTtlPartitionProgress expected = state.getTenantTtlPartitionProgressManager().get(key).orElse(null);
        BlockedPlan blocked = blockedPlans.get(key);
        if (blocked != null && blocked.stillApplies(state, pending)) {
            ExecutionStatus status = report(pending, null, ExecutionState.BLOCKED, 0,
                    blocked.code, blocked.detail, observer);
            activeStatus = null;
            return status;
        }
        blockedPlans.remove(key);
        int successful = 0;
        long processedThrough = Long.MAX_VALUE;
        ExecutionStatus failure = null;
        try {
            for (ReplicaTaskSpec spec : plan.getReplicaTasks()) {
                if (!current.getAsBoolean()) {
                    return report(pending, spec, ExecutionState.REPLAN_REQUIRED, successful, null,
                            "leadership changed", observer);
                }
                ReplicaResult result = executeReplica(state, pending, spec, current, successful, observer);
                if (result.state == ExecutionState.COMPLETED) {
                    successful++;
                    processedThrough = Math.min(processedThrough, result.processedThrough);
                } else {
                    failure = report(pending, spec, result.state, successful, result.code, result.detail, observer);
                    if (result.state == ExecutionState.BLOCKED) {
                        synchronized (lifecycleLock) {
                            if (current.getAsBoolean()) {
                                blockedPlans.put(key, new BlockedPlan(state, pending, spec, result.code, result.detail));
                            }
                        }
                        return failure;
                    }
                    if (result.state == ExecutionState.REPLAN_REQUIRED) {
                        return failure;
                    }
                }
            }
            if (failure != null) {
                return report(pending, null, failure.state, successful, failure.lastResultCode, failure.detail, observer);
            }
            if (successful == 0 || !publishCompleted(state, pending, expected, processedThrough, current)) {
                return report(pending, null, ExecutionState.REPLAN_REQUIRED, successful, null,
                        "completed plan changed before progress publication", observer);
            }
            return report(pending, null, ExecutionState.COMPLETED, successful, null, "", observer);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return report(pending, null, ExecutionState.REPLAN_REQUIRED, successful, null,
                    "scheduler interrupted", observer);
        } catch (RuntimeException e) {
            LOG.warn("Tenant-TTL partition execution failed. key={}", key, e);
            return report(pending, null, ExecutionState.FAILED, successful, null, rootMessage(e), observer);
        } finally {
            activeStatus = null;
        }
    }

    private ReplicaResult executeReplica(GlobalStateMgr state, TenantTtlScheduler.PendingRewritePlan pending,
                                         ReplicaTaskSpec spec, BooleanSupplier current, int successful,
                                         Consumer<ExecutionStatus> observer) throws InterruptedException {
        TenantTtlExecutionBudget budget = new TenantTtlExecutionBudget(Config.tenant_ttl_agent_task_max_attempts,
                Config.tenant_ttl_agent_task_soft_timeout_seconds, monotonicMillis, waiter);
        String detail = "";
        TTenantTtlTaskCode lastCode = null;
        while (current.getAsBoolean() && budget.tryStartAttempt()) {
            Validation validation = validateCurrentPlanAndTarget(state, pending, spec, true);
            if (validation.type == ValidationType.REPLAN) {
                return ReplicaResult.failure(ExecutionState.REPLAN_REQUIRED, null, validation.detail);
            }
            if (validation.type == ValidationType.WAITING_REPLICA) {
                detail = validation.detail;
                report(pending, spec, ExecutionState.WAITING_REPLICA, successful, null, detail, observer);
            } else {
                TenantTtlCompactionTask task = new TenantTtlCompactionTask(spec);
                try {
                    synchronized (lifecycleLock) {
                        if (!current.getAsBoolean() || budget.remainingMillis() == 0) {
                            break;
                        }
                        activeTask = task;
                        report(pending, spec, ExecutionState.RUNNING, successful, null, "", observer);
                        try {
                            taskSubmitter.submit(task);
                        } catch (RuntimeException e) {
                            // Delivery may have reached BE. Do not turn this into a second business attempt.
                            LOG.warn("Tenant-TTL submission outcome unknown. taskId={}", spec.getTaskId(), e);
                            detail = "submission outcome unknown: " + rootMessage(e);
                        }
                    }
                    while (current.getAsBoolean() && budget.remainingMillis() > 0) {
                        Optional<TTenantTtlCompactionResult> result = task.getResult();
                        if (result.isPresent()) {
                            TTenantTtlCompactionResult value = result.get();
                            lastCode = value.getCode();
                            detail = detail(value);
                            switch (classifyResult(lastCode)) {
                                case SUCCESS:
                                    return new ReplicaResult(ExecutionState.COMPLETED, lastCode, "",
                                            value.getProcessed_through_version());
                                case REPLAN:
                                    return ReplicaResult.failure(ExecutionState.REPLAN_REQUIRED, lastCode, detail);
                                case BLOCKED:
                                    return ReplicaResult.failure(ExecutionState.BLOCKED, lastCode, detail);
                                case DEFINITE_RETRY:
                                    break;
                                case WAIT_FOR_RESULT:
                                    // Defensive: the task adapter normally keeps this out of terminal results.
                                    budget.await(TenantTtlExecutionBudget.POLL_MILLIS, current);
                                    continue;
                                default:
                                    throw new IllegalStateException("unclassified Tenant-TTL result " + lastCode);
                            }
                            break;
                        }
                        budget.await(TenantTtlExecutionBudget.POLL_MILLIS, current);
                    }
                } finally {
                    task.close();
                    synchronized (lifecycleLock) {
                        removeQueuedTask(task);
                        if (activeTask == task) {
                            activeTask = null;
                        }
                    }
                }
            }
            if (!current.getAsBoolean() || budget.remainingMillis() == 0 || !budget.hasAttemptsRemaining()) {
                break;
            }
            report(pending, spec, ExecutionState.RETRY_BACKOFF, successful, lastCode, detail, observer);
            budget.await(TenantTtlExecutionBudget.RETRY_MILLIS, current);
        }
        if (!current.getAsBoolean()) {
            return ReplicaResult.failure(ExecutionState.REPLAN_REQUIRED, lastCode, "leadership changed");
        }
        ExecutionState outcome = budget.remainingMillis() == 0 ? ExecutionState.TIMED_OUT :
                ExecutionState.ATTEMPTS_EXHAUSTED;
        return ReplicaResult.failure(outcome, lastCode, outcome + " after " + budget.getAttempts() +
                " attempt(s)" + (detail.isEmpty() ? "" : ": " + detail));
    }

    /** Catalog -> lifecycle -> progress is the lock order, shared with DROP and FE NOOP publication. */
    private boolean publishCompleted(GlobalStateMgr state, TenantTtlScheduler.PendingRewritePlan pending,
                                     TenantTtlPartitionProgress expected, long processedThrough,
                                     BooleanSupplier current) {
        TenantTtlEvaluationContext context = pending.getContext();
        PartitionPlan plan = pending.getPartitionPlan();
        Database db = state.getLocalMetastore().getDb(context.getDbId());
        if (db == null) {
            return false;
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            if (validateCurrentPlanAndTarget(state, pending, null, false).type != ValidationType.CURRENT) {
                return false;
            }
            TenantTtlPartitionProgress candidate = new TenantTtlPartitionProgress(context.getDbId(),
                    context.getTableId(), plan.getPhysicalPartitionId(), context.getTableBindingFingerprint(),
                    plan.getPolicyPlan().getTablePolicyFingerprint(), plan.getBoundaryFingerprint(),
                    plan.getPolicyPlan().getCompletedExpiryCursorEpochSeconds(), processedThrough,
                    context.getSnapshotTxnId(), context.getEvaluationTimeEpochSeconds());
            synchronized (lifecycleLock) {
                if (!current.getAsBoolean()) {
                    return false;
                }
                TenantTtlPartitionProgressManager.AdvanceResult result = state.getTenantTtlPartitionProgressManager()
                        .compareAndAdvanceCompletedPlan(expected, candidate);
                return result == TenantTtlPartitionProgressManager.AdvanceResult.ADVANCED ||
                        result == TenantTtlPartitionProgressManager.AdvanceResult.UNCHANGED;
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
    }

    boolean runIfCurrent(GlobalStateMgr state, BooleanSupplier current, Runnable action) {
        synchronized (lifecycleLock) {
            if (!state.isLeader() || !current.getAsBoolean()) {
                return false;
            }
            action.run();
            return true;
        }
    }

    public void resetForLeadershipLoss() {
        synchronized (lifecycleLock) {
            generation++;
            if (activeTask != null) {
                activeTask.close();
                removeQueuedTask(activeTask);
            }
            activeTask = null;
            activeStatus = null;
            blockedPlans.clear();
        }
    }

    /** Cleanup consults the Catalog, never treats an unscanned batch as deleted. */
    public void discardOrphanedBlocks(GlobalStateMgr state, Set<TableRef> tables) {
        for (ProgressKey key : blockedPlans.keySet()) {
            Database db = state.getLocalMetastore().getDb(key.getDbId());
            if (db == null || !tables.contains(new TableRef(key.getDbId(), key.getTableId()))) {
                blockedPlans.remove(key);
                continue;
            }
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                Table table = db.getTable(key.getTableId());
                if (!(table instanceof OlapTable) ||
                        ((OlapTable) table).getPhysicalPartition(key.getPhysicalPartitionId()) == null) {
                    blockedPlans.remove(key);
                }
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
        }
    }

    public void forgetPartition(ProgressKey key) {
        blockedPlans.remove(key);
    }

    public List<ExecutionStatus> getExecutionStatuses() {
        ExecutionStatus status = activeStatus;
        return status == null ? Collections.emptyList() : Collections.singletonList(status);
    }

    public Optional<TenantTtlCompactionTask> getActiveTask() {
        return Optional.ofNullable(activeTask);
    }

    private ExecutionStatus report(TenantTtlScheduler.PendingRewritePlan pending, ReplicaTaskSpec spec,
                                   ExecutionState state, int successful, TTenantTtlTaskCode code, String detail,
                                   Consumer<ExecutionStatus> observer) {
        ExecutionStatus status = new ExecutionStatus(key(pending), state, spec == null ? 0 : spec.getTaskId(),
                successful, pending.getPartitionPlan().getReplicaTasks().size(), code, detail);
        activeStatus = status;
        observer.accept(status);
        return status;
    }

    private static void removeQueuedTask(TenantTtlCompactionTask task) {
        task.removeFromQueue();
    }

    public static ResultDisposition classifyResult(TTenantTtlTaskCode code) {
        switch (Objects.requireNonNull(code, "Tenant-TTL result code is null")) {
            case SUCCESS:
            case NOOP_VERIFIED:
                return ResultDisposition.SUCCESS;
            case TABLET_BUSY:
            case REPLICA_NOT_CAUGHT_UP:
            case STALE_ROWSET:
            case CANCELLED:
                return ResultDisposition.DEFINITE_RETRY;
            case TTL_ALREADY_RUNNING:
                return ResultDisposition.WAIT_FOR_RESULT;
            case SCHEMA_CHANGED:
            case TABLET_NOT_FOUND:
                return ResultDisposition.REPLAN;
            case INVALID_ARGUMENT:
            case NOT_SUPPORTED:
            case DATA_INVARIANT_VIOLATION:
            case INTERNAL_ERROR:
                return ResultDisposition.BLOCKED;
            default:
                throw new IllegalArgumentException("unrecognized Tenant-TTL result code " + code);
        }
    }

    private Validation validateCurrentPlanAndTarget(GlobalStateMgr state, TenantTtlScheduler.PendingRewritePlan pending,
                                                    ReplicaTaskSpec target, boolean requireAvailableReplica) {
        TenantTtlEvaluationContext context = pending.getContext();
        PartitionPlan oldPlan = pending.getPartitionPlan();
        Database db = state.getLocalMetastore().getDb(context.getDbId());
        if (db == null) {
            return Validation.replan("database disappeared");
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            Table current = db.getTable(context.getTableId());
            if (!(current instanceof OlapTable)) {
                return Validation.replan("table disappeared");
            }
            OlapTable table = (OlapTable) current;
            if (table.getState() != OlapTable.OlapTableState.NORMAL ||
                    !context.validateTableBindingLocked(db, table)) {
                return Validation.replan("table state, binding, or Base Index schema changed");
            }
            PhysicalPartition physical = table.getPhysicalPartition(oldPlan.getPhysicalPartitionId());
            Partition logical = table.getPartition(oldPlan.getLogicalPartitionId());
            if (physical == null || logical == null || physical.getParentId() != logical.getId() ||
                    !oldPlan.getLogicalPartitionName().equals(logical.getName())) {
                return Validation.replan("partition identity changed");
            }
            TenantTtlTableBinding tableBinding = table.getTableProperty().getTenantTtlTableBinding();
            TenantTtlPartitionBoundResolver.Resolution boundary =
                    TenantTtlPartitionBoundResolver.resolve(table, physical, tableBinding);
            String boundaryFingerprint = TenantTtlEvaluationContext.partitionBoundaryFingerprint(
                    logical.getId(), physical.getId(), boundary);
            if (!boundary.isProvable() || !oldPlan.getBoundaryFingerprint().equals(boundaryFingerprint)) {
                return Validation.replan("partition boundary changed or is no longer provable");
            }

            TenantTtlDictionaryBinding dictionaryBinding = table.getTableProperty().getTenantTtlDictionaryBinding();
            Optional<TenantTtlPolicySnapshot> snapshot = state.getTenantTtlPolicySnapshotManager()
                    .getCurrentSnapshot(dictionaryBinding.getDictionaryId());
            if (!snapshot.isPresent()) {
                return Validation.replan("current Tenant-TTL policy snapshot is unavailable");
            }
            TenantTtlPolicyPlanner.Plan currentPolicy = TenantTtlPolicyPlanner.fromConfig().plan(snapshot.get(),
                    TenantTtlByteKey.utf8(dictionaryBinding.getTableKey()), dictionaryBinding.getDefaultDays(),
                    boundary.getPartitionUpperEpochSecond(), context.getEvaluationTimeEpochSeconds());
            if (currentPolicy.getType() != TenantTtlPolicyPlanner.PlanType.ROWSET_REWRITE ||
                    !samePolicyPlan(oldPlan.getPolicyPlan(), currentPolicy)) {
                return Validation.replan("effective Tenant-TTL policy changed");
            }

            MaterializedIndex baseIndex = physical.getBaseIndex();
            if (baseIndex == null || baseIndex.getId() != context.getBaseIndexId()) {
                return Validation.replan("Base Index identity changed");
            }
            List<ReplicaTopologyEntry> entries = new ArrayList<>();
            for (com.starrocks.catalog.Tablet tablet : baseIndex.getTablets()) {
                if (!(tablet instanceof LocalTablet)) {
                    return Validation.replan("Base Index is no longer composed of local tablets");
                }
                for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                    entries.add(new ReplicaTopologyEntry(physical.getId(), baseIndex.getId(), tablet.getId(),
                            replica.getId(), replica.getBackendId()));
                }
            }
            if (!oldPlan.getTopologyFingerprint().equals(
                    TenantTtlEvaluationContext.replicaTopologyFingerprint(entries))) {
                return Validation.replan("Replica topology changed");
            }
            if (target != null) {
                com.starrocks.catalog.Tablet tablet = baseIndex.getTablet(target.getTabletId());
                if (!(tablet instanceof LocalTablet)) {
                    return Validation.replan("target Tablet disappeared");
                }
                Replica replica = ((LocalTablet) tablet).getReplicaById(target.getReplicaId());
                if (replica == null || replica.getBackendId() != target.getBackendId()) {
                    return Validation.replan("target Replica identity changed");
                }
                if (requireAvailableReplica && (replica.getState() != Replica.ReplicaState.NORMAL ||
                        replica.isBad() || replica.getLastFailedVersion() > 0 ||
                        replica.getVersion() < target.getObservedMaxVersion())) {
                    return Validation.waiting("target Replica is not healthy or caught up");
                }
            }
        } catch (RuntimeException | com.starrocks.common.DdlException e) {
            return Validation.replan("metadata revalidation failed: " + rootMessage(e));
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        if (target != null && requireAvailableReplica) {
            ComputeNode backend = state.getNodeMgr().getClusterInfo().getBackendOrComputeNode(target.getBackendId());
            if (backend == null || !backend.isAlive() || backend.isDecommissioned()) {
                return Validation.waiting("target backend is unavailable");
            }
        }
        return Validation.current();
    }

    private static boolean samePolicyPlan(TenantTtlPolicyPlanner.Plan left, TenantTtlPolicyPlanner.Plan right) {
        return left != null && right != null && left.getType() == right.getType() &&
                left.getFilterMode() == right.getFilterMode() &&
                left.getTablePolicyFingerprint().equals(right.getTablePolicyFingerprint()) &&
                left.getTenants().equals(right.getTenants());
    }


    private static ProgressKey key(TenantTtlScheduler.PendingRewritePlan pending) {
        return new ProgressKey(pending.getContext().getDbId(), pending.getContext().getTableId(),
                pending.getPartitionPlan().getPhysicalPartitionId());
    }

    private static String detail(TTenantTtlCompactionResult result) {
        return result.getDetail_status().getError_msgs() == null ||
                result.getDetail_status().getError_msgs().isEmpty() ? result.getCode().name() :
                String.join("; ", result.getDetail_status().getError_msgs());
    }

    private static String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }

    @FunctionalInterface
    public interface TaskSubmitter {
        void submit(TenantTtlCompactionTask task);
    }

    @FunctionalInterface
    public interface Waiter {
        void sleep(long millis) throws InterruptedException;
    }

    private static final class AgentTaskSubmitter implements TaskSubmitter {
        @Override
        public void submit(TenantTtlCompactionTask task) {
            synchronized (AgentTaskQueue.class) {
                AgentTask existing = AgentTaskQueue.getTask(
                        task.getBackendId(), TTaskType.TENANT_TTL_COMPACTION, task.getSignature());
                if (existing != null || !AgentTaskQueue.addTask(task)) {
                    throw new IllegalStateException("Tenant-TTL task ID registration collision");
                }
            }
            AgentTaskExecutor.submit(new AgentBatchTask(task));
        }
    }

    public enum ExecutionState {
        RUNNING, WAITING_REPLICA, RETRY_BACKOFF, BLOCKED, REPLAN_REQUIRED, COMPLETED,
        TIMED_OUT, ATTEMPTS_EXHAUSTED, FAILED
    }

    public enum ResultDisposition {
        SUCCESS, DEFINITE_RETRY, WAIT_FOR_RESULT, REPLAN, BLOCKED
    }

    public static final class ExecutionStatus {
        private final ProgressKey key;
        private final ExecutionState state;
        private final long taskId;
        private final int successfulReplicas;
        private final int requiredReplicas;
        private final TTenantTtlTaskCode lastResultCode;
        private final String detail;

        private ExecutionStatus(ProgressKey key, ExecutionState state, long taskId, int successfulReplicas,
                                int requiredReplicas, TTenantTtlTaskCode lastResultCode, String detail) {
            this.key = key;
            this.state = state;
            this.taskId = taskId;
            this.successfulReplicas = successfulReplicas;
            this.requiredReplicas = requiredReplicas;
            this.lastResultCode = lastResultCode;
            this.detail = detail;
        }

        public ProgressKey getKey() {
            return key;
        }
        public ExecutionState getState() {
            return state;
        }
        public long getTaskId() {
            return taskId;
        }
        public int getSuccessfulReplicas() {
            return successfulReplicas;
        }
        public int getRequiredReplicas() {
            return requiredReplicas;
        }
        public TTenantTtlTaskCode getLastResultCode() {
            return lastResultCode;
        }
        public String getDetail() {
            return detail;
        }
    }

    private static final class ReplicaResult {
        private final ExecutionState state;
        private final TTenantTtlTaskCode code;
        private final String detail;
        private final long processedThrough;

        private ReplicaResult(ExecutionState state, TTenantTtlTaskCode code, String detail, long processedThrough) {
            this.state = state;
            this.code = code;
            this.detail = detail;
            this.processedThrough = processedThrough;
        }

        private static ReplicaResult failure(ExecutionState state, TTenantTtlTaskCode code, String detail) {
            return new ReplicaResult(state, code, detail, -1);
        }
    }

    /** No task/context/filter reference: only the identity needed to suppress an unchanged bad plan. */
    private static final class BlockedPlan {
        private final String identity;
        private final long backendId;
        private final long backendStartTime;
        private final long observedVersion;
        private final TTenantTtlTaskCode code;
        private final String detail;

        private BlockedPlan(GlobalStateMgr state, TenantTtlScheduler.PendingRewritePlan pending,
                            ReplicaTaskSpec spec, TTenantTtlTaskCode code, String detail) {
            this.identity = identity(pending);
            this.backendId = spec.getBackendId();
            this.backendStartTime = backendStartTime(state, backendId);
            this.observedVersion = pending.getPartitionPlan().getObservedVisibleVersion();
            this.code = code;
            this.detail = detail;
        }

        private boolean stillApplies(GlobalStateMgr state, TenantTtlScheduler.PendingRewritePlan pending) {
            if (!identity.equals(identity(pending))) {
                return false;
            }
            if (code == TTenantTtlTaskCode.DATA_INVARIANT_VIOLATION || code == TTenantTtlTaskCode.INTERNAL_ERROR) {
                if (observedVersion != pending.getPartitionPlan().getObservedVisibleVersion()) {
                    return false;
                }
            }
            long currentStart = backendStartTime(state, backendId);
            return code == TTenantTtlTaskCode.INVALID_ARGUMENT || backendStartTime <= 0 ||
                    currentStart <= backendStartTime;
        }

        private static long backendStartTime(GlobalStateMgr state, long backendId) {
            ComputeNode node = state.getNodeMgr().getClusterInfo().getBackendOrComputeNode(backendId);
            return node == null ? 0 : node.getLastStartTime();
        }

        private static String identity(TenantTtlScheduler.PendingRewritePlan pending) {
            TenantTtlEvaluationContext context = pending.getContext();
            PartitionPlan plan = pending.getPartitionPlan();
            return context.getTableBindingFingerprint() + ":" + context.getSchemaId() + ":" +
                    context.getSchemaVersion() + ":" + plan.getBoundaryFingerprint() + ":" +
                    plan.getTopologyFingerprint() + ":" + plan.getPolicyPlan().getTablePolicyFingerprint() + ":" +
                    plan.getPolicyPlan().getFilterMode() + ":" +
                    plan.getPolicyPlan().getCompletedExpiryCursorEpochSeconds();
        }
    }

    private enum ValidationType { CURRENT, WAITING_REPLICA, REPLAN }

    private static final class Validation {
        private final ValidationType type;
        private final String detail;

        private Validation(ValidationType type, String detail) {
            this.type = type;
            this.detail = detail;
        }

        private static Validation current() {
            return new Validation(ValidationType.CURRENT, "");
        }
        private static Validation waiting(String detail) {
            return new Validation(ValidationType.WAITING_REPLICA, detail);
        }
        private static Validation replan(String detail) {
            return new Validation(ValidationType.REPLAN, detail);
        }
    }
}
