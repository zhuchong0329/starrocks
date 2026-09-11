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
import com.starrocks.tenantttl.scheduler.TenantTtlPartitionProgress.ProgressKey;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TTenantTtlCompactionResult;
import com.starrocks.thrift.TTenantTtlTaskCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

/**
 * Leader-local orchestration for immutable Tenant-TTL Replica tasks.
 *
 * <p>The coordinator deliberately persists only completed partition progress. Partial Replica success and
 * in-flight Agent tasks are fenced by immutable request identity and are rebuilt from durable progress after
 * a Leader change.</p>
 */
public final class TenantTtlRewriteCoordinator implements TenantTtlScheduler.RewriteExecutionView {
    private static final Logger LOG = LogManager.getLogger(TenantTtlRewriteCoordinator.class);
    private static final long INITIAL_RETRY_MILLIS = 10_000L;
    private static final long MAX_RETRY_MILLIS = 10L * 60L * 1000L;
    private static final double JITTER_FRACTION = 0.20;

    private final Map<ProgressKey, PartitionExecution> executions = new LinkedHashMap<>();
    private final TaskSubmitter taskSubmitter;
    private final LongSupplier clockMillis;
    private final DoubleSupplier randomUnit;
    private ActiveTask activeTask;

    public TenantTtlRewriteCoordinator() {
        Random random = new Random();
        this.taskSubmitter = new AgentTaskSubmitter();
        this.clockMillis = System::currentTimeMillis;
        this.randomUnit = random::nextDouble;
    }

    public TenantTtlRewriteCoordinator(TaskSubmitter taskSubmitter, LongSupplier clockMillis,
                                       DoubleSupplier randomUnit) {
        this.taskSubmitter = Objects.requireNonNull(taskSubmitter, "task submitter is null");
        this.clockMillis = Objects.requireNonNull(clockMillis, "clock is null");
        this.randomUnit = Objects.requireNonNull(randomUnit, "jitter source is null");
    }

    /** Reconcile retained Leader-local executions with the scheduler's current immutable plans. */
    public synchronized void reconcile(GlobalStateMgr state,
                                       List<TenantTtlScheduler.PendingRewritePlan> currentPlans) {
        Objects.requireNonNull(state, "global state is null");
        Objects.requireNonNull(currentPlans, "current plans are null");
        if (!state.isLeader()) {
            resetForLeadershipLoss();
            return;
        }

        Map<ProgressKey, TenantTtlScheduler.PendingRewritePlan> current = indexPlans(currentPlans);
        for (PartitionExecution execution : executions.values()) {
            TenantTtlScheduler.PendingRewritePlan plan = current.get(execution.key);
            if (plan == null || !execution.matchesStablePlan(plan)) {
                execution.invalidated = true;
                execution.detail = "current binding, policy, boundary, topology, or plan type changed";
            }
        }
        discardInactiveInvalidatedExecutions();
        addMissingExecutions(state, current);

        pollActiveTask(state);
        discardInactiveInvalidatedExecutions();
        publishCompletedExecutions(state, current);
    }

    /** Submit at most one Replica task. Explicit terminal retries do not prevent another plan from running. */
    public synchronized void dispatchNext(GlobalStateMgr state) {
        Objects.requireNonNull(state, "global state is null");
        if (!state.isLeader()) {
            resetForLeadershipLoss();
            return;
        }
        if (activeTask != null || hasUnknownExclusiveExecution()) {
            PartitionExecution exclusive = unknownExclusiveExecution();
            if (activeTask != null || exclusive == null || clockMillis.getAsLong() < exclusive.nextAttemptMillis) {
                return;
            }
            dispatch(state, exclusive);
            return;
        }

        List<PartitionExecution> candidates = new ArrayList<>(executions.values());
        candidates.sort(PartitionExecution.ORDER);
        long now = clockMillis.getAsLong();
        for (PartitionExecution execution : candidates) {
            if (execution.invalidated || execution.state == ExecutionState.BLOCKED || execution.allReplicasSucceeded() ||
                    now < execution.nextAttemptMillis) {
                continue;
            }
            Validation validation = validateCurrentPlanAndTarget(state, execution, execution.nextSpec(), true);
            if (validation.type == ValidationType.REPLAN) {
                execution.invalidated = true;
                execution.state = ExecutionState.REPLAN_REQUIRED;
                execution.detail = validation.detail;
                continue;
            }
            if (validation.type == ValidationType.WAITING_REPLICA) {
                scheduleRetry(execution, ExecutionState.WAITING_REPLICA, validation.detail, false);
                continue;
            }
            dispatch(state, execution);
            return;
        }
        discardInactiveInvalidatedExecutions();
    }

    private void dispatch(GlobalStateMgr state, PartitionExecution execution) {
        if (!state.isLeader() || execution.invalidated && !execution.unknownExclusive) {
            return;
        }
        ReplicaTaskSpec spec = execution.nextSpec();
        if (spec == null) {
            return;
        }
        Validation validation = execution.unknownExclusive ? validateUnknownRetryBackend(state, spec) :
                validateCurrentPlanAndTarget(state, execution, spec, true);
        if (validation.type != ValidationType.CURRENT) {
            if (validation.type == ValidationType.WAITING_REPLICA && !execution.unknownExclusive) {
                scheduleRetry(execution, ExecutionState.WAITING_REPLICA, validation.detail, false);
            } else if (!execution.unknownExclusive) {
                execution.invalidated = true;
                execution.state = ExecutionState.REPLAN_REQUIRED;
                execution.detail = validation.detail;
            }
            return;
        }

        TenantTtlCompactionTask task = execution.currentTask;
        if (task == null || task.getSignature() != spec.getTaskId()) {
            task = new TenantTtlCompactionTask(spec);
            execution.currentTask = task;
        } else {
            task.prepareForRetry();
        }
        execution.unknownExclusive = false;
        execution.state = ExecutionState.RUNNING;
        execution.detail = "";
        execution.nextAttemptMillis = 0;
        activeTask = new ActiveTask(execution, spec, task, clockMillis.getAsLong(), task.getFailedTimes());
        try {
            taskSubmitter.submit(task);
        } catch (RuntimeException e) {
            LOG.warn("Tenant-TTL Agent task submission became unknown. backendId={}, tabletId={}, taskId={}",
                    spec.getBackendId(), spec.getTabletId(), spec.getTaskId(), e);
            activeTask = null;
            scheduleRetry(execution, ExecutionState.UNKNOWN_RETRY,
                    "Agent task submission outcome is unknown: " + rootMessage(e), true);
        }
    }

    private static Validation validateUnknownRetryBackend(GlobalStateMgr state, ReplicaTaskSpec spec) {
        ComputeNode backend = state.getNodeMgr().getClusterInfo().getBackendOrComputeNode(spec.getBackendId());
        return backend != null && backend.isAlive() && !backend.isDecommissioned() ? Validation.current() :
                Validation.waiting("backend for an unknown prior request is unavailable");
    }

    private void pollActiveTask(GlobalStateMgr state) {
        if (activeTask == null) {
            return;
        }
        ActiveTask active = activeTask;
        PartitionExecution execution = active.execution;
        Optional<TTenantTtlCompactionResult> result = active.task.getResult();
        if (result.isPresent()) {
            activeTask = null;
            handleBusinessResult(state, execution, active.spec, result.get());
            return;
        }

        long elapsed = Math.max(0, clockMillis.getAsLong() - active.sentAtMillis);
        long softTimeout = Math.multiplyExact(
                (long) Math.max(1, Config.tenant_ttl_agent_task_soft_timeout_seconds), 1000L);
        if (active.task.getFailedTimes() > active.observedFailedTimes || elapsed >= softTimeout) {
            activeTask = null;
            String detail = active.task.getFailedTimes() > active.observedFailedTimes ?
                    "Agent task transport failed without a business result: " + active.task.getErrorMsg() :
                    "Agent task exceeded the soft timeout without a confirmed result";
            scheduleRetry(execution, ExecutionState.UNKNOWN_RETRY, detail, true);
        }
    }

    private void handleBusinessResult(GlobalStateMgr state, PartitionExecution execution, ReplicaTaskSpec spec,
                                      TTenantTtlCompactionResult result) {
        TTenantTtlTaskCode code = result.getCode();
        execution.lastResultCode = code;
        execution.detail = detail(result);
        switch (classifyResult(code)) {
            case SUCCESS:
                execution.currentTask = null;
                execution.retryAttempt = 0;
                execution.unknownExclusive = false;
                if (execution.invalidated) {
                    executions.remove(execution.key);
                    return;
                }
                execution.successVersions.put(spec.getTaskId(), result.getProcessed_through_version());
                execution.state = ExecutionState.READY;
                if (execution.allReplicasSucceeded()) {
                    publishCompletedExecution(state, execution);
                }
                break;
            case DEFINITE_RETRY:
                if (execution.invalidated) {
                    executions.remove(execution.key);
                } else {
                    scheduleRetry(execution, ExecutionState.RETRY_BACKOFF, execution.detail, false);
                }
                break;
            case UNKNOWN_RETRY:
                scheduleRetry(execution, ExecutionState.UNKNOWN_RETRY, execution.detail, true);
                break;
            case REPLAN:
                execution.currentTask = null;
                execution.invalidated = true;
                execution.state = ExecutionState.REPLAN_REQUIRED;
                executions.remove(execution.key);
                break;
            case BLOCKED:
                execution.currentTask = null;
                execution.unknownExclusive = false;
                execution.state = ExecutionState.BLOCKED;
                break;
            default:
                throw new IllegalStateException("unclassified Tenant-TTL result disposition");
        }
    }

    public static ResultDisposition classifyResult(TTenantTtlTaskCode code) {
        Objects.requireNonNull(code, "Tenant-TTL result code is null");
        switch (code) {
            case SUCCESS:
            case NOOP_VERIFIED:
                return ResultDisposition.SUCCESS;
            case TABLET_BUSY:
            case REPLICA_NOT_CAUGHT_UP:
            case STALE_ROWSET:
            case CANCELLED:
                return ResultDisposition.DEFINITE_RETRY;
            case TTL_ALREADY_RUNNING:
                return ResultDisposition.UNKNOWN_RETRY;
            case SCHEMA_CHANGED:
            case TABLET_NOT_FOUND:
                return ResultDisposition.REPLAN;
            case INVALID_ARGUMENT:
            case NOT_SUPPORTED:
            case DATA_INVARIANT_VIOLATION:
            case INTERNAL_ERROR:
                return ResultDisposition.BLOCKED;
            default:
                throw new IllegalArgumentException("unrecognized Tenant-TTL result code: " + code);
        }
    }

    private void publishCompletedExecutions(GlobalStateMgr state,
                                            Map<ProgressKey, TenantTtlScheduler.PendingRewritePlan> current) {
        List<PartitionExecution> completed = new ArrayList<>();
        for (PartitionExecution execution : executions.values()) {
            if (execution.allReplicasSucceeded() && !execution.invalidated) {
                TenantTtlScheduler.PendingRewritePlan plan = current.get(execution.key);
                if (plan == null || !execution.matchesStablePlan(plan)) {
                    execution.invalidated = true;
                } else {
                    completed.add(execution);
                }
            }
        }
        completed.sort(PartitionExecution.ORDER);
        for (PartitionExecution execution : completed) {
            publishCompletedExecution(state, execution);
        }
    }

    private void publishCompletedExecution(GlobalStateMgr state, PartitionExecution execution) {
        Validation validation = validateCurrentPlanAndTarget(state, execution, null, false);
        if (validation.type != ValidationType.CURRENT) {
            execution.invalidated = true;
            execution.state = ExecutionState.REPLAN_REQUIRED;
            execution.detail = validation.detail;
            executions.remove(execution.key);
            return;
        }
        long processedThrough = Long.MAX_VALUE;
        for (Long version : execution.successVersions.values()) {
            processedThrough = Math.min(processedThrough, version);
        }
        if (processedThrough == Long.MAX_VALUE) {
            throw new IllegalStateException("completed Tenant-TTL execution has no Replica result");
        }
        TenantTtlEvaluationContext context = execution.plan.getContext();
        PartitionPlan plan = execution.plan.getPartitionPlan();
        TenantTtlPartitionProgress candidate = new TenantTtlPartitionProgress(
                context.getDbId(), context.getTableId(), plan.getPhysicalPartitionId(),
                context.getTableBindingFingerprint(), plan.getPolicyPlan().getTablePolicyFingerprint(),
                plan.getBoundaryFingerprint(), plan.getPolicyPlan().getCompletedExpiryCursorEpochSeconds(),
                processedThrough, context.getSnapshotTxnId(), context.getEvaluationTimeEpochSeconds());
        TenantTtlPartitionProgressManager.AdvanceResult advance = state.getTenantTtlPartitionProgressManager()
                .compareAndAdvanceCompletedPlan(execution.expectedProgress, candidate);
        if (advance == TenantTtlPartitionProgressManager.AdvanceResult.ADVANCED ||
                advance == TenantTtlPartitionProgressManager.AdvanceResult.UNCHANGED) {
            execution.state = ExecutionState.COMPLETED;
            execution.detail = "";
        } else {
            execution.state = ExecutionState.REPLAN_REQUIRED;
            execution.detail = "partition progress changed before publication: " + advance;
        }
        executions.remove(execution.key);
    }

    private Validation validateCurrentPlanAndTarget(GlobalStateMgr state, PartitionExecution execution,
                                                    ReplicaTaskSpec target, boolean requireAvailableReplica) {
        TenantTtlEvaluationContext context = execution.plan.getContext();
        PartitionPlan oldPlan = execution.plan.getPartitionPlan();
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

    private void scheduleRetry(PartitionExecution execution, ExecutionState state, String detail,
                               boolean unknownExclusive) {
        execution.retryAttempt++;
        execution.state = state;
        execution.detail = detail == null ? "" : detail;
        execution.unknownExclusive = unknownExclusive;
        execution.nextAttemptMillis = saturatedAdd(clockMillis.getAsLong(), retryDelay(execution.retryAttempt));
    }

    private long retryDelay(int attempt) {
        long base = INITIAL_RETRY_MILLIS;
        for (int i = 1; i < attempt && base < MAX_RETRY_MILLIS; ++i) {
            base = Math.min(MAX_RETRY_MILLIS, base * 2);
        }
        double unit = Math.max(0, Math.min(1, randomUnit.getAsDouble()));
        double factor = 1.0 - JITTER_FRACTION + 2.0 * JITTER_FRACTION * unit;
        return Math.max(1, Math.round(base * factor));
    }

    private static long saturatedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private void addMissingExecutions(GlobalStateMgr state,
                                      Map<ProgressKey, TenantTtlScheduler.PendingRewritePlan> current) {
        for (Map.Entry<ProgressKey, TenantTtlScheduler.PendingRewritePlan> entry : current.entrySet()) {
            if (!executions.containsKey(entry.getKey())) {
                TenantTtlPartitionProgress expected = state.getTenantTtlPartitionProgressManager()
                        .get(entry.getKey()).orElse(null);
                executions.put(entry.getKey(), new PartitionExecution(entry.getKey(), entry.getValue(), expected));
            }
        }
    }

    private static Map<ProgressKey, TenantTtlScheduler.PendingRewritePlan> indexPlans(
            List<TenantTtlScheduler.PendingRewritePlan> plans) {
        List<TenantTtlScheduler.PendingRewritePlan> sorted = new ArrayList<>(plans);
        sorted.sort(Comparator.comparingLong((TenantTtlScheduler.PendingRewritePlan plan) ->
                        plan.getContext().getDbId())
                .thenComparingLong(plan -> plan.getContext().getTableId())
                .thenComparingLong(plan -> plan.getPartitionPlan().getPhysicalPartitionId()));
        Map<ProgressKey, TenantTtlScheduler.PendingRewritePlan> result = new LinkedHashMap<>();
        for (TenantTtlScheduler.PendingRewritePlan plan : sorted) {
            ProgressKey key = key(plan);
            if (result.put(key, plan) != null) {
                throw new IllegalArgumentException("duplicate Tenant-TTL rewrite plan for " + key);
            }
        }
        return result;
    }

    private void discardInactiveInvalidatedExecutions() {
        Iterator<Map.Entry<ProgressKey, PartitionExecution>> iterator = executions.entrySet().iterator();
        while (iterator.hasNext()) {
            PartitionExecution execution = iterator.next().getValue();
            if (execution.invalidated && !execution.unknownExclusive &&
                    (activeTask == null || activeTask.execution != execution)) {
                removeQueuedTask(execution.currentTask);
                iterator.remove();
            }
        }
    }

    public synchronized void resetForLeadershipLoss() {
        if (activeTask != null) {
            removeQueuedTask(activeTask.task);
        }
        for (PartitionExecution execution : executions.values()) {
            removeQueuedTask(execution.currentTask);
        }
        activeTask = null;
        executions.clear();
    }

    private static void removeQueuedTask(TenantTtlCompactionTask task) {
        if (task != null) {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.TENANT_TTL_COMPACTION, task.getSignature());
        }
    }

    @Override
    public synchronized boolean isRetryPending(ProgressKey key) {
        PartitionExecution execution = executions.get(key);
        return execution != null && (execution.state == ExecutionState.RETRY_BACKOFF ||
                execution.state == ExecutionState.UNKNOWN_RETRY || execution.state == ExecutionState.WAITING_REPLICA);
    }

    @Override
    public synchronized boolean hasRewriteInFlight(long dbId, long tableId, long logicalPartitionId) {
        if (activeTask != null && activeTask.execution.matchesLogicalPartition(dbId, tableId, logicalPartitionId)) {
            return true;
        }
        PartitionExecution exclusive = unknownExclusiveExecution();
        return exclusive != null && exclusive.matchesLogicalPartition(dbId, tableId, logicalPartitionId);
    }

    @Override
    public synchronized boolean hasDestructiveExecutionInFlight() {
        return activeTask != null || hasUnknownExclusiveExecution();
    }

    public synchronized List<ExecutionStatus> getExecutionStatuses() {
        List<ExecutionStatus> statuses = new ArrayList<>(executions.size());
        for (PartitionExecution execution : executions.values()) {
            ReplicaTaskSpec spec = execution.nextSpec();
            statuses.add(new ExecutionStatus(execution.key, execution.plan.getPartitionPlan().getLogicalPartitionId(),
                    execution.state, spec == null ? 0 : spec.getTaskId(), spec == null ? 0 : spec.getTabletId(),
                    spec == null ? 0 : spec.getBackendId(), execution.successVersions.size(),
                    execution.plan.getPartitionPlan().getReplicaTasks().size(), execution.nextAttemptMillis,
                    execution.lastResultCode, execution.detail));
        }
        statuses.sort(Comparator.comparing(ExecutionStatus::getKey));
        return Collections.unmodifiableList(statuses);
    }

    public synchronized Optional<TenantTtlCompactionTask> getActiveTask() {
        return activeTask == null ? Optional.empty() : Optional.of(activeTask.task);
    }

    private boolean hasUnknownExclusiveExecution() {
        return unknownExclusiveExecution() != null;
    }

    private PartitionExecution unknownExclusiveExecution() {
        for (PartitionExecution execution : executions.values()) {
            if (execution.unknownExclusive) {
                return execution;
            }
        }
        return null;
    }

    private static ProgressKey key(TenantTtlScheduler.PendingRewritePlan plan) {
        return new ProgressKey(plan.getContext().getDbId(), plan.getContext().getTableId(),
                plan.getPartitionPlan().getPhysicalPartitionId());
    }

    private static String detail(TTenantTtlCompactionResult result) {
        if (result.getDetail_status() == null || result.getDetail_status().getError_msgs() == null ||
                result.getDetail_status().getError_msgs().isEmpty()) {
            return result.getCode().name();
        }
        return String.join("; ", result.getDetail_status().getError_msgs());
    }

    private static String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }

    public interface TaskSubmitter {
        void submit(TenantTtlCompactionTask task);
    }

    private static final class AgentTaskSubmitter implements TaskSubmitter {
        @Override
        public void submit(TenantTtlCompactionTask task) {
            AgentTask existing = AgentTaskQueue.getTask(
                    task.getBackendId(), TTaskType.TENANT_TTL_COMPACTION, task.getSignature());
            if (existing != null && existing != task) {
                throw new IllegalStateException("Tenant-TTL task ID collides with another Agent task");
            }
            if (existing == null && !AgentTaskQueue.addTask(task)) {
                throw new IllegalStateException("failed to register Tenant-TTL Agent task");
            }
            AgentTaskExecutor.submit(new AgentBatchTask(task));
        }
    }

    public enum ExecutionState {
        READY,
        RUNNING,
        WAITING_REPLICA,
        RETRY_BACKOFF,
        UNKNOWN_RETRY,
        BLOCKED,
        REPLAN_REQUIRED,
        COMPLETED
    }

    public enum ResultDisposition {
        SUCCESS,
        DEFINITE_RETRY,
        UNKNOWN_RETRY,
        REPLAN,
        BLOCKED
    }

    public static final class ExecutionStatus {
        private final ProgressKey key;
        private final long logicalPartitionId;
        private final ExecutionState state;
        private final long taskId;
        private final long tabletId;
        private final long backendId;
        private final int successfulReplicas;
        private final int requiredReplicas;
        private final long nextAttemptMillis;
        private final TTenantTtlTaskCode lastResultCode;
        private final String detail;

        private ExecutionStatus(ProgressKey key, long logicalPartitionId, ExecutionState state, long taskId,
                                long tabletId, long backendId, int successfulReplicas, int requiredReplicas,
                                long nextAttemptMillis, TTenantTtlTaskCode lastResultCode, String detail) {
            this.key = key;
            this.logicalPartitionId = logicalPartitionId;
            this.state = state;
            this.taskId = taskId;
            this.tabletId = tabletId;
            this.backendId = backendId;
            this.successfulReplicas = successfulReplicas;
            this.requiredReplicas = requiredReplicas;
            this.nextAttemptMillis = nextAttemptMillis;
            this.lastResultCode = lastResultCode;
            this.detail = detail == null ? "" : detail;
        }

        public ProgressKey getKey() {
            return key;
        }

        public long getLogicalPartitionId() {
            return logicalPartitionId;
        }

        public ExecutionState getState() {
            return state;
        }

        public long getTaskId() {
            return taskId;
        }

        public long getTabletId() {
            return tabletId;
        }

        public long getBackendId() {
            return backendId;
        }

        public int getSuccessfulReplicas() {
            return successfulReplicas;
        }

        public int getRequiredReplicas() {
            return requiredReplicas;
        }

        public long getNextAttemptMillis() {
            return nextAttemptMillis;
        }

        public TTenantTtlTaskCode getLastResultCode() {
            return lastResultCode;
        }

        public String getDetail() {
            return detail;
        }
    }

    private static final class PartitionExecution {
        private static final Comparator<PartitionExecution> ORDER =
                Comparator.comparing(execution -> execution.key);

        private final ProgressKey key;
        private final TenantTtlScheduler.PendingRewritePlan plan;
        private final TenantTtlPartitionProgress expectedProgress;
        private final Map<Long, Long> successVersions = new HashMap<>();
        private TenantTtlCompactionTask currentTask;
        private ExecutionState state = ExecutionState.READY;
        private TTenantTtlTaskCode lastResultCode;
        private int retryAttempt;
        private long nextAttemptMillis;
        private boolean unknownExclusive;
        private boolean invalidated;
        private String detail = "";

        private PartitionExecution(ProgressKey key, TenantTtlScheduler.PendingRewritePlan plan,
                                   TenantTtlPartitionProgress expectedProgress) {
            this.key = key;
            this.plan = plan;
            this.expectedProgress = expectedProgress;
        }

        private boolean matchesStablePlan(TenantTtlScheduler.PendingRewritePlan current) {
            TenantTtlEvaluationContext oldContext = plan.getContext();
            TenantTtlEvaluationContext newContext = current.getContext();
            PartitionPlan oldPlan = plan.getPartitionPlan();
            PartitionPlan newPlan = current.getPartitionPlan();
            return oldPlan.getType() == TenantTtlPolicyPlanner.PlanType.ROWSET_REWRITE &&
                    newPlan.getType() == TenantTtlPolicyPlanner.PlanType.ROWSET_REWRITE &&
                    oldContext.getTableBindingFingerprint().equals(newContext.getTableBindingFingerprint()) &&
                    oldPlan.getPolicyPlan().getTablePolicyFingerprint()
                            .equals(newPlan.getPolicyPlan().getTablePolicyFingerprint()) &&
                    oldPlan.getBoundaryFingerprint().equals(newPlan.getBoundaryFingerprint()) &&
                    oldPlan.getTopologyFingerprint().equals(newPlan.getTopologyFingerprint());
        }

        private ReplicaTaskSpec nextSpec() {
            for (ReplicaTaskSpec spec : plan.getPartitionPlan().getReplicaTasks()) {
                if (!successVersions.containsKey(spec.getTaskId())) {
                    return spec;
                }
            }
            return null;
        }

        private boolean allReplicasSucceeded() {
            List<ReplicaTaskSpec> tasks = plan.getPartitionPlan().getReplicaTasks();
            return !tasks.isEmpty() && successVersions.size() == tasks.size();
        }

        private boolean matchesLogicalPartition(long dbId, long tableId, long logicalPartitionId) {
            return key.getDbId() == dbId && key.getTableId() == tableId &&
                    plan.getPartitionPlan().getLogicalPartitionId() == logicalPartitionId;
        }
    }

    private static final class ActiveTask {
        private final PartitionExecution execution;
        private final ReplicaTaskSpec spec;
        private final TenantTtlCompactionTask task;
        private final long sentAtMillis;
        private final int observedFailedTimes;

        private ActiveTask(PartitionExecution execution, ReplicaTaskSpec spec, TenantTtlCompactionTask task,
                           long sentAtMillis, int observedFailedTimes) {
            this.execution = execution;
            this.spec = spec;
            this.task = task;
            this.sentAtMillis = sentAtMillis;
            this.observedFailedTimes = observedFailedTimes;
        }
    }

    private enum ValidationType {
        CURRENT,
        WAITING_REPLICA,
        REPLAN
    }

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
