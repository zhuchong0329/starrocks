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

import com.starrocks.catalog.DictionaryMgr.TenantTtlRefreshDisposition;
import com.starrocks.catalog.DictionaryMgr.TenantTtlRefreshRequest;
import com.starrocks.catalog.TenantTtlDictionaryBinding;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.proto.PExportDictionaryCacheRequest;
import com.starrocks.proto.PExportDictionaryCacheResult;
import com.starrocks.proto.PUniqueId;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Leader-local owner of the enumerable Tenant-TTL policy snapshots derived from persisted table bindings.
 *
 * <p>Ordinary Dictionary refresh remains the source of truth. This manager runs only a post-commit export and never
 * changes Dictionary success or failure state. All RPC and candidate construction happens without holding the manager
 * monitor; publication rechecks leader identity, lifecycle epoch, Dictionary generation and references.</p>
 */
public class TenantTtlPolicySnapshotManager {
    private static final Logger LOG = LoggerFactory.getLogger(TenantTtlPolicySnapshotManager.class);
    private static final int MAX_EXPORT_NODES_PER_ATTEMPT = 2;
    private static final long RETRY_INITIAL_DELAY_MS = 10_000L;
    private static final long RETRY_MAX_DELAY_MS = 600_000L;
    private static final double RETRY_JITTER_RATIO = 0.20;
    private static final long RECOVERY_CHECK_INTERVAL_MS = 10_000;

    private final RuntimeEnvironment runtime;
    private final TaskDispatcher dispatcher;
    private final SnapshotBuilderFactory snapshotBuilderFactory;

    private final Map<Long, Set<TableRef>> references = new HashMap<>();
    private final Map<Long, Long> generations = new HashMap<>();
    private final Map<Long, SnapshotState> snapshotStates = new HashMap<>();
    private long lifecycleEpoch;

    public TenantTtlPolicySnapshotManager() {
        this(new ProductionRuntimeEnvironment(), new ProductionTaskDispatcher(),
                TenantTtlPolicySnapshotBuilder::new);
    }

    TenantTtlPolicySnapshotManager(RuntimeEnvironment runtime, TaskDispatcher dispatcher,
                                   TenantTtlPolicySnapshotBuilder snapshotBuilder) {
        this(runtime, dispatcher, () -> snapshotBuilder);
    }

    TenantTtlPolicySnapshotManager(RuntimeEnvironment runtime, TaskDispatcher dispatcher,
                                   SnapshotBuilderFactory snapshotBuilderFactory) {
        this.runtime = Objects.requireNonNull(runtime, "runtime is null");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher is null");
        this.snapshotBuilderFactory = Objects.requireNonNull(snapshotBuilderFactory, "snapshotBuilderFactory is null");
    }

    public void updateBinding(long dbId, long tableId, TenantTtlDictionaryBinding oldBinding,
                              TenantTtlDictionaryBinding newBinding) {
        Long activatedDictionaryId = null;
        long activatedGeneration = 0;
        long activatedEpoch = 0;
        synchronized (this) {
            TableRef tableRef = new TableRef(dbId, tableId);
            if (oldBinding != null && (newBinding == null ||
                    oldBinding.getDictionaryId() != newBinding.getDictionaryId())) {
                removeReferenceUnlocked(oldBinding.getDictionaryId(), tableRef);
            }
            if (newBinding != null) {
                long dictionaryId = newBinding.getDictionaryId();
                Set<TableRef> tableRefs = references.computeIfAbsent(dictionaryId, ignored -> new HashSet<>());
                boolean wasEmpty = tableRefs.isEmpty();
                boolean added = tableRefs.add(tableRef);
                if (wasEmpty && added) {
                    snapshotStates.computeIfAbsent(dictionaryId, ignored -> new SnapshotState());
                    activatedDictionaryId = dictionaryId;
                    activatedGeneration = getGenerationUnlocked(dictionaryId);
                    activatedEpoch = lifecycleEpoch;
                }
            }
        }
        if (activatedDictionaryId != null && runtime.isLeader()) {
            scheduleFirstReferenceRefresh(activatedDictionaryId, activatedGeneration, activatedEpoch);
        }
    }

    public synchronized void removeTable(long dbId, long tableId, TenantTtlDictionaryBinding binding) {
        if (binding != null) {
            removeReferenceUnlocked(binding.getDictionaryId(), new TableRef(dbId, tableId));
        }
    }

    public synchronized void onDictionaryDropped(long dictionaryId) {
        generations.merge(dictionaryId, 1L, Long::sum);
        snapshotStates.remove(dictionaryId);
    }

    /** Called after the ordinary Dictionary result and its last-success transaction have been committed. */
    public void onDictionaryRefreshFinished(long dictionaryId, long txnId, boolean success,
                                            List<TNetworkAddress> participatingNodes) {
        DictionaryView dictionary = runtime.getDictionary(dictionaryId);
        onDictionaryRefreshFinished(dictionaryId, txnId, success, participatingNodes,
                dictionary == null ? 0 : dictionary.refreshId);
    }

    public void onDictionaryRefreshFinished(long dictionaryId, long txnId, boolean success,
                                            List<TNetworkAddress> participatingNodes, long refreshId) {
        boolean requestPostBindingRefresh = false;
        synchronized (this) {
            SnapshotState state = snapshotStates.get(dictionaryId);
            if (!runtime.isLeader() || !isReferencedUnlocked(dictionaryId) || state == null) {
                return;
            }

            boolean completed = state.recovery.finishRefresh(refreshId, success, runtime.monotonicMillis());

            if (success) {
                if (txnId > state.targetTxnId) {
                    state.targetTxnId = txnId;
                    state.sweep = null;
                    state.recovery.resetEvidence(txnId);
                    state.retryFailureCount = 0;
                    state.retryTxnId = 0;
                    state.retryToken++;
                    if (state.blockedTxnId != null && state.blockedTxnId < txnId) {
                        state.blockedTxnId = null;
                    }
                }
                scheduleExportNowUnlocked(dictionaryId, state);
            }
            if (state.refreshAfterPreBindingTask && completed) {
                state.refreshAfterPreBindingTask = false;
                requestPostBindingRefresh = true;
            }
            scheduleRecoveryCheckUnlocked(dictionaryId, state);
        }
        if (requestPostBindingRefresh) {
            scheduleFullRefresh(dictionaryId, getGeneration(dictionaryId), getLifecycleEpoch());
        }
    }

    /** Replaces follower-derived runtime state with a catalog scan made after journal replay on the new Leader. */
    public void onLeaderActivated(Collection<RecoveredReference> recoveredReferences) {
        Set<Long> dictionaryIds;
        synchronized (this) {
            lifecycleEpoch++;
            references.clear();
            snapshotStates.clear();
            for (RecoveredReference recovered : recoveredReferences) {
                references.computeIfAbsent(recovered.getDictionaryId(), ignored -> new HashSet<>())
                        .add(recovered.getTableRef());
            }
            for (Long dictionaryId : references.keySet()) {
                snapshotStates.put(dictionaryId, new SnapshotState());
            }
            dictionaryIds = new HashSet<>(references.keySet());
        }

        for (Long dictionaryId : dictionaryIds) {
            recoverDictionary(dictionaryId);
        }
    }

    public synchronized Optional<TenantTtlPolicySnapshot> getCurrentSnapshot(long dictionaryId) {
        SnapshotState state = snapshotStates.get(dictionaryId);
        return state == null ? Optional.empty() : Optional.ofNullable(state.currentSnapshot);
    }

    public synchronized SnapshotStatus getStatus(long dictionaryId) {
        SnapshotState state = snapshotStates.get(dictionaryId);
        if (state == null) {
            return SnapshotStatus.empty(getGenerationUnlocked(dictionaryId), referenceCountUnlocked(dictionaryId));
        }
        return new SnapshotStatus(getGenerationUnlocked(dictionaryId), referenceCountUnlocked(dictionaryId),
                state.currentSnapshot == null ? 0 : state.currentSnapshot.getDictionaryTxnId(), state.targetTxnId,
                state.blockedTxnId == null ? 0 : state.blockedTxnId, state.lastAttemptTxnId,
                state.lastAttemptTimeMillis, state.retryFailureCount, state.nextRetryTimeMillis,
                state.lastFailureCode, state.lastFailureMessage, state.lastFailureTimeMillis,
                recoveryDiagnostic(state));
    }

    public synchronized Set<TableRef> getReferencedTables(long dictionaryId) {
        Set<TableRef> tableRefs = references.get(dictionaryId);
        if (tableRefs == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(new HashSet<>(tableRefs));
    }

    public synchronized Set<TableRef> getAllReferencedTables() {
        Set<TableRef> tableRefs = new HashSet<>();
        for (Set<TableRef> referencesForDictionary : references.values()) {
            tableRefs.addAll(referencesForDictionary);
        }
        return Collections.unmodifiableSet(tableRefs);
    }

    public synchronized boolean isReferenced(long dictionaryId) {
        return isReferencedUnlocked(dictionaryId);
    }

    public synchronized long getGeneration(long dictionaryId) {
        return getGenerationUnlocked(dictionaryId);
    }

    public synchronized void clearReferencesForRebuild() {
        lifecycleEpoch++;
        references.clear();
        snapshotStates.clear();
        generations.clear();
    }

    private void recoverDictionary(long dictionaryId) {
        DictionaryView dictionary = runtime.getDictionary(dictionaryId);
        if (dictionary == null) {
            return;
        }
        if (dictionary.getLastSuccessTxnId() <= 0) {
            scheduleFullRefresh(dictionaryId, getGeneration(dictionaryId), getLifecycleEpoch());
            return;
        }
        synchronized (this) {
            SnapshotState state = snapshotStates.get(dictionaryId);
            if (!runtime.isLeader() || !isReferencedUnlocked(dictionaryId) || state == null) {
                return;
            }
            state.targetTxnId = dictionary.getLastSuccessTxnId();
            state.sweep = null;
            scheduleExportNowUnlocked(dictionaryId, state);
        }
    }

    private void scheduleFirstReferenceRefresh(long dictionaryId, long generation, long epoch) {
        dispatcher.execute(() -> requestInitialRefresh(dictionaryId, generation, epoch, true));
    }

    private void scheduleFullRefresh(long dictionaryId, long generation, long epoch) {
        dispatcher.execute(() -> requestInitialRefresh(dictionaryId, generation, epoch, false));
    }

    private synchronized void requestInitialRefresh(long dictionaryId, long generation, long epoch,
                                                     boolean firstReference) {
        SnapshotState state = snapshotStates.get(dictionaryId);
        DictionaryView dictionary = runtime.getDictionary(dictionaryId);
        if (!isEligibleUnlocked(dictionaryId, generation, epoch) || state == null || dictionary == null) {
            return;
        }
        TenantTtlRefreshRequest result = requestRefreshUnlocked(dictionaryId, dictionary, state);
        if (firstReference && result != null && result.disposition == TenantTtlRefreshDisposition.JOIN_EXISTING) {
            state.refreshAfterPreBindingTask = true;
        }
        scheduleRecoveryCheckUnlocked(dictionaryId, state);
    }

    private TenantTtlRefreshRequest requestRefreshUnlocked(long dictionaryId, DictionaryView dictionary,
                                                           SnapshotState state) {
        try {
            TenantTtlRefreshRequest result = runtime.requestRecoveryRefresh(dictionaryId, dictionary.lastSuccessTxnId);
            if (result.disposition == TenantTtlRefreshDisposition.ENQUEUED ||
                    result.disposition == TenantTtlRefreshDisposition.JOIN_EXISTING) {
                state.recovery.waitingRefreshId = result.refreshId;
                state.recovery.resetEvidence(dictionary.lastSuccessTxnId);
                setRecoveryPhase(dictionaryId, state, "WAITING_REFRESH");
                LOG.info("Tenant-TTL Dictionary {} refresh {}: {}", dictionaryId, result.refreshId, result.disposition);
            }
            return result;
        } catch (Exception e) {
            state.recovery.waitingRefreshId = 0;
            state.recovery.cooling = true;
            state.recovery.refreshFinishedAt = runtime.monotonicMillis();
            state.recovery.lastRefreshResult = "ADMISSION_FAILED: " + rootMessage(e);
            LOG.warn("Failed to request Tenant-TTL full refresh for Dictionary {}", dictionaryId, e);
            return null;
        }
    }

    private void scheduleRecoveryCheckUnlocked(long dictionaryId, SnapshotState state) {
        if (state.currentSnapshot != null || state.recoveryCheckScheduled ||
                (state.blockedTxnId != null && state.blockedTxnId == state.targetTxnId)) {
            return;
        }
        state.recoveryCheckScheduled = true;
        long token = ++state.recoveryCheckToken;
        long generation = getGenerationUnlocked(dictionaryId);
        long epoch = lifecycleEpoch;
        state.nextRecoveryCheckMillis = saturatedAdd(runtime.currentTimeMillis(), RECOVERY_CHECK_INTERVAL_MS);
        dispatcher.schedule(() -> {
            List<ExportNode> nodes = runtime.getExportNodes(dictionaryId);
            synchronized (TenantTtlPolicySnapshotManager.this) {
                SnapshotState current = snapshotStates.get(dictionaryId);
                if (!isEligibleUnlocked(dictionaryId, generation, epoch) || current == null ||
                        current.recoveryCheckToken != token) {
                    return;
                }
                current.recoveryCheckScheduled = false;
                current.nextRecoveryCheckMillis = 0;
                evaluateRecoveryUnlocked(dictionaryId, current, nodes.stream().anyMatch(node -> node.alive));
                scheduleRecoveryCheckUnlocked(dictionaryId, current);
            }
        }, RECOVERY_CHECK_INTERVAL_MS);
    }

    private void evaluateRecoveryUnlocked(long dictionaryId, SnapshotState state, boolean hasLiveNode) {
        if (state.currentSnapshot != null) {
            return;
        }
        DictionaryView dictionary = runtime.getDictionary(dictionaryId);
        if (dictionary == null) {
            return;
        }
        if (dictionary.lastSuccessTxnId > state.targetTxnId) {
            state.targetTxnId = dictionary.lastSuccessTxnId;
            state.sweep = null;
            state.recovery.resetEvidence(state.targetTxnId);
            state.retryFailureCount = 0;
            state.retryToken++;
            scheduleExportNowUnlocked(dictionaryId, state);
            return;
        }
        if (state.blockedTxnId != null && state.blockedTxnId == state.targetTxnId) {
            setRecoveryPhase(dictionaryId, state, "BLOCKED");
        } else if (dictionary.refreshing || state.recovery.waitingRefreshId > 0) {
            if (dictionary.refreshId > 0) {
                state.recovery.waitingRefreshId = dictionary.refreshId;
            }
            setRecoveryPhase(dictionaryId, state, "WAITING_REFRESH");
        } else if (!Config.tenant_ttl_policy_snapshot_auto_recover_enabled) {
            setRecoveryPhase(dictionaryId, state, "DISABLED");
        } else if (!hasLiveNode) {
            setRecoveryPhase(dictionaryId, state, "WAITING_NODES");
        } else if (state.recovery.cooldownRemaining(runtime.monotonicMillis(),
                Config.tenant_ttl_policy_snapshot_recovery_cooldown_seconds) > 0) {
            setRecoveryPhase(dictionaryId, state, "COOLDOWN");
        } else if (dictionary.lastSuccessTxnId <= 0 || state.recovery.needed) {
            requestRefreshUnlocked(dictionaryId, dictionary, state);
        } else {
            setRecoveryPhase(dictionaryId, state, "PROBING");
        }
    }

    private void setRecoveryPhase(long dictionaryId, SnapshotState state, String phase) {
        if (!phase.equals(state.recovery.phase)) {
            state.recovery.phase = phase;
            LOG.info("Tenant-TTL Dictionary {} target {} recovery {}", dictionaryId, state.targetTxnId, phase);
        }
    }

    private String recoveryDiagnostic(SnapshotState state) {
        if (state.currentSnapshot != null) {
            return null;
        }
        return "recovery=" + state.recovery.phase + ", target=" + state.targetTxnId +
                ", visited=" + state.recovery.visited + "/" + state.recovery.candidates +
                ", uncertainSweeps=" + state.recovery.uncertainSweeps +
                ", refresh=" + state.recovery.waitingRefreshId + ", lastRefresh=" + state.recovery.lastRefreshResult +
                ", nextCheckMillis=" + state.nextRecoveryCheckMillis +
                ", cooldownRemainingMillis=" + state.recovery.cooldownRemaining(runtime.monotonicMillis(),
                Config.tenant_ttl_policy_snapshot_recovery_cooldown_seconds) +
                (Config.tenant_ttl_policy_snapshot_recovery_cooldown_seconds <= 0 ? ", invalidCooldownUsingDefault=300" : "");
    }

    private void scheduleExportNowUnlocked(long dictionaryId, SnapshotState state) {
        if (state.exportActive || state.targetTxnId <= 0 ||
                (state.currentSnapshot != null && state.currentSnapshot.getDictionaryTxnId() >= state.targetTxnId) ||
                (state.blockedTxnId != null && state.blockedTxnId == state.targetTxnId)) {
            return;
        }
        state.exportActive = true;
        long generation = getGenerationUnlocked(dictionaryId);
        long epoch = lifecycleEpoch;
        dispatcher.execute(() -> runExport(dictionaryId, generation, epoch));
    }

    private void runExport(long dictionaryId, long generation, long epoch) {
        ExportContext context = prepareExport(dictionaryId, generation, epoch);
        if (context == null) {
            clearExportActive(dictionaryId, generation, epoch);
            return;
        }

        ExportResult result = exportCandidate(context);
        finishExport(context, result);
    }

    private ExportContext prepareExport(long dictionaryId, long generation, long epoch) {
        DictionaryView dictionary = runtime.getDictionary(dictionaryId);
        List<ExportNode> catalogNodes = sortedDistinctNodes(runtime.getExportNodes(dictionaryId));
        synchronized (this) {
            SnapshotState state = snapshotStates.get(dictionaryId);
            if (!isEligibleUnlocked(dictionaryId, generation, epoch) || state == null || dictionary == null ||
                    dictionary.getDictionaryId() != dictionaryId) {
                return null;
            }
            long latestTxnId = dictionary.getLastSuccessTxnId();
            if (latestTxnId <= 0) {
                return null;
            }
            if (latestTxnId > state.targetTxnId) {
                state.targetTxnId = latestTxnId;
                state.sweep = null;
                state.recovery.resetEvidence(latestTxnId);
                state.retryFailureCount = 0;
                state.retryTxnId = 0;
                state.retryToken++;
                if (state.blockedTxnId != null && state.blockedTxnId < latestTxnId) {
                    state.blockedTxnId = null;
                }
            }
            if ((state.currentSnapshot != null && state.currentSnapshot.getDictionaryTxnId() >= latestTxnId) ||
                    (state.blockedTxnId != null && state.blockedTxnId == latestTxnId)) {
                return null;
            }
            state.lastAttemptTxnId = latestTxnId;
            state.lastAttemptTimeMillis = runtime.currentTimeMillis();
            if (state.sweep == null || state.sweep.txnId != latestTxnId) {
                state.sweep = new ExportSweep(latestTxnId, rotateAfter(catalogNodes, state.lastVisitedNode),
                        runtime.monotonicMillis());
                state.recovery.visited = 0;
                state.recovery.candidates = state.sweep.nodes.size();
            }
            ExportSweep sweep = state.sweep;
            List<ExportNode> nodes = new ArrayList<>();
            while (sweep.nextIndex < sweep.nodes.size() && nodes.size() < MAX_EXPORT_NODES_PER_ATTEMPT) {
                ExportNode captured = sweep.nodes.get(sweep.nextIndex++);
                // A removed/replaced endpoint must not be contacted using a stale refresh participant list.
                ExportNode current = catalogNodes.stream().filter(captured::sameIdentity).findFirst().orElse(null);
                nodes.add(current == null ? new ExportNode(captured.id, captured.address, false) : current);
            }
            return new ExportContext(dictionaryId, dictionary.getDictionaryName(), latestTxnId, generation, epoch,
                    state.lastAttemptTimeMillis, nodes, sweep,
                    Config.tenant_ttl_policy_snapshot_export_max_rows,
                    Config.tenant_ttl_policy_snapshot_export_max_uncompressed_bytes,
                    Config.tenant_ttl_policy_snapshot_export_max_response_bytes);
        }
    }

    private ExportResult exportCandidate(ExportContext context) {
        TenantTtlPolicySnapshotBuilder snapshotBuilder;
        try {
            snapshotBuilder = snapshotBuilderFactory.create();
        } catch (RuntimeException e) {
            return ExportResult.failure(TenantTtlPolicySnapshotBuildException.Classification.DETERMINISTIC,
                    "TENANT_TTL_POLICY_LIMIT_EXCEEDED", rootMessage(e));
        }
        if (context.nodes.isEmpty()) {
            return ExportResult.retryable("TENANT_TTL_POLICY_EXPORT_NO_NODE",
                    "No BE/CN is available for Dictionary snapshot export");
        }

        ExportResult lastRetryable = null;
        for (ExportNode node : context.nodes) {
            context.sweep.visited++;
            if (!node.alive) {
                lastRetryable = ExportResult.retryable("TENANT_TTL_POLICY_EXPORT_NODE_UNAVAILABLE",
                        node.address + ": captured node is offline, removed or replaced").visited(node);
                context.sweep.uncertain++;
                continue;
            }
            try {
                Future<PExportDictionaryCacheResult> future = runtime.export(node.address, newExportRequest(context));
                PExportDictionaryCacheResult response;
                try {
                    response = future.get(Math.max(1, Config.tenant_ttl_policy_snapshot_export_rpc_timeout_ms),
                            TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    future.cancel(true);
                    throw e;
                }
                TenantTtlPolicySnapshot snapshot = snapshotBuilder.build(
                        context.dictionaryId, context.dictionaryName, context.txnId,
                        Instant.ofEpochMilli(runtime.currentTimeMillis()), response);
                return ExportResult.success(snapshot).visited(node);
            } catch (TenantTtlPolicySnapshotBuildException e) {
                ExportResult failure = ExportResult.failure(e.getClassification(), e.getErrorCode().name(),
                        node.address + ": " + e.getMessage()).visited(node);
                if (e.getClassification() == TenantTtlPolicySnapshotBuildException.Classification.DETERMINISTIC) {
                    return failure;
                }
                switch (e.getErrorCode()) {
                    case TENANT_TTL_POLICY_CACHE_NOT_FOUND:
                    case TENANT_TTL_POLICY_VERSION_MISMATCH:
                        context.sweep.missing++;
                        break;
                    default:
                        // A server's non-OK status is not proof of a network failure or lost cache.
                        context.sweep.other++;
                        break;
                }
                lastRetryable = failure;
            } catch (Exception e) {
                context.sweep.uncertain++;
                lastRetryable = ExportResult.retryable("TENANT_TTL_POLICY_EXPORT_RPC_ERROR",
                        node.address + ": " + rootMessage(e)).visited(node);
            }
        }
        return lastRetryable == null ? ExportResult.retryable("TENANT_TTL_POLICY_EXPORT_RPC_ERROR",
                "Dictionary snapshot export failed without a response") : lastRetryable;
    }

    private PExportDictionaryCacheRequest newExportRequest(ExportContext context) {
        PExportDictionaryCacheRequest request = new PExportDictionaryCacheRequest();
        request.protocolVersion = TenantTtlPolicySnapshotBuilder.PROTOCOL_VERSION;
        TUniqueId requestId = UUIDUtil.genTUniqueId();
        request.requestId = new PUniqueId();
        request.requestId.hi = requestId.hi;
        request.requestId.lo = requestId.lo;
        request.dictionaryId = context.dictionaryId;
        request.expectedTxnId = context.txnId;
        request.maxRows = context.maxRows;
        request.maxUncompressedBytes = context.maxUncompressedBytes;
        request.maxResponseBytes = context.maxResponseBytes;
        return request;
    }

    private void finishExport(ExportContext context, ExportResult result) {
        synchronized (this) {
            SnapshotState state = snapshotStates.get(context.dictionaryId);
            if (!isEligibleUnlocked(context.dictionaryId, context.generation, context.epoch) || state == null) {
                return;
            }
            state.exportActive = false;
            if (result.lastVisitedNode != null) {
                state.lastVisitedNode = result.lastVisitedNode;
            }
            state.lastAttemptTimeMillis = runtime.currentTimeMillis();
            state.recovery.visited = context.sweep.visited;
            if (result.snapshot != null) {
                TenantTtlPolicySnapshot current = state.currentSnapshot;
                if (current == null || result.snapshot.getDictionaryTxnId() > current.getDictionaryTxnId()) {
                    state.candidateSnapshot = result.snapshot;
                    state.currentSnapshot = state.candidateSnapshot;
                    state.candidateSnapshot = null;
                }
                state.blockedTxnId = null;
                state.retryFailureCount = 0;
                state.retryTxnId = 0;
                state.nextRetryTimeMillis = 0;
                state.retryToken++;
                state.sweep = null;
                state.recovery.resetEvidence(context.txnId);
                state.recovery.waitingRefreshId = 0;
                state.recoveryCheckToken++;
                state.recoveryCheckScheduled = false;
                state.nextRecoveryCheckMillis = 0;
                setRecoveryPhase(context.dictionaryId, state, "RECOVERED");
                state.clearFailure();
                DictionaryView dictionary = runtime.getDictionary(context.dictionaryId);
                if (dictionary != null && dictionary.getLastSuccessTxnId() >
                        state.currentSnapshot.getDictionaryTxnId()) {
                    state.targetTxnId = dictionary.getLastSuccessTxnId();
                    scheduleExportNowUnlocked(context.dictionaryId, state);
                }
                return;
            }

            state.lastFailureCode = result.errorCode;
            state.lastFailureMessage = result.message;
            state.lastFailureTimeMillis = runtime.currentTimeMillis();
            DictionaryView dictionary = runtime.getDictionary(context.dictionaryId);
            if (state.targetTxnId > context.txnId ||
                    (dictionary != null && dictionary.getLastSuccessTxnId() > context.txnId)) {
                if (dictionary != null) {
                    state.targetTxnId = Math.max(state.targetTxnId, dictionary.getLastSuccessTxnId());
                }
                state.sweep = null;
                state.recovery.resetEvidence(state.targetTxnId);
                state.retryFailureCount = 0;
                state.retryTxnId = 0;
                state.nextRetryTimeMillis = 0;
                state.retryToken++;
                scheduleExportNowUnlocked(context.dictionaryId, state);
                return;
            }
            if (result.classification == TenantTtlPolicySnapshotBuildException.Classification.DETERMINISTIC) {
                state.blockedTxnId = context.txnId;
                state.nextRetryTimeMillis = 0;
                state.sweep = null;
                state.recovery.resetEvidence(context.txnId);
                state.recoveryCheckToken++;
                state.recoveryCheckScheduled = false;
                state.nextRecoveryCheckMillis = 0;
                setRecoveryPhase(context.dictionaryId, state, "BLOCKED");
                return;
            }
            if (state.sweep == context.sweep && context.sweep.nextIndex < context.sweep.nodes.size()) {
                scheduleExportNowUnlocked(context.dictionaryId, state);
                return;
            }
            boolean hasLiveNode = context.sweep.nodes.stream().anyMatch(node -> node.alive);
            if (hasLiveNode) {
                state.recovery.recordSweep(context.txnId, context.sweep.startedAt, runtime.monotonicMillis(),
                        context.sweep.missing, context.sweep.uncertain, context.sweep.other);
            } else {
                state.recovery.resetEvidence(context.txnId);
            }
            state.sweep = null;
            scheduleRetryUnlocked(context.dictionaryId, context.txnId, state);
            evaluateRecoveryUnlocked(context.dictionaryId, state, hasLiveNode);
            scheduleRecoveryCheckUnlocked(context.dictionaryId, state);
        }
    }

    private void scheduleRetryUnlocked(long dictionaryId, long failedTxnId, SnapshotState state) {
        if (state.retryTxnId != failedTxnId) {
            state.retryTxnId = failedTxnId;
            state.retryFailureCount = 0;
        }
        state.retryFailureCount++;
        long delayMs = retryDelayMs(state.retryFailureCount, runtime.retryJitter());
        state.nextRetryTimeMillis = saturatedAdd(runtime.currentTimeMillis(), delayMs);
        long token = ++state.retryToken;
        long generation = getGenerationUnlocked(dictionaryId);
        long epoch = lifecycleEpoch;
        dispatcher.schedule(() -> {
            synchronized (TenantTtlPolicySnapshotManager.this) {
                SnapshotState current = snapshotStates.get(dictionaryId);
                if (!isEligibleUnlocked(dictionaryId, generation, epoch) || current == null ||
                        current.retryToken != token) {
                    return;
                }
                current.nextRetryTimeMillis = 0;
                scheduleExportNowUnlocked(dictionaryId, current);
            }
        }, delayMs);
    }

    private synchronized void clearExportActive(long dictionaryId, long generation, long epoch) {
        SnapshotState state = snapshotStates.get(dictionaryId);
        if (isEligibleUnlocked(dictionaryId, generation, epoch) && state != null) {
            state.exportActive = false;
        }
    }

    private boolean isEligibleUnlocked(long dictionaryId, long generation, long epoch) {
        return runtime.isLeader() && lifecycleEpoch == epoch && getGenerationUnlocked(dictionaryId) == generation &&
                isReferencedUnlocked(dictionaryId);
    }

    private synchronized long getLifecycleEpoch() {
        return lifecycleEpoch;
    }

    private boolean isReferencedUnlocked(long dictionaryId) {
        Set<TableRef> tableRefs = references.get(dictionaryId);
        return tableRefs != null && !tableRefs.isEmpty();
    }

    private int referenceCountUnlocked(long dictionaryId) {
        Set<TableRef> tableRefs = references.get(dictionaryId);
        return tableRefs == null ? 0 : tableRefs.size();
    }

    private long getGenerationUnlocked(long dictionaryId) {
        return generations.getOrDefault(dictionaryId, 0L);
    }

    private void removeReferenceUnlocked(long dictionaryId, TableRef tableRef) {
        Set<TableRef> tableRefs = references.get(dictionaryId);
        if (tableRefs == null || !tableRefs.remove(tableRef)) {
            return;
        }
        if (tableRefs.isEmpty()) {
            references.remove(dictionaryId);
            generations.merge(dictionaryId, 1L, Long::sum);
            snapshotStates.remove(dictionaryId);
        }
    }

    private static List<ExportNode> sortedDistinctNodes(List<ExportNode> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return Collections.emptyList();
        }
        List<ExportNode> copy = new ArrayList<>();
        for (ExportNode node : nodes) {
            if (copy.stream().noneMatch(node::sameIdentity)) {
                copy.add(node);
            }
        }
        copy.sort(ExportNode.ORDER);
        return copy;
    }

    private static List<ExportNode> rotateAfter(List<ExportNode> nodes, ExportNode lastVisited) {
        if (nodes.isEmpty() || lastVisited == null) {
            return nodes;
        }
        // An address/id removed between sweeps still has a stable insertion point in the ordering.
        int start = 0;
        while (start < nodes.size() && ExportNode.ORDER.compare(nodes.get(start), lastVisited) <= 0) {
            start++;
        }
        List<ExportNode> rotated = new ArrayList<>(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            rotated.add(nodes.get((start + i) % nodes.size()));
        }
        return rotated;
    }

    static long retryDelayMs(int failureCount, double unitJitter) {
        int shift = Math.max(0, Math.min(30, failureCount - 1));
        long multiplier = 1L << shift;
        long nominal;
        try {
            nominal = Math.min(RETRY_MAX_DELAY_MS, Math.multiplyExact(RETRY_INITIAL_DELAY_MS, multiplier));
        } catch (ArithmeticException e) {
            nominal = RETRY_MAX_DELAY_MS;
        }
        double boundedJitter = Math.max(-1.0, Math.min(1.0, unitJitter));
        return Math.round(nominal * (1.0 + RETRY_JITTER_RATIO * boundedJitter));
    }

    private static long saturatedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static String rootMessage(Exception exception) {
        Throwable cause = exception;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
    }

    interface RuntimeEnvironment {
        boolean isLeader();

        DictionaryView getDictionary(long dictionaryId);

        TenantTtlRefreshRequest requestRecoveryRefresh(long dictionaryId, long expectedSuccessTxnId) throws Exception;

        List<ExportNode> getExportNodes(long dictionaryId);

        Future<PExportDictionaryCacheResult> export(TNetworkAddress node, PExportDictionaryCacheRequest request)
                throws Exception;

        long currentTimeMillis();

        long monotonicMillis();

        double retryJitter();
    }

    interface TaskDispatcher {
        void execute(Runnable task);

        void schedule(Runnable task, long delayMs);
    }

    interface SnapshotBuilderFactory {
        TenantTtlPolicySnapshotBuilder create();
    }

    static final class ExportNode {
        private static final Comparator<ExportNode> ORDER = Comparator
                .comparing((ExportNode node) -> node.address.getHostname())
                .thenComparingInt(node -> node.address.getPort()).thenComparingLong(node -> node.id);
        private final long id;
        private final TNetworkAddress address;
        private final boolean alive;

        ExportNode(long id, TNetworkAddress address, boolean alive) {
            this.id = id;
            this.address = new TNetworkAddress(address);
            this.alive = alive;
        }

        private boolean sameIdentity(ExportNode other) {
            return id == other.id && address.equals(other.address);
        }
    }

    private static final class ExportSweep {
        private final long txnId;
        private final List<ExportNode> nodes;
        private int nextIndex;
        private int visited;
        private final long startedAt;
        private int missing;
        private int uncertain;
        private int other;

        private ExportSweep(long txnId, List<ExportNode> nodes, long startedAt) {
            this.txnId = txnId;
            this.nodes = nodes;
            this.startedAt = startedAt;
        }
    }

    public static final class DictionaryView {
        private final long dictionaryId;
        private final String dictionaryName;
        private final long lastSuccessTxnId;
        private final boolean refreshing;
        private final long refreshId;

        public DictionaryView(long dictionaryId, String dictionaryName, long lastSuccessTxnId, boolean refreshing,
                              long refreshId) {
            this.dictionaryId = dictionaryId;
            this.dictionaryName = dictionaryName;
            this.lastSuccessTxnId = lastSuccessTxnId;
            this.refreshing = refreshing;
            this.refreshId = refreshId;
        }

        long getDictionaryId() {
            return dictionaryId;
        }

        String getDictionaryName() {
            return dictionaryName;
        }

        long getLastSuccessTxnId() {
            return lastSuccessTxnId;
        }

        boolean isRefreshing() {
            return refreshing;
        }
    }

    public static final class RecoveredReference {
        private final long dictionaryId;
        private final TableRef tableRef;

        public RecoveredReference(long dictionaryId, long dbId, long tableId) {
            this.dictionaryId = dictionaryId;
            this.tableRef = new TableRef(dbId, tableId);
        }

        public long getDictionaryId() {
            return dictionaryId;
        }

        public TableRef getTableRef() {
            return tableRef;
        }
    }

    public static final class SnapshotStatus {
        private final long generation;
        private final int referenceCount;
        private final long snapshotTxnId;
        private final long targetTxnId;
        private final long blockedTxnId;
        private final long lastAttemptTxnId;
        private final long lastAttemptTimeMillis;
        private final int retryFailureCount;
        private final long nextRetryTimeMillis;
        private final String lastFailureCode;
        private final String lastFailureMessage;
        private final long lastFailureTimeMillis;
        private final String recoveryDiagnostic;

        SnapshotStatus(long generation, int referenceCount, long snapshotTxnId, long targetTxnId,
                       long blockedTxnId, long lastAttemptTxnId, long lastAttemptTimeMillis,
                       int retryFailureCount, long nextRetryTimeMillis, String lastFailureCode,
                       String lastFailureMessage, long lastFailureTimeMillis) {
            this(generation, referenceCount, snapshotTxnId, targetTxnId, blockedTxnId, lastAttemptTxnId,
                    lastAttemptTimeMillis, retryFailureCount, nextRetryTimeMillis, lastFailureCode,
                    lastFailureMessage, lastFailureTimeMillis, null);
        }

        SnapshotStatus(long generation, int referenceCount, long snapshotTxnId, long targetTxnId,
                       long blockedTxnId, long lastAttemptTxnId, long lastAttemptTimeMillis,
                       int retryFailureCount, long nextRetryTimeMillis, String lastFailureCode,
                       String lastFailureMessage, long lastFailureTimeMillis, String recoveryDiagnostic) {
            this.generation = generation;
            this.referenceCount = referenceCount;
            this.snapshotTxnId = snapshotTxnId;
            this.targetTxnId = targetTxnId;
            this.blockedTxnId = blockedTxnId;
            this.lastAttemptTxnId = lastAttemptTxnId;
            this.lastAttemptTimeMillis = lastAttemptTimeMillis;
            this.retryFailureCount = retryFailureCount;
            this.nextRetryTimeMillis = nextRetryTimeMillis;
            this.lastFailureCode = lastFailureCode;
            this.lastFailureMessage = lastFailureMessage;
            this.lastFailureTimeMillis = lastFailureTimeMillis;
            this.recoveryDiagnostic = recoveryDiagnostic;
        }

        public String getRecoveryDiagnostic() {
            return recoveryDiagnostic;
        }

        private static SnapshotStatus empty(long generation, int referenceCount) {
            return new SnapshotStatus(generation, referenceCount, 0, 0, 0, 0, 0, 0, 0, null, null, 0);
        }

        public long getGeneration() {
            return generation;
        }

        public int getReferenceCount() {
            return referenceCount;
        }

        public long getSnapshotTxnId() {
            return snapshotTxnId;
        }

        public long getTargetTxnId() {
            return targetTxnId;
        }

        public long getBlockedTxnId() {
            return blockedTxnId;
        }

        public long getLastAttemptTxnId() {
            return lastAttemptTxnId;
        }

        public long getLastAttemptTimeMillis() {
            return lastAttemptTimeMillis;
        }

        public int getRetryFailureCount() {
            return retryFailureCount;
        }

        public long getNextRetryTimeMillis() {
            return nextRetryTimeMillis;
        }

        public String getLastFailureCode() {
            return lastFailureCode;
        }

        public String getLastFailureMessage() {
            return lastFailureMessage;
        }

        public long getLastFailureTimeMillis() {
            return lastFailureTimeMillis;
        }
    }

    public static final class TableRef {
        private final long dbId;
        private final long tableId;

        public TableRef(long dbId, long tableId) {
            this.dbId = dbId;
            this.tableId = tableId;
        }

        public long getDbId() {
            return dbId;
        }

        public long getTableId() {
            return tableId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TableRef)) {
                return false;
            }
            TableRef tableRef = (TableRef) o;
            return dbId == tableRef.dbId && tableId == tableRef.tableId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbId, tableId);
        }
    }

    private static final class SnapshotState {
        private TenantTtlPolicySnapshot currentSnapshot;
        private TenantTtlPolicySnapshot candidateSnapshot;
        private long targetTxnId;
        private ExportSweep sweep;
        private ExportNode lastVisitedNode;
        private Long blockedTxnId;
        private boolean exportActive;
        private boolean refreshAfterPreBindingTask;
        private long lastAttemptTxnId;
        private long lastAttemptTimeMillis;
        private long retryTxnId;
        private int retryFailureCount;
        private long nextRetryTimeMillis;
        private long retryToken;
        private String lastFailureCode;
        private String lastFailureMessage;
        private long lastFailureTimeMillis;
        private final TenantTtlSnapshotRecovery recovery = new TenantTtlSnapshotRecovery();
        private boolean recoveryCheckScheduled;
        private long recoveryCheckToken;
        private long nextRecoveryCheckMillis;

        private void clearFailure() {
            lastFailureCode = null;
            lastFailureMessage = null;
            lastFailureTimeMillis = 0;
        }
    }

    private static final class ExportContext {
        private final long dictionaryId;
        private final String dictionaryName;
        private final long txnId;
        private final long generation;
        private final long epoch;
        private final long attemptTimeMillis;
        private final List<ExportNode> nodes;
        private final ExportSweep sweep;
        private final long maxRows;
        private final long maxUncompressedBytes;
        private final long maxResponseBytes;

        private ExportContext(long dictionaryId, String dictionaryName, long txnId, long generation, long epoch,
                              long attemptTimeMillis, List<ExportNode> nodes, ExportSweep sweep, long maxRows,
                              long maxUncompressedBytes, long maxResponseBytes) {
            this.dictionaryId = dictionaryId;
            this.dictionaryName = dictionaryName;
            this.txnId = txnId;
            this.generation = generation;
            this.epoch = epoch;
            this.attemptTimeMillis = attemptTimeMillis;
            this.nodes = nodes;
            this.sweep = sweep;
            this.maxRows = maxRows;
            this.maxUncompressedBytes = maxUncompressedBytes;
            this.maxResponseBytes = maxResponseBytes;
        }
    }

    private static final class ExportResult {
        private final TenantTtlPolicySnapshot snapshot;
        private final TenantTtlPolicySnapshotBuildException.Classification classification;
        private final String errorCode;
        private final String message;
        private ExportNode lastVisitedNode;

        private ExportResult(TenantTtlPolicySnapshot snapshot,
                             TenantTtlPolicySnapshotBuildException.Classification classification,
                             String errorCode, String message) {
            this.snapshot = snapshot;
            this.classification = classification;
            this.errorCode = errorCode;
            this.message = message;
        }

        private static ExportResult success(TenantTtlPolicySnapshot snapshot) {
            return new ExportResult(snapshot, null, null, null);
        }

        private ExportResult visited(ExportNode node) {
            lastVisitedNode = node;
            return this;
        }

        private static ExportResult retryable(String errorCode, String message) {
            return failure(TenantTtlPolicySnapshotBuildException.Classification.RETRYABLE, errorCode, message);
        }

        private static ExportResult failure(TenantTtlPolicySnapshotBuildException.Classification classification,
                                            String errorCode, String message) {
            return new ExportResult(null, classification, errorCode, message);
        }
    }

    private static final class ProductionTaskDispatcher implements TaskDispatcher {
        private final ScheduledThreadPoolExecutor executor = ThreadPoolManager.newDaemonScheduledThreadPool(
                1, "tenant-ttl-policy-snapshot", true);

        @Override
        public void execute(Runnable task) {
            executor.execute(task);
        }

        @Override
        public void schedule(Runnable task, long delayMs) {
            executor.schedule(task, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    private static final class ProductionRuntimeEnvironment implements RuntimeEnvironment {
        @Override
        public boolean isLeader() {
            return GlobalStateMgr.getCurrentState().isLeader();
        }

        @Override
        public DictionaryView getDictionary(long dictionaryId) {
            return GlobalStateMgr.getCurrentState().getDictionaryMgr().getTenantTtlDictionaryView(dictionaryId);
        }

        @Override
        public TenantTtlRefreshRequest requestRecoveryRefresh(long dictionaryId, long expectedSuccessTxnId) {
            return GlobalStateMgr.getCurrentState().getDictionaryMgr()
                    .requestTenantTtlRecoveryRefresh(dictionaryId, expectedSuccessTxnId);
        }

        @Override
        public List<ExportNode> getExportNodes(long dictionaryId) {
            SystemInfoService cluster = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            List<ComputeNode> candidates = new ArrayList<>(cluster.getBackends());
            candidates.addAll(cluster.getComputeNodes());
            List<ExportNode> nodes = new ArrayList<>();
            for (ComputeNode node : candidates) {
                nodes.add(new ExportNode(node.getId(), node.getBrpcAddress(), node.isAlive()));
            }
            return nodes;
        }

        @Override
        public Future<PExportDictionaryCacheResult> export(TNetworkAddress node,
                                                           PExportDictionaryCacheRequest request) throws Exception {
            return BackendServiceClient.getInstance().exportDictionaryCache(node, request);
        }

        @Override
        public long currentTimeMillis() {
            return System.currentTimeMillis();
        }

        @Override
        public long monotonicMillis() {
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        }

        @Override
        public double retryJitter() {
            return ThreadLocalRandom.current().nextDouble(-1.0, 1.0);
        }
    }
}
