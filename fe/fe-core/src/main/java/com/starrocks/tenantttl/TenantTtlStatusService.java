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

package com.starrocks.tenantttl;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Dictionary;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TenantTtlBindingAnalyzer;
import com.starrocks.catalog.TenantTtlDictionaryBinding;
import com.starrocks.catalog.TenantTtlPropertyParser;
import com.starrocks.catalog.TenantTtlTableBinding;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.tenantttl.policy.TablePolicy;
import com.starrocks.tenantttl.policy.TenantTtlByteKey;
import com.starrocks.tenantttl.policy.TenantTtlPolicyPlanner;
import com.starrocks.tenantttl.policy.TenantTtlPolicySnapshot;
import com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotManager;
import com.starrocks.tenantttl.scheduler.TenantTtlRewriteCoordinator;
import com.starrocks.tenantttl.scheduler.TenantTtlScheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Builds the single bounded row returned by SHOW TENANT TTL STATUS. */
public final class TenantTtlStatusService {
    private static final int MAX_ERROR_COMPONENT_LENGTH = 1024;
    private static final int MAX_ERROR_MESSAGE_LENGTH = 4096;

    private TenantTtlStatusService() {
    }

    public static List<String> buildRow(GlobalStateMgr state, Database db, OlapTable table, String tenant) {
        Objects.requireNonNull(state, "global state is null");
        Objects.requireNonNull(db, "database is null");
        Objects.requireNonNull(table, "table is null");

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            if (db.getTable(table.getId()) != table) {
                throw new IllegalStateException("table identity changed while collecting Tenant-TTL status");
            }
            return buildRowLocked(state, db, table, tenant);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
    }

    private static List<String> buildRowLocked(GlobalStateMgr state, Database db, OlapTable table, String tenant) {
        TableProperty property = table.getTableProperty();
        String condition = property == null ? null : property.getCompactionRetentionCondition();
        boolean enabled = condition != null;
        TenantTtlDictionaryBinding dictionaryBinding = property == null ? null :
                property.getTenantTtlDictionaryBinding();
        TenantTtlTableBinding tableBinding = property == null ? null : property.getTenantTtlTableBinding();

        MutableStatus status = new MutableStatus(db.getFullName() + "." + table.getName(), enabled,
                dictionaryBinding, tableBinding);
        if (!enabled) {
            status.bindingState = BindingState.DISABLED;
            status.schedulerState = "DISABLED";
            return status.toRow(tenant);
        }

        if (!validatePersistedMetadata(condition, dictionaryBinding, tableBinding, status)) {
            status.bindingState = BindingState.INVALID;
            status.schedulerState = "INVALID";
            return status.toRow(tenant);
        }

        Dictionary dictionary = state.getDictionaryMgr().getDictionaryById(dictionaryBinding.getDictionaryId());
        if (dictionary == null) {
            Dictionary sameName = state.getDictionaryMgr().getDictionaryByName(dictionaryBinding.getDictionaryName());
            status.bindingState = BindingState.PAUSED;
            status.schedulerState = "PAUSED";
            if (sameName == null) {
                status.addError("bound Dictionary ID " + dictionaryBinding.getDictionaryId() + " no longer exists");
            } else {
                status.addError("bound Dictionary ID " + dictionaryBinding.getDictionaryId() +
                        " no longer exists; same-name Dictionary now has ID " + sameName.getDictionaryId());
            }
            return status.toRow(tenant);
        }
        status.dictionary = dictionary;

        if (!dictionaryBinding.getDictionaryName().equals(dictionary.getDictionaryName())) {
            status.bindingState = BindingState.PAUSED;
            status.schedulerState = "PAUSED";
            status.addError("bound Dictionary ID resolves to an unexpected Dictionary name");
            return status.toRow(tenant);
        }
        try {
            TenantTtlBindingAnalyzer.validateDictionaryBinding(dictionary);
            TenantTtlBindingAnalyzer.TableBindingResult current = TenantTtlBindingAnalyzer.analyzeTableBinding(
                    db, table, property.getCompactionRetentionTimeZone(), property.getProperties());
            if (!tableBinding.equals(current.getTableBinding())) {
                throw new DdlException("current table layout does not match the persisted Tenant-TTL binding");
            }
        } catch (DdlException | RuntimeException e) {
            status.bindingState = BindingState.PAUSED;
            status.schedulerState = "PAUSED";
            status.addError(rootMessage(e));
            status.addDictionaryError();
            return status.toRow(tenant);
        }

        TenantTtlPolicySnapshotManager snapshotManager = state.getTenantTtlPolicySnapshotManager();
        TenantTtlPolicySnapshotManager.SnapshotStatus snapshotStatus =
                snapshotManager.getStatus(dictionaryBinding.getDictionaryId());
        status.snapshotStatus = snapshotStatus;
        status.addSnapshotError();
        status.addDictionaryError();

        if (dictionary.getLastSuccessVersion() <= 0) {
            status.bindingState = BindingState.WAITING_DICTIONARY;
            status.schedulerState = "WAITING_DICTIONARY";
            return status.toRow(tenant);
        }

        Optional<TenantTtlPolicySnapshot> currentSnapshot =
                snapshotManager.getCurrentSnapshot(dictionaryBinding.getDictionaryId());
        if (!currentSnapshot.isPresent()) {
            status.bindingState = BindingState.WAITING_POLICY_SNAPSHOT;
            status.schedulerState = "WAITING_POLICY_SNAPSHOT";
            return status.toRow(tenant);
        }

        status.bindingState = BindingState.ACTIVE;
        status.snapshot = currentSnapshot.get();
        status.populatePolicyMatch();
        status.populatePartitionAndSchedulerState(state, db, table);
        status.resolveTenant(tenant);
        return status.toRow(tenant);
    }

    private static boolean validatePersistedMetadata(String condition,
                                                     TenantTtlDictionaryBinding dictionaryBinding,
                                                     TenantTtlTableBinding tableBinding,
                                                     MutableStatus status) {
        TenantTtlPropertyParser.ParsedCondition parsed;
        try {
            parsed = TenantTtlPropertyParser.parse(condition);
            status.configuredDictionaryName = parsed.getDictionaryName();
            status.configuredTableKey = parsed.getTableKey();
            status.configuredDefaultDays = parsed.getDefaultDays();
        } catch (RuntimeException e) {
            status.addError(rootMessage(e));
            return false;
        }
        if (dictionaryBinding == null || tableBinding == null) {
            status.addError("Tenant-TTL persistent binding metadata is missing");
            return false;
        }
        if (dictionaryBinding.getFormatVersion() != TenantTtlDictionaryBinding.CURRENT_FORMAT_VERSION ||
                tableBinding.getFormatVersion() != TenantTtlTableBinding.CURRENT_FORMAT_VERSION) {
            status.addError("Tenant-TTL persistent binding format is unsupported");
            return false;
        }
        if (!dictionaryBinding.getDictionaryName().equals(parsed.getDictionaryName()) ||
                !dictionaryBinding.getTableKey().equals(parsed.getTableKey()) ||
                dictionaryBinding.getDefaultDays() != parsed.getDefaultDays()) {
            status.addError("compaction_retention_condition does not match persistent Tenant-TTL binding");
            return false;
        }
        return true;
    }

    private static String nullableLong(long value) {
        return value <= 0 ? null : Long.toString(value);
    }

    private static String nullableTimeMillis(long timeMillis) {
        return timeMillis <= 0 ? null : TimeUtils.longToTimeString(timeMillis);
    }

    private static String nullableTimeSeconds(long epochSeconds) {
        if (epochSeconds <= 0) {
            return null;
        }
        try {
            return nullableTimeMillis(Math.multiplyExact(epochSeconds, 1000L));
        } catch (ArithmeticException e) {
            return null;
        }
    }

    private static String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        String message = current.getMessage();
        return message == null || message.isEmpty() ? current.getClass().getSimpleName() : message;
    }

    public enum BindingState {
        DISABLED,
        WAITING_DICTIONARY,
        WAITING_POLICY_SNAPSHOT,
        ACTIVE,
        PAUSED,
        INVALID
    }

    private static final class MutableStatus {
        private final String physicalTable;
        private final boolean enabled;
        private final TenantTtlDictionaryBinding dictionaryBinding;
        private final TenantTtlTableBinding tableBinding;
        private final Set<String> errors = new LinkedHashSet<>();
        private BindingState bindingState;
        private String configuredDictionaryName;
        private String configuredTableKey;
        private Integer configuredDefaultDays;
        private Dictionary dictionary;
        private TenantTtlPolicySnapshotManager.SnapshotStatus snapshotStatus;
        private TenantTtlPolicySnapshot snapshot;
        private TenantTtlPolicyPlanner.TableKeyMatch tableKeyMatch = TenantTtlPolicyPlanner.TableKeyMatch.UNKNOWN;
        private TenantTtlPolicyPlanner.TableDefaultMatch tableDefaultMatch =
                TenantTtlPolicyPlanner.TableDefaultMatch.UNKNOWN;
        private TenantTtlPolicyPlanner.TenantResolution tenantResolution;
        private long provablePhysicalPartitions;
        private long unprovablePhysicalPartitions;
        private String schedulerState;
        private long pendingRewritePartitions;
        private long runningReplicaTasks;
        private long completedPhysicalPartitions;
        private long nextExpiryEpochSeconds;

        private MutableStatus(String physicalTable, boolean enabled,
                              TenantTtlDictionaryBinding dictionaryBinding,
                              TenantTtlTableBinding tableBinding) {
            this.physicalTable = physicalTable;
            this.enabled = enabled;
            this.dictionaryBinding = dictionaryBinding;
            this.tableBinding = tableBinding;
        }

        private void populatePolicyMatch() {
            Optional<TablePolicy> tablePolicy = snapshot.getTablePolicy(dictionaryBinding.getTableKey());
            if (tablePolicy.isPresent()) {
                tableKeyMatch = TenantTtlPolicyPlanner.TableKeyMatch.MATCHED;
                tableDefaultMatch = tablePolicy.get().getTableDefaultDays().isPresent() ?
                        TenantTtlPolicyPlanner.TableDefaultMatch.MATCHED :
                        TenantTtlPolicyPlanner.TableDefaultMatch.NOT_FOUND;
            } else {
                tableKeyMatch = TenantTtlPolicyPlanner.TableKeyMatch.NOT_FOUND_USE_DEFAULT;
                tableDefaultMatch = TenantTtlPolicyPlanner.TableDefaultMatch.NOT_FOUND;
            }
        }

        private void resolveTenant(String tenant) {
            if (tenant == null) {
                return;
            }
            tenantResolution = TenantTtlPolicyPlanner.fromConfig().resolveTenant(snapshot,
                    TenantTtlByteKey.utf8(dictionaryBinding.getTableKey()), TenantTtlByteKey.utf8(tenant),
                    dictionaryBinding.getDefaultDays());
        }

        private void populatePartitionAndSchedulerState(GlobalStateMgr state, Database db, OlapTable table) {
            String firstUnprovable = null;
            for (PhysicalPartition partition : table.getAllPhysicalPartitions()) {
                TenantTtlPartitionBoundResolver.Resolution resolution =
                        TenantTtlPartitionBoundResolver.resolve(table, partition, tableBinding);
                if (resolution.isProvable()) {
                    provablePhysicalPartitions++;
                } else {
                    unprovablePhysicalPartitions++;
                    if (firstUnprovable == null) {
                        firstUnprovable = resolution.getReason() + ": " + resolution.getDetail();
                    }
                }
            }
            if (firstUnprovable != null) {
                addError("unprovable Physical Partition: " + firstUnprovable);
            }

            TenantTtlScheduler scheduler = state.getTenantTtlScheduler();
            for (TenantTtlScheduler.PendingRewritePlan pending : scheduler.getPendingRewritePlans()) {
                if (pending.getContext().getDbId() == db.getId() &&
                        pending.getContext().getTableId() == table.getId()) {
                    pendingRewritePartitions++;
                }
            }
            for (TenantTtlRewriteCoordinator.ExecutionStatus execution :
                    scheduler.getRewriteCoordinator().getExecutionStatuses()) {
                if (execution.getKey().getDbId() == db.getId() && execution.getKey().getTableId() == table.getId() &&
                        execution.getState() == TenantTtlRewriteCoordinator.ExecutionState.RUNNING) {
                    runningReplicaTasks++;
                }
            }
            completedPhysicalPartitions = state.getTenantTtlPartitionProgressManager()
                    .countTable(db.getId(), table.getId());
            for (TenantTtlScheduler.NextExpiry expiry : scheduler.getNextExpiries()) {
                if (expiry.getKey().getDbId() == db.getId() && expiry.getKey().getTableId() == table.getId() &&
                        (nextExpiryEpochSeconds == 0 || expiry.getExpiryEpochSeconds() < nextExpiryEpochSeconds)) {
                    nextExpiryEpochSeconds = expiry.getExpiryEpochSeconds();
                }
            }
            populateSchedulerState(scheduler, db.getId(), table.getId());
        }

        private void populateSchedulerState(TenantTtlScheduler scheduler, long dbId, long tableId) {
            Optional<TenantTtlScheduler.TableRuntimeStatus> tableStatus = scheduler.getTableStatus(dbId, tableId);
            if (!tableStatus.isPresent()) {
                schedulerState = "NOT_EVALUATED";
                return;
            }
            if (!tableStatus.get().isActive()) {
                schedulerState = "CAPTURE_" + tableStatus.get().getCaptureFailure();
                addError(tableStatus.get().getDetail());
                return;
            }
            List<TenantTtlScheduler.PartitionRuntimeStatus> statuses =
                    scheduler.getPartitionStatuses(dbId, tableId);
            TenantTtlScheduler.PartitionState selected = null;
            int selectedPriority = -1;
            for (TenantTtlScheduler.PartitionRuntimeStatus partitionStatus : statuses) {
                if (partitionStatus.getSnapshotTxnId() != snapshot.getDictionaryTxnId()) {
                    continue;
                }
                int priority = schedulerPriority(partitionStatus.getState());
                if (priority > selectedPriority) {
                    selectedPriority = priority;
                    selected = partitionStatus.getState();
                }
                if (!partitionStatus.getDetail().isEmpty() && priority >= 70) {
                    addError(partitionStatus.getDetail());
                }
            }
            schedulerState = selected == null ? "ACTIVE" : selected.name();
        }

        private static int schedulerPriority(TenantTtlScheduler.PartitionState state) {
            switch (state) {
                case BLOCKED:
                    return 100;
                case FAIL_CLOSED:
                    return 95;
                case REPLAN_REQUIRED:
                    return 90;
                case RETRY_PENDING:
                    return 80;
                case DROP_SKIPPED_REWRITE_IN_FLIGHT:
                    return 75;
                case REWRITE_RUNNING:
                    return 70;
                case WAITING_REPLICA:
                case WAITING_REWRITE:
                case WAITING_CATALOG_DROP:
                case WAITING_LOGICAL_PARTITION:
                    return 60;
                case FE_NOOP:
                    return 20;
                case IDLE:
                default:
                    return 10;
            }
        }

        private void addSnapshotError() {
            if (snapshotStatus == null) {
                return;
            }
            if (snapshotStatus.getLastFailureCode() != null && !snapshotStatus.getLastFailureCode().isEmpty()) {
                addError(snapshotStatus.getLastFailureCode() + ": " + snapshotStatus.getLastFailureMessage());
            } else {
                addError(snapshotStatus.getLastFailureMessage());
            }
        }

        private void addDictionaryError() {
            if (dictionary != null) {
                addError(dictionary.getRuntimeErrorMessage());
            }
        }

        private void addError(String error) {
            if (error == null || error.isEmpty()) {
                return;
            }
            errors.add(error.length() <= MAX_ERROR_COMPONENT_LENGTH ? error :
                    error.substring(0, MAX_ERROR_COMPONENT_LENGTH) + "...");
        }

        private String errorMessage() {
            if (errors.isEmpty()) {
                return null;
            }
            String joined = String.join("; ", errors);
            return joined.length() <= MAX_ERROR_MESSAGE_LENGTH ? joined :
                    joined.substring(0, MAX_ERROR_MESSAGE_LENGTH) + "...";
        }

        private List<String> toRow(String tenant) {
            List<String> row = new ArrayList<>();
            boolean active = bindingState == BindingState.ACTIVE;
            row.add(physicalTable);
            row.add(Boolean.toString(enabled));
            row.add(bindingState.name());
            row.add(configuredDictionaryName);
            row.add(dictionaryBinding == null ? null : Long.toString(dictionaryBinding.getDictionaryId()));
            row.add(configuredTableKey);
            row.add(configuredDefaultDays == null ? null : Integer.toString(configuredDefaultDays));
            row.add(dictionary == null ? null : nullableLong(dictionary.getLastSuccessVersion()));
            row.add(dictionary == null || dictionary.getLastSuccessVersion() <= 0 ? null :
                    nullableTimeMillis(dictionary.getLastSuccessFinishedTime()));
            row.add(snapshot == null ? null : Long.toString(snapshot.getDictionaryTxnId()));
            row.add(snapshot == null ? null : nullableTimeMillis(snapshot.getSnapshotTime().toEpochMilli()));
            row.add(snapshotStatus == null ? null : nullableLong(snapshotStatus.getLastAttemptTxnId()));
            row.add(snapshotStatus == null ? null : nullableTimeMillis(snapshotStatus.getLastAttemptTimeMillis()));
            row.add(tableKeyMatch.name());
            row.add(tableDefaultMatch.name());
            row.add(tableBinding == null ? null : tableBinding.getTenantColumnId());
            row.add(tableBinding == null ? null : Integer.toString(tableBinding.getTenantColumnUniqueId()));
            row.add(tableBinding == null ? null : tableBinding.getTimeColumnId());
            row.add(tableBinding == null ? null : Integer.toString(tableBinding.getTimeColumnUniqueId()));
            row.add(tableBinding == null ? null : tableBinding.getPartitionExpressionType());
            row.add(tableBinding == null ? null : tableBinding.getNormalizedTimeZone());
            row.add(active ? Long.toString(provablePhysicalPartitions) : null);
            row.add(active ? Long.toString(unprovablePhysicalPartitions) : null);
            row.add(schedulerState);
            row.add(active ? Long.toString(pendingRewritePartitions) : null);
            row.add(active ? Long.toString(runningReplicaTasks) : null);
            row.add(active ? Long.toString(completedPhysicalPartitions) : null);
            row.add(nullableTimeSeconds(nextExpiryEpochSeconds));
            row.add(snapshot == null ? null : Long.toString(snapshot.getIgnoredZeroRetentionRows()));
            row.add(errorMessage());
            if (tenant != null) {
                row.add(tenant);
                row.add(tenantResolution == null ||
                        tenantResolution.getType() == TenantTtlPolicyPlanner.ResolutionType.UNRESOLVED ? null :
                        Integer.toString(tenantResolution.getRetentionDays()));
                row.add(tenantResolution == null ? TenantTtlPolicyPlanner.ResolutionType.UNRESOLVED.name() :
                        tenantResolution.getType().name());
            }
            return Collections.unmodifiableList(row);
        }
    }
}
