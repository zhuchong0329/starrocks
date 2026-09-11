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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TenantTtlBindingAnalyzer;
import com.starrocks.catalog.TenantTtlDictionaryBinding;
import com.starrocks.catalog.TenantTtlTableBinding;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.tenantttl.TenantTtlPartitionBoundResolver;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;

/** Immutable table-scoped inputs and physical task specifications for one Tenant-TTL evaluation round. */
public final class TenantTtlEvaluationContext {
    public static final int PROTOCOL_VERSION = 1;
    private static final String TABLE_BINDING_DOMAIN = "tenant-ttl-table-binding-v1";
    private static final String PARTITION_BOUNDARY_DOMAIN = "tenant-ttl-partition-boundary-v1";
    private static final String REPLICA_TOPOLOGY_DOMAIN = "tenant-ttl-replica-topology-v1";
    private static final String REQUEST_DOMAIN = "tenant-ttl-request-v1";

    private final long dbId;
    private final long tableId;
    private final String tableName;
    private final TenantTtlDictionaryBinding dictionaryBinding;
    private final TenantTtlTableBinding tableBinding;
    private final String tableBindingFingerprint;
    private final TenantTtlPolicySnapshot snapshot;
    private final long evaluationTimeEpochSeconds;
    private final long baseIndexId;
    private final long schemaId;
    private final int schemaVersion;
    private final List<PartitionPlan> partitionPlans;

    private TenantTtlEvaluationContext(long dbId, long tableId, String tableName,
                                       TenantTtlDictionaryBinding dictionaryBinding,
                                       TenantTtlTableBinding tableBinding, String tableBindingFingerprint,
                                       TenantTtlPolicySnapshot snapshot, long evaluationTimeEpochSeconds,
                                       long baseIndexId, long schemaId, int schemaVersion,
                                       List<PartitionPlan> partitionPlans) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.dictionaryBinding = new TenantTtlDictionaryBinding(dictionaryBinding);
        this.tableBinding = new TenantTtlTableBinding(tableBinding);
        this.tableBindingFingerprint = tableBindingFingerprint;
        this.snapshot = snapshot;
        this.evaluationTimeEpochSeconds = evaluationTimeEpochSeconds;
        this.baseIndexId = baseIndexId;
        this.schemaId = schemaId;
        this.schemaVersion = schemaVersion;
        this.partitionPlans = Collections.unmodifiableList(new ArrayList<>(partitionPlans));
    }

    public static CaptureResult capture(GlobalStateMgr state, long dbId, long tableId) {
        return capture(state, dbId, tableId, () -> System.currentTimeMillis() / 1000L, state::getNextId);
    }

    public static CaptureResult capture(GlobalStateMgr state, long dbId, long tableId,
                                        LongSupplier epochSecondsSupplier, LongSupplier taskIdSupplier) {
        Objects.requireNonNull(state, "state is null");
        Database db = state.getLocalMetastore().getDb(dbId);
        if (db == null) {
            return CaptureResult.failure(CaptureFailure.TABLE_NOT_FOUND, "database does not exist");
        }
        Locker locker = new Locker();
        locker.lockDatabase(dbId, LockType.READ);
        try {
            Table table = db.getTable(tableId);
            if (!(table instanceof OlapTable)) {
                return CaptureResult.failure(CaptureFailure.TABLE_NOT_FOUND,
                        "Tenant-TTL table does not exist or is not an OLAP table");
            }
            OlapTable olapTable = (OlapTable) table;
            TableProperty property = olapTable.getTableProperty();
            TenantTtlDictionaryBinding dictionaryBinding = property == null ? null :
                    property.getTenantTtlDictionaryBinding();
            if (dictionaryBinding == null) {
                return CaptureResult.failure(CaptureFailure.NOT_BOUND, "Tenant-TTL Dictionary binding is missing");
            }
            TenantTtlPolicySnapshotManager snapshotManager = state.getTenantTtlPolicySnapshotManager();
            if (snapshotManager == null) {
                return CaptureResult.failure(CaptureFailure.SNAPSHOT_UNAVAILABLE,
                        "Tenant-TTL policy snapshot manager is not initialized");
            }
            Optional<TenantTtlPolicySnapshot> snapshot = snapshotManager
                    .getCurrentSnapshot(dictionaryBinding.getDictionaryId());
            if (!snapshot.isPresent()) {
                return CaptureResult.failure(CaptureFailure.SNAPSHOT_UNAVAILABLE,
                        "Tenant-TTL policy snapshot is not available");
            }
            return captureLocked(db, olapTable, snapshot.get(), epochSecondsSupplier, taskIdSupplier);
        } finally {
            locker.unLockDatabase(dbId, LockType.READ);
        }
    }

    static CaptureResult captureLocked(Database db, OlapTable table, TenantTtlPolicySnapshot snapshot,
                                       LongSupplier epochSecondsSupplier, LongSupplier taskIdSupplier) {
        try {
            TableProperty property = table.getTableProperty();
            TenantTtlDictionaryBinding dictionaryBinding = property == null ? null :
                    property.getTenantTtlDictionaryBinding();
            TenantTtlTableBinding tableBinding = property == null ? null : property.getTenantTtlTableBinding();
            if (dictionaryBinding == null || tableBinding == null) {
                return CaptureResult.failure(CaptureFailure.NOT_BOUND, "Tenant-TTL binding is incomplete");
            }
            TenantTtlBindingAnalyzer.TableBindingResult currentBinding =
                    TenantTtlBindingAnalyzer.analyzeTableBinding(db, table,
                            property.getCompactionRetentionTimeZone(), property.getProperties());
            if (!tableBinding.equals(currentBinding.getTableBinding())) {
                return CaptureResult.failure(CaptureFailure.BINDING_INVALID,
                        "persisted Tenant-TTL table binding does not match current metadata");
            }
            if (snapshot == null || snapshot.getDictionaryId() != dictionaryBinding.getDictionaryId() ||
                    snapshot.getDictionaryTxnId() <= 0) {
                return CaptureResult.failure(CaptureFailure.SNAPSHOT_UNAVAILABLE,
                        "policy snapshot does not match the bound Dictionary");
            }

            long evaluationTime = epochSecondsSupplier.getAsLong();
            if (evaluationTime <= 0) {
                return CaptureResult.failure(CaptureFailure.INVALID_EVALUATION_TIME,
                        "evaluation time must be a positive Unix epoch second");
            }
            long baseIndexId = table.getBaseIndexId();
            MaterializedIndexMeta schema = table.getIndexMetaByIndexId(baseIndexId);
            if (baseIndexId <= 0 || schema == null || schema.getSchemaId() <= 0 || schema.getSchemaVersion() < 0) {
                return CaptureResult.failure(CaptureFailure.BINDING_INVALID,
                        "base index schema identity is unavailable");
            }
            String tableFingerprint = tableBindingFingerprint(db.getId(), table.getId(), baseIndexId,
                    dictionaryBinding, tableBinding, schema.getSchemaId(), schema.getSchemaVersion());
            TenantTtlPolicyPlanner planner = TenantTtlPolicyPlanner.fromConfig();
            List<PhysicalPartition> physicalPartitions = new ArrayList<>(table.getPhysicalPartitions());
            physicalPartitions.sort(Comparator.comparingLong(PhysicalPartition::getId));
            List<PartitionPlan> plans = new ArrayList<>(physicalPartitions.size());
            Set<Long> allocatedTaskIds = new HashSet<>();
            for (PhysicalPartition physicalPartition : physicalPartitions) {
                plans.add(planPartition(db, table, physicalPartition, dictionaryBinding, tableBinding,
                        tableFingerprint, snapshot, evaluationTime, schema, planner, taskIdSupplier,
                        allocatedTaskIds));
            }
            return CaptureResult.success(new TenantTtlEvaluationContext(db.getId(), table.getId(), table.getName(),
                    dictionaryBinding, tableBinding, tableFingerprint, snapshot, evaluationTime,
                    baseIndexId, schema.getSchemaId(), schema.getSchemaVersion(), plans));
        } catch (DdlException | RuntimeException e) {
            return CaptureResult.failure(CaptureFailure.BINDING_INVALID,
                    e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
    }

    private static PartitionPlan planPartition(Database db, OlapTable table, PhysicalPartition physicalPartition,
                                               TenantTtlDictionaryBinding dictionaryBinding,
                                               TenantTtlTableBinding tableBinding, String tableFingerprint,
                                               TenantTtlPolicySnapshot snapshot, long evaluationTime,
                                               MaterializedIndexMeta schema, TenantTtlPolicyPlanner planner,
                                               LongSupplier taskIdSupplier, Set<Long> allocatedTaskIds) {
        Partition logicalPartition = table.getPartition(physicalPartition.getParentId());
        if (logicalPartition == null) {
            return PartitionPlan.failClosed(physicalPartition.getParentId(), physicalPartition.getId(), "",
                    "logical partition metadata is missing", "", "", null);
        }
        TenantTtlPartitionBoundResolver.Resolution boundary =
                TenantTtlPartitionBoundResolver.resolve(table, physicalPartition, tableBinding);
        String boundaryFingerprint = partitionBoundaryFingerprint(physicalPartition.getParentId(),
                physicalPartition.getId(), boundary);
        if (!boundary.isProvable()) {
            return PartitionPlan.failClosed(logicalPartition.getId(), physicalPartition.getId(),
                    logicalPartition.getName(), boundary.getReason() + ": " + boundary.getDetail(),
                    boundaryFingerprint, "", null);
        }

        TenantTtlPolicyPlanner.Plan policyPlan = planner.plan(snapshot,
                TenantTtlByteKey.utf8(dictionaryBinding.getTableKey()), dictionaryBinding.getDefaultDays(),
                boundary.getPartitionUpperEpochSecond(), evaluationTime);
        if (policyPlan.getType() == TenantTtlPolicyPlanner.PlanType.FAIL_CLOSED) {
            return PartitionPlan.failClosed(logicalPartition.getId(), physicalPartition.getId(),
                    logicalPartition.getName(), "policy plan failed closed: " + policyPlan.getFailReason(),
                    boundaryFingerprint, "", policyPlan);
        }
        if (policyPlan.getType() != TenantTtlPolicyPlanner.PlanType.ROWSET_REWRITE) {
            return new PartitionPlan(policyPlan.getType(), logicalPartition.getId(), physicalPartition.getId(),
                    logicalPartition.getName(), physicalPartition.getVisibleVersion(),
                    boundary.getPartitionUpperEpochSecond(), boundaryFingerprint, "", policyPlan,
                    Collections.emptyList(), "");
        }

        MaterializedIndex baseIndex = physicalPartition.getBaseIndex();
        if (baseIndex == null || baseIndex.getId() != table.getBaseIndexId()) {
            return PartitionPlan.failClosed(logicalPartition.getId(), physicalPartition.getId(),
                    logicalPartition.getName(), "base index identity changed during planning",
                    boundaryFingerprint, "", policyPlan);
        }
        List<ReplicaTopologyEntry> topology = new ArrayList<>();
        for (Tablet tablet : baseIndex.getTablets()) {
            if (!(tablet instanceof LocalTablet)) {
                return PartitionPlan.failClosed(logicalPartition.getId(), physicalPartition.getId(),
                        logicalPartition.getName(), "Tenant-TTL requires local tablets",
                        boundaryFingerprint, "", policyPlan);
            }
            for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                topology.add(new ReplicaTopologyEntry(physicalPartition.getId(), baseIndex.getId(), tablet.getId(),
                        replica.getId(), replica.getBackendId()));
            }
        }
        topology.sort(ReplicaTopologyEntry.ORDER);
        String topologyFingerprint = replicaTopologyFingerprint(topology);
        if (topology.isEmpty()) {
            return PartitionPlan.failClosed(logicalPartition.getId(), physicalPartition.getId(),
                    logicalPartition.getName(), "base index has no Catalog replicas",
                    boundaryFingerprint, topologyFingerprint, policyPlan);
        }

        List<ReplicaTaskSpec> tasks = new ArrayList<>(topology.size());
        for (ReplicaTopologyEntry entry : topology) {
            long taskId = taskIdSupplier.getAsLong();
            if (taskId <= 0 || !allocatedTaskIds.add(taskId)) {
                throw new IllegalStateException("task ID supplier returned a non-positive or duplicate ID");
            }
            String requestFingerprint = requestFingerprint(taskId, db.getId(), table.getId(), entry,
                    tableBinding.getTenantColumnUniqueId(), policyPlan, dictionaryBinding.getDictionaryId(),
                    snapshot.getDictionaryTxnId(), evaluationTime, schema.getSchemaId(), schema.getSchemaVersion(),
                    physicalPartition.getVisibleVersion(), tableFingerprint, policyPlan.getTablePolicyFingerprint(),
                    boundaryFingerprint, topologyFingerprint);
            tasks.add(new ReplicaTaskSpec(taskId, db.getId(), table.getId(), physicalPartition.getId(),
                    baseIndex.getId(), entry.tabletId, entry.replicaId, entry.backendId,
                    tableBinding.getTenantColumnUniqueId(), schema.getSchemaId(), schema.getSchemaVersion(),
                    physicalPartition.getVisibleVersion(), policyPlan.getFilterMode(), policyPlan.getTenants(),
                    dictionaryBinding.getDictionaryId(), snapshot.getDictionaryTxnId(), evaluationTime,
                    tableFingerprint, policyPlan.getTablePolicyFingerprint(), boundaryFingerprint,
                    topologyFingerprint, requestFingerprint));
        }
        return new PartitionPlan(TenantTtlPolicyPlanner.PlanType.ROWSET_REWRITE, logicalPartition.getId(),
                physicalPartition.getId(), logicalPartition.getName(), physicalPartition.getVisibleVersion(),
                boundary.getPartitionUpperEpochSecond(), boundaryFingerprint, topologyFingerprint,
                policyPlan, tasks, "");
    }

    public boolean validateForPublication(GlobalStateMgr state) {
        Database db = state.getLocalMetastore().getDb(dbId);
        if (db == null) {
            return false;
        }
        Locker locker = new Locker();
        locker.lockDatabase(dbId, LockType.READ);
        try {
            Table current = db.getTable(tableId);
            if (!(current instanceof OlapTable)) {
                return false;
            }
            OlapTable table = (OlapTable) current;
            TableProperty property = table.getTableProperty();
            if (property == null || property.getTenantTtlDictionaryBinding() == null ||
                    property.getTenantTtlTableBinding() == null) {
                return false;
            }
            TenantTtlBindingAnalyzer.TableBindingResult layout = TenantTtlBindingAnalyzer.analyzeTableBinding(
                    db, table, property.getCompactionRetentionTimeZone(), property.getProperties());
            if (!property.getTenantTtlTableBinding().equals(layout.getTableBinding())) {
                return false;
            }
            MaterializedIndexMeta currentSchema = table.getIndexMetaByIndexId(table.getBaseIndexId());
            if (currentSchema == null) {
                return false;
            }
            String currentFingerprint = tableBindingFingerprint(dbId, tableId, table.getBaseIndexId(),
                    property.getTenantTtlDictionaryBinding(), property.getTenantTtlTableBinding(),
                    currentSchema.getSchemaId(), currentSchema.getSchemaVersion());
            return tableBindingFingerprint.equals(currentFingerprint);
        } catch (DdlException | RuntimeException e) {
            return false;
        } finally {
            locker.unLockDatabase(dbId, LockType.READ);
        }
    }

    private static String tableBindingFingerprint(long dbId, long tableId, long baseIndexId,
                                                  TenantTtlDictionaryBinding dictionary,
                                                  TenantTtlTableBinding table, long schemaId, int schemaVersion) {
        Fingerprint fingerprint = new Fingerprint(TABLE_BINDING_DOMAIN);
        fingerprint.putLong(1, dbId);
        fingerprint.putLong(2, tableId);
        fingerprint.putLong(3, baseIndexId);
        fingerprint.putInt(4, dictionary.getFormatVersion());
        fingerprint.putLong(5, dictionary.getDictionaryId());
        fingerprint.putString(6, dictionary.getDictionaryName());
        fingerprint.putString(7, dictionary.getTableKey());
        fingerprint.putInt(8, dictionary.getDefaultDays());
        fingerprint.putInt(9, table.getFormatVersion());
        fingerprint.putString(10, table.getTenantColumnId());
        fingerprint.putInt(11, table.getTenantColumnUniqueId());
        fingerprint.putString(12, table.getTimeColumnId());
        fingerprint.putInt(13, table.getTimeColumnUniqueId());
        fingerprint.putString(14, table.getPartitionExpressionType());
        fingerprint.putInt(15, table.getListTimeComponentIndex());
        fingerprint.putString(16, table.getNormalizedTimeZone());
        fingerprint.putString(17, table.getPartitionExpressionFingerprint());
        fingerprint.putLong(18, schemaId);
        fingerprint.putInt(19, schemaVersion);
        return fingerprint.finishHex();
    }

    public static String partitionBoundaryFingerprint(long logicalPartitionId, long physicalPartitionId,
                                                      TenantTtlPartitionBoundResolver.Resolution resolution) {
        Fingerprint fingerprint = new Fingerprint(PARTITION_BOUNDARY_DOMAIN);
        fingerprint.putLong(1, logicalPartitionId);
        fingerprint.putLong(2, physicalPartitionId);
        fingerprint.putBoolean(3, resolution.isProvable());
        if (resolution.isProvable()) {
            fingerprint.putInt(4, resolution.getIntervals().size());
            int tag = 100;
            for (TenantTtlPartitionBoundResolver.TimeInterval interval : resolution.getIntervals()) {
                fingerprint.putBoolean(tag++, interval.isUnboundedBelow());
                if (!interval.isUnboundedBelow()) {
                    fingerprint.putLong(tag++, interval.getLowerInclusive());
                }
                fingerprint.putLong(tag++, interval.getUpperExclusive());
            }
        } else {
            fingerprint.putString(5, resolution.getReason().name());
        }
        return fingerprint.finishHex();
    }

    public static String replicaTopologyFingerprint(List<ReplicaTopologyEntry> entries) {
        List<ReplicaTopologyEntry> sorted = new ArrayList<>(entries);
        sorted.sort(ReplicaTopologyEntry.ORDER);
        Fingerprint fingerprint = new Fingerprint(REPLICA_TOPOLOGY_DOMAIN);
        fingerprint.putInt(1, sorted.size());
        int tag = 100;
        for (ReplicaTopologyEntry entry : sorted) {
            fingerprint.putLong(tag++, entry.physicalPartitionId);
            fingerprint.putLong(tag++, entry.baseIndexId);
            fingerprint.putLong(tag++, entry.tabletId);
            fingerprint.putLong(tag++, entry.replicaId);
            fingerprint.putLong(tag++, entry.backendId);
        }
        return fingerprint.finishHex();
    }

    private static String requestFingerprint(long taskId, long dbId, long tableId, ReplicaTopologyEntry entry,
                                             int tenantColumnUniqueId, TenantTtlPolicyPlanner.Plan plan,
                                             long dictionaryId, long snapshotTxnId, long evaluationTime,
                                             long schemaId, int schemaVersion, long observedVersion,
                                             String tableFingerprint, String policyFingerprint,
                                             String boundaryFingerprint, String topologyFingerprint) {
        Fingerprint fingerprint = new Fingerprint(REQUEST_DOMAIN);
        fingerprint.putInt(1, PROTOCOL_VERSION);
        fingerprint.putLong(2, taskId);
        fingerprint.putLong(3, dbId);
        fingerprint.putLong(4, tableId);
        fingerprint.putLong(5, entry.physicalPartitionId);
        fingerprint.putLong(6, entry.baseIndexId);
        fingerprint.putLong(7, entry.tabletId);
        fingerprint.putLong(8, entry.replicaId);
        fingerprint.putLong(9, entry.backendId);
        fingerprint.putInt(10, tenantColumnUniqueId);
        fingerprint.putString(11, plan.getFilterMode().name());
        fingerprint.putInt(12, plan.getTenants().size());
        int tenantTag = 100;
        for (TenantTtlByteKey tenant : plan.getTenants()) {
            fingerprint.putBytes(tenantTag++, tenant.copyBytes());
        }
        fingerprint.putLong(13, dictionaryId);
        fingerprint.putLong(14, snapshotTxnId);
        fingerprint.putLong(15, evaluationTime);
        fingerprint.putLong(16, schemaId);
        fingerprint.putInt(17, schemaVersion);
        fingerprint.putLong(18, observedVersion);
        fingerprint.putString(19, tableFingerprint);
        fingerprint.putString(20, policyFingerprint);
        fingerprint.putString(21, boundaryFingerprint);
        fingerprint.putString(22, topologyFingerprint);
        return fingerprint.finishHex();
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public TenantTtlDictionaryBinding getDictionaryBinding() {
        return new TenantTtlDictionaryBinding(dictionaryBinding);
    }

    public TenantTtlTableBinding getTableBinding() {
        return new TenantTtlTableBinding(tableBinding);
    }

    public String getTableBindingFingerprint() {
        return tableBindingFingerprint;
    }

    public TenantTtlPolicySnapshot getSnapshot() {
        return snapshot;
    }

    public long getDictionaryId() {
        return snapshot.getDictionaryId();
    }

    public long getSnapshotTxnId() {
        return snapshot.getDictionaryTxnId();
    }

    public long getEvaluationTimeEpochSeconds() {
        return evaluationTimeEpochSeconds;
    }

    public long getBaseIndexId() {
        return baseIndexId;
    }

    public long getSchemaId() {
        return schemaId;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public List<PartitionPlan> getPartitionPlans() {
        return partitionPlans;
    }

    public enum CaptureFailure {
        TABLE_NOT_FOUND,
        NOT_BOUND,
        SNAPSHOT_UNAVAILABLE,
        BINDING_INVALID,
        INVALID_EVALUATION_TIME
    }

    public static final class CaptureResult {
        private final TenantTtlEvaluationContext context;
        private final CaptureFailure failure;
        private final String detail;

        private CaptureResult(TenantTtlEvaluationContext context, CaptureFailure failure, String detail) {
            this.context = context;
            this.failure = failure;
            this.detail = detail;
        }

        private static CaptureResult success(TenantTtlEvaluationContext context) {
            return new CaptureResult(context, null, "");
        }

        private static CaptureResult failure(CaptureFailure failure, String detail) {
            return new CaptureResult(null, failure, detail == null ? "" : detail);
        }

        public boolean isSuccess() {
            return context != null;
        }

        public TenantTtlEvaluationContext getContext() {
            if (context == null) {
                throw new IllegalStateException("evaluation context capture failed: " + detail);
            }
            return context;
        }

        public CaptureFailure getFailure() {
            return failure;
        }

        public String getDetail() {
            return detail;
        }
    }

    public static final class ReplicaTopologyEntry {
        private static final Comparator<ReplicaTopologyEntry> ORDER = Comparator
                .comparingLong(ReplicaTopologyEntry::getTabletId)
                .thenComparingLong(ReplicaTopologyEntry::getBackendId)
                .thenComparingLong(ReplicaTopologyEntry::getReplicaId)
                .thenComparingLong(ReplicaTopologyEntry::getPhysicalPartitionId)
                .thenComparingLong(ReplicaTopologyEntry::getBaseIndexId);

        private final long physicalPartitionId;
        private final long baseIndexId;
        private final long tabletId;
        private final long replicaId;
        private final long backendId;

        public ReplicaTopologyEntry(long physicalPartitionId, long baseIndexId, long tabletId,
                                    long replicaId, long backendId) {
            this.physicalPartitionId = physicalPartitionId;
            this.baseIndexId = baseIndexId;
            this.tabletId = tabletId;
            this.replicaId = replicaId;
            this.backendId = backendId;
        }

        public long getPhysicalPartitionId() {
            return physicalPartitionId;
        }

        public long getBaseIndexId() {
            return baseIndexId;
        }

        public long getTabletId() {
            return tabletId;
        }

        public long getReplicaId() {
            return replicaId;
        }

        public long getBackendId() {
            return backendId;
        }
    }

    public static final class PartitionPlan {
        private final TenantTtlPolicyPlanner.PlanType type;
        private final long logicalPartitionId;
        private final long physicalPartitionId;
        private final String logicalPartitionName;
        private final long observedVisibleVersion;
        private final long partitionUpperEpochSecond;
        private final String boundaryFingerprint;
        private final String topologyFingerprint;
        private final TenantTtlPolicyPlanner.Plan policyPlan;
        private final List<ReplicaTaskSpec> replicaTasks;
        private final String failureDetail;

        private PartitionPlan(TenantTtlPolicyPlanner.PlanType type, long logicalPartitionId,
                              long physicalPartitionId, String logicalPartitionName,
                              long observedVisibleVersion, long partitionUpperEpochSecond,
                              String boundaryFingerprint, String topologyFingerprint,
                              TenantTtlPolicyPlanner.Plan policyPlan, List<ReplicaTaskSpec> replicaTasks,
                              String failureDetail) {
            this.type = type;
            this.logicalPartitionId = logicalPartitionId;
            this.physicalPartitionId = physicalPartitionId;
            this.logicalPartitionName = logicalPartitionName;
            this.observedVisibleVersion = observedVisibleVersion;
            this.partitionUpperEpochSecond = partitionUpperEpochSecond;
            this.boundaryFingerprint = boundaryFingerprint;
            this.topologyFingerprint = topologyFingerprint;
            this.policyPlan = policyPlan;
            this.replicaTasks = Collections.unmodifiableList(new ArrayList<>(replicaTasks));
            this.failureDetail = failureDetail;
        }

        private static PartitionPlan failClosed(long logicalPartitionId, long physicalPartitionId,
                                                String logicalPartitionName, String detail,
                                                String boundaryFingerprint, String topologyFingerprint,
                                                TenantTtlPolicyPlanner.Plan policyPlan) {
            return new PartitionPlan(TenantTtlPolicyPlanner.PlanType.FAIL_CLOSED, logicalPartitionId,
                    physicalPartitionId, logicalPartitionName, -1, 0, boundaryFingerprint,
                    topologyFingerprint, policyPlan, Collections.emptyList(), detail);
        }

        public TenantTtlPolicyPlanner.PlanType getType() {
            return type;
        }

        public long getLogicalPartitionId() {
            return logicalPartitionId;
        }

        public long getPhysicalPartitionId() {
            return physicalPartitionId;
        }

        public String getLogicalPartitionName() {
            return logicalPartitionName;
        }

        public long getObservedVisibleVersion() {
            return observedVisibleVersion;
        }

        public long getPartitionUpperEpochSecond() {
            return partitionUpperEpochSecond;
        }

        public String getBoundaryFingerprint() {
            return boundaryFingerprint;
        }

        public String getTopologyFingerprint() {
            return topologyFingerprint;
        }

        public TenantTtlPolicyPlanner.Plan getPolicyPlan() {
            return policyPlan;
        }

        public List<ReplicaTaskSpec> getReplicaTasks() {
            return replicaTasks;
        }

        public String getFailureDetail() {
            return failureDetail;
        }
    }

    public static final class ReplicaTaskSpec {
        private final long taskId;
        private final long dbId;
        private final long tableId;
        private final long physicalPartitionId;
        private final long indexId;
        private final long tabletId;
        private final long replicaId;
        private final long backendId;
        private final int tenantColumnUniqueId;
        private final long schemaId;
        private final int schemaVersion;
        private final long observedMaxVersion;
        private final TenantTtlPolicyPlanner.FilterMode filterMode;
        private final List<TenantTtlByteKey> tenants;
        private final long dictionaryId;
        private final long snapshotTxnId;
        private final long evaluationTimeEpochSeconds;
        private final String tableBindingFingerprint;
        private final String tablePolicyFingerprint;
        private final String partitionBoundaryFingerprint;
        private final String replicaTopologyFingerprint;
        private final String requestFingerprint;

        private ReplicaTaskSpec(long taskId, long dbId, long tableId, long physicalPartitionId, long indexId,
                                long tabletId, long replicaId, long backendId, int tenantColumnUniqueId,
                                long schemaId, int schemaVersion, long observedMaxVersion,
                                TenantTtlPolicyPlanner.FilterMode filterMode, List<TenantTtlByteKey> tenants,
                                long dictionaryId, long snapshotTxnId, long evaluationTimeEpochSeconds,
                                String tableBindingFingerprint, String tablePolicyFingerprint,
                                String partitionBoundaryFingerprint, String replicaTopologyFingerprint,
                                String requestFingerprint) {
            this.taskId = taskId;
            this.dbId = dbId;
            this.tableId = tableId;
            this.physicalPartitionId = physicalPartitionId;
            this.indexId = indexId;
            this.tabletId = tabletId;
            this.replicaId = replicaId;
            this.backendId = backendId;
            this.tenantColumnUniqueId = tenantColumnUniqueId;
            this.schemaId = schemaId;
            this.schemaVersion = schemaVersion;
            this.observedMaxVersion = observedMaxVersion;
            this.filterMode = filterMode;
            this.tenants = Collections.unmodifiableList(new ArrayList<>(tenants));
            this.dictionaryId = dictionaryId;
            this.snapshotTxnId = snapshotTxnId;
            this.evaluationTimeEpochSeconds = evaluationTimeEpochSeconds;
            this.tableBindingFingerprint = tableBindingFingerprint;
            this.tablePolicyFingerprint = tablePolicyFingerprint;
            this.partitionBoundaryFingerprint = partitionBoundaryFingerprint;
            this.replicaTopologyFingerprint = replicaTopologyFingerprint;
            this.requestFingerprint = requestFingerprint;
        }

        public long getTaskId() {
            return taskId;
        }

        public long getDbId() {
            return dbId;
        }

        public long getTableId() {
            return tableId;
        }

        public long getPhysicalPartitionId() {
            return physicalPartitionId;
        }

        public long getIndexId() {
            return indexId;
        }

        public long getTabletId() {
            return tabletId;
        }

        public long getReplicaId() {
            return replicaId;
        }

        public long getBackendId() {
            return backendId;
        }

        public int getTenantColumnUniqueId() {
            return tenantColumnUniqueId;
        }

        public long getSchemaId() {
            return schemaId;
        }

        public int getSchemaVersion() {
            return schemaVersion;
        }

        public long getObservedMaxVersion() {
            return observedMaxVersion;
        }

        public TenantTtlPolicyPlanner.FilterMode getFilterMode() {
            return filterMode;
        }

        public List<TenantTtlByteKey> getTenants() {
            return tenants;
        }

        public long getDictionaryId() {
            return dictionaryId;
        }

        public long getSnapshotTxnId() {
            return snapshotTxnId;
        }

        public long getEvaluationTimeEpochSeconds() {
            return evaluationTimeEpochSeconds;
        }

        public String getTableBindingFingerprint() {
            return tableBindingFingerprint;
        }

        public String getTablePolicyFingerprint() {
            return tablePolicyFingerprint;
        }

        public String getPartitionBoundaryFingerprint() {
            return partitionBoundaryFingerprint;
        }

        public String getReplicaTopologyFingerprint() {
            return replicaTopologyFingerprint;
        }

        public String getRequestFingerprint() {
            return requestFingerprint;
        }
    }

    private static final class Fingerprint {
        private final MessageDigest digest;

        private Fingerprint(String domain) {
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 is unavailable", e);
            }
            putString(0, domain);
        }

        private void putBoolean(int tag, boolean value) {
            putBytes(tag, new byte[] {(byte) (value ? 1 : 0)});
        }

        private void putInt(int tag, int value) {
            putBytes(tag, new byte[] {
                    (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value});
        }

        private void putLong(int tag, long value) {
            putBytes(tag, new byte[] {
                    (byte) (value >>> 56), (byte) (value >>> 48), (byte) (value >>> 40), (byte) (value >>> 32),
                    (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value});
        }

        private void putString(int tag, String value) {
            putBytes(tag, value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8));
        }

        private void putBytes(int tag, byte[] value) {
            updateInt(tag);
            updateInt(value.length);
            digest.update(value);
        }

        private void updateInt(int value) {
            digest.update((byte) (value >>> 24));
            digest.update((byte) (value >>> 16));
            digest.update((byte) (value >>> 8));
            digest.update((byte) value);
        }

        private String finishHex() {
            return TenantTtlPolicyFingerprint.toHex(digest.digest());
        }
    }
}
