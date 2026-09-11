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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Dictionary;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TenantTtlDictionaryBinding;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowTenantTtlStatusStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.tenantttl.TenantTtlStatusService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TenantTtlStatusServiceTest {
    private static final String DB_NAME = "tenant_ttl_status_test";
    private static final String POLICY_TABLE = "tenant_ttl_policy";
    private static final String ACTIVE_DICTIONARY = "tenant_ttl_status_active_dict";
    private static final String WAITING_DICTIONARY = "tenant_ttl_status_waiting_dict";
    private static final String SNAPSHOT_DICTIONARY = "tenant_ttl_status_snapshot_dict";
    private static final String TABLE_KEY = "business.http_log";
    private static final long ACTIVE_DICTIONARY_ID = 98001L;
    private static final long WAITING_DICTIONARY_ID = 98002L;
    private static final long SNAPSHOT_DICTIONARY_ID = 98003L;
    private static final long DICTIONARY_TXN_ID = 151L;
    private static final long SNAPSHOT_TXN_ID = 149L;

    private static GlobalStateMgr state;
    private static Database db;
    private static ConnectContext context;
    private static TenantTtlPolicySnapshotManager originalSnapshotManager;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        state = GlobalStateMgr.getCurrentState();
        context = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(context);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        starRocksAssert.withTable("CREATE TABLE " + DB_NAME + "." + POLICY_TABLE + " (" +
                "tenant VARCHAR(128) NOT NULL, table_name VARCHAR(256) NOT NULL, retention_days INT NOT NULL" +
                ") PRIMARY KEY(tenant, table_name) DISTRIBUTED BY HASH(tenant) BUCKETS 1 " +
                "PROPERTIES('replication_num'='1')");

        Dictionary active = dictionary(ACTIVE_DICTIONARY_ID, ACTIVE_DICTIONARY);
        state.getDictionaryMgr().addDictionary(active);
        active.setFinished(1700000000000L, DICTIONARY_TXN_ID);
        state.getDictionaryMgr().addDictionary(dictionary(WAITING_DICTIONARY_ID, WAITING_DICTIONARY));
        Dictionary snapshotWaiting = dictionary(SNAPSHOT_DICTIONARY_ID, SNAPSHOT_DICTIONARY);
        state.getDictionaryMgr().addDictionary(snapshotWaiting);
        snapshotWaiting.setFinished(1700000100000L, 88L);

        starRocksAssert.withTable(disabledTableSql("disabled_log"));
        starRocksAssert.withTable(enabledTableSql("active_log", ACTIVE_DICTIONARY, TABLE_KEY));
        starRocksAssert.withTable(enabledTableSql("property_default_log", ACTIVE_DICTIONARY,
                "business.not_configured"));
        starRocksAssert.withTable(maxValueTableSql("unprovable_log"));
        starRocksAssert.withTable(enabledTableSql("waiting_dictionary_log", WAITING_DICTIONARY, TABLE_KEY));
        starRocksAssert.withTable(enabledTableSql("waiting_snapshot_log", SNAPSHOT_DICTIONARY, TABLE_KEY));
        starRocksAssert.withTable(enabledTableSql("paused_log", ACTIVE_DICTIONARY, TABLE_KEY));
        starRocksAssert.withTable(enabledTableSql("invalid_log", ACTIVE_DICTIONARY, TABLE_KEY));

        db = state.getLocalMetastore().getDb(DB_NAME);
        OlapTable paused = table("paused_log");
        paused.getTableProperty().setTenantTtlDictionaryBinding(new TenantTtlDictionaryBinding(
                999999L, ACTIVE_DICTIONARY, TABLE_KEY, 180));
        OlapTable invalid = table("invalid_log");
        invalid.getTableProperty().setTenantTtlDictionaryBinding(new TenantTtlDictionaryBinding(
                ACTIVE_DICTIONARY_ID, ACTIVE_DICTIONARY, "business.wrong", 180));

        originalSnapshotManager = state.getTenantTtlPolicySnapshotManager();
        Deencapsulation.setField(state, "tenantTtlPolicySnapshotManager",
                new FixedSnapshotManager(activeSnapshot()));
    }

    @AfterAll
    public static void afterClass() {
        if (state != null && originalSnapshotManager != null) {
            Deencapsulation.setField(state, "tenantTtlPolicySnapshotManager", originalSnapshotManager);
        }
    }

    @Test
    public void testAnalyzerNormalizesAndRejectsMissingObjects() {
        ShowTenantTtlStatusStmt statement = parse("SHOW TENANT TTL STATUS FROM active_log");
        Analyzer.analyze(statement, context);
        Assertions.assertEquals(DB_NAME, statement.getTableName().getDb());
        Assertions.assertThrows(SemanticException.class, () -> Analyzer.analyze(
                parse("SHOW TENANT TTL STATUS FROM missing_db.missing_table"), context));
        Assertions.assertThrows(SemanticException.class, () -> Analyzer.analyze(
                parse("SHOW TENANT TTL STATUS FROM " + DB_NAME + ".missing_table"), context));
    }

    @Test
    public void testBindingStateMatrixAndNullSemantics() {
        Map<String, String> disabled = status("disabled_log", null);
        Assertions.assertEquals("false", disabled.get("Enabled"));
        Assertions.assertEquals("DISABLED", disabled.get("BindingState"));
        Assertions.assertNull(disabled.get("SnapshotTxnId"));

        Map<String, String> waitingDictionary = status("waiting_dictionary_log", null);
        Assertions.assertEquals("WAITING_DICTIONARY", waitingDictionary.get("BindingState"));
        Assertions.assertNull(waitingDictionary.get("DictionaryLastSuccessTxnId"));
        Assertions.assertNull(waitingDictionary.get("SnapshotTxnId"));
        Assertions.assertEquals("UNKNOWN", waitingDictionary.get("TableKeyMatch"));

        Map<String, String> waitingSnapshot = status("waiting_snapshot_log", null);
        Assertions.assertEquals("WAITING_POLICY_SNAPSHOT", waitingSnapshot.get("BindingState"));
        Assertions.assertEquals("88", waitingSnapshot.get("DictionaryLastSuccessTxnId"));
        Assertions.assertNull(waitingSnapshot.get("SnapshotTxnId"));

        Map<String, String> paused = status("paused_log", null);
        Assertions.assertEquals("PAUSED", paused.get("BindingState"));
        Assertions.assertTrue(paused.get("ErrorMessage").contains("same-name Dictionary now has ID"));

        Map<String, String> invalid = status("invalid_log", null);
        Assertions.assertEquals("INVALID", invalid.get("BindingState"));
        Assertions.assertTrue(invalid.get("ErrorMessage").contains("does not match persistent"));
    }

    @Test
    public void testActiveDoubleWatermarksAndTenantResolution() {
        Map<String, String> override = status("active_log", "tenant_a");
        Assertions.assertEquals("ACTIVE", override.get("BindingState"));
        Assertions.assertEquals(Long.toString(DICTIONARY_TXN_ID),
                override.get("DictionaryLastSuccessTxnId"));
        Assertions.assertEquals(Long.toString(SNAPSHOT_TXN_ID), override.get("SnapshotTxnId"));
        Assertions.assertEquals("MATCHED", override.get("TableKeyMatch"));
        Assertions.assertEquals("MATCHED", override.get("TableDefaultMatch"));
        Assertions.assertEquals("30", override.get("EffectiveRetentionDays"));
        Assertions.assertEquals("TENANT_OVERRIDE", override.get("ResolutionType"));
        Assertions.assertEquals("7", override.get("IgnoredZeroRows"));
        Assertions.assertEquals("1", override.get("ProvablePhysicalPartitions"));
        Assertions.assertEquals(Long.toString(DICTIONARY_TXN_ID),
                override.get("LastSnapshotAttemptTxnId"));
        Assertions.assertTrue(override.get("ErrorMessage").contains("TENANT_TTL_POLICY_RPC_ERROR"));

        Map<String, String> tableDefault = status("active_log", "tenant_b");
        Assertions.assertEquals("180", tableDefault.get("EffectiveRetentionDays"));
        Assertions.assertEquals("TABLE_DEFAULT", tableDefault.get("ResolutionType"));

        Map<String, String> propertyDefault = status("property_default_log", "tenant_b");
        Assertions.assertEquals("NOT_FOUND_USE_DEFAULT", propertyDefault.get("TableKeyMatch"));
        Assertions.assertEquals("NOT_FOUND", propertyDefault.get("TableDefaultMatch"));
        Assertions.assertEquals("180", propertyDefault.get("EffectiveRetentionDays"));
        Assertions.assertEquals("PROPERTY_DEFAULT", propertyDefault.get("ResolutionType"));

        Map<String, String> unprovable = status("unprovable_log", null);
        Assertions.assertEquals("0", unprovable.get("ProvablePhysicalPartitions"));
        Assertions.assertEquals("1", unprovable.get("UnprovablePhysicalPartitions"));
        Assertions.assertTrue(unprovable.get("ErrorMessage").contains("MAXVALUE_UPPER_BOUND"));
    }

    @Test
    public void testShowExecutorReturnsOneBoundedRow() {
        ShowTenantTtlStatusStmt statement = parse(
                "SHOW TENANT TTL STATUS FROM " + DB_NAME + ".active_log FOR TENANT 'tenant_a'");
        Analyzer.analyze(statement, context);
        ShowResultSet result = ShowExecutor.execute(statement, context);
        Assertions.assertEquals(1, result.getResultRows().size());
        Assertions.assertEquals(ShowTenantTtlStatusStmt.BASE_TITLE_NAMES.size() +
                ShowTenantTtlStatusStmt.TENANT_TITLE_NAMES.size(), result.numColumns());
        Assertions.assertEquals("ACTIVE", result.getResultRows().get(0).get(2));
        Assertions.assertEquals("tenant_a", result.getResultRows().get(0).get(30));
        Assertions.assertEquals("30", result.getResultRows().get(0).get(31));
    }

    private static Dictionary dictionary(long id, String name) {
        return new Dictionary(id, name, POLICY_TABLE, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME,
                ImmutableList.of("tenant", "table_name"), ImmutableList.of("retention_days"), new HashMap<>());
    }

    private static TenantTtlPolicySnapshot activeSnapshot() {
        Map<TenantTtlByteKey, Integer> overrides = new HashMap<>();
        overrides.put(TenantTtlByteKey.utf8("tenant_a"), 30);
        Map<TenantTtlByteKey, TablePolicy> policies = new HashMap<>();
        policies.put(TenantTtlByteKey.utf8(TABLE_KEY), new TablePolicy(180, overrides));
        return new TenantTtlPolicySnapshot(ACTIVE_DICTIONARY_ID, ACTIVE_DICTIONARY, SNAPSHOT_TXN_ID,
                Instant.ofEpochMilli(1700000200000L), 7, 1024, 2048, policies);
    }

    private static String disabledTableSql(String name) {
        return "CREATE TABLE " + DB_NAME + "." + name +
                " (tenant VARCHAR(128), recordTimestamp BIGINT NOT NULL) " +
                "DUPLICATE KEY(tenant, recordTimestamp) PARTITION BY RANGE(recordTimestamp) " +
                "(PARTITION p0 VALUES LESS THAN ('100')) DISTRIBUTED BY HASH(tenant) BUCKETS 1 " +
                "PROPERTIES('replication_num'='1')";
    }

    private static String enabledTableSql(String name, String dictionaryName, String tableKey) {
        return disabledTableSql(name).replace("PROPERTIES('replication_num'='1')",
                "PROPERTIES('replication_num'='1', 'compaction_retention_condition'=" +
                        "\"dictionary_ttl('" + dictionaryName + "', '" + tableKey + "', 180)\")");
    }

    private static String maxValueTableSql(String name) {
        return "CREATE TABLE " + DB_NAME + "." + name +
                " (tenant VARCHAR(128), recordTimestamp BIGINT NOT NULL) " +
                "DUPLICATE KEY(tenant, recordTimestamp) PARTITION BY RANGE(recordTimestamp) " +
                "(PARTITION p0 VALUES LESS THAN (MAXVALUE)) DISTRIBUTED BY HASH(tenant) BUCKETS 1 " +
                "PROPERTIES('replication_num'='1', 'compaction_retention_condition'=" +
                "\"dictionary_ttl('" + ACTIVE_DICTIONARY + "', '" + TABLE_KEY + "', 180)\")";
    }

    private static OlapTable table(String name) {
        return (OlapTable) db.getTable(name);
    }

    private static ShowTenantTtlStatusStmt parse(String sql) {
        return (ShowTenantTtlStatusStmt) SqlParser.parseSingleStatement(sql, 0);
    }

    private static Map<String, String> status(String tableName, String tenant) {
        List<String> row = TenantTtlStatusService.buildRow(state, db, table(tableName), tenant);
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < ShowTenantTtlStatusStmt.BASE_TITLE_NAMES.size(); ++i) {
            result.put(ShowTenantTtlStatusStmt.BASE_TITLE_NAMES.get(i), row.get(i));
        }
        if (tenant != null) {
            int offset = ShowTenantTtlStatusStmt.BASE_TITLE_NAMES.size();
            for (int i = 0; i < ShowTenantTtlStatusStmt.TENANT_TITLE_NAMES.size(); ++i) {
                result.put(ShowTenantTtlStatusStmt.TENANT_TITLE_NAMES.get(i), row.get(offset + i));
            }
        }
        return result;
    }

    private static final class FixedSnapshotManager extends TenantTtlPolicySnapshotManager {
        private final TenantTtlPolicySnapshot snapshot;

        private FixedSnapshotManager(TenantTtlPolicySnapshot snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public synchronized Optional<TenantTtlPolicySnapshot> getCurrentSnapshot(long dictionaryId) {
            return dictionaryId == snapshot.getDictionaryId() ? Optional.of(snapshot) : Optional.empty();
        }

        @Override
        public synchronized SnapshotStatus getStatus(long dictionaryId) {
            if (dictionaryId == ACTIVE_DICTIONARY_ID) {
                return new SnapshotStatus(1, 3, SNAPSHOT_TXN_ID, DICTIONARY_TXN_ID, 0,
                        DICTIONARY_TXN_ID, 1700000300000L, 1, 1700000400000L,
                        "TENANT_TTL_POLICY_RPC_ERROR", "transient export failure", 1700000300000L);
            }
            if (dictionaryId == SNAPSHOT_DICTIONARY_ID) {
                return new SnapshotStatus(1, 1, 0, 88, 0, 88, 1700000300000L,
                        1, 1700000400000L, "TENANT_TTL_POLICY_CACHE_NOT_FOUND",
                        "Dictionary Cache is unavailable", 1700000300000L);
            }
            return super.getStatus(dictionaryId);
        }
    }
}
