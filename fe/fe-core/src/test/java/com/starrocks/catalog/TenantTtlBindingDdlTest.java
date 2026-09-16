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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableList;
import com.starrocks.alter.AlterJobException;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.tenantttl.TenantTtlPartitionBoundResolver;
import com.starrocks.tenantttl.policy.TenantTtlPolicySnapshotManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TenantTtlBindingDdlTest {
    private static final String DB_NAME = "tenant_ttl_binding_test";
    private static final String DICTIONARY_NAME = "tenant_ttl_binding_dict";
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        starRocksAssert.withTable("CREATE TABLE " + DB_NAME + ".tenant_ttl_policy (\n" +
                "tenant VARCHAR(128) NOT NULL,\n" +
                "table_name VARCHAR(256) NOT NULL,\n" +
                "retention_days INT NOT NULL\n" +
                ") PRIMARY KEY(tenant, table_name)\n" +
                "DISTRIBUTED BY HASH(tenant) BUCKETS 1\n" +
                "PROPERTIES('replication_num' = '1')");
        GlobalStateMgr.getCurrentState().getDictionaryMgr().addDictionary(new Dictionary(
                91001L, DICTIONARY_NAME, "tenant_ttl_policy",
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME,
                ImmutableList.of("tenant", "table_name"), ImmutableList.of("retention_days"), new HashMap<>()));
    }

    @Test
    public void testCreateAndAlterPersistBindings() throws Exception {
        starRocksAssert.withTable("CREATE TABLE " + DB_NAME + ".tenant_ttl_range (\n" +
                "tenant VARCHAR(128) NULL,\n" +
                "recordTimestamp BIGINT NOT NULL,\n" +
                "payload VARCHAR(64) NULL\n" +
                ") DUPLICATE KEY(tenant, recordTimestamp)\n" +
                "PARTITION BY RANGE(recordTimestamp) (PARTITION p0 VALUES LESS THAN ('100'))\n" +
                "DISTRIBUTED BY HASH(tenant) BUCKETS 1\n" +
                "PROPERTIES(\n" +
                "'replication_num' = '1',\n" +
                "'compaction_retention_condition' = \"dictionary_ttl('" + DICTIONARY_NAME +
                "', 'business.http_log', 180)\")");

        OlapTable table = getTable("tenant_ttl_range");
        TenantTtlDictionaryBinding dictionaryBinding = table.getTableProperty().getTenantTtlDictionaryBinding();
        TenantTtlTableBinding tableBinding = table.getTableProperty().getTenantTtlTableBinding();
        Assertions.assertEquals(91001L, dictionaryBinding.getDictionaryId());
        Assertions.assertEquals("business.http_log", dictionaryBinding.getTableKey());
        Assertions.assertEquals(180, dictionaryBinding.getDefaultDays());
        Assertions.assertEquals(TenantTtlBindingAnalyzer.RANGE_DIRECT_UNIX_SECONDS,
                tableBinding.getPartitionExpressionType());
        Assertions.assertEquals(TenantTtlBindingAnalyzer.NOT_APPLICABLE_TIME_ZONE,
                tableBinding.getNormalizedTimeZone());
        Assertions.assertEquals(table.getColumn("tenant").getUniqueId(), tableBinding.getTenantColumnUniqueId());
        Assertions.assertEquals("dictionary_ttl('" + DICTIONARY_NAME + "', 'business.http_log', 180)",
                table.getProperties().get(PropertyAnalyzer.PROPERTIES_COMPACTION_RETENTION_CONDITION));
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getTenantTtlPolicySnapshotManager()
                .getReferencedTables(91001L)
                .contains(new TenantTtlPolicySnapshotManager.TableRef(
                        GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME).getId(), table.getId())));

        GlobalStateMgr.getCurrentState().getDictionaryMgr().addDictionary(new Dictionary(
                91002L, DICTIONARY_NAME, "tenant_ttl_policy",
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME,
                ImmutableList.of("tenant", "table_name"), ImmutableList.of("retention_days"), new HashMap<>()));
        starRocksAssert.alterTableProperties("ALTER TABLE tenant_ttl_range SET (" +
                "'compaction_retention_condition' = \"dictionary_ttl('" + DICTIONARY_NAME +
                "', 'business.http_log', 180)\")");
        Assertions.assertEquals(91002L,
                table.getTableProperty().getTenantTtlDictionaryBinding().getDictionaryId());
        Assertions.assertFalse(GlobalStateMgr.getCurrentState().getTenantTtlPolicySnapshotManager()
                .getReferencedTables(91001L)
                .contains(new TenantTtlPolicySnapshotManager.TableRef(
                        GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME).getId(), table.getId())));
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getTenantTtlPolicySnapshotManager()
                .isReferenced(91002L));
    }

    @Test
    public void testFromUnixTimeRequiresAndNormalizesTimeZone() throws Exception {
        Assertions.assertThrows(DdlException.class, () -> starRocksAssert.withTable(
                fromUnixTimeTableSql("tenant_ttl_missing_tz", null)));

        starRocksAssert.withTable(fromUnixTimeTableSql("tenant_ttl_from_unixtime", "CST"));
        OlapTable table = getTable("tenant_ttl_from_unixtime");
        Assertions.assertEquals("Asia/Shanghai",
                table.getTableProperty().getProperties().get(
                        PropertyAnalyzer.PROPERTIES_COMPACTION_RETENTION_TIME_ZONE));
        Assertions.assertEquals("Asia/Shanghai",
                table.getProperties().get(PropertyAnalyzer.PROPERTIES_COMPACTION_RETENTION_TIME_ZONE));
        Assertions.assertEquals("Asia/Shanghai",
                table.getTableProperty().getTenantTtlTableBinding().getNormalizedTimeZone());
        Assertions.assertEquals(TenantTtlBindingAnalyzer.RANGE_FROM_UNIXTIME,
                table.getTableProperty().getTenantTtlTableBinding().getPartitionExpressionType());
    }

    @Test
    public void testSupportedListPartitionBindings() throws Exception {
        starRocksAssert.withTable("CREATE TABLE " + DB_NAME + ".tenant_ttl_list_direct (\n" +
                "tenant VARCHAR(128) NULL, recordTimestamp BIGINT NOT NULL\n" +
                ") DUPLICATE KEY(tenant, recordTimestamp)\n" +
                "PARTITION BY LIST(recordTimestamp) (PARTITION p0 VALUES IN ('1', '2'))\n" +
                "DISTRIBUTED BY HASH(tenant) BUCKETS 1\n" + tenantTtlProperties() + ")");
        TenantTtlTableBinding directBinding = getTable("tenant_ttl_list_direct")
                .getTableProperty().getTenantTtlTableBinding();
        Assertions.assertEquals(TenantTtlBindingAnalyzer.LIST_DIRECT_UNIX_SECONDS,
                directBinding.getPartitionExpressionType());
        Assertions.assertEquals(0, directBinding.getListTimeComponentIndex());

        starRocksAssert.withTable("CREATE TABLE " + DB_NAME + ".tenant_ttl_list_expr (\n" +
                "tenant VARCHAR(128) NULL, recordTimestamp BIGINT NOT NULL, tenant_bucket INT NOT NULL\n" +
                ") DUPLICATE KEY(tenant, recordTimestamp, tenant_bucket)\n" +
                "PARTITION BY (tenant_bucket, from_unixtime(recordTimestamp, '%Y%m%d'))\n" +
                "DISTRIBUTED BY HASH(tenant) BUCKETS 1\n" +
                "PROPERTIES('replication_num' = '1', " +
                "'compaction_retention_condition' = \"dictionary_ttl('" + DICTIONARY_NAME +
                "', 'business.http_log', 180)\", " +
                "'compaction_retention_time_zone' = 'Asia/Shanghai')");
        TenantTtlTableBinding expressionBinding = getTable("tenant_ttl_list_expr")
                .getTableProperty().getTenantTtlTableBinding();
        Assertions.assertEquals(TenantTtlBindingAnalyzer.LIST_FROM_UNIXTIME_YYYYMMDD,
                expressionBinding.getPartitionExpressionType());
        Assertions.assertEquals(1, expressionBinding.getListTimeComponentIndex());
    }

    @Test
    public void testDateTruncDayCreateAlterAndPersistence() throws Exception {
        String[] expressions = {
                "date_trunc('day', from_unixtime(recordTimestamp))",
                "(tenant_bucket, DATE_TRUNC('DAY', FROM_UNIXTIME(recordTimestamp)))",
                "date_trunc('day', CAST(from_unixtime(recordTimestamp) AS DATETIME))"
        };
        for (int i = 0; i < expressions.length; i++) {
            String name = "tenant_ttl_truncated_day_" + i;
            starRocksAssert.withTable(dateTruncTableSql(name, expressions[i], "CST", true));
            OlapTable table = getTable(name);
            Assertions.assertInstanceOf(ListPartitionInfo.class, table.getPartitionInfo());
            TenantTtlTableBinding binding = table.getTableProperty().getTenantTtlTableBinding();
            Assertions.assertEquals(TenantTtlBindingAnalyzer.LIST_DATE_TRUNC_DAY_FROM_UNIXTIME,
                    binding.getPartitionExpressionType());
            Assertions.assertEquals(i == 1 ? 1 : 0, binding.getListTimeComponentIndex());
            Assertions.assertEquals("Asia/Shanghai", binding.getNormalizedTimeZone());
            Assertions.assertEquals(table.getColumn("recordTimestamp").getUniqueId(), binding.getTimeColumnUniqueId());
            TableProperty restoredProperty = GsonUtils.GSON.fromJson(
                    GsonUtils.GSON.toJson(table.getTableProperty()), TableProperty.class);
            Assertions.assertEquals(binding, restoredProperty.getTenantTtlTableBinding());
            ModifyTablePropertyOperationLog log = new ModifyTablePropertyOperationLog(
                    1, table.getId(), new HashMap<>(table.getProperties()),
                    table.getTableProperty().getTenantTtlDictionaryBinding(), binding);
            Assertions.assertEquals(binding, GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(log),
                    ModifyTablePropertyOperationLog.class).getTenantTtlTableBinding());
            List<String> ddl = new ArrayList<>();
            AstToStringBuilder.getDdlStmt(table, ddl, null, null, false, true);
            Assertions.assertTrue(ddl.get(0).toLowerCase().contains("date_trunc"));
            Assertions.assertTrue(ddl.get(0).contains("compaction_retention_condition"));
            Assertions.assertTrue(ddl.get(0).contains("Asia/Shanghai"));
        }

        String name = "tenant_ttl_truncated_day_alter";
        starRocksAssert.withTable(dateTruncTableSql(name, expressions[0], null, false));
        starRocksAssert.alterTable("ALTER TABLE " + name +
                " ADD PARTITION p0 VALUES IN ('2024-03-10 00:00:00')");
        OlapTable table = getTable(name);
        PhysicalPartition partition = table.getPartition("p0").getDefaultPhysicalPartition();
        long baseIndexId = table.getBaseIndexId();
        Assertions.assertNull(table.getTableProperty().getTenantTtlTableBinding());
        Assertions.assertThrows(AlterJobException.class, () -> starRocksAssert.alterTableProperties(
                "ALTER TABLE " + name + " SET ('compaction_retention_condition' = " +
                        "\"dictionary_ttl('" + DICTIONARY_NAME + "', 'business.http_log', 180)\")"));
        Assertions.assertNull(table.getTableProperty().getTenantTtlDictionaryBinding());
        Assertions.assertNull(table.getTableProperty().getTenantTtlTableBinding());
        starRocksAssert.alterTableProperties("ALTER TABLE " + name + " SET (" +
                "'compaction_retention_condition' = \"dictionary_ttl('" + DICTIONARY_NAME +
                "', 'business.http_log', 180)\", 'compaction_retention_time_zone' = 'America/Los_Angeles')");
        Assertions.assertEquals(baseIndexId, table.getBaseIndexId());
        Assertions.assertSame(partition, table.getPartition("p0").getDefaultPhysicalPartition());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertTrue(TenantTtlPartitionBoundResolver.resolve(db, table, partition).isProvable());
        table.getTableProperty().getProperties().put(DynamicPartitionProperty.TIME_ZONE, "UTC");
        try {
            Assertions.assertEquals(TenantTtlPartitionBoundResolver.UnprovableReason.BINDING_MISMATCH,
                    TenantTtlPartitionBoundResolver.resolve(db, table, partition).getReason());
        } finally {
            table.getTableProperty().getProperties().remove(DynamicPartitionProperty.TIME_ZONE);
        }
    }

    @Test
    public void testDateTruncRejectsUnprovenExpressions() throws Exception {
        String[] expressions = {
                "date_trunc('hour', from_unixtime(recordTimestamp))",
                "date_trunc('month', from_unixtime(recordTimestamp))",
                "date_trunc('year', from_unixtime(recordTimestamp))",
                "date_trunc(tenant, from_unixtime(recordTimestamp))",
                "date_trunc('day', from_unixtime(otherTimestamp))",
                "date_trunc('day', from_unixtime(recordTimestamp, '%Y%m%d'))",
                "date_trunc('day', from_unixtime(recordTimestamp / 1000))",
                "date_trunc('day', CAST(from_unixtime(recordTimestamp) AS DATE))",
                "date_trunc('day', CAST(CAST(from_unixtime(recordTimestamp) AS DATE) AS DATETIME))",
                "date_trunc('day', date_trunc('day', from_unixtime(recordTimestamp)))",
                "(recordTimestamp, date_trunc('day', from_unixtime(recordTimestamp)))",
                "(abs(tenant_bucket), date_trunc('day', from_unixtime(recordTimestamp)))"
        };
        for (int i = 0; i < expressions.length; i++) {
            String name = "tenant_ttl_reject_trunc_" + i;
            String sql = dateTruncTableSql(name, expressions[i], "UTC", true);
            Assertions.assertThrows(Exception.class, () -> starRocksAssert.withTable(sql), expressions[i]);
            Assertions.assertNull(getTable(name), expressions[i]);
        }
        Assertions.assertThrows(DdlException.class, () -> starRocksAssert.withTable(dateTruncTableSql(
                "tenant_ttl_trunc_no_zone", "date_trunc('day', from_unixtime(recordTimestamp))", null, true)));
    }

    @Test
    public void testRejectUnsupportedStaticMetadata() throws Exception {
        Assertions.assertThrows(DdlException.class, () -> starRocksAssert.withTable(
                "CREATE TABLE " + DB_NAME + ".tenant_ttl_wrong_key (\n" +
                        "tenant VARCHAR(128) NOT NULL, recordTimestamp BIGINT NOT NULL\n" +
                        ") PRIMARY KEY(tenant, recordTimestamp)\n" +
                        "PARTITION BY RANGE(recordTimestamp) (PARTITION p0 VALUES LESS THAN ('100'))\n" +
                        "DISTRIBUTED BY HASH(tenant) BUCKETS 1\n" + tenantTtlProperties() + ")"));
        Assertions.assertThrows(DdlException.class, () -> starRocksAssert.withTable(
                "CREATE TABLE " + DB_NAME + ".tenant_ttl_wrong_tenant (\n" +
                        "tenant CHAR(10) NOT NULL, recordTimestamp BIGINT NOT NULL\n" +
                        ") DUPLICATE KEY(tenant, recordTimestamp)\n" +
                        "PARTITION BY RANGE(recordTimestamp) (PARTITION p0 VALUES LESS THAN ('100'))\n" +
                        "DISTRIBUTED BY HASH(tenant) BUCKETS 1\n" + tenantTtlProperties() + ")"));
        Assertions.assertThrows(DdlException.class, () -> starRocksAssert.withTable(
                baseTableSql("tenant_ttl_unpartitioned", "DUPLICATE KEY(tenant, recordTimestamp)", "")));
    }

    @Test
    public void testBindingLogRoundTripAndReferenceIdempotence() {
        TenantTtlDictionaryBinding dictionaryBinding =
                new TenantTtlDictionaryBinding(55L, "d", "db.table", 30);
        TenantTtlTableBinding tableBinding = new TenantTtlTableBinding(
                "tenant", 1, "recordTimestamp", 2, TenantTtlBindingAnalyzer.RANGE_DIRECT_UNIX_SECONDS,
                TenantTtlTableBinding.NO_LIST_TIME_COMPONENT,
                TenantTtlBindingAnalyzer.NOT_APPLICABLE_TIME_ZONE, "recordTimestamp");
        ModifyTablePropertyOperationLog original = new ModifyTablePropertyOperationLog(
                1, 2, new HashMap<>(), dictionaryBinding, tableBinding);
        ModifyTablePropertyOperationLog restored = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(original), ModifyTablePropertyOperationLog.class);
        Assertions.assertEquals(dictionaryBinding, restored.getTenantTtlDictionaryBinding());
        Assertions.assertEquals(tableBinding, restored.getTenantTtlTableBinding());

        TenantTtlPolicySnapshotManager manager = new TenantTtlPolicySnapshotManager();
        manager.updateBinding(1, 2, null, dictionaryBinding);
        manager.updateBinding(1, 2, null, dictionaryBinding);
        Assertions.assertEquals(1, manager.getReferencedTables(55L).size());
        manager.removeTable(1, 2, dictionaryBinding);
        manager.removeTable(1, 2, dictionaryBinding);
        Assertions.assertFalse(manager.isReferenced(55L));
        Assertions.assertEquals(1, manager.getGeneration(55L));
    }

    @Test
    public void testRuntimePartitionBindingRevalidation() throws Exception {
        starRocksAssert.withTable(fromUnixTimeTableSql("tenant_ttl_runtime_revalidation", "Asia/Shanghai"));
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = getTable("tenant_ttl_runtime_revalidation");
        PhysicalPartition physicalPartition = table.getPhysicalPartitions().iterator().next();

        TenantTtlPartitionBoundResolver.Resolution resolution =
                TenantTtlPartitionBoundResolver.resolve(db, table, physicalPartition);
        Assertions.assertTrue(resolution.isProvable());

        table.getTableProperty().getProperties().put(DynamicPartitionProperty.TIME_ZONE, "UTC");
        resolution = TenantTtlPartitionBoundResolver.resolve(db, table, physicalPartition);
        Assertions.assertFalse(resolution.isProvable());
        Assertions.assertEquals(TenantTtlPartitionBoundResolver.UnprovableReason.BINDING_MISMATCH,
                resolution.getReason());
        table.getTableProperty().getProperties().remove(DynamicPartitionProperty.TIME_ZONE);
    }

    private static String fromUnixTimeTableSql(String tableName, String timeZone) {
        String timeZoneProperty = timeZone == null ? "" :
                ", 'compaction_retention_time_zone' = '" + timeZone + "'";
        return "CREATE TABLE " + DB_NAME + "." + tableName + " (\n" +
                "tenant VARCHAR(128) NULL, recordTimestamp BIGINT NOT NULL\n" +
                ") DUPLICATE KEY(tenant, recordTimestamp)\n" +
                "PARTITION BY RANGE(from_unixtime(recordTimestamp)) (\n" +
                "PARTITION p0 VALUES LESS THAN ('2026-01-02'))\n" +
                "DISTRIBUTED BY HASH(tenant) BUCKETS 1\n" +
                "PROPERTIES('replication_num' = '1', " +
                "'compaction_retention_condition' = \"dictionary_ttl('" + DICTIONARY_NAME +
                "', 'business.http_log', 180)\"" + timeZoneProperty + ")";
    }

    private static String baseTableSql(String tableName, String keyClause, String partitionClause) {
        return "CREATE TABLE " + DB_NAME + "." + tableName + " (\n" +
                "tenant VARCHAR(128) NULL, recordTimestamp BIGINT NOT NULL\n" +
                ") " + keyClause + "\n" + partitionClause + "\n" +
                "DISTRIBUTED BY HASH(tenant) BUCKETS 1\n" + tenantTtlProperties() + ")";
    }

    private static String dateTruncTableSql(String name, String expression, String timeZone, boolean bind) {
        return "CREATE TABLE " + DB_NAME + "." + name + " (" +
                "tenant VARCHAR(128) NULL, recordTimestamp BIGINT NOT NULL, " +
                "tenant_bucket INT NOT NULL, otherTimestamp BIGINT NOT NULL) " +
                "DUPLICATE KEY(tenant, recordTimestamp) PARTITION BY " + expression +
                " DISTRIBUTED BY HASH(tenant) BUCKETS 1 " +
                (bind ? tenantTtlProperties() : "PROPERTIES('replication_num' = '1'") +
                (timeZone == null ? "" : ", 'compaction_retention_time_zone' = '" + timeZone + "'") + ")";
    }

    private static String tenantTtlProperties() {
        return "PROPERTIES('replication_num' = '1', " +
                "'compaction_retention_condition' = \"dictionary_ttl('" + DICTIONARY_NAME +
                "', 'business.http_log', 180)\"";
    }

    private static OlapTable getTable(String tableName) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(DB_NAME).getTable(tableName);
    }
}
