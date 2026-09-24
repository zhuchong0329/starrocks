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

package com.starrocks.sql.plan;

import com.starrocks.catalog.Dictionary;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TabletStatMgr;
import com.starrocks.catalog.TenantTtlDictionaryBinding;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.statistics.ColumnMinMaxMgr;
import com.starrocks.sql.optimizer.statistics.IMinMaxStatsMgr;
import com.starrocks.sql.optimizer.statistics.StatsVersion;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class TenantTtlAggregateMetaTest extends PlanTestBase {
    private static final String TABLE = "tenant_ttl_aggregate_guard";
    private static final String DICTIONARY = "tenant_ttl_aggregate_dict";
    private static final String CONDITION = "dictionary_ttl('" + DICTIONARY + "', 'test.events', 180)";
    private static final long STALE_COUNT = 7919;
    private final AtomicInteger minMaxReads = new AtomicInteger();
    private OlapTable table;
    private TableProperty savedProperty;
    private boolean savedRewrite;
    private int savedPartitionLimit;
    private boolean partialMinMax;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable(tableSql(TABLE, false));
        starRocksAssert.withTable("CREATE TABLE tenant_ttl_aggregate_policy (tenant VARCHAR(128) NOT NULL, " +
                "table_name VARCHAR(256) NOT NULL, retention_days INT NOT NULL) PRIMARY KEY(tenant, table_name) " +
                "DISTRIBUTED BY HASH(tenant) BUCKETS 1 PROPERTIES('replication_num'='1')");
        GlobalStateMgr.getCurrentState().getDictionaryMgr().addDictionary(new Dictionary(
                992039L, DICTIONARY, "tenant_ttl_aggregate_policy", InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                "test", List.of("tenant", "table_name"), List.of("retention_days"), new HashMap<>()));
    }

    @BeforeEach
    public void prepareCachedStatistics() {
        table = getOlapTable(TABLE);
        savedProperty = table.getTableProperty();
        savedRewrite = connectContext.getSessionVariable().isEnableRewriteSimpleAggToMetaScan();
        savedPartitionLimit = connectContext.getSessionVariable().getScanOlapPartitionNumLimit();
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(true);
        connectContext.getSessionVariable().setScanOlapPartitionNumLimit(0);
        new MockUp<MaterializedIndex>() {
            @Mock
            public long getRowCount() {
                return STALE_COUNT;
            }
        };
        new MockUp<TabletStatMgr>() {
            @Mock
            public boolean workTimeIsMustAfter(LocalDateTime time) {
                return true;
            }
        };
        new MockUp<ColumnMinMaxMgr>() {
            @Mock
            public Optional<IMinMaxStatsMgr.ColumnMinMax> getStats(ColumnIdentifier identifier, StatsVersion version) {
                minMaxReads.incrementAndGet();
                if (partialMinMax && identifier.getColumnName().getId().equals("recordTimestamp")) {
                    return Optional.empty();
                }
                if (identifier.getColumnName().getId().equals("event_date")) {
                    return Optional.of(new IMinMaxStatsMgr.ColumnMinMax("2000-01-01", "2099-01-01"));
                }
                return Optional.of(new IMinMaxStatsMgr.ColumnMinMax("-991731", "991733"));
            }
        };
    }

    @AfterEach
    public void restoreTableAndSession() {
        table.setTableProperty(savedProperty);
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(savedRewrite);
        connectContext.getSessionVariable().setScanOlapPartitionNumLimit(savedPartitionLimit);
    }

    @Test
    public void testCountIgnoresStaleRowCountButKeepsMetaScan() throws Exception {
        bindByProperty();
        for (String aggregate : List.of("count(*)", "count(1)", "count(metric)")) {
            assertMetaScan("SELECT " + aggregate + " FROM " + TABLE);
        }
    }

    @Test
    public void testMinMaxAndMixedAggregatesIgnoreCachedValues() throws Exception {
        bindByProperty();
        assertMetaScan("SELECT min(metric), max(metric) FROM " + TABLE);
        assertMetaScan("SELECT min(event_date), max(event_date) FROM " + TABLE);
        assertMetaScan("SELECT count(*), min(metric), max(recordTimestamp) FROM " + TABLE);
        Assertions.assertEquals(0, minMaxReads.get(), "TTL aggregation must not consult the FE extrema cache");
    }

    @Test
    public void testPartialCacheHitDoesNotReplaceOneAggregate() throws Exception {
        partialMinMax = true;
        bindByProperty();
        assertMetaScan("SELECT min(metric), max(recordTimestamp) FROM " + TABLE);
        Assertions.assertEquals(0, minMaxReads.get());
    }

    @Test
    public void testOrdinaryTablesStillUseCachedConstants() throws Exception {
        for (TableProperty property : new TableProperty[] {null, new TableProperty(new HashMap<>()), savedProperty}) {
            table.setTableProperty(property);
            String plan = getFragmentPlan("SELECT count(*), min(metric), max(metric) FROM " + TABLE);
            assertContains(plan, "constant exprs", Long.toString(STALE_COUNT), "-991731", "991733");
            assertNotContains(plan, "MetaScan");
        }
        Assertions.assertTrue(minMaxReads.get() > 0);
    }

    @Test
    public void testBindingWithoutConditionAlsoPreventsConstants() throws Exception {
        TableProperty property = savedProperty.copy();
        property.setTenantTtlDictionaryBinding(new TenantTtlDictionaryBinding(992039L, DICTIONARY, "test.events", 180));
        table.setTableProperty(property);
        assertMetaScan("SELECT count(*), min(metric), max(metric) FROM " + TABLE);
    }

    @Test
    public void testCopiedAndRestoredPropertiesPreserveGuard() throws Exception {
        bindByProperty();
        OlapTable queryCopy = new OlapTable();
        table.copyOnlyForQuery(queryCopy);
        TableProperty copied = queryCopy.getTableProperty();
        Assertions.assertNotSame(table.getTableProperty(), copied);
        TableProperty restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(copied), TableProperty.class);
        for (TableProperty property : List.of(copied, restored)) {
            table.setTableProperty(property);
            assertMetaScan("SELECT count(*), min(metric) FROM " + TABLE);
        }
    }

    @Test
    public void testOriginalMetaScanRestrictionsAndSwitchArePreserved() throws Exception {
        bindByProperty();
        for (String sql : List.of(
                "SELECT count(nullable_metric) FROM " + TABLE,
                "SELECT count(*) FROM " + TABLE + " WHERE metric > 0",
                "SELECT tenant, count(*) FROM " + TABLE + " GROUP BY tenant",
                "SELECT count(DISTINCT metric) FROM " + TABLE)) {
            assertNotContains(getFragmentPlan(sql), "MetaScan", "constant exprs");
        }
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(false);
        assertNotContains(getFragmentPlan("SELECT count(*) FROM " + TABLE), "MetaScan", "constant exprs");
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(true);
        assertMetaScan("SELECT 1 + 2, count(*) FROM " + TABLE);
        table.getTableProperty().setHasDelete(true);
        assertNotContains(getFragmentPlan("SELECT count(*) FROM " + TABLE), "MetaScan", "constant exprs");
    }

    @Test
    public void testCreateAndAlterBindingsGuardQueries() throws Exception {
        String created = "tenant_ttl_aggregate_created";
        String altered = "tenant_ttl_aggregate_altered";
        starRocksAssert.withTable(tableSql(created, true));
        starRocksAssert.withTable(tableSql(altered, false));
        assertContains(getFragmentPlan("SELECT count(*) FROM " + altered), "constant exprs");
        starRocksAssert.alterTableProperties("ALTER TABLE " + altered +
                " SET ('compaction_retention_condition'=\"" + CONDITION + "\")");
        for (String name : List.of(created, altered)) {
            Assertions.assertNotNull(getOlapTable(name).getTableProperty().getTenantTtlDictionaryBinding());
            assertMetaScan("SELECT count(*), min(metric), max(metric) FROM " + name);
        }
    }

    private void bindByProperty() {
        TableProperty property = savedProperty.copy();
        property.getProperties().put(PropertyAnalyzer.PROPERTIES_COMPACTION_RETENTION_CONDITION, CONDITION);
        table.setTableProperty(property.buildCompactionRetentionProperties());
    }

    private void assertMetaScan(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MetaScan", "AGGREGATE");
        assertNotContains(plan, "constant exprs", Long.toString(STALE_COUNT), "-991731", "991733");
    }

    private static String tableSql(String name, boolean bound) {
        return "CREATE TABLE " + name + " (tenant VARCHAR(128) NULL, recordTimestamp BIGINT NOT NULL, " +
                "metric BIGINT NOT NULL, nullable_metric BIGINT NULL, event_date DATE NOT NULL) " +
                "DUPLICATE KEY(tenant, recordTimestamp) " +
                "PARTITION BY RANGE(recordTimestamp) (PARTITION p0 VALUES LESS THAN ('100')) " +
                "DISTRIBUTED BY HASH(tenant) BUCKETS 1 PROPERTIES('replication_num'='1'" +
                (bound ? ", 'compaction_retention_condition'=\"" + CONDITION + "\"" : "") + ")";
    }
}
