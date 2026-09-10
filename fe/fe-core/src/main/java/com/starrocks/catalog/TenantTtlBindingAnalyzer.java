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

import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Performs the metadata-only admission checks required to bind an OLAP table to Tenant-TTL. */
public final class TenantTtlBindingAnalyzer {
    public static final String TENANT_COLUMN_NAME = "tenant";
    public static final String TIME_COLUMN_NAME = "recordTimestamp";
    public static final String NOT_APPLICABLE_TIME_ZONE = "NOT_APPLICABLE";

    public static final String RANGE_DIRECT_UNIX_SECONDS = "RANGE_DIRECT_UNIX_SECONDS";
    public static final String RANGE_FROM_UNIXTIME = "RANGE_FROM_UNIXTIME";
    public static final String LIST_DIRECT_UNIX_SECONDS = "LIST_DIRECT_UNIX_SECONDS";
    public static final String LIST_FROM_UNIXTIME_YYYYMMDD = "LIST_FROM_UNIXTIME_YYYYMMDD";

    private static final String LIST_DATE_FORMAT = "%Y%m%d";

    private TenantTtlBindingAnalyzer() {
    }

    public static BindingResult analyze(Database db, OlapTable table, String condition, String timeZone,
                                        Map<String, String> candidateProperties) throws DdlException {
        TenantTtlPropertyParser.ParsedCondition parsed;
        try {
            parsed = TenantTtlPropertyParser.parse(condition);
        } catch (RuntimeException e) {
            throw new DdlException(e.getMessage());
        }

        validateBusinessTable(table);
        Column tenantColumn = requireColumn(table, TENANT_COLUMN_NAME);
        if (!tenantColumn.getType().isVarchar()) {
            throw new DdlException("Tenant-TTL column 'tenant' must be VARCHAR");
        }
        validateColumnIdentity(tenantColumn, TENANT_COLUMN_NAME);

        Column timeColumn = requireColumn(table, TIME_COLUMN_NAME);
        if (!timeColumn.getType().isBigint()) {
            throw new DdlException("Tenant-TTL column 'recordTimestamp' must be BIGINT Unix seconds");
        }
        validateColumnIdentity(timeColumn, TIME_COLUMN_NAME);

        PartitionBinding partitionBinding = analyzePartition(db, table, timeColumn);
        String normalizedPropertyTimeZone = normalizeOptionalTimeZone(timeZone);
        String normalizedTimeZone = analyzeTimeZone(table, partitionBinding.usesTimeZone,
                normalizedPropertyTimeZone,
                candidateProperties == null ? Collections.emptyMap() : candidateProperties);

        Dictionary dictionary = requireDictionary(parsed.getDictionaryName());
        validateDictionarySchema(dictionary);
        validateDictionarySource(dictionary);

        TenantTtlDictionaryBinding dictionaryBinding = new TenantTtlDictionaryBinding(
                dictionary.getDictionaryId(), dictionary.getDictionaryName(), parsed.getTableKey(),
                parsed.getDefaultDays());
        TenantTtlTableBinding tableBinding = new TenantTtlTableBinding(
                tenantColumn.getColumnId().getId(), tenantColumn.getUniqueId(),
                timeColumn.getColumnId().getId(), timeColumn.getUniqueId(),
                partitionBinding.expressionType, partitionBinding.listTimeComponentIndex,
                normalizedTimeZone, partitionBinding.expressionFingerprint);
        return new BindingResult(dictionaryBinding, tableBinding, normalizedPropertyTimeZone);
    }

    private static void validateBusinessTable(OlapTable table) throws DdlException {
        if (!table.isOlapTable() || table.isCloudNativeTable() || table.isMaterializedView() || table.isTemporaryTable()) {
            throw new DdlException("Tenant-TTL only supports local shared-nothing OLAP base tables");
        }
        if (table.getKeysType() != KeysType.DUP_KEYS) {
            throw new DdlException("Tenant-TTL only supports DUP_KEYS business tables");
        }
        if (table.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new DdlException("Tenant-TTL requires the table state to be NORMAL");
        }
        if (!table.getIndexIdListExceptBaseIndex().isEmpty()) {
            throw new DdlException("Tenant-TTL does not support Rollup or synchronous materialized indexes");
        }
    }

    private static Column requireColumn(OlapTable table, String name) throws DdlException {
        Column column = table.getColumn(name);
        if (column == null) {
            throw new DdlException("Tenant-TTL requires column '" + name + "'");
        }
        return column;
    }

    private static void validateColumnIdentity(Column column, String name) throws DdlException {
        if (column.getColumnId() == null || column.getColumnId().getId() == null ||
                column.getColumnId().getId().isEmpty() || column.getUniqueId() < 0) {
            throw new DdlException("Tenant-TTL cannot bind a valid identity for column '" + name + "'");
        }
    }

    private static PartitionBinding analyzePartition(Database db, OlapTable table, Column timeColumn)
            throws DdlException {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        if (partitionInfo == null || !partitionInfo.isPartitioned()) {
            throw new DdlException("Tenant-TTL requires a Range or List partitioned table");
        }

        if (partitionInfo.isRangePartition()) {
            List<Expr> expressions = getRangeExpressions(table, partitionInfo);
            if (expressions.size() != 1) {
                throw new DdlException("Tenant-TTL Range partition must have exactly one time expression");
            }
            Expr expression = expressions.get(0);
            if (isTimeSlot(expression, timeColumn)) {
                return new PartitionBinding(RANGE_DIRECT_UNIX_SECONDS,
                        TenantTtlTableBinding.NO_LIST_TIME_COMPONENT, false, fingerprint(expressions));
            }
            if (isRangeFromUnixTime(expression, timeColumn)) {
                return new PartitionBinding(RANGE_FROM_UNIXTIME,
                        TenantTtlTableBinding.NO_LIST_TIME_COMPONENT, true, fingerprint(expressions));
            }
            throw new DdlException("Unsupported Tenant-TTL Range partition expression: " + expression.toSql());
        }

        if (partitionInfo.isListPartition()) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            List<Expr> expressions = listPartitionInfo.getPartitionExprs(
                    new TableName(db.getFullName(), table.getName()), table.getIdToColumn());
            int timeComponentIndex = TenantTtlTableBinding.NO_LIST_TIME_COMPONENT;
            String expressionType = null;
            for (int i = 0; i < expressions.size(); i++) {
                Expr expression = expressions.get(i);
                String currentType = null;
                if (isTimeSlot(expression, timeColumn)) {
                    currentType = LIST_DIRECT_UNIX_SECONDS;
                } else if (isListFromUnixTime(expression, timeColumn)) {
                    currentType = LIST_FROM_UNIXTIME_YYYYMMDD;
                } else if (!(expression instanceof SlotRef)) {
                    throw new DdlException("Tenant-TTL List non-time components must be plain column references: " +
                            expression.toSql());
                }
                if (currentType != null) {
                    if (timeComponentIndex != TenantTtlTableBinding.NO_LIST_TIME_COMPONENT) {
                        throw new DdlException("Tenant-TTL List partition must contain exactly one time component");
                    }
                    timeComponentIndex = i;
                    expressionType = currentType;
                }
            }
            if (timeComponentIndex == TenantTtlTableBinding.NO_LIST_TIME_COMPONENT) {
                throw new DdlException("Tenant-TTL List partition must contain a supported recordTimestamp component");
            }
            return new PartitionBinding(expressionType, timeComponentIndex,
                    LIST_FROM_UNIXTIME_YYYYMMDD.equals(expressionType), fingerprint(expressions));
        }
        throw new DdlException("Tenant-TTL only supports Range or List partitioning");
    }

    private static List<Expr> getRangeExpressions(OlapTable table, PartitionInfo partitionInfo) {
        if (partitionInfo instanceof ExpressionRangePartitionInfoV2) {
            return ((ExpressionRangePartitionInfoV2) partitionInfo).getPartitionExprs(table.getIdToColumn());
        }
        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            return ((ExpressionRangePartitionInfo) partitionInfo).getPartitionExprs(table.getIdToColumn());
        }
        List<Column> partitionColumns = partitionInfo.getPartitionColumns(table.getIdToColumn());
        if (partitionColumns.size() != 1) {
            return Collections.emptyList();
        }
        return Collections.singletonList(new SlotRef(null, partitionColumns.get(0).getName()));
    }

    private static boolean isRangeFromUnixTime(Expr expression, Column timeColumn) {
        Expr candidate = expression;
        if (candidate instanceof CastExpr) {
            CastExpr castExpr = (CastExpr) candidate;
            if (!castExpr.getType().isDatetime()) {
                return false;
            }
            candidate = castExpr.getChild(0);
        }
        return isFromUnixTime(candidate, timeColumn, false);
    }

    private static boolean isListFromUnixTime(Expr expression, Column timeColumn) {
        return isFromUnixTime(expression, timeColumn, true);
    }

    private static boolean isFromUnixTime(Expr expression, Column timeColumn, boolean requireDateFormat) {
        if (!(expression instanceof FunctionCallExpr)) {
            return false;
        }
        FunctionCallExpr function = (FunctionCallExpr) expression;
        if (function.getFnName().getDb() != null ||
                !FunctionSet.FROM_UNIXTIME.equalsIgnoreCase(function.getFnName().getFunction())) {
            return false;
        }
        int expectedChildren = requireDateFormat ? 2 : 1;
        if (function.getChildren().size() != expectedChildren || !isTimeSlot(function.getChild(0), timeColumn)) {
            return false;
        }
        return !requireDateFormat || (function.getChild(1) instanceof StringLiteral &&
                LIST_DATE_FORMAT.equals(((StringLiteral) function.getChild(1)).getStringValue()));
    }

    private static boolean isTimeSlot(Expr expression, Column timeColumn) {
        if (!(expression instanceof SlotRef)) {
            return false;
        }
        SlotRef slotRef = (SlotRef) expression;
        if (!TIME_COLUMN_NAME.equalsIgnoreCase(slotRef.getColumnName())) {
            return false;
        }
        return slotRef.getColumnId() == null || slotRef.getColumnId().equals(timeColumn.getColumnId());
    }

    private static String fingerprint(List<Expr> expressions) {
        return expressions.stream().map(Expr::toSql).collect(Collectors.joining("|"));
    }

    private static String normalizeOptionalTimeZone(String timeZone) throws DdlException {
        if (timeZone == null) {
            return null;
        }
        String standard = TimeUtils.checkTimeZoneValidAndStandardize(timeZone);
        return ZoneId.of(standard, TimeUtils.TIME_ZONE_ALIAS_MAP).getId();
    }

    private static String analyzeTimeZone(OlapTable table, boolean required, String normalized,
                                          Map<String, String> candidateProperties) throws DdlException {
        if (!required) {
            return NOT_APPLICABLE_TIME_ZONE;
        }
        if (normalized == null) {
            throw new DdlException("compaction_retention_time_zone is required for from_unixtime partitioning");
        }

        String dynamicTimeZone = candidateProperties.get(DynamicPartitionProperty.TIME_ZONE);
        if (dynamicTimeZone == null && table.getTableProperty() != null &&
                table.getTableProperty().getDynamicPartitionProperty() != null &&
                table.getTableProperty().getDynamicPartitionProperty().isExists()) {
            dynamicTimeZone = table.getTableProperty().getDynamicPartitionProperty().getTimeZone().getID();
        }
        if (dynamicTimeZone != null) {
            String standardDynamic = TimeUtils.checkTimeZoneValidAndStandardize(dynamicTimeZone);
            String normalizedDynamic = ZoneId.of(standardDynamic, TimeUtils.TIME_ZONE_ALIAS_MAP).getId();
            if (!normalized.equals(normalizedDynamic)) {
                throw new DdlException("compaction_retention_time_zone must match dynamic_partition.time_zone");
            }
        }
        return normalized;
    }

    private static Dictionary requireDictionary(String dictionaryName) throws DdlException {
        Dictionary dictionary = GlobalStateMgr.getCurrentState().getDictionaryMgr()
                .getDictionaryByName(dictionaryName);
        if (dictionary == null) {
            throw new DdlException("Tenant-TTL Dictionary '" + dictionaryName + "' does not exist");
        }
        return dictionary;
    }

    private static void validateDictionarySchema(Dictionary dictionary) throws DdlException {
        if (dictionary.getKeys().size() != 2 || dictionary.getValues().size() != 1 ||
                !TENANT_COLUMN_NAME.equalsIgnoreCase(dictionary.getKeys().get(0)) ||
                !"table_name".equalsIgnoreCase(dictionary.getKeys().get(1)) ||
                !"retention_days".equalsIgnoreCase(dictionary.getValues().get(0))) {
            throw new DdlException("Tenant-TTL Dictionary must define KEY(tenant, table_name) and " +
                    "VALUE(retention_days) in that order");
        }
    }

    private static void validateDictionarySource(Dictionary dictionary) throws DdlException {
        if (!InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME.equals(dictionary.getCatalogName())) {
            throw new DdlException("Tenant-TTL Dictionary source must be in the Internal Catalog");
        }

        SourceName sourceName = parseSourceName(dictionary);
        Database sourceDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(sourceName.dbName);
        Table source = sourceDb == null ? null : sourceDb.getTable(sourceName.tableName);
        if (!(source instanceof OlapTable) || !source.isOlapTable() || source.isCloudNativeTable() ||
                source.isMaterializedView()) {
            throw new DdlException("Tenant-TTL Dictionary source must be an existing local OLAP table");
        }
        OlapTable sourceTable = (OlapTable) source;
        if (sourceTable.getKeysType() != KeysType.PRIMARY_KEYS) {
            throw new DdlException("Tenant-TTL Dictionary source must be a PRIMARY KEY table");
        }

        List<Column> columns = sourceTable.getBaseSchema();
        if (columns.size() != 3) {
            throw new DdlException("Tenant-TTL policy source must contain exactly three columns");
        }
        validatePolicyColumn(columns.get(0), TENANT_COLUMN_NAME, true);
        validatePolicyColumn(columns.get(1), "table_name", true);
        validatePolicyColumn(columns.get(2), "retention_days", false);

        List<Column> keyColumns = new ArrayList<>(sourceTable.getKeyColumnsInOrder());
        if (keyColumns.size() != 2 ||
                !TENANT_COLUMN_NAME.equalsIgnoreCase(keyColumns.get(0).getName()) ||
                !"table_name".equalsIgnoreCase(keyColumns.get(1).getName())) {
            throw new DdlException("Tenant-TTL policy source PRIMARY KEY must be (tenant, table_name)");
        }
    }

    private static void validatePolicyColumn(Column column, String expectedName, boolean varchar)
            throws DdlException {
        boolean correctType = varchar ? column.getType().isVarchar() : column.getType().isInt();
        if (!expectedName.equalsIgnoreCase(column.getName()) || !correctType || column.isAllowNull()) {
            throw new DdlException("Tenant-TTL policy source column '" + expectedName + "' has an invalid definition");
        }
    }

    private static SourceName parseSourceName(Dictionary dictionary) throws DdlException {
        String[] parts = dictionary.getQueryableObject().split("\\.", -1);
        if (parts.length == 1 && !parts[0].isEmpty()) {
            return new SourceName(dictionary.getDbName(), parts[0]);
        }
        if (parts.length == 2 && !parts[0].isEmpty() && !parts[1].isEmpty()) {
            return new SourceName(parts[0], parts[1]);
        }
        if (parts.length == 3 && InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME.equals(parts[0]) &&
                !parts[1].isEmpty() && !parts[2].isEmpty()) {
            return new SourceName(parts[1], parts[2]);
        }
        throw new DdlException("Invalid Tenant-TTL Dictionary source name: " + dictionary.getQueryableObject());
    }

    public static final class BindingResult {
        private final TenantTtlDictionaryBinding dictionaryBinding;
        private final TenantTtlTableBinding tableBinding;
        private final String normalizedPropertyTimeZone;

        private BindingResult(TenantTtlDictionaryBinding dictionaryBinding, TenantTtlTableBinding tableBinding,
                              String normalizedPropertyTimeZone) {
            this.dictionaryBinding = dictionaryBinding;
            this.tableBinding = tableBinding;
            this.normalizedPropertyTimeZone = normalizedPropertyTimeZone;
        }

        public TenantTtlDictionaryBinding getDictionaryBinding() {
            return dictionaryBinding;
        }

        public TenantTtlTableBinding getTableBinding() {
            return tableBinding;
        }

        public String getNormalizedPropertyTimeZone() {
            return normalizedPropertyTimeZone;
        }
    }

    private static final class PartitionBinding {
        private final String expressionType;
        private final int listTimeComponentIndex;
        private final boolean usesTimeZone;
        private final String expressionFingerprint;

        private PartitionBinding(String expressionType, int listTimeComponentIndex, boolean usesTimeZone,
                                 String expressionFingerprint) {
            this.expressionType = expressionType;
            this.listTimeComponentIndex = listTimeComponentIndex;
            this.usesTimeZone = usesTimeZone;
            this.expressionFingerprint = expressionFingerprint;
        }
    }

    private static final class SourceName {
        private final String dbName;
        private final String tableName;

        private SourceName(String dbName, String tableName) {
            this.dbName = dbName;
            this.tableName = tableName;
        }
    }
}
