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

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.SqlParser;

import java.util.Objects;
import java.util.regex.Pattern;

/** Parser for the restricted dictionary_ttl table-property syntax. */
public final class TenantTtlPropertyParser {
    public static final String FUNCTION_NAME = "dictionary_ttl";
    private static final Pattern TABLE_KEY_PATTERN = Pattern.compile("^[^\\s.]+\\.[^\\s.]+$");

    private TenantTtlPropertyParser() {
    }

    public static ParsedCondition parse(String condition) {
        if (condition == null || condition.trim().isEmpty()) {
            throw invalid("value must not be empty");
        }

        final Expr expression;
        try {
            expression = SqlParser.parseSqlToExpr(condition, SqlModeHelper.MODE_DEFAULT);
        } catch (Exception e) {
            throw invalid("failed to parse expression");
        }
        if (!(expression instanceof FunctionCallExpr)) {
            throw invalid("only dictionary_ttl(dict_name, table_key, default_days) is supported");
        }

        FunctionCallExpr function = (FunctionCallExpr) expression;
        if (function.getFnName().getDb() != null ||
                !FUNCTION_NAME.equalsIgnoreCase(function.getFnName().getFunction())) {
            throw invalid("function name must be dictionary_ttl");
        }
        if (function.getChildren().size() != 3) {
            throw invalid("dictionary_ttl requires exactly three arguments");
        }
        if (!(function.getChild(0) instanceof StringLiteral) ||
                !(function.getChild(1) instanceof StringLiteral) ||
                !(function.getChild(2) instanceof IntLiteral)) {
            throw invalid("arguments must be two string literals followed by a positive INT literal");
        }

        String dictionaryName = ((StringLiteral) function.getChild(0)).getStringValue();
        String tableKey = ((StringLiteral) function.getChild(1)).getStringValue();
        long defaultDays = ((IntLiteral) function.getChild(2)).getLongValue();
        if (dictionaryName == null || dictionaryName.isEmpty()) {
            throw invalid("dictionary name must not be empty");
        }
        if (tableKey == null || !tableKey.equals(tableKey.trim()) ||
                !TABLE_KEY_PATTERN.matcher(tableKey).matches()) {
            throw invalid("table key must use the non-empty db.table form without whitespace");
        }
        if (defaultDays < 1 || defaultDays > Integer.MAX_VALUE) {
            throw invalid("default_days must be in [1, 2147483647]");
        }
        return new ParsedCondition(dictionaryName, tableKey, (int) defaultDays);
    }

    private static SemanticException invalid(String detail) {
        return new SemanticException("Invalid compaction_retention_condition: " + detail);
    }

    public static final class ParsedCondition {
        private final String dictionaryName;
        private final String tableKey;
        private final int defaultDays;

        private ParsedCondition(String dictionaryName, String tableKey, int defaultDays) {
            this.dictionaryName = dictionaryName;
            this.tableKey = tableKey;
            this.defaultDays = defaultDays;
        }

        public String getDictionaryName() {
            return dictionaryName;
        }

        public String getTableKey() {
            return tableKey;
        }

        public int getDefaultDays() {
            return defaultDays;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ParsedCondition)) {
                return false;
            }
            ParsedCondition that = (ParsedCondition) o;
            return defaultDays == that.defaultDays && Objects.equals(dictionaryName, that.dictionaryName) &&
                    Objects.equals(tableKey, that.tableKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dictionaryName, tableKey, defaultDays);
        }
    }
}
