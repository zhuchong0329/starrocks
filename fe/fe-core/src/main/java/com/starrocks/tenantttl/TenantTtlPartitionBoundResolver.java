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

import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.TenantTtlBindingAnalyzer;
import com.starrocks.catalog.TenantTtlTableBinding;
import com.starrocks.common.DdlException;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Converts supported Range/List partition metadata into conservative Unix-second bounds. */
public final class TenantTtlPartitionBoundResolver {
    private static final DateTimeFormatter YYYYMMDD =
            DateTimeFormatter.ofPattern("uuuuMMdd").withResolverStyle(ResolverStyle.STRICT);

    private TenantTtlPartitionBoundResolver() {
    }

    public static Resolution resolve(Database db, OlapTable table, PhysicalPartition physicalPartition) {
        if (db == null || table == null || physicalPartition == null || table.getTableProperty() == null) {
            return Resolution.unprovable(UnprovableReason.PARTITION_METADATA_MISSING,
                    "database, table, physical partition, or table properties are missing");
        }
        TenantTtlTableBinding persisted = table.getTableProperty().getTenantTtlTableBinding();
        if (persisted == null) {
            return Resolution.unprovable(UnprovableReason.BINDING_MISMATCH,
                    "Tenant-TTL table binding is missing");
        }
        try {
            TenantTtlBindingAnalyzer.TableBindingResult current = TenantTtlBindingAnalyzer.analyzeTableBinding(
                    db, table, table.getTableProperty().getCompactionRetentionTimeZone(),
                    table.getTableProperty().getProperties());
            if (!persisted.equals(current.getTableBinding())) {
                return Resolution.unprovable(UnprovableReason.BINDING_MISMATCH,
                        "current table layout does not match the persisted Tenant-TTL binding");
            }
        } catch (DdlException | RuntimeException e) {
            return Resolution.unprovable(UnprovableReason.BINDING_MISMATCH, e.getMessage());
        }
        return resolve(table, physicalPartition, persisted);
    }

    public static Resolution resolve(OlapTable table, PhysicalPartition physicalPartition,
                                     TenantTtlTableBinding binding) {
        if (table == null || physicalPartition == null) {
            return Resolution.unprovable(UnprovableReason.PARTITION_METADATA_MISSING,
                    "table or physical partition is missing");
        }
        if (binding == null || binding.getFormatVersion() != TenantTtlTableBinding.CURRENT_FORMAT_VERSION) {
            return Resolution.unprovable(UnprovableReason.BINDING_MISMATCH,
                    "Tenant-TTL table binding is missing or has an unsupported format");
        }
        Column timeColumn = table.getColumn(ColumnId.create(binding.getTimeColumnId()));
        if (timeColumn == null || timeColumn.getUniqueId() != binding.getTimeColumnUniqueId() ||
                !timeColumn.getType().isBigint()) {
            return Resolution.unprovable(UnprovableReason.BINDING_MISMATCH,
                    "bound recordTimestamp column identity no longer matches the table");
        }
        return resolve(table.getPartitionInfo(), timeColumn, physicalPartition.getParentId(), binding);
    }

    public static Resolution resolve(PartitionInfo partitionInfo, Column timeColumn, long logicalPartitionId,
                                     TenantTtlTableBinding binding) {
        if (partitionInfo == null || timeColumn == null || binding == null) {
            return Resolution.unprovable(UnprovableReason.PARTITION_METADATA_MISSING,
                    "partition metadata, time column, or table binding is missing");
        }
        String expressionType = binding.getPartitionExpressionType();
        try {
            if (TenantTtlBindingAnalyzer.RANGE_DIRECT_UNIX_SECONDS.equals(expressionType) ||
                    TenantTtlBindingAnalyzer.RANGE_FROM_UNIXTIME.equals(expressionType)) {
                if (!(partitionInfo instanceof RangePartitionInfo)) {
                    return Resolution.unprovable(UnprovableReason.BINDING_MISMATCH,
                            "bound Range expression no longer matches partition metadata");
                }
                return resolveRange((RangePartitionInfo) partitionInfo, timeColumn, logicalPartitionId, binding);
            }
            if (TenantTtlBindingAnalyzer.LIST_DIRECT_UNIX_SECONDS.equals(expressionType) ||
                    TenantTtlBindingAnalyzer.LIST_FROM_UNIXTIME_YYYYMMDD.equals(expressionType)) {
                if (!(partitionInfo instanceof ListPartitionInfo)) {
                    return Resolution.unprovable(UnprovableReason.BINDING_MISMATCH,
                            "bound List expression no longer matches partition metadata");
                }
                return resolveList((ListPartitionInfo) partitionInfo, logicalPartitionId, binding);
            }
            return Resolution.unprovable(UnprovableReason.UNSUPPORTED_EXPRESSION,
                    "unsupported partition expression type: " + expressionType);
        } catch (ArithmeticException e) {
            return Resolution.unprovable(UnprovableReason.EXACT_ARITHMETIC_OVERFLOW, e.getMessage());
        } catch (DateTimeException e) {
            return Resolution.unprovable(UnprovableReason.INVALID_TIME_ZONE_OR_DATE, e.getMessage());
        } catch (IllegalArgumentException e) {
            return Resolution.unprovable(UnprovableReason.INVALID_BOUND_LITERAL, e.getMessage());
        }
    }

    private static Resolution resolveRange(RangePartitionInfo partitionInfo, Column timeColumn,
                                           long logicalPartitionId, TenantTtlTableBinding binding) {
        Range<PartitionKey> range = partitionInfo.getRange(logicalPartitionId);
        if (range == null || !range.hasLowerBound() || !range.hasUpperBound()) {
            return Resolution.unprovable(UnprovableReason.PARTITION_METADATA_MISSING,
                    "Range partition has no closed-open metadata");
        }
        PartitionKey lowerKey = range.lowerEndpoint();
        PartitionKey upperKey = range.upperEndpoint();
        if (lowerKey.getKeys().size() != 1 || upperKey.getKeys().size() != 1) {
            return Resolution.unprovable(UnprovableReason.TUPLE_ARITY_MISMATCH,
                    "Range partition must have exactly one bound component");
        }
        if (upperKey.isMaxValue() || upperKey.getKeys().get(0) == MaxLiteral.MAX_VALUE) {
            return Resolution.unprovable(UnprovableReason.MAXVALUE_UPPER_BOUND,
                    "Range partition upper bound is MAXVALUE");
        }
        if (lowerKey.isMinValue() && timeColumn.isAllowNull()) {
            return Resolution.unprovable(UnprovableReason.NULLABLE_MINVALUE_PARTITION,
                    "nullable recordTimestamp may place NULL in the MINVALUE partition");
        }

        boolean fromUnixTime = TenantTtlBindingAnalyzer.RANGE_FROM_UNIXTIME.equals(
                binding.getPartitionExpressionType());
        ZoneId zone = fromUnixTime ? requireZone(binding) : null;
        LiteralExpr upperLiteral = upperKey.getKeys().get(0);
        long upper = fromUnixTime ? dateTimeBound(upperLiteral, zone, true) : integerBound(upperLiteral);
        Long lower = null;
        if (!lowerKey.isMinValue()) {
            LiteralExpr lowerLiteral = lowerKey.getKeys().get(0);
            lower = fromUnixTime ? dateTimeBound(lowerLiteral, zone, false) : integerBound(lowerLiteral);
            if (lower >= upper) {
                return Resolution.unprovable(UnprovableReason.INVALID_PARTITION_INTERVAL,
                        "Range partition lower bound is not less than its upper bound");
            }
        }
        return Resolution.provable(Collections.singletonList(new TimeInterval(lower, upper)));
    }

    private static Resolution resolveList(ListPartitionInfo partitionInfo, long logicalPartitionId,
                                          TenantTtlTableBinding binding) {
        int timeIndex = binding.getListTimeComponentIndex();
        if (timeIndex < 0 || timeIndex >= partitionInfo.getPartitionColumnsSize()) {
            return Resolution.unprovable(UnprovableReason.TUPLE_ARITY_MISMATCH,
                    "bound List time component index is outside the partition tuple");
        }
        List<LiteralExpr> singleValues = valueOrNull(partitionInfo.getLiteralExprValues(), logicalPartitionId);
        List<List<LiteralExpr>> multiValues = valueOrNull(partitionInfo.getMultiLiteralExprValues(), logicalPartitionId);
        if (singleValues != null && multiValues != null) {
            return Resolution.unprovable(UnprovableReason.PARTITION_METADATA_MISSING,
                    "List partition contains both single and multi-column metadata");
        }

        List<LiteralExpr> timeValues = new ArrayList<>();
        if (singleValues != null) {
            if (partitionInfo.getPartitionColumnsSize() != 1 || timeIndex != 0) {
                return Resolution.unprovable(UnprovableReason.TUPLE_ARITY_MISMATCH,
                        "single List values do not match the bound partition tuple");
            }
            timeValues.addAll(singleValues);
        } else if (multiValues != null) {
            int expectedArity = partitionInfo.getPartitionColumnsSize();
            for (List<LiteralExpr> tuple : multiValues) {
                if (tuple == null || tuple.size() != expectedArity) {
                    return Resolution.unprovable(UnprovableReason.TUPLE_ARITY_MISMATCH,
                            "List partition tuple arity does not match partition expressions");
                }
                timeValues.add(tuple.get(timeIndex));
            }
        } else {
            return Resolution.unprovable(UnprovableReason.DEFAULT_OR_MISSING_LIST_VALUES,
                    "List partition has DEFAULT or missing values");
        }
        if (timeValues.isEmpty()) {
            return Resolution.unprovable(UnprovableReason.DEFAULT_OR_MISSING_LIST_VALUES,
                    "List partition has no explicit values");
        }

        boolean formattedDate = TenantTtlBindingAnalyzer.LIST_FROM_UNIXTIME_YYYYMMDD.equals(
                binding.getPartitionExpressionType());
        ZoneId zone = formattedDate ? requireZone(binding) : null;
        List<TimeInterval> intervals = new ArrayList<>(timeValues.size());
        for (LiteralExpr value : timeValues) {
            if (value == null || value instanceof NullLiteral) {
                return Resolution.unprovable(UnprovableReason.NULL_OR_DEFAULT_LIST_VALUE,
                        "List partition time component is NULL or DEFAULT");
            }
            if (formattedDate) {
                if (!(value instanceof StringLiteral)) {
                    return Resolution.unprovable(UnprovableReason.INVALID_BOUND_LITERAL,
                            "formatted List time component must be an 8-digit string");
                }
                LocalDate date;
                try {
                    date = LocalDate.parse(value.getStringValue(), YYYYMMDD);
                } catch (DateTimeParseException e) {
                    return Resolution.unprovable(UnprovableReason.INVALID_TIME_ZONE_OR_DATE,
                            "invalid %Y%m%d List value: " + value.getStringValue());
                }
                long lower = date.atStartOfDay(zone).toEpochSecond();
                long upper = date.plusDays(1).atStartOfDay(zone).toEpochSecond();
                intervals.add(new TimeInterval(lower, upper));
            } else {
                long lower = integerBound(value);
                intervals.add(new TimeInterval(lower, Math.addExact(lower, 1L)));
            }
        }
        intervals.sort(Comparator.comparingLong(interval -> interval.getLowerInclusive()));
        return Resolution.provable(intervals);
    }

    private static long integerBound(LiteralExpr literal) {
        if (!(literal instanceof IntLiteral)) {
            throw new IllegalArgumentException("Unix-second partition bound must be an integer literal");
        }
        return literal.getLongValue();
    }

    private static long dateTimeBound(LiteralExpr literal, ZoneId zone, boolean upper) {
        if (!(literal instanceof DateLiteral)) {
            throw new IllegalArgumentException("from_unixtime Range bound must be a DATE or DATETIME literal");
        }
        DateLiteral dateLiteral = (DateLiteral) literal;
        if (dateLiteral.getMicrosecond() != 0) {
            throw new IllegalArgumentException("sub-second from_unixtime Range bounds are not supported");
        }
        return localDateTimeBound(dateLiteral.toLocalDateTime(), zone, upper);
    }

    private static long localDateTimeBound(LocalDateTime value, ZoneId zone, boolean upper) {
        ZoneRules rules = zone.getRules();
        List<ZoneOffset> offsets = rules.getValidOffsets(value);
        if (offsets.size() == 1) {
            return value.toEpochSecond(offsets.get(0));
        }
        if (offsets.size() == 2) {
            ZoneOffset selected = upper ? offsets.stream().min(Comparator.comparingInt(ZoneOffset::getTotalSeconds)).get() :
                    offsets.stream().max(Comparator.comparingInt(ZoneOffset::getTotalSeconds)).get();
            return value.toEpochSecond(selected);
        }
        ZoneOffsetTransition transition = rules.getTransition(value);
        if (transition == null) {
            throw new DateTimeException("cannot resolve local partition boundary " + value + " in " + zone);
        }
        return transition.getInstant().getEpochSecond();
    }

    private static ZoneId requireZone(TenantTtlTableBinding binding) {
        String zoneId = binding.getNormalizedTimeZone();
        if (zoneId == null || TenantTtlBindingAnalyzer.NOT_APPLICABLE_TIME_ZONE.equals(zoneId)) {
            throw new DateTimeException("bound partition expression requires compaction_retention_time_zone");
        }
        return ZoneId.of(zoneId);
    }

    private static <T> T valueOrNull(Map<Long, T> values, long partitionId) {
        return values == null ? null : values.get(partitionId);
    }

    public enum UnprovableReason {
        BINDING_MISMATCH,
        UNSUPPORTED_EXPRESSION,
        PARTITION_METADATA_MISSING,
        MAXVALUE_UPPER_BOUND,
        NULLABLE_MINVALUE_PARTITION,
        DEFAULT_OR_MISSING_LIST_VALUES,
        NULL_OR_DEFAULT_LIST_VALUE,
        INVALID_TIME_ZONE_OR_DATE,
        TUPLE_ARITY_MISMATCH,
        EXACT_ARITHMETIC_OVERFLOW,
        INVALID_BOUND_LITERAL,
        INVALID_PARTITION_INTERVAL
    }

    public static final class TimeInterval {
        private final Long lowerInclusive;
        private final long upperExclusive;

        private TimeInterval(Long lowerInclusive, long upperExclusive) {
            this.lowerInclusive = lowerInclusive;
            this.upperExclusive = upperExclusive;
        }

        public boolean isUnboundedBelow() {
            return lowerInclusive == null;
        }

        public long getLowerInclusive() {
            if (lowerInclusive == null) {
                throw new IllegalStateException("interval is unbounded below");
            }
            return lowerInclusive;
        }

        public long getUpperExclusive() {
            return upperExclusive;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TimeInterval)) {
                return false;
            }
            TimeInterval that = (TimeInterval) o;
            return upperExclusive == that.upperExclusive && Objects.equals(lowerInclusive, that.lowerInclusive);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lowerInclusive, upperExclusive);
        }
    }

    public static final class Resolution {
        private final List<TimeInterval> intervals;
        private final Long partitionUpperEpochSecond;
        private final UnprovableReason reason;
        private final String detail;

        private Resolution(List<TimeInterval> intervals, Long partitionUpperEpochSecond,
                           UnprovableReason reason, String detail) {
            this.intervals = intervals;
            this.partitionUpperEpochSecond = partitionUpperEpochSecond;
            this.reason = reason;
            this.detail = detail;
        }

        private static Resolution provable(List<TimeInterval> intervals) {
            if (intervals.isEmpty()) {
                throw new IllegalArgumentException("provable partition must contain at least one interval");
            }
            List<TimeInterval> immutable = Collections.unmodifiableList(new ArrayList<>(intervals));
            long upper = immutable.stream().mapToLong(TimeInterval::getUpperExclusive).max().getAsLong();
            return new Resolution(immutable, upper, null, "");
        }

        private static Resolution unprovable(UnprovableReason reason, String detail) {
            return new Resolution(Collections.emptyList(), null, Objects.requireNonNull(reason),
                    detail == null ? "" : detail);
        }

        public boolean isProvable() {
            return partitionUpperEpochSecond != null;
        }

        public List<TimeInterval> getIntervals() {
            return intervals;
        }

        public long getPartitionUpperEpochSecond() {
            if (partitionUpperEpochSecond == null) {
                throw new IllegalStateException("partition does not have a provable upper bound");
            }
            return partitionUpperEpochSecond;
        }

        public UnprovableReason getReason() {
            return reason;
        }

        public String getDetail() {
            return detail;
        }
    }
}
