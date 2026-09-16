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
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.TenantTtlBindingAnalyzer;
import com.starrocks.catalog.TenantTtlTableBinding;
import com.starrocks.catalog.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TenantTtlPartitionBoundResolverTest {
    private static final long PARTITION_ID = 101L;

    @Test
    public void testDirectRangeFiniteAndMinValueRules() throws Exception {
        Column timeColumn = new Column("recordTimestamp", Type.BIGINT, false);
        RangePartitionInfo finite = rangeInfo(timeColumn, bigintKey(100), bigintKey(370));
        TenantTtlPartitionBoundResolver.Resolution result = resolve(finite, timeColumn,
                TenantTtlBindingAnalyzer.RANGE_DIRECT_UNIX_SECONDS, -1,
                TenantTtlBindingAnalyzer.NOT_APPLICABLE_TIME_ZONE);
        Assertions.assertTrue(result.isProvable());
        Assertions.assertEquals(370, result.getPartitionUpperEpochSecond());
        Assertions.assertEquals(1, result.getIntervals().size());
        Assertions.assertEquals(100, result.getIntervals().get(0).getLowerInclusive());
        List<TenantTtlPartitionBoundResolver.TimeInterval> immutableIntervals = result.getIntervals();
        Assertions.assertThrows(UnsupportedOperationException.class, immutableIntervals::clear);

        PartitionKey min = PartitionKey.createInfinityPartitionKey(Collections.singletonList(timeColumn), false);
        RangePartitionInfo minRange = rangeInfo(timeColumn, min, bigintKey(100));
        result = resolve(minRange, timeColumn, TenantTtlBindingAnalyzer.RANGE_DIRECT_UNIX_SECONDS, -1,
                TenantTtlBindingAnalyzer.NOT_APPLICABLE_TIME_ZONE);
        Assertions.assertTrue(result.isProvable());
        Assertions.assertTrue(result.getIntervals().get(0).isUnboundedBelow());

        Column nullableTime = new Column("recordTimestamp", Type.BIGINT, true);
        RangePartitionInfo nullableMin = rangeInfo(nullableTime,
                PartitionKey.createInfinityPartitionKey(Collections.singletonList(nullableTime), false), bigintKey(100));
        result = resolve(nullableMin, nullableTime, TenantTtlBindingAnalyzer.RANGE_DIRECT_UNIX_SECONDS, -1,
                TenantTtlBindingAnalyzer.NOT_APPLICABLE_TIME_ZONE);
        assertUnprovable(result, TenantTtlPartitionBoundResolver.UnprovableReason.NULLABLE_MINVALUE_PARTITION);
    }

    @Test
    public void testRangeMaxValueAndFromUnixTimeZone() throws Exception {
        Column directColumn = new Column("recordTimestamp", Type.BIGINT, false);
        RangePartitionInfo maxRange = rangeInfo(directColumn, bigintKey(1),
                PartitionKey.createInfinityPartitionKey(Collections.singletonList(directColumn), true));
        TenantTtlPartitionBoundResolver.Resolution result = resolve(maxRange, directColumn,
                TenantTtlBindingAnalyzer.RANGE_DIRECT_UNIX_SECONDS, -1,
                TenantTtlBindingAnalyzer.NOT_APPLICABLE_TIME_ZONE);
        assertUnprovable(result, TenantTtlPartitionBoundResolver.UnprovableReason.MAXVALUE_UPPER_BOUND);

        Column dateColumn = new Column("generated_date", Type.DATE, false);
        RangePartitionInfo dateRange = rangeInfo(dateColumn,
                dateKey("2024-03-10"), dateKey("2024-03-11"));
        result = resolve(dateRange, directColumn, TenantTtlBindingAnalyzer.RANGE_FROM_UNIXTIME, -1,
                "America/Los_Angeles");
        Assertions.assertTrue(result.isProvable());
        TenantTtlPartitionBoundResolver.TimeInterval interval = result.getIntervals().get(0);
        Assertions.assertEquals(23 * 60 * 60,
                interval.getUpperExclusive() - interval.getLowerInclusive());
    }

    @Test
    public void testDirectListMultipleValuesAndOverflow() {
        Column timeColumn = new Column("recordTimestamp", Type.BIGINT, false);
        ListPartitionInfo listInfo = listInfo(Collections.singletonList(timeColumn));
        listInfo.setDirectLiteralExprValues(PARTITION_ID,
                Arrays.asList(new IntLiteral(9, Type.BIGINT), new IntLiteral(3, Type.BIGINT)));
        TenantTtlPartitionBoundResolver.Resolution result = resolve(listInfo, timeColumn,
                TenantTtlBindingAnalyzer.LIST_DIRECT_UNIX_SECONDS, 0,
                TenantTtlBindingAnalyzer.NOT_APPLICABLE_TIME_ZONE);
        Assertions.assertTrue(result.isProvable());
        Assertions.assertEquals(2, result.getIntervals().size());
        Assertions.assertEquals(3, result.getIntervals().get(0).getLowerInclusive());
        Assertions.assertEquals(10, result.getPartitionUpperEpochSecond());

        listInfo.setDirectLiteralExprValues(PARTITION_ID,
                Collections.singletonList(new IntLiteral(Long.MAX_VALUE, Type.BIGINT)));
        result = resolve(listInfo, timeColumn, TenantTtlBindingAnalyzer.LIST_DIRECT_UNIX_SECONDS, 0,
                TenantTtlBindingAnalyzer.NOT_APPLICABLE_TIME_ZONE);
        assertUnprovable(result, TenantTtlPartitionBoundResolver.UnprovableReason.EXACT_ARITHMETIC_OVERFLOW);
    }

    @Test
    public void testFormattedListStrictDatesAndDst() {
        Column generatedDate = new Column("generated_date", Type.VARCHAR, false);
        Column timeColumn = new Column("recordTimestamp", Type.BIGINT, false);
        ListPartitionInfo listInfo = listInfo(Collections.singletonList(generatedDate));
        listInfo.setDirectLiteralExprValues(PARTITION_ID,
                Arrays.asList(new StringLiteral("20240310"), new StringLiteral("20241103")));
        TenantTtlPartitionBoundResolver.Resolution result = resolve(listInfo, timeColumn,
                TenantTtlBindingAnalyzer.LIST_FROM_UNIXTIME_YYYYMMDD, 0, "America/Los_Angeles");
        Assertions.assertTrue(result.isProvable());
        Assertions.assertEquals(23 * 60 * 60,
                result.getIntervals().get(0).getUpperExclusive() -
                        result.getIntervals().get(0).getLowerInclusive());
        Assertions.assertEquals(25 * 60 * 60,
                result.getIntervals().get(1).getUpperExclusive() -
                        result.getIntervals().get(1).getLowerInclusive());
        long expectedUpper = LocalDate.of(2024, 11, 4).atStartOfDay(ZoneId.of("America/Los_Angeles"))
                .toEpochSecond();
        Assertions.assertEquals(expectedUpper, result.getPartitionUpperEpochSecond());

        listInfo.setDirectLiteralExprValues(PARTITION_ID,
                Collections.singletonList(new StringLiteral("20240230")));
        result = resolve(listInfo, timeColumn, TenantTtlBindingAnalyzer.LIST_FROM_UNIXTIME_YYYYMMDD, 0,
                "America/Los_Angeles");
        assertUnprovable(result, TenantTtlPartitionBoundResolver.UnprovableReason.INVALID_TIME_ZONE_OR_DATE);
    }

    @Test
    public void testMultiListUsesOnlyBoundTimeComponentAndMaxUpper() {
        Column bucket = new Column("tenant_bucket", Type.INT, false);
        Column generatedDate = new Column("generated_date", Type.VARCHAR, false);
        Column timeColumn = new Column("recordTimestamp", Type.BIGINT, false);
        ListPartitionInfo listInfo = listInfo(Arrays.asList(bucket, generatedDate));
        List<LiteralExpr> first = Arrays.asList(new IntLiteral(7, Type.INT), new StringLiteral("20240229"));
        List<LiteralExpr> second = Arrays.asList(new IntLiteral(9, Type.INT), new StringLiteral("20240302"));
        listInfo.setDirectMultiLiteralExprValues(PARTITION_ID, Arrays.asList(first, second));
        TenantTtlPartitionBoundResolver.Resolution result = resolve(listInfo, timeColumn,
                TenantTtlBindingAnalyzer.LIST_FROM_UNIXTIME_YYYYMMDD, 1, "Asia/Shanghai");
        Assertions.assertTrue(result.isProvable());
        Assertions.assertEquals(2, result.getIntervals().size());
        Assertions.assertEquals(LocalDate.of(2024, 3, 3).atStartOfDay(ZoneId.of("Asia/Shanghai")).toEpochSecond(),
                result.getPartitionUpperEpochSecond());

        listInfo.setDirectMultiLiteralExprValues(PARTITION_ID,
                Collections.singletonList(Collections.singletonList(new IntLiteral(7, Type.INT))));
        result = resolve(listInfo, timeColumn, TenantTtlBindingAnalyzer.LIST_FROM_UNIXTIME_YYYYMMDD, 1,
                "Asia/Shanghai");
        assertUnprovable(result, TenantTtlPartitionBoundResolver.UnprovableReason.TUPLE_ARITY_MISMATCH);
    }

    @Test
    public void testDateTruncDayCalendarBoundsMatchFormattedDates() throws Exception {
        Column timeColumn = new Column("recordTimestamp", Type.BIGINT, false);
        ListPartitionInfo truncated = listInfo(Collections.singletonList(new Column("day", Type.DATETIME, false)));
        ListPartitionInfo formatted = listInfo(Collections.singletonList(new Column("day", Type.VARCHAR, false)));
        String[] dates = {"2024-03-10", "2024-11-03", "2024-02-29", "2024-12-31", "2024-04-30"};
        long[] hours = {23, 25, 24, 24, 24};
        for (int i = 0; i < dates.length; i++) {
            truncated.setDirectLiteralExprValues(PARTITION_ID,
                    Collections.singletonList(new DateLiteral(dates[i] + " 00:00:00", Type.DATETIME)));
            formatted.setDirectLiteralExprValues(PARTITION_ID,
                    Collections.singletonList(new StringLiteral(dates[i].replace("-", ""))));
            TenantTtlPartitionBoundResolver.Resolution result = resolve(truncated, timeColumn,
                    TenantTtlBindingAnalyzer.LIST_DATE_TRUNC_DAY_FROM_UNIXTIME, 0, "America/Los_Angeles");
            Assertions.assertTrue(result.isProvable());
            TenantTtlPartitionBoundResolver.TimeInterval interval = result.getIntervals().get(0);
            Assertions.assertEquals(hours[i] * 3600, interval.getUpperExclusive() - interval.getLowerInclusive());
            Assertions.assertEquals(LocalDate.parse(dates[i]).plusDays(1)
                            .atStartOfDay(ZoneId.of("America/Los_Angeles")).toEpochSecond(),
                    result.getPartitionUpperEpochSecond());
            TenantTtlPartitionBoundResolver.Resolution old = resolve(formatted, timeColumn,
                    TenantTtlBindingAnalyzer.LIST_FROM_UNIXTIME_YYYYMMDD, 0, "America/Los_Angeles");
            Assertions.assertEquals(old.getPartitionUpperEpochSecond(), result.getPartitionUpperEpochSecond());
            Assertions.assertEquals(old.getIntervals().get(0).getLowerInclusive(), interval.getLowerInclusive());
        }
    }

    @Test
    public void testDateTruncMultiListMaxUpperAndInvalidTupleFailClosed() throws Exception {
        Column timeColumn = new Column("recordTimestamp", Type.BIGINT, false);
        ListPartitionInfo info = listInfo(Arrays.asList(new Column("bucket", Type.INT, false),
                new Column("day", Type.DATETIME, false)));
        List<LiteralExpr> later = Arrays.asList(new IntLiteral(1), new DateLiteral("2024-03-02", Type.DATETIME));
        List<LiteralExpr> earlier = Arrays.asList(new IntLiteral(63), new DateLiteral("2024-02-29", Type.DATETIME));
        info.setDirectMultiLiteralExprValues(PARTITION_ID, Arrays.asList(later, earlier));
        TenantTtlPartitionBoundResolver.Resolution result = resolve(info, timeColumn,
                TenantTtlBindingAnalyzer.LIST_DATE_TRUNC_DAY_FROM_UNIXTIME, 1, "Asia/Shanghai");
        Assertions.assertTrue(result.isProvable());
        Assertions.assertEquals(2, result.getIntervals().size());
        Assertions.assertEquals(LocalDate.of(2024, 3, 3).atStartOfDay(ZoneId.of("Asia/Shanghai")).toEpochSecond(),
                result.getPartitionUpperEpochSecond());
        info.setDirectMultiLiteralExprValues(PARTITION_ID, Arrays.asList(later,
                Arrays.asList(new IntLiteral(63), new DateLiteral("2024-02-29 12:00:00", Type.DATETIME))));
        assertUnprovable(resolve(info, timeColumn,
                        TenantTtlBindingAnalyzer.LIST_DATE_TRUNC_DAY_FROM_UNIXTIME, 1, "Asia/Shanghai"),
                TenantTtlPartitionBoundResolver.UnprovableReason.INVALID_BOUND_LITERAL);
        info.setDirectMultiLiteralExprValues(PARTITION_ID, Arrays.asList(later, Collections.singletonList(new IntLiteral(1))));
        assertUnprovable(resolve(info, timeColumn,
                        TenantTtlBindingAnalyzer.LIST_DATE_TRUNC_DAY_FROM_UNIXTIME, 1, "Asia/Shanghai"),
                TenantTtlPartitionBoundResolver.UnprovableReason.TUPLE_ARITY_MISMATCH);
    }

    @Test
    public void testDateTruncRejectsNonMidnightWrongTypesNullAndMalformedDates() throws Exception {
        Column timeColumn = new Column("recordTimestamp", Type.BIGINT, false);
        ListPartitionInfo info = listInfo(Collections.singletonList(new Column("day", Type.DATETIME, true)));
        String type = TenantTtlBindingAnalyzer.LIST_DATE_TRUNC_DAY_FROM_UNIXTIME;
        assertUnprovable(resolve(info, timeColumn, type, 0, "UTC"),
                TenantTtlPartitionBoundResolver.UnprovableReason.DEFAULT_OR_MISSING_LIST_VALUES);
        LiteralExpr[] wrongValues = {
                new StringLiteral("2024-03-10 00:00:00"), new IntLiteral(20240310),
                new DateLiteral("2024-03-10", Type.DATE),
                new DateLiteral("2024-03-10 01:00:00", Type.DATETIME),
                new DateLiteral("2024-03-10 00:01:00", Type.DATETIME),
                new DateLiteral("2024-03-10 00:00:01", Type.DATETIME),
                new DateLiteral(2024, 3, 10, 0, 0, 0, 1)
        };
        for (LiteralExpr value : wrongValues) {
            info.setDirectLiteralExprValues(PARTITION_ID, Arrays.asList(
                    new DateLiteral("2024-03-09", Type.DATETIME), value));
            assertUnprovable(resolve(info, timeColumn, type, 0, "UTC"),
                    TenantTtlPartitionBoundResolver.UnprovableReason.INVALID_BOUND_LITERAL);
        }
        info.setDirectLiteralExprValues(PARTITION_ID, Collections.singletonList(NullLiteral.create(Type.DATETIME)));
        assertUnprovable(resolve(info, timeColumn, type, 0, "UTC"),
                TenantTtlPartitionBoundResolver.UnprovableReason.NULL_OR_DEFAULT_LIST_VALUE);
        info.setDirectLiteralExprValues(PARTITION_ID, Collections.emptyList());
        assertUnprovable(resolve(info, timeColumn, type, 0, "UTC"),
                TenantTtlPartitionBoundResolver.UnprovableReason.DEFAULT_OR_MISSING_LIST_VALUES);
        for (DateLiteral invalid : Arrays.asList(new DateLiteral(2024, 2, 30, 0, 0, 0, 0),
                new DateLiteral(10000, 1, 1, 0, 0, 0, 0), new DateLiteral(2024, 0, 1, 0, 0, 0, 0))) {
            info.setDirectLiteralExprValues(PARTITION_ID, Collections.singletonList(invalid));
            assertUnprovable(resolve(info, timeColumn, type, 0, "UTC"),
                    TenantTtlPartitionBoundResolver.UnprovableReason.INVALID_TIME_ZONE_OR_DATE);
        }
        info.setDirectLiteralExprValues(PARTITION_ID,
                Collections.singletonList(new DateLiteral("2024-03-10", Type.DATETIME)));
        assertUnprovable(resolve(info, timeColumn, type, 0, "Invalid/Zone"),
                TenantTtlPartitionBoundResolver.UnprovableReason.INVALID_TIME_ZONE_OR_DATE);
    }

    @Test
    public void testNullDefaultAndMissingListValuesFailClosed() {
        Column timeColumn = new Column("recordTimestamp", Type.BIGINT, false);
        ListPartitionInfo listInfo = listInfo(Collections.singletonList(timeColumn));
        TenantTtlPartitionBoundResolver.Resolution result = resolve(listInfo, timeColumn,
                TenantTtlBindingAnalyzer.LIST_DIRECT_UNIX_SECONDS, 0,
                TenantTtlBindingAnalyzer.NOT_APPLICABLE_TIME_ZONE);
        assertUnprovable(result,
                TenantTtlPartitionBoundResolver.UnprovableReason.DEFAULT_OR_MISSING_LIST_VALUES);

        listInfo.setDirectLiteralExprValues(PARTITION_ID,
                Collections.singletonList(NullLiteral.create(Type.BIGINT)));
        result = resolve(listInfo, timeColumn, TenantTtlBindingAnalyzer.LIST_DIRECT_UNIX_SECONDS, 0,
                TenantTtlBindingAnalyzer.NOT_APPLICABLE_TIME_ZONE);
        assertUnprovable(result, TenantTtlPartitionBoundResolver.UnprovableReason.NULL_OR_DEFAULT_LIST_VALUE);
    }

    private static TenantTtlPartitionBoundResolver.Resolution resolve(
            PartitionInfo partitionInfo, Column timeColumn, String expressionType, int listTimeIndex, String zone) {
        TenantTtlTableBinding binding = new TenantTtlTableBinding(
                "tenant", 1, "recordTimestamp", 2, expressionType, listTimeIndex, zone, "fingerprint");
        return TenantTtlPartitionBoundResolver.resolve(partitionInfo, timeColumn, PARTITION_ID, binding);
    }

    private static RangePartitionInfo rangeInfo(Column partitionColumn, PartitionKey lower, PartitionKey upper) {
        RangePartitionInfo partitionInfo = new RangePartitionInfo(Collections.singletonList(partitionColumn));
        partitionInfo.addPartition(PARTITION_ID, false, Range.closedOpen(lower, upper),
                DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, false);
        return partitionInfo;
    }

    private static ListPartitionInfo listInfo(List<Column> columns) {
        return new ListPartitionInfo(PartitionType.LIST, columns);
    }

    private static PartitionKey bigintKey(long value) {
        return new PartitionKey(Collections.singletonList(new IntLiteral(value, Type.BIGINT)),
                Collections.singletonList(PrimitiveType.BIGINT));
    }

    private static PartitionKey dateKey(String value) throws Exception {
        return new PartitionKey(Collections.singletonList(new DateLiteral(value, Type.DATE)),
                Collections.singletonList(PrimitiveType.DATE));
    }

    private static void assertUnprovable(TenantTtlPartitionBoundResolver.Resolution result,
                                         TenantTtlPartitionBoundResolver.UnprovableReason reason) {
        Assertions.assertFalse(result.isProvable());
        Assertions.assertEquals(reason, result.getReason());
        Assertions.assertThrows(IllegalStateException.class, result::getPartitionUpperEpochSecond);
    }
}
