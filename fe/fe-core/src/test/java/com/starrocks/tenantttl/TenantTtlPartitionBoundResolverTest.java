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
