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

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class TenantTtlPropertyTest {
    @Test
    public void testParseValidCondition() {
        TenantTtlPropertyParser.ParsedCondition parsed = TenantTtlPropertyParser.parse(
                "dictionary_ttl('tenant_policy', 'business.http_log', 2147483647)");
        Assertions.assertEquals("tenant_policy", parsed.getDictionaryName());
        Assertions.assertEquals("business.http_log", parsed.getTableKey());
        Assertions.assertEquals(Integer.MAX_VALUE, parsed.getDefaultDays());

        parsed = TenantTtlPropertyParser.parse(
                "DICTIONARY_TTL('tenant_policy', 'logical_db.logical_table', 1)");
        Assertions.assertEquals(1, parsed.getDefaultDays());
    }

    @Test
    public void testRejectInvalidConditionShape() {
        assertInvalid(null);
        assertInvalid("");
        assertInvalid("1 + 1");
        assertInvalid("other('tenant_policy', 'business.http_log', 30)");
        assertInvalid("db.dictionary_ttl('tenant_policy', 'business.http_log', 30)");
        assertInvalid("dictionary_ttl('tenant_policy', 'business.http_log')");
        assertInvalid("dictionary_ttl('tenant_policy', 'business.http_log', 30, 'extra')");
        assertInvalid("dictionary_ttl(concat('tenant', '_policy'), 'business.http_log', 30)");
        assertInvalid("dictionary_ttl('tenant_policy', concat('business', '.http_log'), 30)");
        assertInvalid("dictionary_ttl('tenant_policy', 'business.http_log', cast(30 as int))");
    }

    @Test
    public void testRejectInvalidArguments() {
        assertInvalid("dictionary_ttl('', 'business.http_log', 30)");
        assertInvalid("dictionary_ttl('tenant_policy', '', 30)");
        assertInvalid("dictionary_ttl('tenant_policy', 'business', 30)");
        assertInvalid("dictionary_ttl('tenant_policy', '.http_log', 30)");
        assertInvalid("dictionary_ttl('tenant_policy', 'business.http.log', 30)");
        assertInvalid("dictionary_ttl('tenant_policy', ' business.http_log', 30)");
        assertInvalid("dictionary_ttl('tenant_policy', 'business.http_log ', 30)");
        assertInvalid("dictionary_ttl('tenant_policy', 'business .http_log', 30)");
        assertInvalid("dictionary_ttl('tenant_policy', 'business.http_log', 0)");
        assertInvalid("dictionary_ttl('tenant_policy', 'business.http_log', -1)");
        assertInvalid("dictionary_ttl('tenant_policy', 'business.http_log', 2147483648)");
        assertInvalid("dictionary_ttl('tenant_policy', 'business.http_log', 1.0)");
    }

    @Test
    public void testTablePropertyCopyAndJsonRoundTrip() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_COMPACTION_RETENTION_CONDITION,
                "dictionary_ttl('tenant_policy', 'business.http_log', 180)");
        properties.put(PropertyAnalyzer.PROPERTIES_COMPACTION_RETENTION_TIME_ZONE, "Asia/Shanghai");
        TableProperty original = new TableProperty(properties).buildCompactionRetentionProperties();

        TenantTtlDictionaryBinding dictionaryBinding =
                new TenantTtlDictionaryBinding(101L, "tenant_policy", "business.http_log", 180);
        TenantTtlTableBinding tableBinding = new TenantTtlTableBinding(
                "tenant", 7, "recordTimestamp", 8, "RANGE_DIRECT_UNIX_SECONDS",
                TenantTtlTableBinding.NO_LIST_TIME_COMPONENT, "Asia/Shanghai", "partition-fingerprint");
        original.setTenantTtlDictionaryBinding(dictionaryBinding);
        original.setTenantTtlTableBinding(tableBinding);

        TableProperty copied = original.copy();
        assertBindingsAndProperties(copied, dictionaryBinding, tableBinding);
        Assertions.assertNotSame(dictionaryBinding, copied.getTenantTtlDictionaryBinding());
        Assertions.assertNotSame(tableBinding, copied.getTenantTtlTableBinding());

        String json = GsonUtils.GSON.toJson(original);
        TableProperty restored = GsonUtils.GSON.fromJson(json, TableProperty.class);
        assertBindingsAndProperties(restored, dictionaryBinding, tableBinding);
    }

    @Test
    public void testLegacyTablePropertyJsonWithoutBindings() {
        String json = GsonUtils.GSON.toJson(new TableProperty(
                ImmutableMap.of("replication_num", "1")));
        TableProperty restored = GsonUtils.GSON.fromJson(json, TableProperty.class);
        Assertions.assertNull(restored.getTenantTtlDictionaryBinding());
        Assertions.assertNull(restored.getTenantTtlTableBinding());
        Assertions.assertNull(restored.getCompactionRetentionCondition());
        Assertions.assertNull(restored.getCompactionRetentionTimeZone());
    }

    private static void assertBindingsAndProperties(TableProperty tableProperty,
                                                    TenantTtlDictionaryBinding dictionaryBinding,
                                                    TenantTtlTableBinding tableBinding) {
        Assertions.assertEquals(dictionaryBinding, tableProperty.getTenantTtlDictionaryBinding());
        Assertions.assertEquals(tableBinding, tableProperty.getTenantTtlTableBinding());
        Assertions.assertEquals("dictionary_ttl('tenant_policy', 'business.http_log', 180)",
                tableProperty.getCompactionRetentionCondition());
        Assertions.assertEquals("Asia/Shanghai", tableProperty.getCompactionRetentionTimeZone());
    }

    private static void assertInvalid(String condition) {
        Assertions.assertThrows(SemanticException.class, () -> TenantTtlPropertyParser.parse(condition));
    }
}
