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

package com.starrocks.sql.parser;

import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.ShowTenantTtlStatusStmt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowTenantTtlStatusStmtTest {
    @Test
    public void testParseTableAndTenantForms() {
        ShowTenantTtlStatusStmt tableStatus = (ShowTenantTtlStatusStmt) SqlParser.parseSingleStatement(
                "SHOW TENANT TTL STATUS FROM business.http_log", 0);
        Assertions.assertEquals("business", tableStatus.getTableName().getDb());
        Assertions.assertEquals("http_log", tableStatus.getTableName().getTbl());
        Assertions.assertFalse(tableStatus.hasTenant());

        ShowTenantTtlStatusStmt tenantStatus = (ShowTenantTtlStatusStmt) SqlParser.parseSingleStatement(
                "SHOW TENANT TTL STATUS FROM `tenant`.`ttl` FOR TENANT ' Tenant_A '", 0);
        Assertions.assertEquals("tenant", tenantStatus.getTableName().getDb());
        Assertions.assertEquals("ttl", tenantStatus.getTableName().getTbl());
        Assertions.assertTrue(tenantStatus.hasTenant());
        Assertions.assertEquals(" Tenant_A ", tenantStatus.getTenant());
    }

    @Test
    public void testTenantAndTtlRemainUsableAsIdentifiers() {
        Assertions.assertDoesNotThrow(() -> SqlParser.parseSingleStatement(
                "SELECT tenant, ttl FROM tenant.ttl", 0));
    }

    @Test
    public void testMetadataShapeAndNullCapableTypes() {
        ShowTenantTtlStatusStmt tableStatus = (ShowTenantTtlStatusStmt) SqlParser.parseSingleStatement(
                "SHOW TENANT TTL STATUS FROM business.http_log", 0);
        ShowResultSetMetaData tableMetadata = new ShowResultMetaFactory().getMetadata(tableStatus);
        Assertions.assertEquals(ShowTenantTtlStatusStmt.BASE_TITLE_NAMES.size(), tableMetadata.getColumnCount());
        for (int i = 0; i < ShowTenantTtlStatusStmt.BASE_TITLE_NAMES.size(); ++i) {
            Assertions.assertEquals(ShowTenantTtlStatusStmt.BASE_TITLE_NAMES.get(i),
                    tableMetadata.getColumn(i).getName());
        }

        ShowTenantTtlStatusStmt tenantStatus = (ShowTenantTtlStatusStmt) SqlParser.parseSingleStatement(
                "SHOW TENANT TTL STATUS FROM business.http_log FOR TENANT 'tenant_a'", 0);
        ShowResultSetMetaData tenantMetadata = new ShowResultMetaFactory().getMetadata(tenantStatus);
        Assertions.assertEquals(ShowTenantTtlStatusStmt.BASE_TITLE_NAMES.size() +
                ShowTenantTtlStatusStmt.TENANT_TITLE_NAMES.size(), tenantMetadata.getColumnCount());
        Assertions.assertTrue(tenantMetadata.getColumn(4).getType().isBigint());
        Assertions.assertTrue(tenantMetadata.getColumn(6).getType().isIntegerType());
        Assertions.assertEquals("Tenant", tenantMetadata.getColumn(30).getName());
        Assertions.assertEquals("EffectiveRetentionDays", tenantMetadata.getColumn(31).getName());
        Assertions.assertEquals("ResolutionType", tenantMetadata.getColumn(32).getName());
    }

    @Test
    public void testRejectIncompleteSyntax() {
        Assertions.assertThrows(ParsingException.class, () -> SqlParser.parseSingleStatement(
                "SHOW TENANT TTL STATUS business.http_log", 0));
        Assertions.assertThrows(ParsingException.class, () -> SqlParser.parseSingleStatement(
                "SHOW TENANT TTL STATUS FROM business.http_log FOR TENANT tenant_a", 0));
    }
}
