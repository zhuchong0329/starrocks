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

package com.starrocks.pseudocluster;

import com.starrocks.common.Config;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.qe.QueryCorruptionWarning;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Real socket/JDBC/FE protocol test; only BE result statistics are simulated, not disk corruption. */
class QueryCorruptionJdbcTest {
    private static PseudoCluster cluster;
    private static boolean originalConfig;

    @BeforeAll
    static void start() throws Exception {
        originalConfig = Config.enable_query_corruption_tolerance;
        cluster = PseudoCluster.getOrCreateWithRandomPort(true, 1);
        cluster.runSql(null, "create database qct_jdbc");
        cluster.runSql("qct_jdbc", "create table events(k bigint, v int) duplicate key(k) "
                + "distributed by hash(k) buckets 1 properties('replication_num'='1')");
        cluster.runSql("qct_jdbc", "insert into events values (1, 2)");
    }

    @AfterAll
    static void stop() {
        Config.enable_query_corruption_tolerance = originalConfig;
        if (cluster != null) {
            cluster.shutdown(false); // Preserve this task's integration logs.
        }
    }

    private void injectDiagnostic() {
        new MockUp<PseudoBackend.QueryProgress>() {
            @Mock
            Future<PFetchDataResult> getFetchDataResult(Invocation invocation) {
                CompletableFuture<PFetchDataResult> original = invocation.proceed();
                return original.thenApply(result -> {
                    result.queryStatistics.queryCorruptionDetected = true;
                    return result;
                });
            }
        };
    }

    private void exercise(Connection connection) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute("set enable_short_circuit = false");
            statement.execute("set enable_query_cache = false");
            Config.enable_query_corruption_tolerance = true;
            try (ResultSet rows = statement.executeQuery("select * from qct_jdbc.events")) {
                assertFalse(rows.next()); // Entirely unreadable input may produce zero rows + Warning.
                assertEquals(2, rows.getMetaData().getColumnCount());
            }
            SQLWarning warning = statement.getWarnings();
            assertNotNull(warning);
            assertEquals(QueryCorruptionWarning.CODE, warning.getErrorCode());
            assertTrue(warning.getMessage().contains(QueryCorruptionWarning.NAME));
            assertNull(warning.getNextWarning());
            try (ResultSet errors = statement.executeQuery("show errors")) {
                assertFalse(errors.next()); // Warning must not become an error or erase SHOW WARNINGS.
            }
            try (ResultSet warnings = statement.executeQuery("show warnings")) {
                assertTrue(warnings.next());
                assertEquals("Warning", warnings.getString(1));
                assertEquals(QueryCorruptionWarning.CODE, warnings.getInt(2));
                assertFalse(warnings.next());
            }
            try (ResultSet rows = statement.executeQuery("select 1")) {
                assertTrue(rows.next());
            }
            assertNull(statement.getWarnings());
            try (ResultSet warnings = statement.executeQuery("show warnings")) {
                assertFalse(warnings.next());
            }
            try (PreparedStatement prepared = connection.prepareStatement(
                    "select * from qct_jdbc.events where k >= ?")) {
                prepared.setLong(1, 0);
                try (ResultSet rows = prepared.executeQuery()) {
                    assertFalse(rows.next());
                }
                SQLWarning preparedWarning = prepared.getWarnings();
                assertNotNull(preparedWarning);
                assertEquals(QueryCorruptionWarning.CODE, preparedWarning.getErrorCode());
            }
            assertThrows(SQLException.class,
                    () -> statement.executeQuery("select missing_qct_column from qct_jdbc.events"));
            try (ResultSet warnings = statement.executeQuery("show warnings")) {
                assertFalse(warnings.next());
            }
            Config.enable_query_corruption_tolerance = false;
            try (ResultSet rows = statement.executeQuery("select * from qct_jdbc.events")) {
                assertFalse(rows.next());
            }
            assertNull(statement.getWarnings());
        } finally {
            Config.enable_query_corruption_tolerance = originalConfig;
        }
    }

    @Test
    void mariaDbJdbcTextAndPrepared() throws Exception {
        injectDiagnostic();
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        try (Connection connection = new org.mariadb.jdbc.Driver().connect(
                "jdbc:mariadb://127.0.0.1:" + cluster.queryPort + "/?useServerPrepStmts=true", properties)) {
            exercise(connection);
        }
    }

    @Test
    void mysqlConnectorJTextAndPrepared() throws Exception {
        String jar = System.getProperty("qct.mysql.driver.jar");
        assumeTrue(jar != null, "Pass -Dqct.mysql.driver.jar to verify Connector/J without adding it to product dependencies");
        injectDiagnostic();
        try (URLClassLoader loader = new URLClassLoader(new URL[] {Path.of(jar).toUri().toURL()},
                getClass().getClassLoader())) {
            Driver driver = (Driver) loader.loadClass("com.mysql.cj.jdbc.Driver").getDeclaredConstructor().newInstance();
            Properties properties = new Properties();
            properties.setProperty("user", "root");
            try (Connection connection = driver.connect("jdbc:mysql://127.0.0.1:" + cluster.queryPort
                    + "/?useSSL=false&useServerPrepStmts=true&allowPublicKeyRetrieval=true", properties)) {
                exercise(connection);
            }
        }
    }
}
