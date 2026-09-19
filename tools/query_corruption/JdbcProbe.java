// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Locale;

/** Real JDBC client for the dedicated physical-BE fault tests, with no simulated server. */
public final class JdbcProbe {
    private static final Path ROOT = Path.of("/query-corruption-workspace/runtime/qct-cluster");

    private static void require(boolean condition, String description) {
        if (!condition) {
            throw new AssertionError(description);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException("mysql|mariadb text|prepared healthy|partial|strict SELECT_SQL");
        }
        require(ROOT.toRealPath().equals(ROOT) && Files.isRegularFile(ROOT.resolve(".qct-test-cluster")),
                "Only the dedicated marked QCT cluster is permitted");
        require(args[0].equals("mysql") || args[0].equals("mariadb"), "Unsupported driver");
        require(args[1].equals("text") || args[1].equals("prepared"), "Unsupported protocol");
        require(args[2].equals("healthy") || args[2].equals("partial") || args[2].equals("strict"),
                "Unsupported expectation");
        String sql = args[3].trim();
        require(sql.toLowerCase(Locale.ROOT).startsWith("select ") && !sql.contains(";"),
                "Use a single SELECT against the dedicated fixtures");
        String url = "jdbc:" + args[0] + "://127.0.0.1:19303/?useSSL=false&useServerPrepStmts=true";
        try (Connection connection = DriverManager.getConnection(url, "root", "");
                Statement setup = connection.createStatement()) {
            setup.execute("set enable_query_cache=false");
            setup.execute("set enable_tablet_internal_parallel=false");
            setup.execute("set enable_short_circuit=false");
            setup.execute("set enable_global_runtime_filter=false");
            boolean prepared = args[1].equals("prepared");
            try (Statement statement = prepared ? connection.prepareStatement(sql) : connection.createStatement()) {
                long rows = 0;
                int columns;
                try {
                    if (prepared && sql.contains("?")) {
                        ((PreparedStatement) statement).setLong(1, 0);
                    }
                    try (ResultSet result = prepared ? ((PreparedStatement) statement).executeQuery()
                            : statement.executeQuery(sql)) {
                        columns = result.getMetaData().getColumnCount();
                        while (result.next()) {
                            for (int column = 1; column <= columns; ++column) {
                                result.getObject(column);
                            }
                            ++rows;
                        }
                    }
                } catch (SQLException error) {
                    if (!args[2].equals("strict")) {
                        throw error;
                    }
                    // The SQL protocol can omit the BE status name and preserve only its message.
                    String message = error.getMessage();
                    require(message != null && (message.contains("Corruption") || message.contains("Bad page:")
                                    || message.contains("Bad segment file")),
                            "Expected physical corruption, received a different SQL failure: " + error);
                    System.out.println("PASS strict driver=" + args[0] + " protocol=" + args[1]
                            + " code=" + error.getErrorCode() + " state=" + error.getSQLState()
                            + " error=" + error.getMessage());
                    return;
                }
                require(!args[2].equals("strict"), "Expected a strict error, received success");
                SQLWarning warning = statement.getWarnings(); // Consume once; driver behavior differs on repeat calls.
                if (args[2].equals("partial")) {
                    require(warning != null && warning.getErrorCode() == 9000
                            && warning.getMessage().contains("SR_QUERY_PARTIAL_RESULT"), "Missing partial warning");
                    require(warning.getNextWarning() == null, "Expected one aggregated warning");
                } else {
                    require(warning == null, "Healthy SELECT inherited a warning");
                }
                System.out.println("PASS " + args[2] + " driver=" + args[0] + " protocol=" + args[1]
                        + " rows=" + rows + " columns=" + columns
                        + " warning=" + (warning == null ? "none" : warning.getMessage()));
            }
        }
    }
}
