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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.common.Config;
import com.starrocks.http.HttpServer;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.qe.QueryCorruptionWarning;
import com.starrocks.thrift.TStatusCode;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real HTTP socket/FE/NDJSON tests. BE diagnostics are simulated, not physical file faults. */
class QueryCorruptionHttpTest {
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final String QUERY = "select * from qct_http.events";
    private static PseudoCluster cluster;
    private static HttpServer server;
    private static boolean originalConfig;

    @BeforeAll
    static void start() throws Exception {
        originalConfig = Config.enable_query_corruption_tolerance;
        cluster = PseudoCluster.getOrCreateWithRandomPort(true, 1);
        cluster.runSql(null, "create database qct_http");
        cluster.runSql("qct_http", "create table events(k bigint, v int) duplicate key(k) "
                + "distributed by hash(k) buckets 1 properties('replication_num'='1')");
        cluster.runSql("qct_http", "insert into events values (1, 2)");
        server = new HttpServer(0);
        server.setup();
        server.start();
        for (int attempt = 0; attempt < 100 && !server.isStarted(); attempt++) {
            Thread.sleep(100);
        }
        assertTrue(server.isStarted());
    }

    @AfterAll
    static void stop() {
        Config.enable_query_corruption_tolerance = originalConfig;
        if (server != null) {
            server.shutDown();
        }
        if (cluster != null) {
            cluster.shutdown(false);
        }
    }

    private OkHttpClient client() {
        return new OkHttpClient.Builder().readTimeout(10, TimeUnit.SECONDS).build();
    }

    private Request request(String sql, boolean raw) {
        JsonObject body = new JsonObject();
        body.addProperty("query", sql);
        body.addProperty("onlyOutputResultRaw", raw);
        JsonObject variables = new JsonObject();
        variables.addProperty("enable_short_circuit", "false");
        variables.addProperty("enable_query_cache", "false");
        body.add("sessionVariables", variables);
        return new Request.Builder()
                .url("http://127.0.0.1:" + server.getPort() + "/api/v1/catalogs/default_catalog/sql")
                .header("Authorization", Credentials.basic("root", ""))
                .post(RequestBody.create(JSON, body.toString())).build();
    }

    private void diagnostic(AtomicBoolean corrupt, boolean strictFailure) {
        new MockUp<PseudoBackend.QueryProgress>() {
            @Mock
            Future<PFetchDataResult> getFetchDataResult(Invocation invocation) {
                CompletableFuture<PFetchDataResult> original = invocation.proceed();
                return original.thenApply(result -> {
                    result.queryStatistics.queryCorruptionDetected = corrupt.get();
                    if (strictFailure) {
                        result.status.statusCode = TStatusCode.INTERNAL_ERROR.getValue();
                        result.status.errorMsgs = List.of("shared preparation failed");
                    }
                    return result;
                });
            }
        };
    }

    private List<JsonObject> records(OkHttpClient client, String sql, boolean raw) throws IOException {
        try (Response response = client.newCall(request(sql, raw)).execute()) {
            assertEquals(200, response.code());
            assertNotNull(response.body());
            return response.body().string().lines().filter(line -> !line.isBlank())
                    .map(line -> JsonParser.parseString(line).getAsJsonObject()).toList();
        }
    }

    @Test
    void partialThenHealthyReuseTheSameConnection() throws Exception {
        AtomicBoolean corrupt = new AtomicBoolean(true);
        diagnostic(corrupt, false);
        Config.enable_query_corruption_tolerance = true;
        OkHttpClient client = client();
        try {
            List<JsonObject> partial = records(client, QUERY, false);
            assertEquals(3, partial.size()); // connectionId, unchanged metadata, final statistics; no rows.
            JsonObject trailer = partial.get(2);
            assertTrue(trailer.get("partial_result").getAsBoolean());
            assertEquals(1, trailer.getAsJsonArray("warnings").size());
            assertEquals(QueryCorruptionWarning.NAME,
                    trailer.getAsJsonArray("warnings").get(0).getAsJsonObject().get("code").getAsString());
            assertEquals(2, partial.get(1).getAsJsonArray("meta").size());

            corrupt.set(false);
            List<JsonObject> healthy = records(client, QUERY, false);
            assertEquals(partial.get(0).get("connectionId"), healthy.get(0).get("connectionId"));
            assertFalse(healthy.get(2).get("partial_result").getAsBoolean());
            assertTrue(healthy.get(2).getAsJsonArray("warnings").isEmpty());

            // A pre-execution syntax error cannot retain the previous statement's diagnostic.
            try (Response failure = client.newCall(request("select from", false)).execute()) {
                assertEquals(500, failure.code());
                assertFalse(failure.body().string().contains(QueryCorruptionWarning.NAME));
            }
            List<JsonObject> afterError = records(client, QUERY, false);
            assertFalse(afterError.get(2).get("partial_result").getAsBoolean());
        } finally {
            client.connectionPool().evictAll();
            Config.enable_query_corruption_tolerance = originalConfig;
        }
    }

    @Test
    void disabledAndRawKeepTheirExistingWireFormat() throws Exception {
        diagnostic(new AtomicBoolean(true), false);
        OkHttpClient client = client();
        try {
            Config.enable_query_corruption_tolerance = false;
            List<JsonObject> disabled = records(client, QUERY, false);
            assertTrue(disabled.get(2).has("statistics"));
            assertFalse(disabled.get(2).has("partial_result"));
            assertFalse(disabled.get(2).has("warnings"));
            Config.enable_query_corruption_tolerance = true;
            List<JsonObject> raw = records(client, QUERY, true);
            assertEquals(1, raw.size()); // Existing raw mode still emits column metadata.
            assertTrue(raw.get(0).has("meta"));
            assertFalse(raw.get(0).has("partial_result"));
        } finally {
            client.connectionPool().evictAll();
            Config.enable_query_corruption_tolerance = originalConfig;
        }
    }

    @Test
    void strictFailureClosesStreamWithoutASuccessTrailer() throws Exception {
        diagnostic(new AtomicBoolean(true), true);
        Config.enable_query_corruption_tolerance = true;
        OkHttpClient client = client();
        try (Response response = client.newCall(request(QUERY, false)).execute()) {
            assertEquals(200, response.code()); // Existing headers precede BE execution completion.
            assertThrows(IOException.class, () -> response.body().string()); // No terminating HTTP chunk.
        } finally {
            client.connectionPool().evictAll();
            Config.enable_query_corruption_tolerance = originalConfig;
        }
    }
}
