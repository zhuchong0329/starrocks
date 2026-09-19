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

package com.starrocks.http;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.QueryCorruptionWarning;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryCorruptionJsonTest {
    private JsonObject decode(ByteBuf buffer) {
        try {
            String json = buffer.toString(StandardCharsets.UTF_8);
            assertTrue(json.endsWith("\n"));
            return JsonParser.parseString(json).getAsJsonObject();
        } finally {
            buffer.release();
        }
    }

    @Test
    void offPreservesLegacyShapeEvenIfMessageIsPresent() throws IOException {
        JsonObject legacy = decode(JsonSerializer.getStatistic(null));
        JsonObject off = decode(JsonSerializer.getStatistic(null, false, "not exposed"));
        assertEquals(legacy, off);
        assertFalse(off.has("partial_result"));
        assertFalse(off.has("warnings"));
    }

    @Test
    void enabledHealthyHasFalseAndEmptyWarnings() throws IOException {
        JsonObject json = decode(JsonSerializer.getStatistic(null, true, null));
        assertFalse(json.get("partial_result").getAsBoolean());
        assertTrue(json.getAsJsonArray("warnings").isEmpty());
        assertEquals(0, json.getAsJsonObject("statistics").get("returnRows").getAsLong());
    }

    @Test
    void allUnreadableAndQuotedDiagnosticAreValidNdjson() throws IOException {
        PQueryStatistics statistics = new PQueryStatistics();
        statistics.returnedRows = 0L;
        statistics.queryCorruptionDetected = true;
        String message = "partial: \"quoted\"\nnot a second NDJSON record";
        JsonObject json = decode(JsonSerializer.getStatistic(statistics, true, message));
        assertTrue(json.get("partial_result").getAsBoolean());
        assertEquals(1, json.getAsJsonArray("warnings").size());
        JsonObject warning = json.getAsJsonArray("warnings").get(0).getAsJsonObject();
        assertEquals(QueryCorruptionWarning.NAME, warning.get("code").getAsString());
        assertEquals(message, warning.get("message").getAsString());
        assertTrue(json.has("statistics"));
        assertFalse(json.has("data"));
    }
}
