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
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.QueryCorruptionWarning;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.RowBatch;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TResultBatch;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class QueryCorruptionHttpSenderTest {
    private HttpConnectContext context(EmbeddedChannel channel) {
        HttpConnectContext context = mock(HttpConnectContext.class, CALLS_REAL_METHODS);
        Deencapsulation.setField(context, "state", new QueryState());
        context.setQueryId(new UUID(1, 2));
        context.setNettyChannel(channel.pipeline().firstContext());
        context.setKeepAlive(true);
        return context;
    }

    private ExecPlan plan() {
        ExecPlan plan = mock(ExecPlan.class);
        when(plan.getColNames()).thenReturn(List.of());
        when(plan.getOutputExprs()).thenReturn(List.of());
        return plan;
    }

    private RowBatch eos(boolean corrupt) {
        RowBatch batch = new RowBatch();
        PQueryStatistics statistics = new PQueryStatistics();
        statistics.queryCorruptionDetected = corrupt;
        statistics.returnedRows = 0L;
        batch.setQueryStatistics(statistics);
        return batch;
    }

    private JsonObject drain(EmbeddedChannel channel, boolean expectLast) {
        JsonObject statistics = null;
        boolean last = false;
        Object item;
        while ((item = channel.readOutbound()) != null) {
            try {
                assertFalse(last, "No output may follow the existing final HTTP chunk");
                if (item instanceof ByteBuf) {
                    JsonObject json = JsonParser.parseString(((ByteBuf) item).toString(StandardCharsets.UTF_8))
                            .getAsJsonObject();
                    if (json.has("statistics")) {
                        assertNull(statistics, "Exactly one final statistics record");
                        statistics = json;
                    }
                }
                last = item instanceof LastHttpContent;
            } finally {
                ReferenceCountUtil.release(item);
            }
        }
        assertEquals(expectLast, last);
        return statistics;
    }

    @Test
    void partialZeroRowsUsesExistingEosAndKeepAliveDoesNotLeak() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelDuplexHandler());
        try {
            HttpConnectContext context = context(channel);
            Coordinator coordinator = mock(Coordinator.class);
            when(coordinator.getNext()).thenReturn(eos(true), eos(false));
            context.getState().setQueryCorruptionToleranceEnabled(true);
            new HttpResultSender(context).sendQueryResult(coordinator, plan(), "select * from events");
            JsonObject partial = drain(channel, true);
            assertNotNull(partial);
            assertTrue(partial.get("partial_result").getAsBoolean());
            assertEquals(1, partial.getAsJsonArray("warnings").size());
            assertTrue(channel.isActive());

            // Same reset/statement boundary as HttpConnectProcessor and StmtExecutor.
            context.getState().reset();
            QueryCorruptionWarning.beginStatement(context, mock(QueryStatement.class), false);
            context.getState().setQueryCorruptionToleranceEnabled(true);
            new HttpResultSender(context).sendQueryResult(coordinator, plan(), "select * from healthy");
            JsonObject healthy = drain(channel, true);
            assertFalse(healthy.get("partial_result").getAsBoolean());
            assertTrue(healthy.getAsJsonArray("warnings").isEmpty());
            verify(coordinator, times(2)).getNext(); // No extra final-status request.
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void strictFailureDoesNotProduceSuccessTrailer() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelDuplexHandler());
        try {
            HttpConnectContext context = context(channel);
            context.getState().setQueryCorruptionToleranceEnabled(true);
            Coordinator coordinator = mock(Coordinator.class);
            when(coordinator.getNext()).thenThrow(new IllegalStateException("shared preparation failed"));
            assertThrows(IllegalStateException.class,
                    () -> new HttpResultSender(context).sendQueryResult(coordinator, plan(), "select * from events"));
            assertNull(drain(channel, false));
            assertNull(context.getQueryCorruptionWarning());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void rawOutputHasNoStatisticsOrPartialTrailer() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelDuplexHandler());
        try {
            HttpConnectContext context = context(channel);
            context.setOnlyOutputResultRaw(true);
            Coordinator coordinator = mock(Coordinator.class);
            when(coordinator.getNext()).thenReturn(eos(false));
            new HttpResultSender(context).sendQueryResult(coordinator, plan(), "select * from events");
            assertNull(drain(channel, true));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void clientDisconnectUsesExistingCancellationWithoutDiagnosticWait() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelDuplexHandler());
        try {
            HttpConnectContext context = context(channel);
            context.getState().setQueryCorruptionToleranceEnabled(true);
            Coordinator coordinator = mock(Coordinator.class);
            RowBatch data = new RowBatch();
            data.setEos(false);
            TResultBatch rows = new TResultBatch();
            rows.setRows(List.of(ByteBuffer.wrap("{\"data\":[1]}\n".getBytes(StandardCharsets.UTF_8))));
            data.setBatch(rows);
            when(coordinator.getNext()).thenAnswer(invocation -> {
                channel.close().syncUninterruptibly();
                return data;
            }).thenThrow(new IllegalStateException("query cancelled after client disconnect"));
            assertThrows(IllegalStateException.class,
                    () -> new HttpResultSender(context).sendQueryResult(coordinator, plan(), "select * from events"));
            verify(coordinator).cancel("channel is closed, cancel query");
            verify(coordinator, times(2)).getNext();
            assertNull(context.getQueryCorruptionWarning());
            assertNull(drain(channel, false));
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
