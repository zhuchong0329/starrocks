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

package com.starrocks.tenantttl.policy;

import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.starrocks.catalog.TenantTtlDictionaryBinding;
import com.starrocks.proto.CompressionTypePB;
import com.starrocks.proto.PCompressedTenantTtlPolicyBatchPB;
import com.starrocks.proto.PDictionaryCacheExportOutcome;
import com.starrocks.proto.PExportDictionaryCacheRequest;
import com.starrocks.proto.PExportDictionaryCacheResult;
import com.starrocks.proto.PTenantTtlPolicyBatchPB;
import com.starrocks.proto.PTenantTtlPolicyEntryPB;
import com.starrocks.proto.StatusPB;
import com.starrocks.thrift.TNetworkAddress;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TenantTtlPolicySnapshotManagerTest {
    private static final long DICTIONARY_ID = 101;
    private static final String DICTIONARY_NAME = "tenant_ttl_dict";
    private static final TNetworkAddress NODE_1 = new TNetworkAddress("be-a", 8060);
    private static final TNetworkAddress NODE_2 = new TNetworkAddress("be-b", 8060);
    private static final TNetworkAddress NODE_3 = new TNetworkAddress("be-c", 8060);
    private static final Codec<PTenantTtlPolicyBatchPB> POLICY_BATCH_CODEC =
            ProtobufProxy.create(PTenantTtlPolicyBatchPB.class);

    @Test
    public void testFirstReferenceRefreshAndPreBindingRefreshIsolation() throws Exception {
        FakeRuntime runtime = new FakeRuntime();
        ManualDispatcher dispatcher = new ManualDispatcher();
        runtime.putDictionary(DICTIONARY_ID, DICTIONARY_NAME, 0, false);
        TenantTtlPolicySnapshotManager manager = manager(runtime, dispatcher);

        manager.onDictionaryRefreshFinished(DICTIONARY_ID, 1, true, Collections.singletonList(NODE_1));
        assertEquals(0, dispatcher.immediateSize());

        manager.updateBinding(1, 2, null, binding(DICTIONARY_ID));
        manager.updateBinding(1, 2, null, binding(DICTIONARY_ID));
        assertEquals(1, dispatcher.immediateSize());
        dispatcher.runImmediate();
        assertEquals(Collections.singletonList(DICTIONARY_NAME), runtime.refreshRequests);

        runtime.putDictionary(DICTIONARY_ID, DICTIONARY_NAME, 0, true);
        runtime.refreshRequests.clear();
        manager.clearReferencesForRebuild();
        manager.updateBinding(1, 2, null, binding(DICTIONARY_ID));
        dispatcher.runImmediate();
        assertTrue(runtime.refreshRequests.isEmpty());

        runtime.setDictionaryState(DICTIONARY_ID, 1, false);
        runtime.exportActions.add((node, request) -> completed(validResponse(request.expectedTxnId)));
        manager.onDictionaryRefreshFinished(DICTIONARY_ID, 1, true, Collections.singletonList(NODE_1));
        dispatcher.runImmediate();
        assertEquals(Collections.singletonList(DICTIONARY_NAME), runtime.refreshRequests);
        assertEquals(1, manager.getCurrentSnapshot(DICTIONARY_ID).orElseThrow().getDictionaryTxnId());
    }

    @Test
    public void testPostCommitFailureKeepsOldSnapshotAndRetryCatchesUp() throws Exception {
        FakeRuntime runtime = new FakeRuntime();
        ManualDispatcher dispatcher = new ManualDispatcher();
        runtime.putDictionary(DICTIONARY_ID, DICTIONARY_NAME, 1, false);
        runtime.exportActions.add((node, request) -> completed(validResponse(request.expectedTxnId)));
        TenantTtlPolicySnapshotManager manager = manager(runtime, dispatcher);
        manager.onLeaderActivated(Collections.singletonList(
                new TenantTtlPolicySnapshotManager.RecoveredReference(DICTIONARY_ID, 1, 2)));
        dispatcher.runImmediate();
        assertEquals(1, manager.getCurrentSnapshot(DICTIONARY_ID).orElseThrow().getDictionaryTxnId());

        runtime.setDictionaryState(DICTIONARY_ID, 2, false);
        runtime.exportActions.add((node, request) -> failed(new IllegalStateException("transient RPC failure")));
        runtime.exportActions.add((node, request) -> failed(new IllegalStateException("second node failed")));
        manager.onDictionaryRefreshFinished(DICTIONARY_ID, 2, true, Arrays.asList(NODE_3, NODE_2, NODE_1));
        dispatcher.runImmediate();

        assertEquals(1, manager.getCurrentSnapshot(DICTIONARY_ID).orElseThrow().getDictionaryTxnId());
        TenantTtlPolicySnapshotManager.SnapshotStatus failedStatus = manager.getStatus(DICTIONARY_ID);
        assertEquals(2, failedStatus.getTargetTxnId());
        assertEquals(1, failedStatus.getRetryFailureCount());
        assertEquals("TENANT_TTL_POLICY_EXPORT_RPC_ERROR", failedStatus.getLastFailureCode());
        assertEquals(1, dispatcher.delayedSize());
        assertEquals(10_000, dispatcher.nextDelay());

        runtime.exportActions.add((node, request) -> completed(validResponse(request.expectedTxnId)));
        dispatcher.runNextDelayed();
        dispatcher.runImmediate();
        assertEquals(2, manager.getCurrentSnapshot(DICTIONARY_ID).orElseThrow().getDictionaryTxnId());
        assertNull(manager.getStatus(DICTIONARY_ID).getLastFailureCode());
    }

    @Test
    public void testNewerCommittedVersionCoalescesWhileOlderExportRuns() throws Exception {
        FakeRuntime runtime = new FakeRuntime();
        ManualDispatcher dispatcher = new ManualDispatcher();
        runtime.nodes = Collections.singletonList(NODE_1);
        runtime.putDictionary(DICTIONARY_ID, DICTIONARY_NAME, 10, false);
        TenantTtlPolicySnapshotManager manager = manager(runtime, dispatcher);
        runtime.exportActions.add((node, request) -> {
            runtime.setDictionaryState(DICTIONARY_ID, 12, false);
            manager.onDictionaryRefreshFinished(DICTIONARY_ID, 12, true, Collections.singletonList(NODE_1));
            return failed(new IllegalStateException("old transaction export failed"));
        });
        runtime.exportActions.add((node, request) -> completed(validResponse(request.expectedTxnId)));

        manager.onLeaderActivated(Collections.singletonList(
                new TenantTtlPolicySnapshotManager.RecoveredReference(DICTIONARY_ID, 1, 2)));
        dispatcher.runImmediate();

        assertEquals(Arrays.asList(10L, 12L), runtime.requestedTxnIds);
        assertEquals(12, manager.getCurrentSnapshot(DICTIONARY_ID).orElseThrow().getDictionaryTxnId());
    }

    @Test
    public void testTwoNodeFallbackAndDeterministicBlocking() throws Exception {
        FakeRuntime runtime = new FakeRuntime();
        ManualDispatcher dispatcher = new ManualDispatcher();
        runtime.nodes = Arrays.asList(NODE_2, NODE_1);
        runtime.putDictionary(DICTIONARY_ID, DICTIONARY_NAME, 7, false);
        runtime.exportActions.add((node, request) -> completed(outcomeResponse(
                request.expectedTxnId, PDictionaryCacheExportOutcome.VERSION_MISMATCH)));
        runtime.exportActions.add((node, request) -> completed(validResponse(request.expectedTxnId)));
        TenantTtlPolicySnapshotManager manager = manager(runtime, dispatcher);
        manager.onLeaderActivated(Collections.singletonList(
                new TenantTtlPolicySnapshotManager.RecoveredReference(DICTIONARY_ID, 1, 2)));
        dispatcher.runImmediate();

        assertEquals(Arrays.asList("be-a", "be-b"), runtime.requestedHosts);
        assertEquals(7, manager.getCurrentSnapshot(DICTIONARY_ID).orElseThrow().getDictionaryTxnId());

        runtime.requestedHosts.clear();
        runtime.setDictionaryState(DICTIONARY_ID, 8, false);
        runtime.exportActions.add((node, request) -> completed(duplicateResponse(request.expectedTxnId)));
        runtime.exportActions.add((node, request) -> completed(validResponse(request.expectedTxnId)));
        manager.onDictionaryRefreshFinished(DICTIONARY_ID, 8, true, Arrays.asList(NODE_1, NODE_2));
        dispatcher.runImmediate();

        assertEquals(Collections.singletonList("be-a"), runtime.requestedHosts);
        assertEquals(8, manager.getStatus(DICTIONARY_ID).getBlockedTxnId());
        assertEquals("TENANT_TTL_POLICY_DUPLICATE_KEY", manager.getStatus(DICTIONARY_ID).getLastFailureCode());
        assertEquals(0, dispatcher.delayedSize());

        runtime.setDictionaryState(DICTIONARY_ID, 9, false);
        runtime.exportActions.clear();
        runtime.exportActions.add((node, request) -> completed(validResponse(request.expectedTxnId)));
        manager.onDictionaryRefreshFinished(DICTIONARY_ID, 9, true, Collections.singletonList(NODE_1));
        dispatcher.runImmediate();
        assertEquals(9, manager.getCurrentSnapshot(DICTIONARY_ID).orElseThrow().getDictionaryTxnId());
        assertEquals(0, manager.getStatus(DICTIONARY_ID).getBlockedTxnId());
    }

    @Test
    public void testLateResponseCannotPublishAfterUnbindDropOrLeaderLoss() throws Exception {
        assertLateResponseDiscarded((manager, runtime) ->
                manager.removeTable(1, 2, binding(DICTIONARY_ID)));
        assertLateResponseDiscarded((manager, runtime) -> manager.onDictionaryDropped(DICTIONARY_ID));
        assertLateResponseDiscarded((manager, runtime) -> runtime.leader = false);
    }

    @Test
    public void testLeaderRecoveryExportsTrustedTxnAndRefreshesUnknownTxn() throws Exception {
        FakeRuntime runtime = new FakeRuntime();
        ManualDispatcher dispatcher = new ManualDispatcher();
        runtime.putDictionary(DICTIONARY_ID, DICTIONARY_NAME, 5, false);
        runtime.putDictionary(202, "unknown_txn_dict", 0, false);
        runtime.exportActions.add((node, request) -> completed(validResponse(request.expectedTxnId)));
        TenantTtlPolicySnapshotManager manager = manager(runtime, dispatcher);

        manager.onLeaderActivated(Arrays.asList(
                new TenantTtlPolicySnapshotManager.RecoveredReference(DICTIONARY_ID, 1, 2),
                new TenantTtlPolicySnapshotManager.RecoveredReference(202, 1, 3)));
        dispatcher.runImmediate();

        assertEquals(5, manager.getCurrentSnapshot(DICTIONARY_ID).orElseThrow().getDictionaryTxnId());
        assertEquals(Collections.singletonList("unknown_txn_dict"), runtime.refreshRequests);
        assertFalse(manager.getCurrentSnapshot(202).isPresent());
    }

    @Test
    public void testRefreshCallbacksCoalesceAndRetryPolicyBoundaries() throws Exception {
        FakeRuntime runtime = new FakeRuntime();
        ManualDispatcher dispatcher = new ManualDispatcher();
        runtime.putDictionary(DICTIONARY_ID, DICTIONARY_NAME, 0, false);
        TenantTtlPolicySnapshotManager manager = manager(runtime, dispatcher);
        manager.onLeaderActivated(Collections.singletonList(
                new TenantTtlPolicySnapshotManager.RecoveredReference(DICTIONARY_ID, 1, 2)));
        dispatcher.runImmediate();
        runtime.refreshRequests.clear();

        runtime.setDictionaryState(DICTIONARY_ID, 1, false);
        manager.onDictionaryRefreshFinished(DICTIONARY_ID, 1, true, Collections.singletonList(NODE_1));
        runtime.setDictionaryState(DICTIONARY_ID, 3, false);
        manager.onDictionaryRefreshFinished(DICTIONARY_ID, 3, true, Collections.singletonList(NODE_1));
        runtime.exportActions.add((node, request) -> completed(validResponse(request.expectedTxnId)));
        dispatcher.runImmediate();

        assertEquals(Collections.singletonList(3L), runtime.requestedTxnIds);
        assertEquals(8_000, TenantTtlPolicySnapshotManager.retryDelayMs(1, -1.0));
        assertEquals(12_000, TenantTtlPolicySnapshotManager.retryDelayMs(1, 1.0));
        assertEquals(480_000, TenantTtlPolicySnapshotManager.retryDelayMs(100, -1.0));
        assertEquals(720_000, TenantTtlPolicySnapshotManager.retryDelayMs(100, 1.0));
    }

    @Test
    public void testLastAttemptTimeRecordsCompletion() throws Exception {
        FakeRuntime runtime = new FakeRuntime();
        ManualDispatcher dispatcher = new ManualDispatcher();
        runtime.putDictionary(DICTIONARY_ID, DICTIONARY_NAME, 4, false);
        long startedAt = runtime.nowMillis;
        runtime.exportActions.add((node, request) -> {
            runtime.nowMillis += 1234;
            return completed(validResponse(request.expectedTxnId));
        });
        TenantTtlPolicySnapshotManager manager = manager(runtime, dispatcher);
        manager.onLeaderActivated(Collections.singletonList(
                new TenantTtlPolicySnapshotManager.RecoveredReference(DICTIONARY_ID, 1, 2)));
        dispatcher.runImmediate();

        assertEquals(startedAt + 1234, manager.getStatus(DICTIONARY_ID).getLastAttemptTimeMillis());
    }

    private static void assertLateResponseDiscarded(BiConsumerWithRuntime invalidation) throws Exception {
        FakeRuntime runtime = new FakeRuntime();
        ManualDispatcher dispatcher = new ManualDispatcher();
        runtime.putDictionary(DICTIONARY_ID, DICTIONARY_NAME, 1, false);
        TenantTtlPolicySnapshotManager manager = manager(runtime, dispatcher);
        runtime.exportActions.add((node, request) -> {
            invalidation.accept(manager, runtime);
            return completed(validResponse(request.expectedTxnId));
        });
        manager.onLeaderActivated(Collections.singletonList(
                new TenantTtlPolicySnapshotManager.RecoveredReference(DICTIONARY_ID, 1, 2)));
        dispatcher.runImmediate();
        assertFalse(manager.getCurrentSnapshot(DICTIONARY_ID).isPresent());
    }

    private static TenantTtlPolicySnapshotManager manager(FakeRuntime runtime, ManualDispatcher dispatcher) {
        return new TenantTtlPolicySnapshotManager(runtime, dispatcher, new TenantTtlPolicySnapshotBuilder());
    }

    private static TenantTtlDictionaryBinding binding(long dictionaryId) {
        return new TenantTtlDictionaryBinding(dictionaryId, DICTIONARY_NAME, "business.http_log", 180);
    }

    private static PExportDictionaryCacheResult validResponse(long txnId) throws Exception {
        return response(txnId, Collections.singletonList(entry("default", "business.http_log", 180)));
    }

    private static PExportDictionaryCacheResult duplicateResponse(long txnId) throws Exception {
        return response(txnId, Arrays.asList(
                entry("tenant_a", "business.http_log", 30),
                entry("tenant_a", "business.http_log", 30)));
    }

    private static PExportDictionaryCacheResult outcomeResponse(long txnId,
                                                                PDictionaryCacheExportOutcome outcome) {
        PExportDictionaryCacheResult response = new PExportDictionaryCacheResult();
        response.status = new StatusPB();
        response.status.statusCode = 0;
        response.outcome = outcome;
        response.protocolVersion = TenantTtlPolicySnapshotBuilder.PROTOCOL_VERSION;
        response.dictionaryId = DICTIONARY_ID;
        response.expectedTxnId = txnId;
        response.actualTxnId = txnId + 1;
        response.complete = false;
        return response;
    }

    private static PExportDictionaryCacheResult response(long txnId, List<PTenantTtlPolicyEntryPB> entries)
            throws Exception {
        PTenantTtlPolicyBatchPB policyBatch = new PTenantTtlPolicyBatchPB();
        policyBatch.entries = entries;
        byte[] payload = POLICY_BATCH_CODEC.encode(policyBatch);
        CRC32C crc = new CRC32C();
        crc.update(payload, 0, payload.length);

        PCompressedTenantTtlPolicyBatchPB batch = new PCompressedTenantTtlPolicyBatchPB();
        batch.sequence = 0;
        batch.rowCount = (long) entries.size();
        batch.compressionType = CompressionTypePB.NO_COMPRESSION;
        batch.uncompressedSize = (long) payload.length;
        batch.uncompressedCrc32c = (int) crc.getValue();
        batch.payload = payload;

        PExportDictionaryCacheResult response = new PExportDictionaryCacheResult();
        response.status = new StatusPB();
        response.status.statusCode = 0;
        response.outcome = PDictionaryCacheExportOutcome.EXPORT_OK;
        response.protocolVersion = TenantTtlPolicySnapshotBuilder.PROTOCOL_VERSION;
        response.dictionaryId = DICTIONARY_ID;
        response.expectedTxnId = txnId;
        response.actualTxnId = txnId;
        response.totalRowCount = (long) entries.size();
        response.batchCount = 1;
        response.totalUncompressedBytes = (long) payload.length;
        response.totalPayloadBytes = (long) payload.length;
        response.contentCrc32c = (int) crc.getValue();
        response.complete = true;
        response.batches = Collections.singletonList(batch);
        return response;
    }

    private static PTenantTtlPolicyEntryPB entry(String tenant, String tableName, int days) {
        PTenantTtlPolicyEntryPB entry = new PTenantTtlPolicyEntryPB();
        entry.tenant = tenant.getBytes(StandardCharsets.UTF_8);
        entry.tableName = tableName.getBytes(StandardCharsets.UTF_8);
        entry.retentionDays = days;
        return entry;
    }

    private static Future<PExportDictionaryCacheResult> completed(PExportDictionaryCacheResult response) {
        return CompletableFuture.completedFuture(response);
    }

    private static Future<PExportDictionaryCacheResult> failed(Exception exception) {
        CompletableFuture<PExportDictionaryCacheResult> future = new CompletableFuture<>();
        future.completeExceptionally(exception);
        return future;
    }

    @FunctionalInterface
    private interface BiConsumerWithRuntime {
        void accept(TenantTtlPolicySnapshotManager manager, FakeRuntime runtime);
    }

    @FunctionalInterface
    private interface ExportAction {
        Future<PExportDictionaryCacheResult> apply(TNetworkAddress node, PExportDictionaryCacheRequest request)
                throws Exception;
    }

    private static final class ManualDispatcher implements TenantTtlPolicySnapshotManager.TaskDispatcher {
        private final Deque<Runnable> immediate = new ArrayDeque<>();
        private final Deque<Runnable> delayed = new ArrayDeque<>();
        private final List<Long> delays = new ArrayList<>();

        @Override
        public void execute(Runnable task) {
            immediate.addLast(task);
        }

        @Override
        public void schedule(Runnable task, long delayMs) {
            delayed.addLast(task);
            delays.add(delayMs);
        }

        private int immediateSize() {
            return immediate.size();
        }

        private int delayedSize() {
            return delayed.size();
        }

        private long nextDelay() {
            assertFalse(delays.isEmpty());
            return delays.get(0);
        }

        private void runImmediate() {
            while (!immediate.isEmpty()) {
                immediate.removeFirst().run();
            }
        }

        private void runNextDelayed() {
            assertFalse(delayed.isEmpty());
            delayed.removeFirst().run();
        }
    }

    private static final class FakeRuntime implements TenantTtlPolicySnapshotManager.RuntimeEnvironment {
        private final Map<Long, MutableDictionary> dictionaries = new HashMap<>();
        private final Deque<ExportAction> exportActions = new ArrayDeque<>();
        private final List<String> refreshRequests = new ArrayList<>();
        private final List<Long> requestedTxnIds = new ArrayList<>();
        private final List<String> requestedHosts = new ArrayList<>();
        private List<TNetworkAddress> nodes = Arrays.asList(NODE_1, NODE_2);
        private boolean leader = true;
        private long nowMillis = 1_800_000_000_000L;

        private void putDictionary(long id, String name, long txnId, boolean refreshing) {
            dictionaries.put(id, new MutableDictionary(id, name, txnId, refreshing));
        }

        private void setDictionaryState(long id, long txnId, boolean refreshing) {
            MutableDictionary dictionary = dictionaries.get(id);
            assertNotNull(dictionary);
            dictionary.txnId = txnId;
            dictionary.refreshing = refreshing;
        }

        @Override
        public boolean isLeader() {
            return leader;
        }

        @Override
        public TenantTtlPolicySnapshotManager.DictionaryView getDictionary(long dictionaryId) {
            MutableDictionary dictionary = dictionaries.get(dictionaryId);
            if (dictionary == null) {
                return null;
            }
            return new TenantTtlPolicySnapshotManager.DictionaryView(dictionary.id, dictionary.name,
                    dictionary.txnId, dictionary.refreshing);
        }

        @Override
        public void requestFullRefresh(String dictionaryName) {
            refreshRequests.add(dictionaryName);
            for (MutableDictionary dictionary : dictionaries.values()) {
                if (dictionary.name.equals(dictionaryName)) {
                    dictionary.refreshing = true;
                }
            }
        }

        @Override
        public List<TNetworkAddress> getExportNodes(long dictionaryId) {
            return nodes;
        }

        @Override
        public Future<PExportDictionaryCacheResult> export(TNetworkAddress node,
                                                           PExportDictionaryCacheRequest request) throws Exception {
            requestedTxnIds.add(request.expectedTxnId);
            requestedHosts.add(node.hostname);
            assertFalse(exportActions.isEmpty());
            return exportActions.removeFirst().apply(node, request);
        }

        @Override
        public long currentTimeMillis() {
            return nowMillis;
        }

        @Override
        public double retryJitter() {
            return 0;
        }
    }

    private static final class MutableDictionary {
        private final long id;
        private final String name;
        private long txnId;
        private boolean refreshing;

        private MutableDictionary(long id, String name, long txnId, boolean refreshing) {
            this.id = id;
            this.name = name;
            this.txnId = txnId;
            this.refreshing = refreshing;
        }
    }
}
