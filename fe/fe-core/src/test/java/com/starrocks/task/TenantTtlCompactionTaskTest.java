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

package com.starrocks.task;

import com.google.common.collect.ImmutableList;
import com.starrocks.leader.LeaderImpl;
import com.starrocks.tenantttl.policy.TenantTtlByteKey;
import com.starrocks.tenantttl.policy.TenantTtlPolicyPlanner;
import com.starrocks.thrift.TAgentTaskRequest;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TTenantTtlCompactionResult;
import com.starrocks.thrift.TTenantTtlRowsetAction;
import com.starrocks.thrift.TTenantTtlRowsetResult;
import com.starrocks.thrift.TTenantTtlTaskCode;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TenantTtlCompactionTaskTest {
    private static final long BACKEND_ID = 101;
    private static final long DB_ID = 102;
    private static final long TABLE_ID = 103;
    private static final long PARTITION_ID = 104;
    private static final long INDEX_ID = 105;
    private static final long TABLET_ID = 106;
    private static final long REPLICA_ID = 107;
    private static final long TASK_ID = 108;
    private static final long OBSERVED_VERSION = 109;

    @AfterEach
    public void tearDown() {
        AgentTaskQueue.clearAllTasks();
    }

    @Test
    public void testAgentEnvelopeAndRawTenantBytesRoundTrip() throws Exception {
        List<TenantTtlByteKey> tenants = ImmutableList.of(
                TenantTtlByteKey.copyOf(new byte[] {0, (byte) 0xff}),
                TenantTtlByteKey.copyOf(new byte[] {(byte) 0xff, 0, (byte) 0xfe}));
        TenantTtlCompactionTask task = task(TenantTtlPolicyPlanner.FilterMode.DELETE_LIST, tenants);

        TAgentTaskRequest envelope = AgentBatchTask.toAgentTaskRequest(task);
        Assertions.assertEquals(TTaskType.TENANT_TTL_COMPACTION, envelope.getTask_type());
        Assertions.assertEquals(TASK_ID, envelope.getSignature());
        Assertions.assertTrue(envelope.isSetTenant_ttl_compaction_req());

        byte[] serialized = new TSerializer(new TBinaryProtocol.Factory()).serialize(envelope);
        TAgentTaskRequest restored = new TAgentTaskRequest();
        new TDeserializer(new TBinaryProtocol.Factory()).deserialize(restored, serialized);
        Assertions.assertEquals(TASK_ID, restored.getTenant_ttl_compaction_req().getTask_id());
        Assertions.assertEquals(TABLET_ID, restored.getTenant_ttl_compaction_req().getTablet_id());
        Assertions.assertEquals(PARTITION_ID, restored.getTenant_ttl_compaction_req().getPartition_id());
        Assertions.assertEquals(7, restored.getTenant_ttl_compaction_req().getTenant_column_unique_id());
        Assertions.assertEquals(201, restored.getTenant_ttl_compaction_req().getPolicy_watermark().getDictionary_id());
        Assertions.assertEquals(202,
                restored.getTenant_ttl_compaction_req().getPolicy_watermark().getDictionary_txn_id());
        Assertions.assertEquals(203,
                restored.getTenant_ttl_compaction_req().getPolicy_watermark().getEvaluation_time_epoch_seconds());
        Assertions.assertEquals(301, restored.getTenant_ttl_compaction_req().getExpected_schema().getSchema_id());
        Assertions.assertEquals(2, restored.getTenant_ttl_compaction_req().getExpected_schema().getSchema_version());
        Assertions.assertEquals(OBSERVED_VERSION,
                restored.getTenant_ttl_compaction_req().getFe_observed_max_version());
        Assertions.assertArrayEquals(tenants.get(0).copyBytes(),
                bytes(restored.getTenant_ttl_compaction_req().getFilter().getTenants().get(0)));
        Assertions.assertArrayEquals(tenants.get(1).copyBytes(),
                bytes(restored.getTenant_ttl_compaction_req().getFilter().getTenants().get(1)));
    }

    @Test
    public void testRequestRejectsUnsafeFilterShapes() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> task(TenantTtlPolicyPlanner.FilterMode.KEEP_LIST, Collections.emptyList()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> task(TenantTtlPolicyPlanner.FilterMode.NONE,
                        Collections.singletonList(TenantTtlByteKey.utf8("a"))));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> task(TenantTtlPolicyPlanner.FilterMode.DELETE_LIST, ImmutableList.of(
                        TenantTtlByteKey.utf8("b"), TenantTtlByteKey.utf8("a"))));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> task(TenantTtlPolicyPlanner.FilterMode.DELETE_LIST, ImmutableList.of(
                        TenantTtlByteKey.utf8("a"), TenantTtlByteKey.utf8("a"))));
    }

    @Test
    public void testFinishValidatesIdentityCoverageAndAccounting() {
        TenantTtlCompactionTask task = task(TenantTtlPolicyPlanner.FilterMode.DELETE_LIST,
                Collections.singletonList(TenantTtlByteKey.utf8("a")));
        TTenantTtlCompactionResult wrongIdentity = successResult();
        wrongIdentity.setTablet_id(TABLET_ID + 1);
        Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.IDENTITY_MISMATCH,
                task.finish(wrongIdentity));
        Assertions.assertFalse(task.isFinished());

        TTenantTtlCompactionResult shortCoverage = successResult();
        shortCoverage.setProcessed_through_version(OBSERVED_VERSION - 1);
        Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.MALFORMED_RESULT,
                task.finish(shortCoverage));

        TTenantTtlCompactionResult invalidAccounting = successResult();
        invalidAccounting.setScanned_rows(2);
        Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.MALFORMED_RESULT,
                task.finish(invalidAccounting));

        TTenantTtlCompactionResult valid = successResult();
        Assertions.assertEquals(TenantTtlCompactionTask.FinishResult.ACCEPTED, task.finish(valid));
        valid.setTask_id(TASK_ID + 1);
        Assertions.assertEquals(TASK_ID, task.getResult().orElseThrow(AssertionError::new).getTask_id());
        Assertions.assertTrue(task.isFinished());
    }

    @Test
    public void testBusinessFailureIsPreservedAndLeaderRemovesAcceptedTask() throws Exception {
        TenantTtlCompactionTask task = task(TenantTtlPolicyPlanner.FilterMode.DELETE_LIST,
                Collections.singletonList(TenantTtlByteKey.utf8("a")));
        TTenantTtlCompactionResult result = emptyResult(TTenantTtlTaskCode.TABLET_BUSY,
                TStatusCode.RUNTIME_ERROR, true, -1, -1);
        TFinishTaskRequest finish = new TFinishTaskRequest();
        finish.setTask_status(new TStatus(TStatusCode.OK));
        finish.setTenant_ttl_compaction_result(result);
        AgentTaskQueue.addTask(task);

        invokeFinishHandler(new LeaderImpl(), task, finish);

        Assertions.assertNull(AgentTaskQueue.getTask(BACKEND_ID, TTaskType.TENANT_TTL_COMPACTION, TASK_ID));
        Assertions.assertEquals(TTenantTtlTaskCode.TABLET_BUSY,
                task.getResult().orElseThrow(AssertionError::new).getCode());
        Assertions.assertTrue(task.getResult().orElseThrow(AssertionError::new).isRetryable());
    }

    @Test
    public void testLeaderKeepsTaskWhenBusinessResultIsMissingOrMalformed() throws Exception {
        TenantTtlCompactionTask task = task(TenantTtlPolicyPlanner.FilterMode.DELETE_LIST,
                Collections.singletonList(TenantTtlByteKey.utf8("a")));
        AgentTaskQueue.addTask(task);
        TFinishTaskRequest missing = new TFinishTaskRequest();
        missing.setTask_status(new TStatus(TStatusCode.OK));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> invokeFinishHandler(new LeaderImpl(), task, missing));
        Assertions.assertSame(task,
                AgentTaskQueue.getTask(BACKEND_ID, TTaskType.TENANT_TTL_COMPACTION, TASK_ID));

        TFinishTaskRequest malformed = new TFinishTaskRequest();
        malformed.setTask_status(new TStatus(TStatusCode.OK));
        TTenantTtlCompactionResult result = successResult();
        result.getRowsets().get(0).unsetOutput_rowset_id();
        malformed.setTenant_ttl_compaction_result(result);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> invokeFinishHandler(new LeaderImpl(), task, malformed));
        Assertions.assertSame(task,
                AgentTaskQueue.getTask(BACKEND_ID, TTaskType.TENANT_TTL_COMPACTION, TASK_ID));
    }

    private static TenantTtlCompactionTask task(TenantTtlPolicyPlanner.FilterMode mode,
                                                List<TenantTtlByteKey> tenants) {
        return new TenantTtlCompactionTask(BACKEND_ID, DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID,
                TABLET_ID, REPLICA_ID, TASK_ID, 7, mode, tenants, 201, 202, 203,
                301, 2, OBSERVED_VERSION, "binding", "policy", "boundary", "topology", "request");
    }

    private static TTenantTtlCompactionResult successResult() {
        TTenantTtlRowsetResult rowset = new TTenantTtlRowsetResult();
        rowset.setSource_version_start(0);
        rowset.setSource_version_end(OBSERVED_VERSION);
        rowset.setSource_rowset_id("0200000000000000000000000000000000000000000000000000000000000000");
        rowset.setOutput_rowset_id("0200000000000000000000000000000000000000000000000000000000000001");
        rowset.setAction(TTenantTtlRowsetAction.REWRITE);
        rowset.setSource_rows(10);
        rowset.setKept_rows(8);
        rowset.setDeleted_rows(2);
        rowset.setSource_segments(2);
        rowset.setLinked_segments(1);
        rowset.setDropped_segments(0);
        rowset.setRewritten_segments(1);
        TTenantTtlCompactionResult result = emptyResult(TTenantTtlTaskCode.SUCCESS,
                TStatusCode.OK, false, OBSERVED_VERSION, OBSERVED_VERSION);
        result.setCoverage_digest("0-109@source;");
        result.setRowsets(Collections.singletonList(rowset));
        result.setScanned_rows(10);
        result.setKept_rows(8);
        result.setDeleted_rows(2);
        result.setTenant_rows_read(10);
        result.setRows_pruned_by_segment_zonemap(1);
        result.setRows_pruned_by_page_zonemap(2);
        result.setLinked_bytes(3);
        result.setRewritten_bytes(4);
        return result;
    }

    private static TTenantTtlCompactionResult emptyResult(TTenantTtlTaskCode code, TStatusCode detailCode,
                                                           boolean retryable, long snapshotVersion,
                                                           long processedVersion) {
        TTenantTtlCompactionResult result = new TTenantTtlCompactionResult();
        result.setCode(code);
        result.setDetail_status(new TStatus(detailCode));
        result.setRetryable(retryable);
        result.setTask_id(TASK_ID);
        result.setTablet_id(TABLET_ID);
        result.setPartition_id(PARTITION_ID);
        result.setSnapshot_end_version(snapshotVersion);
        result.setProcessed_through_version(processedVersion);
        result.setCoverage_digest("");
        result.setRowsets(new ArrayList<>());
        result.setScanned_rows(0);
        result.setKept_rows(0);
        result.setDeleted_rows(0);
        result.setTenant_rows_read(0);
        result.setRows_pruned_by_segment_zonemap(0);
        result.setRows_pruned_by_page_zonemap(0);
        result.setLinked_bytes(0);
        result.setRewritten_bytes(0);
        return result;
    }

    private static void invokeFinishHandler(LeaderImpl leader, AgentTask task, TFinishTaskRequest request)
            throws Exception {
        Method method = LeaderImpl.class.getDeclaredMethod(
                "finishTenantTtlCompactionTask", AgentTask.class, TFinishTaskRequest.class);
        method.setAccessible(true);
        try {
            method.invoke(leader, task, request);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            }
            throw e;
        }
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
