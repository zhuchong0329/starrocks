// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.TAgentServiceVersion;
import com.starrocks.thrift.TAgentTaskRequest;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TTenantTtlCompactionReq;
import com.starrocks.thrift.TTenantTtlFilter;
import com.starrocks.thrift.TTenantTtlFilterMode;
import com.starrocks.thrift.TTenantTtlPolicyWatermark;
import com.starrocks.thrift.TTenantTtlSchemaExpectation;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

/** Test-only delayed wire request to localhost in the isolated target BE container. */
public class DelayedRequest {
    public static void main(String[] args) throws Exception {
        TTenantTtlCompactionReq request = new TTenantTtlCompactionReq();
        request.setProtocol_version(1);
        request.setTask_id(Long.parseLong(args[0]));
        request.setTablet_id(Long.parseLong(args[1]));
        request.setPartition_id(Long.parseLong(args[2]));
        request.setTenant_column_unique_id(Integer.parseInt(args[3]));
        request.setFilter(new TTenantTtlFilter(TTenantTtlFilterMode.DELETE_LIST,
                Collections.singletonList(ByteBuffer.wrap(args[4].getBytes(StandardCharsets.UTF_8)))));
        request.setPolicy_watermark(new TTenantTtlPolicyWatermark(Long.parseLong(args[5]),
                Long.parseLong(args[6]), Long.parseLong(args[7])));
        request.setExpected_schema(new TTenantTtlSchemaExpectation(Long.parseLong(args[8]),
                Integer.parseInt(args[9])));
        request.setFe_observed_max_version(Long.parseLong(args[10]));
        TAgentTaskRequest envelope = new TAgentTaskRequest(TAgentServiceVersion.V1,
                TTaskType.TENANT_TTL_COMPACTION, request.getTask_id());
        envelope.setTenant_ttl_compaction_req(request);
        try (TSocket socket = new TSocket("127.0.0.1", 9060, 10000)) {
            socket.open();
            System.out.println(new BackendService.Client(new TBinaryProtocol(socket))
                    .submit_tasks(Collections.singletonList(envelope)));
        }
    }
}
