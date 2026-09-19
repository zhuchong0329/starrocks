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

#include <gtest/gtest.h>

#include <deque>

#include "exec/pipeline/exchange/exchange_sink_operator.h"
#include "exec/pipeline/fragment_context.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Partitions_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks::pipeline {

class AutoIncChunkBuilder {
public:
    AutoIncChunkBuilder(size_t chunk_size = 4096) : _chunk_size(chunk_size) {}

    ChunkPtr get_next() {
        ChunkPtr chunk = std::make_shared<Chunk>();
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BIGINT), false);
        for (size_t i = 0; i < _chunk_size; i++) {
            col->append_datum(Datum(_next_value++));
        }
        chunk->append_column(std::move(col), 0);
        return chunk;
    }
    size_t _next_value = 0;
    size_t _chunk_size;
};

class ExchangePassThroughTest : public ::testing::Test {
public:
    // Pass-through still sends RPC metadata. This fixture has no listening BE;
    // route that metadata through the real manager and complete callbacks only
    // after SinkBuffer releases its send lock, matching asynchronous RPC completion.
    class LocalStub final : public PInternalService_RecoverableStub {
    public:
        LocalStub() : PInternalService_RecoverableStub(butil::EndPoint{}) {}
        void transmit_chunk(google::protobuf::RpcController*, const PTransmitChunkParams* request,
                            PTransmitChunkResult* response, google::protobuf::Closure* done) override {
            Status::OK().to_protobuf(response->mutable_status());
            auto status = ExecEnv::GetInstance()->stream_mgr()->transmit_chunk(*request, &done);
            if (!status.ok()) {
                status.to_protobuf(response->mutable_status());
            }
            if (done != nullptr) {
                pending.push_back(done);
            }
        }
        void finish_rpcs() {
            while (!pending.empty()) {
                auto* done = pending.front();
                pending.pop_front();
                done->Run();
            }
        }
        std::deque<google::protobuf::Closure*> pending;
    };

    void SetUp() override {
        BackendOptions::set_localhost("0.0.0.0");

        _exec_env = ExecEnv::GetInstance();

        _query_context = std::make_shared<QueryContext>();
        _query_context->set_exec_env(_exec_env);
        _query_context->init_mem_tracker(-1, GlobalEnv::GetInstance()->process_mem_tracker());

        TQueryOptions query_options;
        query_options.__set_enable_query_corruption_tolerance(true);
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(_fragment_id, query_options, query_globals, _exec_env);
        _runtime_state->set_query_ctx(_query_context.get());
        _runtime_state->init_instance_mem_tracker();

        _fragment_context = std::make_shared<pipeline::FragmentContext>();
        _fragment_context->set_fragment_instance_id(_fragment_id);
        _fragment_context->set_runtime_state(std::shared_ptr<RuntimeState>{_runtime_state});
        _runtime_state->set_fragment_ctx(_fragment_context.get());
        _fragment_context->prepare_pass_through_chunk_buffer();

        TNetworkAddress address;
        address.__set_hostname(BackendOptions::get_local_ip());
        address.__set_port(config::brpc_port);
        // Save and restore only this endpoint's pool; do not depend on another UT
        // having started a service or registered a query-level buffer.
        ASSERT_NE(nullptr, _exec_env->brpc_stub_cache()->get_stub(address));
        butil::EndPoint endpoint;
        ASSERT_EQ(0, butil::str2endpoint(address.hostname.c_str(), address.port, &endpoint));
        _stub_pool = *_exec_env->brpc_stub_cache()->_stub_map.seek(endpoint);
        _saved_stubs = std::move(_stub_pool->_stubs);
        _saved_stub_index = _stub_pool->_idx;
        _local_stub = std::make_shared<LocalStub>();
        _stub_pool->_stubs = {_local_stub};
        _stub_pool->_idx = 0;
        TPlanFragmentDestination destination;
        destination.__set_fragment_instance_id(_fragment_id);
        destination.__set_brpc_server(address);
        _destinations = {destination};

        _sink_buffer = std::make_shared<SinkBuffer>(_fragment_context.get(), _destinations, /*is_dest_merge*/ false);

        RowDescriptor input_row_desc;
        _recvr = _exec_env->stream_mgr()->create_recvr(
                _runtime_state.get(), input_row_desc, _fragment_id, 0, 1, config::exchg_node_buffer_size_bytes,
                _dest_node_id, std::make_shared<QueryStatisticsRecvr>(),
                /*is_pipeline*/ true, _degree_of_parallelism, /*keep_order*/ false);
        std::stringstream ss;
        ss << "exchange (id=" << _dest_node_id << ")";
        auto runtime_profile = std::make_shared<RuntimeProfile>(ss.str());
        runtime_profile->set_metadata(_dest_node_id);
        _recvr->bind_profile(0, runtime_profile);

        _exchange_sink_factory = std::make_shared<ExchangeSinkOperatorFactory>(
                0, 0, _sink_buffer, TPartitionType::UNPARTITIONED, _destinations, /*is_pipeline_level_shuffle*/ false,
                /*dest_dop*/ 1,
                /*sender_id*/ 0, _dest_node_id, /*partition_exprs*/ std::vector<ExprContext*>(),
                /*enable_exchange_pass_through*/ true, /*enable_exchange_perf*/ false, _fragment_context.get(),
                /*output_columns*/ std::vector<int32_t>(), std::vector<TBucketProperty>());
        _exchange_sink_factory->set_runtime_state(_runtime_state.get());
    }

    void TearDown() override {
        _recvr->close();
        _exec_env->stream_mgr()->close();
        _local_stub->finish_rpcs();
        _stub_pool->_stubs = std::move(_saved_stubs);
        _stub_pool->_idx = _saved_stub_index;
        _fragment_context->destroy_pass_through_chunk_buffer();
        _query_context->set_exec_env(nullptr);
    }

protected:
    Status send(const OperatorPtr& sink) {
        auto status = sink->push_chunk(_runtime_state.get(), _chunk_builder.get_next());
        _local_stub->finish_rpcs();
        return status;
    }

    std::shared_ptr<LocalStub> _local_stub;
    std::shared_ptr<BrpcStubCache::StubPool> _stub_pool;
    std::vector<std::shared_ptr<PInternalService_RecoverableStub>> _saved_stubs;
    int64_t _saved_stub_index = 0;
    TUniqueId _fragment_id;
    ExecEnv* _exec_env;
    std::shared_ptr<QueryContext> _query_context;
    std::shared_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<pipeline::FragmentContext> _fragment_context;

    std::shared_ptr<SinkBuffer> _sink_buffer;
    std::shared_ptr<DataStreamRecvr> _recvr;
    std::shared_ptr<RuntimeProfile> _runtime_profile;
    std::shared_ptr<ExchangeSinkOperatorFactory> _exchange_sink_factory;

    std::vector<TPlanFragmentDestination> _destinations;
    int32_t _dest_node_id = 0;
    int32_t _degree_of_parallelism = 1;

    size_t _chunk_size = 4096;
    AutoIncChunkBuilder _chunk_builder{_chunk_size};
};

TEST_F(ExchangePassThroughTest, test_exchange_pass_through) {
    int32_t driver_sequence = 0;
    auto exchange_sink = _exchange_sink_factory->create(_degree_of_parallelism, driver_sequence);
    ASSERT_OK(exchange_sink->prepare(_runtime_state.get()));
    ASSERT_OK(exchange_sink->prepare_local_state(_runtime_state.get()));

    size_t sent_bytes = 0;
    size_t chunk_bytes = _chunk_builder._chunk_size * 8;
    // data is batched up to max_transmit_batched_bytes. Until then no data is actually sent.
    while (sent_bytes + chunk_bytes < config::max_transmit_batched_bytes) {
        sent_bytes += chunk_bytes;
        ASSERT_OK(send(exchange_sink));
        std::unique_ptr<Chunk> received_chunk = nullptr;
        std::ignore = _recvr->get_chunk_for_pipeline(&received_chunk, driver_sequence);
        EXPECT_TRUE(received_chunk == nullptr);
    }

    // once the sent bytes exceeds max_transmit_batched_bytes, the data is sent.
    ASSERT_OK(send(exchange_sink));
    std::unique_ptr<Chunk> received_chunk = nullptr;
    std::ignore = _recvr->get_chunk_for_pipeline(&received_chunk, driver_sequence);
    ASSERT_TRUE(received_chunk != nullptr);

    // sending chunks without consuming leads to a full sink buffer.
    for (int i = 0; !_sink_buffer->is_full() && i < 4096; ++i) {
        ASSERT_OK(send(exchange_sink));
    }
    ASSERT_TRUE(_sink_buffer->is_full());

    // receiver ready to consume the data.
    EXPECT_TRUE(_recvr->has_output_for_pipeline(driver_sequence));

    // consuming chunks on the reciever side automatically relieves pressure on the sink side.
    do {
        std::ignore = _recvr->get_chunk_for_pipeline(&received_chunk, driver_sequence);
        _local_stub->finish_rpcs();
    } while (received_chunk != nullptr);
    EXPECT_FALSE(_sink_buffer->is_full());

    exchange_sink->close(_runtime_state.get());
}

TEST_F(ExchangePassThroughTest, recv_closed_1) {
    int32_t driver_sequence = 0;
    auto exchange_sink = _exchange_sink_factory->create(_degree_of_parallelism, driver_sequence);
    ASSERT_OK(exchange_sink->prepare(_runtime_state.get()));
    ASSERT_OK(exchange_sink->prepare_local_state(_runtime_state.get()));

    for (int i = 0; !_sink_buffer->is_full() && i < 4096; ++i) {
        ASSERT_OK(send(exchange_sink));
    }
    ASSERT_TRUE(_sink_buffer->is_full());

    ASSERT_OK(exchange_sink->set_finishing(_runtime_state.get()));

    std::thread thr([recvr = _recvr]() { recvr->close(); });
    thr.join();
    _local_stub->finish_rpcs();
    exchange_sink->close(_runtime_state.get());
}

TEST_F(ExchangePassThroughTest, recv_closed_2) {
    int32_t driver_sequence = 0;
    auto exchange_sink = _exchange_sink_factory->create(_degree_of_parallelism, driver_sequence);
    ASSERT_OK(exchange_sink->prepare(_runtime_state.get()));
    ASSERT_OK(exchange_sink->prepare_local_state(_runtime_state.get()));

    for (int i = 0; !_sink_buffer->is_full() && i < 4096; ++i) {
        ASSERT_OK(send(exchange_sink));
    }
    ASSERT_TRUE(_sink_buffer->is_full());

    std::thread thr([recvr = _recvr]() { recvr->close(); });
    thr.join();
    _local_stub->finish_rpcs();

    ASSERT_OK(exchange_sink->set_finishing(_runtime_state.get()));
    _local_stub->finish_rpcs();
    exchange_sink->close(_runtime_state.get());
}

TEST_F(ExchangePassThroughTest, corruption_is_visible_before_pure_eos_without_attach) {
    ASSERT_FALSE(_query_context->query_corruption_detected());
    PTransmitChunkParams request;
    request.mutable_finst_id()->set_hi(_fragment_id.hi);
    request.mutable_finst_id()->set_lo(_fragment_id.lo);
    request.set_node_id(_dest_node_id);
    request.set_sender_id(0);
    request.set_be_number(0);
    request.set_sequence(0);
    request.set_eos(true);
    request.set_query_corruption_detected(true);
    google::protobuf::Closure* done = nullptr;
    ASSERT_OK(_exec_env->stream_mgr()->transmit_chunk(request, &done));
    ASSERT_TRUE(_query_context->query_corruption_detected());
    ASSERT_TRUE(_recvr->is_finished());
    // No attach_query_ctx or additional health/status request was needed.
    _recvr->mark_query_corruption_detected();
    _query_context->set_final_sink();
    PQueryStatistics result;
    _query_context->final_query_statistic()->to_pb(&result);
    ASSERT_TRUE(result.query_corruption_detected());
}

TEST_F(ExchangePassThroughTest, strict_receiver_does_not_accept_partial_diagnostic) {
    _runtime_state->_query_options.__set_enable_query_corruption_tolerance(false);
    TUniqueId other_id;
    other_id.hi = _fragment_id.hi;
    other_id.lo = _fragment_id.lo + 1;
    RowDescriptor row_desc;
    auto strict = _exec_env->stream_mgr()->create_recvr(_runtime_state.get(), row_desc, other_id, 1, 1,
                                                        config::exchg_node_buffer_size_bytes, false,
                                                        std::make_shared<QueryStatisticsRecvr>(), true, 1, false);
    strict->mark_query_corruption_detected();
    ASSERT_FALSE(_query_context->query_corruption_detected());
    strict->close();
}

TEST_F(ExchangePassThroughTest, every_destination_and_pure_eos_carry_sticky_diagnostic) {
    class CapturingStub final : public PInternalService_RecoverableStub {
    public:
        CapturingStub() : PInternalService_RecoverableStub(butil::EndPoint{}) {}
        void transmit_chunk(google::protobuf::RpcController*, const PTransmitChunkParams* request,
                            PTransmitChunkResult* response, google::protobuf::Closure* done) override {
            packets.emplace_back(*request);
            Status::OK().to_protobuf(response->mutable_status());
            pending = done;
        }
        void acknowledge() {
            auto* done = std::exchange(pending, nullptr);
            if (done != nullptr) {
                done->Run();
            }
        }
        std::vector<PTransmitChunkParams> packets;
        google::protobuf::Closure* pending = nullptr;
    };
    auto destinations = _destinations;
    auto second = destinations.front();
    second.fragment_instance_id.lo++;
    destinations.push_back(second);
    auto buffer = std::make_shared<SinkBuffer>(_fragment_context.get(), destinations, false);
    buffer->incr_sinker(_runtime_state.get());
    auto stub = std::make_shared<CapturingStub>();
    // Disables intermediate audit statistics; the independent diagnostic must still arrive.
    _query_context->set_final_sink();
    auto send = [&](size_t destination, bool eos) {
        auto params = std::make_shared<PTransmitChunkParams>();
        params->set_node_id(0);
        params->set_sender_id(0);
        params->set_be_number(0);
        params->set_eos(eos);
        TransmitChunkInfo request{destinations[destination].fragment_instance_id, stub, params, {}, 0,
                                  destinations[destination].brpc_server};
        ASSERT_OK(buffer->add_request(request));
        stub->acknowledge(); // Normal RPC completion, not a diagnostic acknowledgement.
    };
    send(0, false);
    ASSERT_FALSE(stub->packets.back().has_query_corruption_detected());
    _query_context->mark_query_corruption_detected();
    send(0, false);
    send(1, false);
    send(0, true);
    send(1, true);
    ASSERT_EQ(5, stub->packets.size());
    for (size_t i = 1; i < stub->packets.size(); ++i) {
        ASSERT_TRUE(stub->packets[i].query_corruption_detected());
        ASSERT_FALSE(stub->packets[i].has_query_statistics());
    }
    ASSERT_TRUE(buffer->is_finished());
    // Existing cancellation/early LIMIT finish must not create a late diagnostic RPC.
    auto cancelled = std::make_shared<SinkBuffer>(_fragment_context.get(), destinations, false);
    cancelled->incr_sinker(_runtime_state.get());
    cancelled->cancel_one_sinker(_runtime_state.get());
    TransmitChunkInfo ignored{
            destinations[0].fragment_instance_id, stub, std::make_shared<PTransmitChunkParams>(), {}, 0,
            destinations[0].brpc_server};
    ASSERT_OK(cancelled->add_request(ignored));
    ASSERT_EQ(5, stub->packets.size());
    ASSERT_TRUE(cancelled->is_finished());
}

} // namespace starrocks::pipeline
