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

#include "exec/pipeline/scan/olap_scan_operator.h"

#include "column/fixed_length_column.h"
#include "exec/olap_scan_node.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/scan/olap_chunk_source.h"
#include "exec/pipeline/scan/olap_scan_prepare_operator.h"
#include "gtest/gtest.h"
#include "runtime/descriptors.h"
#include "storage/rowset/rowset.h"
#include "util/starrocks_metrics.h"
#include "util/table_metrics.h"

namespace starrocks::pipeline {

class OlapScanOperatorTest : public ::testing::Test {
public:
    void SetUp() override;
    void TearDown() override {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
    }

protected:
    ObjectPool _object_pool;
    RuntimeState _runtime_state;
    TDescriptorTable _thrift_tbl;
    const int64_t _chunk_size = 4096;
    DescriptorTbl* _tbl = nullptr;
    TPlanNode _tnode;
    ChunkBufferLimiterPtr _chunk_buffer_limiter;
    QueryContext _query_ctx;
};

void OlapScanOperatorTest::SetUp() {
    TTableDescriptor t_table_desc;
    t_table_desc.id = 1;
    t_table_desc.tableType = TTableType::OLAP_TABLE;
    _thrift_tbl.tableDescriptors.emplace_back(t_table_desc);

    TTupleDescriptor t_tuple_desc;
    t_tuple_desc.id = 1;
    t_tuple_desc.tableId = 1;
    _thrift_tbl.tupleDescriptors.emplace_back(t_tuple_desc);

    _tnode.row_tuples.emplace_back(1);

    Status st = DescriptorTbl::create(&_runtime_state, &_object_pool, _thrift_tbl, &_tbl, _chunk_size);
    ASSERT_TRUE(st.ok());

    _runtime_state.set_desc_tbl(_tbl);
    _chunk_buffer_limiter = std::make_unique<UnlimitedChunkBufferLimiter>();

    _query_ctx.init_mem_tracker(-1, GlobalEnv::GetInstance()->process_mem_tracker());
    _runtime_state.set_query_ctx(&_query_ctx);
    _runtime_state.init_instance_mem_tracker();
}

TEST_F(OlapScanOperatorTest, test_finish_sequence) {
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("OlapScanPrepareOperator::prepare",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("ScanOperatorFactory::prepare",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("OlapScanContext::parse_conjuncts",
                                          [](void* arg) { *(Status*)arg = Status::EndOfFile(""); });

    Morsels morsels;
    FixedMorselQueue morsel_queue(std::move(morsels));

    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    auto scan_ctx_factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));

    // create operator factory
    OlapScanPrepareOperatorFactory scan_prepare_operator_factory(1, 1, &scan_node, scan_ctx_factory);
    Status st = scan_prepare_operator_factory.prepare(&_runtime_state);
    ASSERT_TRUE(st.ok());

    OlapScanOperatorFactory scan_operator_factory(1, &scan_node, scan_ctx_factory);
    st = scan_operator_factory.prepare(&_runtime_state);
    ASSERT_TRUE(st.ok());

    // create operator
    auto scan_prepare_operator = scan_prepare_operator_factory.create(1, 0);
    ASSERT_TRUE(scan_prepare_operator != nullptr);
    down_cast<OlapScanPrepareOperator*>(scan_prepare_operator.get())->add_morsel_queue(&morsel_queue);

    auto scan_operator = scan_operator_factory.create(1, 0);
    ASSERT_TRUE(scan_operator != nullptr);

    // operator prepare
    st = scan_prepare_operator->prepare(&_runtime_state);
    ASSERT_TRUE(st.ok());

    // pull chunk
    SyncPoint::GetInstance()->SetCallBack("OlapScnPrepareOperator::pull_chunk::before_set_finished",
                                          [&scan_operator](void* arg) { ASSERT_FALSE(scan_operator->has_output()); });
    SyncPoint::GetInstance()->SetCallBack("OlapScnPrepareOperator::pull_chunk::after_set_finished",
                                          [&scan_operator](void* arg) { ASSERT_FALSE(scan_operator->has_output()); });
    SyncPoint::GetInstance()->SetCallBack("OlapScnPrepareOperator::pull_chunk::after_set_prepare_finished",
                                          [&scan_operator](void* arg) { ASSERT_FALSE(scan_operator->has_output()); });

    auto ret = scan_prepare_operator->pull_chunk(&_runtime_state);
    ASSERT_TRUE(ret.status().is_end_of_file());

    scan_node.close(&_runtime_state);

    SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(OlapScanOperatorTest, corruption_is_task_local_and_discards_failed_chunk) {
    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    auto factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));
    OlapScanOperatorFactory op_factory(1, &scan_node, factory);
    auto op = op_factory.create(1, 0);
    auto* scan_op = down_cast<ScanOperator*>(op.get());
    auto ctx = factory->get_or_create(0);
    RuntimeProfile profile("corruption-test");
    TScanRange range;
    range.__set_internal_scan_range(TInternalScanRange());
    OlapChunkSource broken(scan_op, &profile, std::make_unique<ScanMorsel>(1, range), &scan_node, ctx.get());
    OlapChunkSource sibling(scan_op, &profile, std::make_unique<ScanMorsel>(1, range), &scan_node, ctx.get());
    broken._runtime_state = &_runtime_state;
    sibling._runtime_state = &_runtime_state;

    auto col = Int32Column::create();
    col->append(42);
    auto failed = std::make_shared<Chunk>();
    failed->append_column(col, 1);
    const auto corruption = Status::Corruption("bad page checksum");
    ASSERT_FALSE(broken._tolerate_storage_corruption(corruption, failed.get()));
    ASSERT_EQ(1, failed->num_rows());
    ASSERT_FALSE(_query_ctx.query_corruption_detected());

    _runtime_state._query_options.__set_enable_query_corruption_tolerance(true);
    ASSERT_TRUE(broken._tolerate_storage_corruption(corruption, failed.get()));
    ASSERT_EQ(0, failed->num_rows());
    ASSERT_TRUE(_query_ctx.query_corruption_detected());
    ASSERT_TRUE(broken._corruption_tolerated);
    ASSERT_FALSE(sibling._corruption_tolerated);
    // prepare/open failure must terminate without touching a missing/unopened Reader.
    ASSERT_TRUE(broken._read_chunk(&_runtime_state, &failed).is_end_of_file());
    ASSERT_EQ(nullptr, failed);

    // Use the real ChunkSource EOF path: an empty final-owner chunk must reach the
    // cache lane and release its buffer token, even when Reader::open never completed.
    ASSERT_TRUE(broken.buffer_next_batch_chunks_blocking(&_runtime_state, 8, nullptr).is_end_of_file());
    auto& buffer = ctx->get_chunk_buffer();
    ASSERT_EQ(1, buffer.size(0));
    ASSERT_EQ(1, buffer.limiter()->size());
    ASSERT_TRUE(buffer.try_get(0, &failed));
    ASSERT_TRUE(failed->is_empty());
    ASSERT_TRUE(failed->owner_info().is_last_chunk());
    ASSERT_EQ(0, buffer.limiter()->size());
    ASSERT_TRUE(broken.buffer_next_batch_chunks_blocking(&_runtime_state, 8, nullptr).is_end_of_file());
    ASSERT_TRUE(buffer.all_empty());
    ASSERT_TRUE(broken._read_chunk(&_runtime_state, &failed).is_end_of_file());
    ASSERT_EQ(nullptr, failed);

    ASSERT_FALSE(sibling._tolerate_storage_corruption(Status::IOError("disk unavailable")));
    ASSERT_FALSE(sibling._tolerate_storage_corruption(Status::NotFound("missing version")));
    ASSERT_FALSE(sibling._tolerate_storage_corruption(Status::InternalError("expression")));
    ASSERT_FALSE(sibling._tolerate_storage_corruption(Status::EndOfFile("natural EOF")));
    ASSERT_FALSE(sibling._corruption_tolerated);
    _runtime_state.set_is_cancelled(true);
    ASSERT_FALSE(sibling._tolerate_storage_corruption(corruption));
    _runtime_state.set_is_cancelled(false);
    ASSERT_TRUE(sibling._tolerate_storage_corruption(corruption));
    ASSERT_TRUE(sibling._corruption_tolerated);
    scan_node.close(&_runtime_state);
}

TEST_F(OlapScanOperatorTest, shared_prepare_corruption_remains_strict) {
    _runtime_state._query_options.__set_enable_query_corruption_tolerance(true);
    SyncPoint::GetInstance()->EnableProcessing();
    TEST_ENABLE_ERROR_POINT("OlapScanContext::parse_conjuncts", Status::Corruption("shared preparation"));
    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    auto factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));
    OlapScanPrepareOperatorFactory prepare_factory(1, 1, &scan_node, factory);
    auto op = prepare_factory.create(1, 0);
    FixedMorselQueue queue(Morsels{});
    down_cast<OlapScanPrepareOperator*>(op.get())->add_morsel_queue(&queue);
    ASSERT_TRUE(op->pull_chunk(&_runtime_state).status().is_corruption());
    ASSERT_FALSE(_query_ctx.query_corruption_detected());
    ASSERT_TRUE(factory->get_or_create(0)->is_prepare_finished());
    scan_node.close(&_runtime_state);
}

TEST_F(OlapScanOperatorTest, storage_read_failure_discards_only_the_failed_chunk) {
    class PartialIterator final : public ChunkIterator {
    public:
        PartialIterator() : ChunkIterator(Schema{}) {}
        void close() override { closed = true; }
        Status do_get_next(Chunk* chunk) override {
            down_cast<Int32Column*>(chunk->get_column_by_index(0).get())->append(++calls);
            return calls == 1 ? Status::OK() : Status::Corruption("failure after filling a row");
        }
        int calls = 0;
        bool closed = false;
    };
    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    auto factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));
    OlapScanOperatorFactory op_factory(1, &scan_node, factory);
    auto op = op_factory.create(1, 0);
    auto ctx = factory->get_or_create(0);
    RuntimeProfile profile("storage-read-corruption");
    TScanRange range;
    range.__set_internal_scan_range(TInternalScanRange());
    OlapChunkSource source(down_cast<ScanOperator*>(op.get()), &profile, std::make_unique<ScanMorsel>(1, range),
                           &scan_node, ctx.get());
    source._runtime_state = &_runtime_state;
    source._limit = -1;
    source._reader = std::make_shared<TabletReader>(nullptr, Version{0, 1}, TabletSchemaCSPtr{}, Schema{});
    auto reader = std::make_shared<PartialIterator>();
    source._prj_iter = reader;
    auto first = std::make_shared<Chunk>();
    first->append_column(Int32Column::create(), 1);
    ASSERT_TRUE(source._read_chunk_from_storage(&_runtime_state, first.get()).ok());
    ASSERT_EQ(1, first->num_rows());
    ChunkPtr failed = first->clone_empty();
    _runtime_state._query_options.__set_enable_query_corruption_tolerance(true);
    ASSERT_TRUE(source._read_chunk_from_storage(&_runtime_state, failed.get()).is_end_of_file());
    ASSERT_EQ(0, failed->num_rows());
    ASSERT_EQ(1, first->num_rows());
    ASSERT_EQ(1, source._num_rows_read);
    ASSERT_TRUE(_query_ctx.query_corruption_detected());
    ASSERT_TRUE(source._read_chunk(&_runtime_state, &failed).is_end_of_file());
    ASSERT_EQ(2, reader->calls);
    // Counter initialization belongs to prepare(), which this focused iterator test bypasses.
    source._reader.reset();
    source.close(&_runtime_state);
    ASSERT_TRUE(reader->closed);
    scan_node.close(&_runtime_state);
}

TEST_F(OlapScanOperatorTest, failed_reader_prepare_releases_captured_rowsets_on_close) {
    class CorruptRowset final : public Rowset {
    public:
        CorruptRowset(const TabletSchemaCSPtr& schema, RowsetMetaSharedPtr meta)
                : Rowset(schema, "", std::move(meta)) {}
        Status load() override { return Status::Corruption("invalid segment footer during Reader::prepare"); }
    };
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    TabletSchemaCSPtr schema = std::make_shared<TabletSchema>(schema_pb);
    RowsetMetaPB meta;
    meta.set_deprecated_rowset_id(1);
    auto rowset = std::make_shared<CorruptRowset>(schema, std::make_shared<RowsetMeta>(meta));

    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    auto factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));
    OlapScanOperatorFactory op_factory(1, &scan_node, factory);
    auto op = op_factory.create(1, 0);
    auto ctx = factory->get_or_create(0);
    RuntimeProfile profile("reader-prepare-corruption");
    TScanRange range;
    range.__set_internal_scan_range(TInternalScanRange());
    OlapChunkSource source(down_cast<ScanOperator*>(op.get()), &profile, std::make_unique<ScanMorsel>(1, range),
                           &scan_node, ctx.get());
    FragmentContext fragment;
    _runtime_state.set_fragment_ctx(&fragment);
    _runtime_state._query_options.__set_enable_query_corruption_tolerance(true);
    source._runtime_state = &_runtime_state;
    ASSERT_TRUE(source.ChunkSource::prepare(&_runtime_state).ok());
    source._init_counter(&_runtime_state);
    source._table_metrics = StarRocksMetrics::instance()->table_metrics_mgr()->get_table_metrics(1);
    source._reader = std::make_shared<TabletReader>(nullptr, Version{0, 1}, Schema{},
                                                    std::vector<RowsetSharedPtr>{rowset}, &schema);
    source._prj_iter = source._reader;
    auto status = source._reader->prepare();
    ASSERT_TRUE(status.is_corruption());
    ASSERT_EQ(1, rowset->refs_by_reader());
    ASSERT_TRUE(source._tolerate_storage_corruption(status));
    ChunkPtr output;
    ASSERT_TRUE(source._read_chunk(&_runtime_state, &output).is_end_of_file());
    ASSERT_EQ(nullptr, output);
    source.close(&_runtime_state);
    ASSERT_EQ(0, rowset->refs_by_reader());
    _runtime_state.set_fragment_ctx(nullptr);
    scan_node.close(&_runtime_state);
}

} // namespace starrocks::pipeline
