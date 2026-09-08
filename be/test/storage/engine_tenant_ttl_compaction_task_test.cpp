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

#include "storage/task/engine_tenant_ttl_compaction_task.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <mutex>
#include <set>

#include "fs/fs.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet.h"
#include "storage/tenant_ttl_compaction_test_util.h"
#include "testutil/init_test_env.h"
#include "testutil/sync_point.h"

namespace starrocks {
namespace {

struct SingleRowsetCase {
    const char* name;
    TenantFilterMode mode;
    std::vector<std::string> tenants;
    std::vector<TenantTtlTestRow> source;
    std::vector<int64_t> expected_event_ids;
    TenantTtlRowsetAction expected_action;
    TenantTtlTaskCode expected_code;
};

} // namespace

class EngineTenantTtlCompactionTaskTest : public TenantTtlCompactionTestBase {
protected:
    void TearDown() override {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
        TenantTtlCompactionTestBase::TearDown();
    }

    TenantTtlCompactionRequest request(TenantFilterMode mode, std::vector<std::string> tenants,
                                       int64_t task_id = 1001) const {
        return {.task_id = task_id,
                .tablet_id = _tablet_id,
                .partition_id = _partition_id,
                .tenant_column_unique_id = kTenantColumnUniqueId,
                .filter = {.mode = mode, .tenants = std::move(tenants)},
                .policy_watermark = {.dictionary_id = 2001,
                                     .dictionary_txn_id = 2002,
                                     .evaluation_time_epoch_seconds = 2003},
                .expected_schema = {.schema_id = kSchemaId, .schema_version = kSchemaVersion}};
    }

    TenantTtlCompactionResult execute(TenantTtlCompactionRequest request,
                                      const std::atomic<bool>* is_cancelled = nullptr) {
        EngineTenantTtlCompactionTask task(std::move(request), nullptr, is_cancelled);
        const Status status = task.execute();
        EXPECT_EQ(status.ok(), task.result().code == TenantTtlTaskCode::SUCCESS ||
                                       task.result().code == TenantTtlTaskCode::NOOP_VERIFIED)
                << status;
        return task.result();
    }

    RowsetSharedPtr active_rowset(const Version& version) const {
        std::shared_lock lock(_tablet->get_header_lock());
        return _tablet->get_rowset_by_version(version);
    }

    std::vector<RowsetSharedPtr> active_rowsets() const {
        std::shared_lock lock(_tablet->get_header_lock());
        const auto max_rowset = _tablet->rowset_with_max_version();
        if (max_rowset == nullptr) {
            return {};
        }
        std::vector<RowsetSharedPtr> rowsets;
        EXPECT_OK(_tablet->capture_consistent_rowsets(Version(0, max_rowset->end_version()), &rowsets));
        return rowsets;
    }

    std::vector<TenantTtlTestRow> read_rowset(const RowsetSharedPtr& rowset) const {
        EXPECT_OK(rowset->load());
        const Schema schema = ChunkHelper::convert_schema(rowset->schema());
        auto fs_or = FileSystem::CreateSharedFromString(rowset->rowset_path());
        EXPECT_OK(fs_or.status());
        if (!fs_or.ok()) {
            return {};
        }

        std::vector<TenantTtlTestRow> rows;
        for (const auto& segment : rowset->segments()) {
            SegmentReadOptions options;
            options.fs = fs_or.value();
            OlapReaderStatistics stats;
            options.stats = &stats;
            options.tablet_schema = rowset->schema();
            auto iterator_or = segment->new_iterator(schema, options);
            EXPECT_OK(iterator_or.status());
            if (!iterator_or.ok()) {
                return {};
            }
            auto iterator = iterator_or.value();
            EXPECT_OK(iterator->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
            auto chunk = ChunkHelper::new_chunk(schema, 2);
            while (true) {
                chunk->reset();
                const Status status = iterator->get_next(chunk.get());
                if (status.is_end_of_file()) {
                    break;
                }
                EXPECT_OK(status);
                if (!status.ok()) {
                    break;
                }
                for (size_t i = 0; i < chunk->num_rows(); ++i) {
                    const Datum tenant = chunk->get_column_by_index(1)->get(i);
                    rows.emplace_back(TenantTtlTestRow{
                            .event_id = chunk->get_column_by_index(0)->get(i).get_int64(),
                            .tenant = tenant.is_null() ? std::nullopt
                                                       : std::optional<std::string>(tenant.get_slice().to_string()),
                            .payload = chunk->get_column_by_index(2)->get(i).get_int32()});
                }
            }
            iterator->close();
        }
        return rows;
    }

    std::vector<TenantTtlTestRow> read_tablet_rows() const {
        std::vector<RowsetSharedPtr> rowsets;
        {
            std::shared_lock lock(_tablet->get_header_lock());
            const auto max_rowset = _tablet->rowset_with_max_version();
            EXPECT_NE(nullptr, max_rowset);
            if (max_rowset == nullptr) {
                return {};
            }
            EXPECT_OK(_tablet->capture_consistent_rowsets(Version(0, max_rowset->end_version()), &rowsets));
        }
        std::vector<TenantTtlTestRow> result;
        for (const auto& rowset : rowsets) {
            auto rows = read_rowset(rowset);
            result.insert(result.end(), std::make_move_iterator(rows.begin()), std::make_move_iterator(rows.end()));
        }
        return result;
    }

    std::set<std::string> tablet_files() const {
        std::set<std::string> result;
        std::error_code error;
        for (std::filesystem::recursive_directory_iterator it(_tablet->schema_hash_path(), error), end;
             !error && it != end; it.increment(error)) {
            if (it->is_regular_file(error)) {
                result.emplace(it->path().filename().string());
            }
        }
        EXPECT_FALSE(error) << error.message();
        return result;
    }

    void expect_all_guards_released(const std::vector<RowsetSharedPtr>& sources) const {
        EXPECT_EQ(TenantTtlState::IDLE, _tablet->tenant_ttl_state_for_debug().state);
        for (const auto& source : sources) {
            EXPECT_FALSE(source->get_is_compacting());
            EXPECT_EQ(0, source->refs_by_reader());
        }
        const bool base_lock_acquired = _tablet->get_base_lock().try_lock();
        EXPECT_TRUE(base_lock_acquired);
        if (base_lock_acquired) {
            _tablet->get_base_lock().unlock();
        }
        const bool cumulative_lock_acquired = _tablet->get_cumulative_lock().try_lock();
        EXPECT_TRUE(cumulative_lock_acquired);
        if (cumulative_lock_acquired) {
            _tablet->get_cumulative_lock().unlock();
        }
        const bool migration_lock_acquired = _tablet->get_migration_lock().try_lock();
        EXPECT_TRUE(migration_lock_acquired);
        if (migration_lock_acquired) {
            _tablet->get_migration_lock().unlock();
        }
    }

    void expect_multi_segment_overlap_rewrite(SegmentsOverlapPB input_overlap) {
        ASSERT_NE(nullptr, create_tablet());
        auto source = add_rowset(Version(2, 2),
                                 {{{1, "keep", 10}, {2, "keep", 20}},
                                  {{3, "delete", 30}, {4, "delete", 40}},
                                  {{5, "delete", 50}, {6, "keep", 60}}},
                                 input_overlap);
        ASSERT_NE(nullptr, source);

        const auto result = execute(request(TenantFilterMode::DELETE_LIST, {"delete"}));
        ASSERT_EQ(TenantTtlTaskCode::SUCCESS, result.code);
        ASSERT_EQ(2, result.rowsets.size());
        const auto& rowset_result = result.rowsets.back();
        EXPECT_EQ(TenantTtlRowsetAction::REWRITE, rowset_result.action);
        EXPECT_EQ(1, rowset_result.linked_segments);
        EXPECT_EQ(1, rowset_result.dropped_segments);
        EXPECT_EQ(1, rowset_result.rewritten_segments);

        const auto output = active_rowset(Version(2, 2));
        ASSERT_NE(nullptr, output);
        EXPECT_NE(source->rowset_id(), output->rowset_id());
        EXPECT_EQ(2, output->num_segments());
        EXPECT_EQ(input_overlap, output->rowset_meta()->segments_overlap());
        const auto rows = read_tablet_rows();
        ASSERT_EQ(3, rows.size());
        EXPECT_EQ(1, rows[0].event_id);
        EXPECT_EQ(2, rows[1].event_id);
        EXPECT_EQ(6, rows[2].event_id);
    }
};

class EngineTenantTtlSingleRowsetTest : public EngineTenantTtlCompactionTaskTest,
                                        public testing::WithParamInterface<SingleRowsetCase> {};

TEST_P(EngineTenantTtlSingleRowsetTest, ExecutesCompleteActionMatrix) {
    ASSERT_NE(nullptr, create_tablet());
    const auto& test_case = GetParam();
    const auto source = add_rowset(Version(2, 2), {test_case.source});
    ASSERT_NE(nullptr, source);
    const RowsetId source_id = source->rowset_id();

    const auto result = execute(request(test_case.mode, test_case.tenants));
    ASSERT_EQ(test_case.expected_code, result.code);
    ASSERT_EQ(2, result.rowsets.size());
    const auto& rowset_result = result.rowsets.back();
    EXPECT_EQ(test_case.expected_action, rowset_result.action);
    EXPECT_EQ(source->num_rows(), rowset_result.source_rows);
    EXPECT_EQ(source->num_rows(), rowset_result.kept_rows + rowset_result.deleted_rows);
    EXPECT_EQ(2, result.snapshot_end_version);
    EXPECT_EQ(2, result.processed_through_version);

    const auto current = active_rowset(Version(2, 2));
    ASSERT_NE(nullptr, current);
    if (test_case.expected_action == TenantTtlRowsetAction::VERIFIED_NO_CHANGE) {
        EXPECT_EQ(source_id, current->rowset_id());
        EXPECT_FALSE(rowset_result.output_rowset_id.has_value());
    } else {
        EXPECT_NE(source_id, current->rowset_id());
        ASSERT_TRUE(rowset_result.output_rowset_id.has_value());
        EXPECT_EQ(current->rowset_id(), rowset_result.output_rowset_id.value());
    }

    const auto rows = read_rowset(current);
    ASSERT_EQ(test_case.expected_event_ids.size(), rows.size());
    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(test_case.expected_event_ids[i], rows[i].event_id);
        EXPECT_EQ(static_cast<int32_t>(test_case.expected_event_ids[i] * 10), rows[i].payload);
    }
}

INSTANTIATE_TEST_SUITE_P(DeleteAndKeepModes, EngineTenantTtlSingleRowsetTest,
                         testing::Values(SingleRowsetCase{.name = "DeleteKeep",
                                                          .mode = TenantFilterMode::DELETE_LIST,
                                                          .tenants = {"a"},
                                                          .source = {{1, "b", 10}, {2, std::nullopt, 20}},
                                                          .expected_event_ids = {1, 2},
                                                          .expected_action = TenantTtlRowsetAction::VERIFIED_NO_CHANGE,
                                                          .expected_code = TenantTtlTaskCode::NOOP_VERIFIED},
                                         SingleRowsetCase{.name = "DeleteDrop",
                                                          .mode = TenantFilterMode::DELETE_LIST,
                                                          .tenants = {"a"},
                                                          .source = {{1, "a", 10}, {2, "a", 20}},
                                                          .expected_event_ids = {},
                                                          .expected_action = TenantTtlRowsetAction::DROP,
                                                          .expected_code = TenantTtlTaskCode::SUCCESS},
                                         SingleRowsetCase{.name = "DeleteRewrite",
                                                          .mode = TenantFilterMode::DELETE_LIST,
                                                          .tenants = {"a"},
                                                          .source = {{1, "a", 10}, {2, "b", 20}, {3, std::nullopt, 30}},
                                                          .expected_event_ids = {2, 3},
                                                          .expected_action = TenantTtlRowsetAction::REWRITE,
                                                          .expected_code = TenantTtlTaskCode::SUCCESS},
                                         SingleRowsetCase{.name = "KeepKeep",
                                                          .mode = TenantFilterMode::KEEP_LIST,
                                                          .tenants = {"b"},
                                                          .source = {{1, "b", 10}, {2, std::nullopt, 20}},
                                                          .expected_event_ids = {1, 2},
                                                          .expected_action = TenantTtlRowsetAction::VERIFIED_NO_CHANGE,
                                                          .expected_code = TenantTtlTaskCode::NOOP_VERIFIED},
                                         SingleRowsetCase{.name = "KeepDrop",
                                                          .mode = TenantFilterMode::KEEP_LIST,
                                                          .tenants = {"b"},
                                                          .source = {{1, "a", 10}, {2, "c", 20}},
                                                          .expected_event_ids = {},
                                                          .expected_action = TenantTtlRowsetAction::DROP,
                                                          .expected_code = TenantTtlTaskCode::SUCCESS},
                                         SingleRowsetCase{.name = "KeepRewrite",
                                                          .mode = TenantFilterMode::KEEP_LIST,
                                                          .tenants = {"b"},
                                                          .source = {{1, "a", 10}, {2, "b", 20}, {3, std::nullopt, 30}},
                                                          .expected_event_ids = {2, 3},
                                                          .expected_action = TenantTtlRowsetAction::REWRITE,
                                                          .expected_code = TenantTtlTaskCode::SUCCESS}),
                         [](const testing::TestParamInfo<SingleRowsetCase>& info) { return info.param.name; });

TEST_F(EngineTenantTtlCompactionTaskTest, DeleteListCommitsKeepDropRewriteAndThenNoops) {
    ASSERT_NE(nullptr, create_tablet());
    const auto keep = add_rowset(Version(2, 2), {{{1, "b", 10}, {2, "c", 20}, {3, std::nullopt, 30}}});
    const auto drop = add_rowset(Version(3, 3), {{{4, "a", 40}, {5, "a", 50}}});
    const auto rewrite = add_rowset(Version(4, 4), {{{6, "a", 60}, {7, "b", 70}, {8, std::nullopt, 80}}});
    ASSERT_NE(nullptr, keep);
    ASSERT_NE(nullptr, drop);
    ASSERT_NE(nullptr, rewrite);
    const auto before = snapshot_active_rowsets();
    ASSERT_EQ(4, before.size());

    const auto first = execute(request(TenantFilterMode::DELETE_LIST, {"a"}));
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, first.code);
    ASSERT_EQ(4, first.rowsets.size());
    EXPECT_EQ(TenantTtlRowsetAction::VERIFIED_NO_CHANGE, first.rowsets[1].action);
    EXPECT_EQ(TenantTtlRowsetAction::DROP, first.rowsets[2].action);
    EXPECT_EQ(TenantTtlRowsetAction::REWRITE, first.rowsets[3].action);
    EXPECT_EQ(keep->rowset_id(), active_rowset(Version(2, 2))->rowset_id());
    EXPECT_NE(drop->rowset_id(), active_rowset(Version(3, 3))->rowset_id());
    EXPECT_NE(rewrite->rowset_id(), active_rowset(Version(4, 4))->rowset_id());

    const auto after_first = snapshot_active_rowsets();
    const auto second = execute(request(TenantFilterMode::DELETE_LIST, {"a"}, 1002));
    EXPECT_EQ(TenantTtlTaskCode::NOOP_VERIFIED, second.code);
    EXPECT_EQ(after_first, snapshot_active_rowsets());

    const auto rows = read_tablet_rows();
    ASSERT_EQ(5, rows.size());
    EXPECT_EQ(std::vector<int64_t>({1, 2, 3, 7, 8}),
              std::vector<int64_t>(
                      {rows[0].event_id, rows[1].event_id, rows[2].event_id, rows[3].event_id, rows[4].event_id}));
    EXPECT_EQ(std::nullopt, rows[2].tenant);
    EXPECT_EQ(std::nullopt, rows[4].tenant);
}

TEST_F(EngineTenantTtlCompactionTaskTest, KeepListCommitsKeepDropRewriteAndThenNoops) {
    ASSERT_NE(nullptr, create_tablet());
    const auto keep = add_rowset(Version(2, 2), {{{1, "b", 10}, {2, std::nullopt, 20}}});
    const auto drop = add_rowset(Version(3, 3), {{{3, "a", 30}, {4, "c", 40}}});
    const auto rewrite = add_rowset(Version(4, 4), {{{5, "a", 50}, {6, "b", 60}, {7, std::nullopt, 70}}});
    ASSERT_NE(nullptr, keep);
    ASSERT_NE(nullptr, drop);
    ASSERT_NE(nullptr, rewrite);

    const auto first = execute(request(TenantFilterMode::KEEP_LIST, {"b"}));
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, first.code);
    ASSERT_EQ(4, first.rowsets.size());
    EXPECT_EQ(TenantTtlRowsetAction::VERIFIED_NO_CHANGE, first.rowsets[1].action);
    EXPECT_EQ(TenantTtlRowsetAction::DROP, first.rowsets[2].action);
    EXPECT_EQ(TenantTtlRowsetAction::REWRITE, first.rowsets[3].action);
    EXPECT_EQ(keep->rowset_id(), active_rowset(Version(2, 2))->rowset_id());
    EXPECT_NE(drop->rowset_id(), active_rowset(Version(3, 3))->rowset_id());
    EXPECT_NE(rewrite->rowset_id(), active_rowset(Version(4, 4))->rowset_id());

    const auto after_first = snapshot_active_rowsets();
    const auto second = execute(request(TenantFilterMode::KEEP_LIST, {"b"}, 1002));
    EXPECT_EQ(TenantTtlTaskCode::NOOP_VERIFIED, second.code);
    EXPECT_EQ(after_first, snapshot_active_rowsets());

    const auto rows = read_tablet_rows();
    ASSERT_EQ(4, rows.size());
    EXPECT_EQ(std::vector<int64_t>({1, 2, 6, 7}),
              std::vector<int64_t>({rows[0].event_id, rows[1].event_id, rows[2].event_id, rows[3].event_id}));
    EXPECT_EQ(std::nullopt, rows[1].tenant);
    EXPECT_EQ(std::nullopt, rows[3].tenant);
}

TEST_F(EngineTenantTtlCompactionTaskTest, CancellationLeavesAllRowsetsAndTabletStateUntouched) {
    ASSERT_NE(nullptr, create_tablet(false));
    const auto source = add_rowset(Version(2, 2), {{{1, "a", 10}, {2, "b", 20}}});
    ASSERT_NE(nullptr, source);
    const auto sources = active_rowsets();
    const auto before = snapshot_active_rowsets();
    std::atomic<bool> cancelled{true};

    const auto result = execute(request(TenantFilterMode::DELETE_LIST, {"a"}), &cancelled);
    EXPECT_EQ(TenantTtlTaskCode::CANCELLED, result.code);
    EXPECT_EQ(before, snapshot_active_rowsets());
    expect_all_guards_released(sources);
}

TEST_F(EngineTenantTtlCompactionTaskTest, EmptyDeleteListNoopsAfterCoverageValidation) {
    ASSERT_NE(nullptr, create_tablet());
    ASSERT_NE(nullptr, add_rowset(Version(2, 2), {{{1, "a", 10}, {2, std::nullopt, 20}}}));
    const auto before = snapshot_active_rowsets();

    const auto result = execute(request(TenantFilterMode::DELETE_LIST, {}));
    EXPECT_EQ(TenantTtlTaskCode::NOOP_VERIFIED, result.code);
    EXPECT_EQ(2, result.snapshot_end_version);
    EXPECT_EQ(2, result.processed_through_version);
    EXPECT_EQ(0, result.scanned_rows);
    EXPECT_EQ(0, result.deleted_rows);
    EXPECT_EQ(before, snapshot_active_rowsets());
}

TEST_F(EngineTenantTtlCompactionTaskTest, ReaderReferencesProtectSegmentsFromCloseDuringRewrite) {
    ASSERT_NE(nullptr, create_tablet());
    auto source = add_rowset(Version(2, 2), {{{1, "delete", 10}, {2, "keep", 20}, {3, "keep", 30}}});
    ASSERT_NE(nullptr, source);
    ASSERT_EQ(1, source->num_segments());
    ASSERT_OK(source->load());
    ASSERT_EQ(1, source->segments().size());
    const auto sources = active_rowsets();

    int close_calls = 0;
    SyncPoint::GetInstance()->SetCallBack("EngineTenantTtlCompactionTask::coverage_captured", [&](void*) {
        EXPECT_EQ(1, source->refs_by_reader());
        const size_t segment_count = source->segments().size();
        source->close();
        ++close_calls;
        EXPECT_EQ(segment_count, source->segments().size());
    });
    SyncPoint::GetInstance()->EnableProcessing();

    const auto result = execute(request(TenantFilterMode::DELETE_LIST, {"delete"}));
    EXPECT_EQ(TenantTtlTaskCode::SUCCESS, result.code);
    EXPECT_EQ(1, close_calls);
    const auto rows = read_tablet_rows();
    ASSERT_EQ(2, rows.size());
    EXPECT_EQ(2, rows[0].event_id);
    EXPECT_EQ(3, rows[1].event_id);
    expect_all_guards_released(sources);
}

TEST_F(EngineTenantTtlCompactionTaskTest, OverlappingSegmentsAreFilteredIndependentlyAndConservativelyInherited) {
    expect_multi_segment_overlap_rewrite(OVERLAPPING);
}

TEST_F(EngineTenantTtlCompactionTaskTest, UnknownOverlapIsFilteredIndependentlyAndConservativelyInherited) {
    expect_multi_segment_overlap_rewrite(OVERLAP_UNKNOWN);
}

TEST_F(EngineTenantTtlCompactionTaskTest, OverlappingOutputWithOneSegmentIsNormalizedAndNoopKeepsSource) {
    ASSERT_NE(nullptr, create_tablet());
    auto source = add_rowset(Version(2, 2),
                             {{{1, "delete", 10}, {2, "delete", 20}}, {{3, "keep", 30}, {4, "keep", 40}}},
                             OVERLAPPING);
    ASSERT_NE(nullptr, source);

    const auto noop = execute(request(TenantFilterMode::DELETE_LIST, {"missing"}));
    EXPECT_EQ(TenantTtlTaskCode::NOOP_VERIFIED, noop.code);
    EXPECT_EQ(source->rowset_id(), active_rowset(Version(2, 2))->rowset_id());
    EXPECT_EQ(OVERLAPPING, active_rowset(Version(2, 2))->rowset_meta()->segments_overlap());

    const auto rewritten = execute(request(TenantFilterMode::DELETE_LIST, {"delete"}, 1002));
    EXPECT_EQ(TenantTtlTaskCode::SUCCESS, rewritten.code);
    const auto output = active_rowset(Version(2, 2));
    ASSERT_NE(nullptr, output);
    EXPECT_EQ(1, output->num_segments());
    EXPECT_EQ(NONOVERLAPPING, output->rowset_meta()->segments_overlap());
    const auto rows = read_tablet_rows();
    ASSERT_EQ(2, rows.size());
    EXPECT_EQ(3, rows[0].event_id);
    EXPECT_EQ(4, rows[1].event_id);
}

TEST_F(EngineTenantTtlCompactionTaskTest, DeletePredicateRowsetIsPreservedWhileDeleteListFiltersPhysicalRows) {
    ASSERT_NE(nullptr, create_tablet());
    const auto data = add_rowset(Version(2, 2), {{{1, "delete", 10}, {2, "predicate-target", 20}}});
    const auto predicate = add_delete_predicate_rowset(Version(3, 3));
    ASSERT_NE(nullptr, data);
    ASSERT_NE(nullptr, predicate);
    const RowsetId predicate_id = predicate->rowset_id();
    const std::string predicate_bytes = predicate->rowset_meta()->delete_predicate().SerializeAsString();

    const auto first = execute(request(TenantFilterMode::DELETE_LIST, {"delete"}));
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, first.code);
    ASSERT_EQ(3, first.rowsets.size());
    EXPECT_EQ(TenantTtlRowsetAction::VERIFIED_NO_CHANGE, first.rowsets.back().action);
    EXPECT_FALSE(first.rowsets.back().output_rowset_id.has_value());

    const auto current_predicate = active_rowset(Version(3, 3));
    ASSERT_NE(nullptr, current_predicate);
    EXPECT_EQ(predicate_id, current_predicate->rowset_id());
    EXPECT_TRUE(current_predicate->rowset_meta()->has_delete_predicate());
    EXPECT_EQ(predicate_bytes, current_predicate->rowset_meta()->delete_predicate().SerializeAsString());
    const auto rows = read_tablet_rows();
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ(2, rows[0].event_id);
    ASSERT_TRUE(rows[0].tenant.has_value());
    EXPECT_EQ("predicate-target", rows[0].tenant.value());

    const auto after_first = snapshot_active_rowsets();
    const auto second = execute(request(TenantFilterMode::DELETE_LIST, {"delete"}, 1002));
    EXPECT_EQ(TenantTtlTaskCode::NOOP_VERIFIED, second.code);
    EXPECT_EQ(after_first, snapshot_active_rowsets());
    EXPECT_EQ(predicate_id, active_rowset(Version(3, 3))->rowset_id());
}

TEST_F(EngineTenantTtlCompactionTaskTest, DeletePredicateRowsetIsPreservedWhileKeepListFiltersPhysicalRows) {
    ASSERT_NE(nullptr, create_tablet());
    const auto data = add_rowset(Version(2, 2), {{{1, "delete", 10}, {2, "predicate-target", 20}}});
    const auto predicate = add_delete_predicate_rowset(Version(3, 3));
    ASSERT_NE(nullptr, data);
    ASSERT_NE(nullptr, predicate);
    const RowsetId predicate_id = predicate->rowset_id();

    const auto result = execute(request(TenantFilterMode::KEEP_LIST, {"predicate-target"}));
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, result.code);
    ASSERT_EQ(3, result.rowsets.size());
    EXPECT_EQ(TenantTtlRowsetAction::VERIFIED_NO_CHANGE, result.rowsets.back().action);
    EXPECT_EQ(predicate_id, active_rowset(Version(3, 3))->rowset_id());
    const auto rows = read_tablet_rows();
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ(2, rows[0].event_id);
    ASSERT_TRUE(rows[0].tenant.has_value());
    EXPECT_EQ("predicate-target", rows[0].tenant.value());
}

TEST_F(EngineTenantTtlCompactionTaskTest, DataBearingDeletePredicateRowsetFailsBeforeOutputStaging) {
    ASSERT_NE(nullptr, create_tablet());
    const auto malformed =
            add_delete_predicate_rowset(Version(2, 2), {{{1, "delete", 10}, {2, "keep", 20}}});
    ASSERT_NE(nullptr, malformed);
    const auto sources = active_rowsets();
    const auto before_rowsets = snapshot_active_rowsets();
    const auto before_files = tablet_files();
    int staged_outputs = 0;
    SyncPoint::GetInstance()->SetCallBack("EngineTenantTtlCompactionTask::output_staged",
                                          [&](void*) { ++staged_outputs; });
    SyncPoint::GetInstance()->EnableProcessing();

    const auto result = execute(request(TenantFilterMode::DELETE_LIST, {"delete"}));

    EXPECT_EQ(TenantTtlTaskCode::DATA_INVARIANT_VIOLATION, result.code);
    EXPECT_FALSE(result.retryable);
    EXPECT_EQ(0, staged_outputs);
    EXPECT_EQ(before_rowsets, snapshot_active_rowsets());
    EXPECT_EQ(before_files, tablet_files());
    expect_all_guards_released(sources);
}

TEST_F(EngineTenantTtlCompactionTaskTest, DeletePredicateRowsetParticipatesInCoverageCas) {
    ASSERT_NE(nullptr, create_tablet());
    const auto data = add_rowset(Version(2, 2), {{{1, "delete", 10}, {2, "keep", 20}}});
    const auto predicate = add_delete_predicate_rowset(Version(3, 3));
    const auto competing_predicate = build_delete_predicate_rowset(Version(3, 3));
    ASSERT_NE(nullptr, data);
    ASSERT_NE(nullptr, predicate);
    ASSERT_NE(nullptr, competing_predicate);
    const auto sources = active_rowsets();
    int predicate_replacements = 0;
    SyncPoint::GetInstance()->SetCallBack("EngineTenantTtlCompactionTask::before_commit", [&](void*) {
        std::unique_lock meta_lock(_tablet->get_header_lock());
        _tablet->modify_rowsets_without_lock({competing_predicate}, {predicate}, nullptr);
        ++predicate_replacements;
    });
    SyncPoint::GetInstance()->EnableProcessing();

    const auto result = execute(request(TenantFilterMode::DELETE_LIST, {"delete"}));

    EXPECT_EQ(1, predicate_replacements);
    EXPECT_EQ(TenantTtlTaskCode::STALE_ROWSET, result.code);
    EXPECT_TRUE(result.retryable);
    EXPECT_EQ(data->rowset_id(), active_rowset(Version(2, 2))->rowset_id());
    EXPECT_EQ(competing_predicate->rowset_id(), active_rowset(Version(3, 3))->rowset_id());
    expect_all_guards_released(sources);
}

TEST_F(EngineTenantTtlCompactionTaskTest, LaterOutputFailureCleansStagingAndDoesNotPartiallyCommit) {
    ASSERT_NE(nullptr, create_tablet());
    const auto source2 = add_rowset(Version(2, 2), {{{1, "a", 10}, {2, "b", 20}}});
    const auto source3 = add_rowset(Version(3, 3), {{{3, "a", 30}, {4, "c", 40}}});
    const auto source4 = add_rowset(Version(4, 4), {{{5, "a", 50}, {6, "d", 60}}});
    ASSERT_NE(nullptr, source2);
    ASSERT_NE(nullptr, source3);
    ASSERT_NE(nullptr, source4);
    const auto sources = active_rowsets();
    const auto before_rowsets = snapshot_active_rowsets();
    const auto before_files = tablet_files();

    int output_builds = 0;
    SyncPoint::GetInstance()->SetCallBack("EngineTenantTtlCompactionTask::before_output_build", [&](void* arg) {
        if (++output_builds == 2) {
            *static_cast<Status*>(arg) = Status::IOError("injected second Tenant-TTL output failure");
        }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    const auto result = execute(request(TenantFilterMode::DELETE_LIST, {"a"}));
    SyncPoint::GetInstance()->ClearCallBack("EngineTenantTtlCompactionTask::before_output_build");
    SyncPoint::GetInstance()->DisableProcessing();

    EXPECT_EQ(2, output_builds);
    EXPECT_EQ(TenantTtlTaskCode::INTERNAL_ERROR, result.code);
    EXPECT_EQ(before_rowsets, snapshot_active_rowsets());
    EXPECT_EQ(before_files, tablet_files());
    expect_all_guards_released(sources);
}

TEST_F(EngineTenantTtlCompactionTaskTest, CancellationAfterFirstStagedOutputCleansEverything) {
    ASSERT_NE(nullptr, create_tablet());
    const auto source2 = add_rowset(Version(2, 2), {{{1, "a", 10}, {2, "b", 20}}});
    const auto source3 = add_rowset(Version(3, 3), {{{3, "a", 30}, {4, "c", 40}}});
    ASSERT_NE(nullptr, source2);
    ASSERT_NE(nullptr, source3);
    const auto sources = active_rowsets();
    const auto before_rowsets = snapshot_active_rowsets();
    const auto before_files = tablet_files();
    std::atomic<bool> cancelled{false};
    int staged_outputs = 0;

    SyncPoint::GetInstance()->SetCallBack("EngineTenantTtlCompactionTask::output_staged", [&](void*) {
        ++staged_outputs;
        cancelled.store(true, std::memory_order_release);
    });
    SyncPoint::GetInstance()->EnableProcessing();
    const auto result = execute(request(TenantFilterMode::DELETE_LIST, {"a"}), &cancelled);

    EXPECT_EQ(1, staged_outputs);
    EXPECT_EQ(TenantTtlTaskCode::CANCELLED, result.code);
    EXPECT_EQ(before_rowsets, snapshot_active_rowsets());
    EXPECT_EQ(before_files, tablet_files());
    expect_all_guards_released(sources);
}

TEST_F(EngineTenantTtlCompactionTaskTest, RowsetIdChangeBeforeCommitRejectsAndCleansAllStagedOutputs) {
    ASSERT_NE(nullptr, create_tablet());
    const auto source2 = add_rowset(Version(2, 2), {{{1, "a", 10}, {2, "b", 20}}});
    const auto source3 = add_rowset(Version(3, 3), {{{3, "a", 30}, {4, "c", 40}}});
    ASSERT_NE(nullptr, source2);
    ASSERT_NE(nullptr, source3);
    const auto sources = active_rowsets();
    const auto competing = build_rowset(Version(0, 3), {{{10, "other", 100}}});
    ASSERT_NE(nullptr, competing);
    const auto before_files = tablet_files();
    int rowset_changes = 0;

    SyncPoint::GetInstance()->SetCallBack("EngineTenantTtlCompactionTask::before_commit", [&](void*) {
        ++rowset_changes;
        _tablet->overwrite_rowset(competing, 3);
    });
    SyncPoint::GetInstance()->EnableProcessing();
    const auto result = execute(request(TenantFilterMode::DELETE_LIST, {"a"}));

    EXPECT_EQ(1, rowset_changes);
    EXPECT_EQ(TenantTtlTaskCode::STALE_ROWSET, result.code);
    EXPECT_TRUE(result.retryable);
    EXPECT_EQ(before_files, tablet_files());
    const auto active = snapshot_active_rowsets();
    ASSERT_EQ(1, active.size());
    EXPECT_EQ(Version(0, 3), active[0].version);
    EXPECT_EQ(competing->rowset_id(), active[0].rowset_id);
    expect_all_guards_released(sources);
}

} // namespace starrocks

int main(int argc, char** argv) {
    return starrocks::init_test_env(argc, argv);
}
