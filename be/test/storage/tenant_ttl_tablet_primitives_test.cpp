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

#include "storage/compaction_context.h"
#include "storage/compaction_policy.h"
#include "storage/compaction_task.h"
#include "storage/filtered_rowset_writer.h"
#include "storage/tablet.h"
#include "storage/tenant_ttl_compaction_test_util.h"
#include "storage/tenant_ttl_tablet_guard.h"
#include "testutil/init_test_env.h"
#include "testutil/sync_point.h"

namespace starrocks {
namespace {

class NoopCompactionTask final : public CompactionTask {
public:
    NoopCompactionTask() : CompactionTask(HORIZONTAL_COMPACTION) {}

protected:
    Status run_impl() override { return Status::OK(); }
};

class CountingCompactionPolicy final : public CompactionPolicy {
public:
    bool need_compaction(double* score, CompactionType* type) override {
        ++need_calls;
        *score = 1;
        *type = CUMULATIVE_COMPACTION;
        return true;
    }

    std::shared_ptr<CompactionTask> create_compaction(TabletSharedPtr tablet) override {
        ++create_calls;
        auto task = std::make_shared<NoopCompactionTask>();
        task->set_tablet(tablet);
        task->set_compaction_type(CUMULATIVE_COMPACTION);
        return task;
    }

    int need_calls{0};
    int create_calls{0};
};

} // namespace

class TenantTtlTabletPrimitivesTest : public TenantTtlCompactionTestBase {
protected:
    void TearDown() override {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
        TenantTtlCompactionTestBase::TearDown();
    }

    TenantTtlCompactionRequest request(int64_t task_id = 101) const {
        return {.task_id = task_id,
                .tablet_id = _tablet_id,
                .partition_id = _partition_id,
                .tenant_column_unique_id = kTenantColumnUniqueId,
                .filter = {.mode = TenantFilterMode::DELETE_LIST, .tenants = {"delete"}},
                .policy_watermark = {.dictionary_id = 201,
                                     .dictionary_txn_id = 202,
                                     .evaluation_time_epoch_seconds = 203},
                .expected_schema = {.schema_id = kSchemaId, .schema_version = kSchemaVersion}};
    }

    RowsetSharedPtr build_linked_replacement(const RowsetSharedPtr& source,
                                             std::unique_ptr<FilteredRowsetWriter>* writer_owner = nullptr) {
        RowsetWriterContext context;
        context.rowset_id = _engine->next_rowset_id();
        context.tablet_uid = _tablet->tablet_uid();
        context.tablet_id = _tablet->tablet_id();
        context.tablet_schema_hash = _tablet->schema_hash();
        context.partition_id = _tablet->partition_id();
        context.rowset_path_prefix = _tablet->schema_hash_path();
        context.rowset_state = VISIBLE;
        context.tablet_schema = _tablet->tablet_schema();
        context.version = source->version();
        context.segments_overlap = NONOVERLAPPING;
        context.is_compaction = true;

        auto writer = std::make_unique<FilteredRowsetWriter>(context);
        EXPECT_OK(writer->init());
        for (uint32_t segment_id = 0; segment_id < source->num_segments(); ++segment_id) {
            const uint32_t rows = source->segments()[segment_id]->num_rows();
            EXPECT_OK(writer->add_segment(source, {.src_segment_id = segment_id,
                                                   .action = SegmentFilterAction::KEEP,
                                                   .source_rows = rows,
                                                   .kept_rows = rows,
                                                   .deleted_rows = 0}));
        }
        auto output_or = writer->build();
        EXPECT_OK(output_or.status());
        if (!output_or.ok()) {
            return nullptr;
        }
        if (writer_owner != nullptr) {
            *writer_owner = std::move(writer);
        }
        return output_or.value();
    }
};

TEST_F(TenantTtlTabletPrimitivesTest, AdmissionUsesGenerationAsFencingToken) {
    ASSERT_NE(nullptr, create_tablet());
    const auto req = request();

    const auto first = _tablet->try_begin_tenant_ttl(req.task_id, req.policy_watermark);
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, first.code);
    ASSERT_GT(first.generation, 0);
    EXPECT_EQ(TenantTtlState::PENDING, _tablet->tenant_ttl_state_for_debug().state);
    EXPECT_EQ(TenantTtlTaskCode::TTL_ALREADY_RUNNING,
              _tablet->try_begin_tenant_ttl(req.task_id, req.policy_watermark).code);
    EXPECT_EQ(TenantTtlTaskCode::TABLET_BUSY,
              _tablet->try_begin_tenant_ttl(req.task_id + 1, req.policy_watermark).code);
    EXPECT_FALSE(_tablet->mark_tenant_ttl_running(first.generation + 1));
    ASSERT_TRUE(_tablet->mark_tenant_ttl_running(first.generation));
    EXPECT_TRUE(_tablet->tenant_ttl_owner_matches(req.task_id, first.generation, req.policy_watermark));

    _tablet->finish_tenant_ttl(first.generation + 1);
    EXPECT_EQ(TenantTtlState::RUNNING, _tablet->tenant_ttl_state_for_debug().state);
    _tablet->finish_tenant_ttl(first.generation);
    EXPECT_EQ(TenantTtlState::IDLE, _tablet->tenant_ttl_state_for_debug().state);

    const auto second = _tablet->try_begin_tenant_ttl(req.task_id, req.policy_watermark);
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, second.code);
    EXPECT_GT(second.generation, first.generation);
    _tablet->finish_tenant_ttl(first.generation);
    EXPECT_EQ(TenantTtlState::PENDING, _tablet->tenant_ttl_state_for_debug().state);
    _tablet->finish_tenant_ttl(second.generation);
}

TEST_F(TenantTtlTabletPrimitivesTest, GuardReleasesAdmissionAndEarlierLockOnTryLockFailure) {
    ASSERT_NE(nullptr, create_tablet());
    const auto req = request();
    Status detail;

    {
        std::unique_lock base_lock(_tablet->get_base_lock());
        TenantTtlTabletGuard guard(_tablet);
        EXPECT_EQ(TenantTtlTaskCode::TABLET_BUSY, guard.try_acquire(req, &detail));
        EXPECT_EQ(TenantTtlState::IDLE, _tablet->tenant_ttl_state_for_debug().state);
    }
    {
        std::unique_lock cumulative_lock(_tablet->get_cumulative_lock());
        TenantTtlTabletGuard guard(_tablet);
        EXPECT_EQ(TenantTtlTaskCode::TABLET_BUSY, guard.try_acquire(req, &detail));
        EXPECT_EQ(TenantTtlState::IDLE, _tablet->tenant_ttl_state_for_debug().state);
        EXPECT_TRUE(_tablet->get_base_lock().try_lock());
        _tablet->get_base_lock().unlock();
    }
    {
        TenantTtlTabletGuard guard(_tablet);
        ASSERT_EQ(TenantTtlTaskCode::SUCCESS, guard.try_acquire(req, &detail));
        EXPECT_EQ(TenantTtlState::RUNNING, _tablet->tenant_ttl_state_for_debug().state);
    }
    EXPECT_EQ(TenantTtlState::IDLE, _tablet->tenant_ttl_state_for_debug().state);
}

TEST_F(TenantTtlTabletPrimitivesTest, CapturesCompleteMultiRowsetCoverageAndClearsMarks) {
    ASSERT_NE(nullptr, create_tablet());
    ASSERT_NE(nullptr, add_rowset(Version(2, 2), {{{1, "a", 10}}}));
    ASSERT_NE(nullptr, add_rowset(Version(3, 3), {{{2, "b", 20}}}));

    Status detail;
    TenantTtlTabletGuard tablet_guard(_tablet);
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, tablet_guard.try_acquire(request(), &detail));
    {
        TenantTtlCoverageGuard coverage_guard(_tablet);
        ASSERT_EQ(TenantTtlTaskCode::SUCCESS, coverage_guard.capture(request(), &detail));
        const auto& coverage = coverage_guard.coverage();
        ASSERT_EQ(3, coverage.entries.size());
        EXPECT_EQ(Version(0, 1), coverage.entries[0].version);
        EXPECT_EQ(Version(2, 2), coverage.entries[1].version);
        EXPECT_EQ(Version(3, 3), coverage.entries[2].version);
        EXPECT_EQ(3, coverage.snapshot_end_version);
        EXPECT_FALSE(coverage.coverage_digest.empty());
        for (const auto& entry : coverage.entries) {
            EXPECT_TRUE(entry.source->get_is_compacting());
        }

        ASSERT_NE(nullptr, add_rowset(Version(4, 4), {{{3, "c", 30}}}));
        EXPECT_EQ(3, coverage.snapshot_end_version);
        EXPECT_EQ(TenantTtlTaskCode::SUCCESS, _tablet->validate_tenant_ttl_coverage(coverage, &detail));
    }

    std::shared_lock meta_lock(_tablet->get_header_lock());
    std::vector<RowsetSharedPtr> current;
    ASSERT_OK(_tablet->capture_consistent_rowsets(Version(0, 4), &current));
    for (const auto& rowset : current) {
        EXPECT_FALSE(rowset->get_is_compacting());
    }
}

TEST_F(TenantTtlTabletPrimitivesTest, RejectsReplicaBelowFeWatermarkAndActiveVersionHole) {
    ASSERT_NE(nullptr, create_tablet());
    Status detail;
    TenantTtlTabletGuard tablet_guard(_tablet);
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, tablet_guard.try_acquire(request(), &detail));

    auto behind_request = request();
    behind_request.fe_observed_max_version = 2;
    TenantTtlCoverage coverage;
    EXPECT_EQ(TenantTtlTaskCode::REPLICA_NOT_CAUGHT_UP,
              _tablet->capture_tenant_ttl_coverage(behind_request, &coverage, &detail));
    EXPECT_TRUE(coverage.entries.empty());

    ASSERT_NE(nullptr, add_rowset(Version(3, 3), {{{3, "c", 30}}}));
    EXPECT_EQ(TenantTtlTaskCode::DATA_INVARIANT_VIOLATION,
              _tablet->capture_tenant_ttl_coverage(request(), &coverage, &detail));
}

TEST_F(TenantTtlTabletPrimitivesTest, CommitsMultipleSameVersionReplacementsAfterFullCas) {
    ASSERT_NE(nullptr, create_tablet());
    auto source2 = add_rowset(Version(2, 2), {{{1, "a", 10}}});
    auto source3 = add_rowset(Version(3, 3), {{{2, "b", 20}}});
    ASSERT_NE(nullptr, source2);
    ASSERT_NE(nullptr, source3);

    Status detail;
    TenantTtlTabletGuard tablet_guard(_tablet);
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, tablet_guard.try_acquire(request(), &detail));
    TenantTtlCoverageGuard coverage_guard(_tablet);
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, coverage_guard.capture(request(), &detail));
    auto output2 = build_linked_replacement(source2);
    auto output3 = build_linked_replacement(source3);
    ASSERT_NE(nullptr, output2);
    ASSERT_NE(nullptr, output3);

    int save_meta_calls = 0;
    SyncPoint::GetInstance()->SetCallBack("Tablet::commit_tenant_ttl_rowsets:before_save_meta",
                                          [&](void*) { ++save_meta_calls; });
    SyncPoint::GetInstance()->EnableProcessing();
    std::vector<RowsetSharedPtr> replaced_stale;
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS,
              _tablet->commit_tenant_ttl_rowsets(coverage_guard.coverage(),
                                                 {{.source_version = source2->version(),
                                                   .expected_source_rowset_id = source2->rowset_id(),
                                                   .output = output2},
                                                  {.source_version = source3->version(),
                                                   .expected_source_rowset_id = source3->rowset_id(),
                                                   .output = output3}},
                                                 &replaced_stale, &detail));
    EXPECT_EQ(1, save_meta_calls);
    EXPECT_TRUE(replaced_stale.empty());

    std::shared_lock meta_lock(_tablet->get_header_lock());
    EXPECT_EQ(output2->rowset_id(), _tablet->get_rowset_by_version(Version(2, 2))->rowset_id());
    EXPECT_EQ(output3->rowset_id(), _tablet->get_rowset_by_version(Version(3, 3))->rowset_id());
    EXPECT_EQ(coverage_guard.coverage().entries[0].expected_rowset_id,
              _tablet->get_rowset_by_version(Version(0, 1))->rowset_id());
}

TEST_F(TenantTtlTabletPrimitivesTest, FullCasRejectsRowsetIdAndSchemaIdentityChanges) {
    ASSERT_NE(nullptr, create_tablet());
    ASSERT_NE(nullptr, add_rowset(Version(2, 2), {{{1, "a", 10}}}));
    Status detail;
    TenantTtlTabletGuard tablet_guard(_tablet);
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, tablet_guard.try_acquire(request(), &detail));
    TenantTtlCoverageGuard coverage_guard(_tablet);
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, coverage_guard.capture(request(), &detail));

    auto wrong_rowset = coverage_guard.coverage();
    wrong_rowset.entries.back().expected_rowset_id = RowsetId();
    EXPECT_EQ(TenantTtlTaskCode::INVALID_ARGUMENT, _tablet->validate_tenant_ttl_coverage(wrong_rowset, &detail));

    auto wrong_schema = coverage_guard.coverage();
    wrong_schema.schema_identity.captured_schema = TabletSchema::copy(*wrong_schema.schema_identity.captured_schema);
    EXPECT_EQ(TenantTtlTaskCode::SCHEMA_CHANGED, _tablet->validate_tenant_ttl_coverage(wrong_schema, &detail));
}

TEST_F(TenantTtlTabletPrimitivesTest, RejectsMalformedCoverageAndEmptyCommitWithoutMutation) {
    ASSERT_NE(nullptr, create_tablet());
    const auto source = add_rowset(Version(2, 2), {{{1, "a", 10}}});
    ASSERT_NE(nullptr, source);

    Status detail;
    TenantTtlTabletGuard tablet_guard(_tablet);
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, tablet_guard.try_acquire(request(), &detail));
    TenantTtlCoverageGuard coverage_guard(_tablet);
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, coverage_guard.capture(request(), &detail));
    const auto before = snapshot_active_rowsets();

    auto missing_tail = coverage_guard.coverage();
    missing_tail.entries.pop_back();
    EXPECT_EQ(TenantTtlTaskCode::INVALID_ARGUMENT, _tablet->validate_tenant_ttl_coverage(missing_tail, &detail));

    auto version_hole = coverage_guard.coverage();
    version_hole.entries.back().version = Version(3, 3);
    EXPECT_EQ(TenantTtlTaskCode::INVALID_ARGUMENT, _tablet->validate_tenant_ttl_coverage(version_hole, &detail));

    std::vector<RowsetSharedPtr> replaced_stale;
    EXPECT_EQ(TenantTtlTaskCode::INVALID_ARGUMENT,
              _tablet->commit_tenant_ttl_rowsets(coverage_guard.coverage(), {}, &replaced_stale, &detail));
    EXPECT_EQ(before, snapshot_active_rowsets());

    std::unique_ptr<FilteredRowsetWriter> writer;
    auto wrong_output = build_linked_replacement(source, &writer);
    ASSERT_NE(nullptr, wrong_output);
    ASSERT_NE(nullptr, writer);
    wrong_output->rowset_meta()->set_gtid(source->rowset_meta()->gtid() + 1);
    EXPECT_EQ(TenantTtlTaskCode::INVALID_ARGUMENT,
              _tablet->commit_tenant_ttl_rowsets(coverage_guard.coverage(),
                                                 {{.source_version = source->version(),
                                                   .expected_source_rowset_id = source->rowset_id(),
                                                   .output = wrong_output}},
                                                 &replaced_stale, &detail));
    EXPECT_EQ(before, snapshot_active_rowsets());
    wrong_output.reset();
    EXPECT_OK(writer->cleanup());
}

TEST_F(TenantTtlTabletPrimitivesTest, AdmissionAndOrdinaryCompactionCreationBlockEachOther) {
    ASSERT_NE(nullptr, create_tablet());
    auto context = std::make_unique<CompactionContext>();
    auto policy = std::make_unique<CountingCompactionPolicy>();
    auto* policy_ptr = policy.get();
    context->policy = std::move(policy);
    _tablet->set_compaction_context(context);

    EXPECT_TRUE(_tablet->need_compaction());
    EXPECT_EQ(1, policy_ptr->need_calls);

    const auto admission = _tablet->try_begin_tenant_ttl(request().task_id, request().policy_watermark);
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, admission.code);
    EXPECT_FALSE(_tablet->need_compaction());
    EXPECT_FALSE(_tablet->force_base_compaction());
    EXPECT_EQ(nullptr, _tablet->create_compaction_task());
    EXPECT_EQ(1, policy_ptr->need_calls);
    EXPECT_EQ(0, policy_ptr->create_calls);
    _tablet->finish_tenant_ttl(admission.generation);

    auto ordinary_task = _tablet->create_compaction_task();
    ASSERT_NE(nullptr, ordinary_task);
    EXPECT_EQ(1, policy_ptr->create_calls);
    EXPECT_EQ(TenantTtlTaskCode::TABLET_BUSY,
              _tablet->try_begin_tenant_ttl(request().task_id, request().policy_watermark).code);

    _tablet->reset_compaction_status();
    const auto after_reset = _tablet->try_begin_tenant_ttl(request().task_id, request().policy_watermark);
    ASSERT_EQ(TenantTtlTaskCode::SUCCESS, after_reset.code);
    _tablet->finish_tenant_ttl(after_reset.generation);
}

} // namespace starrocks

int main(int argc, char** argv) {
    return starrocks::init_test_env(argc, argv);
}
