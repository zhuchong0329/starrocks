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

#include "storage/tenant_ttl_row_filter.h"

#include "common/config.h"
#include "runtime/mem_tracker.h"
#include "storage/tenant_ttl_compaction_test_util.h"
#include "testutil/init_test_env.h"
#include "testutil/sync_point.h"
#include "util/defer_op.h"

namespace starrocks {

class TenantTtlRowFilterTest : public TenantTtlCompactionTestBase {
protected:
    void TearDown() override {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
        TenantTtlCompactionTestBase::TearDown();
    }

    SegmentSharedPtr only_segment(const RowsetSharedPtr& rowset) {
        EXPECT_OK(rowset->load());
        EXPECT_EQ(1, rowset->segments().size());
        return rowset->segments().empty() ? nullptr : rowset->segments()[0];
    }

    static void expect_range(const SparseRangePtr& ranges, size_t index, rowid_t begin, rowid_t end) {
        ASSERT_NE(nullptr, ranges);
        ASSERT_LT(index, ranges->size());
        EXPECT_EQ(begin, (*ranges)[index].begin());
        EXPECT_EQ(end, (*ranges)[index].end());
    }
};

TEST_F(TenantTtlRowFilterTest, DeleteListBuildsExactRangesAndKeepsNull) {
    ASSERT_NE(nullptr, create_tablet());
    const std::string embedded_nul("tenant\0x", 8);
    auto rowset = add_rowset(Version(2, 2), {{{1, "tenant-a", 1},
                                              {2, "tenant-b", 2},
                                              {3, std::nullopt, 3},
                                              {4, "tenant-a", 4},
                                              {5, "", 5},
                                              {6, embedded_nul, 6}}});
    ASSERT_NE(nullptr, rowset);

    TenantFilter filter{.mode = TenantFilterMode::DELETE_LIST, .tenants = {embedded_nul, "tenant-a", "tenant-a"}};
    TenantTtlRowFilter row_filter(_tablet->tablet_schema(), kTenantColumnUniqueId, std::move(filter), 2);
    ASSERT_OK(row_filter.validate());
    auto plan_or = row_filter.plan_segment(only_segment(rowset), 0);
    ASSERT_OK(plan_or.status());
    const auto& plan = plan_or.value();
    EXPECT_EQ(SegmentFilterAction::REWRITE, plan.action);
    EXPECT_EQ(6, plan.source_rows);
    EXPECT_EQ(3, plan.kept_rows);
    EXPECT_EQ(3, plan.deleted_rows);
    ASSERT_EQ(2, plan.keep_row_ranges->size());
    expect_range(plan.keep_row_ranges, 0, 1, 3);
    expect_range(plan.keep_row_ranges, 1, 4, 5);
}

TEST_F(TenantTtlRowFilterTest, ClassifiesKeepAndDrop) {
    ASSERT_NE(nullptr, create_tablet(false));
    auto rowset = add_rowset(Version(2, 2), {{{1, "a", 1}, {2, "b", 2}, {3, "a", 3}}});
    ASSERT_NE(nullptr, rowset);
    auto segment = only_segment(rowset);

    TenantTtlRowFilter keep_filter(_tablet->tablet_schema(), kTenantColumnUniqueId,
                                   TenantFilter{.mode = TenantFilterMode::DELETE_LIST, .tenants = {"missing"}});
    auto keep_or = keep_filter.plan_segment(segment, 0);
    ASSERT_OK(keep_or.status());
    EXPECT_EQ(SegmentFilterAction::KEEP, keep_or->action);
    EXPECT_EQ(nullptr, keep_or->keep_row_ranges);
    EXPECT_EQ(0, keep_or->deleted_rows);

    TenantTtlRowFilter drop_filter(_tablet->tablet_schema(), kTenantColumnUniqueId,
                                   TenantFilter{.mode = TenantFilterMode::DELETE_LIST, .tenants = {"a", "b"}});
    auto drop_or = drop_filter.plan_segment(segment, 0);
    ASSERT_OK(drop_or.status());
    EXPECT_EQ(SegmentFilterAction::DROP, drop_or->action);
    EXPECT_EQ(nullptr, drop_or->keep_row_ranges);
    EXPECT_EQ(3, drop_or->deleted_rows);
}

TEST_F(TenantTtlRowFilterTest, KeepListRetainsOnlyListedTenantsAndNull) {
    ASSERT_NE(nullptr, create_tablet());
    auto rowset =
            add_rowset(Version(2, 2), {{{1, "a", 1}, {2, "b", 2}, {3, std::nullopt, 3}, {4, "c", 4}, {5, "b", 5}}});
    ASSERT_NE(nullptr, rowset);

    TenantTtlRowFilter row_filter(_tablet->tablet_schema(), kTenantColumnUniqueId,
                                  TenantFilter{.mode = TenantFilterMode::KEEP_LIST, .tenants = {"b"}}, 1);
    auto plan_or = row_filter.plan_segment(only_segment(rowset), 0);
    ASSERT_OK(plan_or.status());
    const auto& plan = plan_or.value();
    EXPECT_EQ(SegmentFilterAction::REWRITE, plan.action);
    EXPECT_EQ(3, plan.kept_rows);
    EXPECT_EQ(2, plan.deleted_rows);
    ASSERT_EQ(2, plan.keep_row_ranges->size());
    expect_range(plan.keep_row_ranges, 0, 1, 3);
    expect_range(plan.keep_row_ranges, 1, 4, 5);
}

TEST_F(TenantTtlRowFilterTest, RejectsMissingOrNonVarcharTenantColumn) {
    ASSERT_NE(nullptr, create_tablet());
    TenantFilter filter{.mode = TenantFilterMode::DELETE_LIST, .tenants = {"a"}};
    TenantTtlRowFilter missing(_tablet->tablet_schema(), 99999, filter);
    EXPECT_TRUE(missing.validate().is_invalid_argument());
    TenantTtlRowFilter bigint(_tablet->tablet_schema(), 1, filter);
    EXPECT_TRUE(bigint.validate().is_invalid_argument());
}

TEST_F(TenantTtlRowFilterTest, CancellationStopsBeforeScanning) {
    ASSERT_NE(nullptr, create_tablet());
    auto rowset = add_rowset(Version(2, 2), {{{1, "a", 1}}});
    ASSERT_NE(nullptr, rowset);
    std::atomic<bool> cancelled{true};
    TenantTtlRowFilter row_filter(_tablet->tablet_schema(), kTenantColumnUniqueId,
                                  TenantFilter{.mode = TenantFilterMode::DELETE_LIST, .tenants = {"a"}}, 1, nullptr,
                                  &cancelled);
    auto plan_or = row_filter.plan_segment(only_segment(rowset), 0);
    EXPECT_TRUE(plan_or.status().is_cancelled());
}

TEST_F(TenantTtlRowFilterTest, MemoryLimitStopsBeforeScanning) {
    ASSERT_NE(nullptr, create_tablet());
    auto rowset = add_rowset(Version(2, 2), {{{1, "a", 1}}});
    ASSERT_NE(nullptr, rowset);
    MemTracker mem_tracker(0, "tenant-ttl-row-filter-test");
    mem_tracker.set(1);
    TenantTtlRowFilter row_filter(_tablet->tablet_schema(), kTenantColumnUniqueId,
                                  TenantFilter{.mode = TenantFilterMode::DELETE_LIST, .tenants = {"a"}}, 1,
                                  &mem_tracker);
    auto plan_or = row_filter.plan_segment(only_segment(rowset), 0);
    EXPECT_TRUE(plan_or.status().is_mem_limit_exceeded());
    mem_tracker.set(0);
}

TEST_F(TenantTtlRowFilterTest, SegmentZoneMapNoCandidateUsesFilterModePolarity) {
    ASSERT_NE(nullptr, create_tablet(false));
    auto rowset = add_rowset(Version(2, 2), {{{1, "a", 1}, {2, "b", 2}, {3, "c", 3}}});
    ASSERT_NE(nullptr, rowset);
    const auto segment = only_segment(rowset);

    TenantTtlRowFilter delete_filter(_tablet->tablet_schema(), kTenantColumnUniqueId,
                                     TenantFilter{.mode = TenantFilterMode::DELETE_LIST, .tenants = {"missing"}});
    auto delete_plan = delete_filter.plan_segment(segment, 0);
    ASSERT_OK(delete_plan.status());
    EXPECT_EQ(SegmentFilterAction::KEEP, delete_plan->action);
    EXPECT_EQ(0, delete_filter.stats().tenant_rows_read);
    EXPECT_EQ(3, delete_filter.stats().rows_pruned_by_segment_zonemap);

    TenantTtlRowFilter keep_filter(_tablet->tablet_schema(), kTenantColumnUniqueId,
                                   TenantFilter{.mode = TenantFilterMode::KEEP_LIST, .tenants = {"missing"}});
    auto keep_plan = keep_filter.plan_segment(segment, 0);
    ASSERT_OK(keep_plan.status());
    EXPECT_EQ(SegmentFilterAction::DROP, keep_plan->action);
    EXPECT_EQ(0, keep_filter.stats().tenant_rows_read);
    EXPECT_EQ(3, keep_filter.stats().rows_pruned_by_segment_zonemap);
}

TEST_F(TenantTtlRowFilterTest, PageZoneMapPrunesTenantReadsAndPreservesExactRanges) {
    const int32_t old_data_page_size = config::data_page_size;
    config::data_page_size = 128;
    DeferOp restore_page_size([&]() { config::data_page_size = old_data_page_size; });
    ASSERT_NE(nullptr, create_tablet(false));

    constexpr rowid_t kRowsPerTenant = 256;
    std::vector<TenantTtlTestRow> rows;
    rows.reserve(kRowsPerTenant * 3);
    for (rowid_t i = 0; i < kRowsPerTenant * 3; ++i) {
        const std::string tenant = i < kRowsPerTenant ? "aaa"
                                   : i < kRowsPerTenant * 2
                                           ? "target"
                                           : "zzz";
        rows.emplace_back(TenantTtlTestRow{.event_id = i, .tenant = tenant, .payload = static_cast<int32_t>(i)});
    }
    auto rowset = add_rowset(Version(2, 2), {rows});
    ASSERT_NE(nullptr, rowset);

    TenantTtlRowFilter filter(_tablet->tablet_schema(), kTenantColumnUniqueId,
                              TenantFilter{.mode = TenantFilterMode::DELETE_LIST, .tenants = {"target"}}, 64);
    auto plan_or = filter.plan_segment(only_segment(rowset), 0);
    ASSERT_OK(plan_or.status());
    const auto& plan = plan_or.value();
    EXPECT_EQ(SegmentFilterAction::REWRITE, plan.action);
    EXPECT_EQ(kRowsPerTenant * 2, plan.kept_rows);
    EXPECT_EQ(kRowsPerTenant, plan.deleted_rows);
    ASSERT_EQ(2, plan.keep_row_ranges->size());
    expect_range(plan.keep_row_ranges, 0, 0, kRowsPerTenant);
    expect_range(plan.keep_row_ranges, 1, kRowsPerTenant * 2, kRowsPerTenant * 3);
    EXPECT_EQ(0, filter.stats().rows_pruned_by_segment_zonemap);
    EXPECT_GT(filter.stats().rows_pruned_by_page_zonemap, 0);
    EXPECT_LT(filter.stats().tenant_rows_read, kRowsPerTenant * 3);
}

TEST_F(TenantTtlRowFilterTest, DisabledZoneMapsFallBackToExactTenantScan) {
    ASSERT_NE(nullptr, create_tablet(false));
    auto rowset = add_rowset(Version(2, 2), {{{1, "a", 1}, {2, "b", 2}, {3, "c", 3}}});
    ASSERT_NE(nullptr, rowset);
    const bool old_segment_zonemap = config::enable_index_segment_level_zonemap_filter;
    const bool old_page_zonemap = config::enable_index_page_level_zonemap_filter;
    config::enable_index_segment_level_zonemap_filter = false;
    config::enable_index_page_level_zonemap_filter = false;
    DeferOp restore_zonemaps([&]() {
        config::enable_index_segment_level_zonemap_filter = old_segment_zonemap;
        config::enable_index_page_level_zonemap_filter = old_page_zonemap;
    });

    TenantTtlRowFilter filter(_tablet->tablet_schema(), kTenantColumnUniqueId,
                              TenantFilter{.mode = TenantFilterMode::DELETE_LIST, .tenants = {"b"}}, 2);
    auto plan_or = filter.plan_segment(only_segment(rowset), 0);
    ASSERT_OK(plan_or.status());
    EXPECT_EQ(SegmentFilterAction::REWRITE, plan_or->action);
    EXPECT_EQ(2, plan_or->kept_rows);
    EXPECT_EQ(1, plan_or->deleted_rows);
    ASSERT_EQ(2, plan_or->keep_row_ranges->size());
    expect_range(plan_or->keep_row_ranges, 0, 0, 1);
    expect_range(plan_or->keep_row_ranges, 1, 2, 3);
    EXPECT_EQ(0, filter.stats().rows_pruned_by_segment_zonemap);
    EXPECT_EQ(0, filter.stats().rows_pruned_by_page_zonemap);
}

TEST_F(TenantTtlRowFilterTest, RejectsDuplicateUnorderedAndOutOfRangeRowids) {
    ASSERT_NE(nullptr, create_tablet(false));
    auto rowset = add_rowset(Version(2, 2), {{{1, "target", 1}, {2, "target", 2}, {3, "target", 3}}});
    ASSERT_NE(nullptr, rowset);
    const auto segment = only_segment(rowset);
    const std::vector<std::vector<rowid_t>> invalid_rowids{{0, 0, 2}, {0, 2, 1}, {0, 1, 3}};

    for (const auto& injected_rowids : invalid_rowids) {
        SyncPoint::GetInstance()->SetCallBack("TenantTtlRowFilter::plan_segment:matching_rowids", [&](void* arg) {
            *static_cast<std::vector<rowid_t>*>(arg) = injected_rowids;
        });
        SyncPoint::GetInstance()->EnableProcessing();
        TenantTtlRowFilter filter(_tablet->tablet_schema(), kTenantColumnUniqueId,
                                  TenantFilter{.mode = TenantFilterMode::DELETE_LIST, .tenants = {"target"}}, 8);
        auto plan_or = filter.plan_segment(segment, 0);
        EXPECT_TRUE(plan_or.status().is_corruption()) << plan_or.status();
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearCallBack("TenantTtlRowFilter::plan_segment:matching_rowids");
    }
}

} // namespace starrocks

int main(int argc, char** argv) {
    return starrocks::init_test_env(argc, argv);
}
