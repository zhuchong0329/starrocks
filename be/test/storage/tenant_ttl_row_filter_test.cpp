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

#include "runtime/mem_tracker.h"
#include "storage/tenant_ttl_compaction_test_util.h"
#include "testutil/init_test_env.h"

namespace starrocks {

class TenantTtlRowFilterTest : public TenantTtlCompactionTestBase {
protected:
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

} // namespace starrocks

int main(int argc, char** argv) {
    return starrocks::init_test_env(argc, argv);
}
