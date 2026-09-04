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

#include "storage/tenant_ttl_compaction_test_util.h"
#include "testutil/init_test_env.h"

namespace starrocks {

TEST_F(TenantTtlCompactionTestBase, BuildsStableSingleAndMultipleRowsetLayouts) {
    ASSERT_NE(nullptr, create_tablet());
    auto first = add_rowset(Version(2, 2), {{{1, "tenant-a", 10}, {2, "tenant-b", 20}}});
    ASSERT_NE(nullptr, first);
    auto second = add_rowset(Version(3, 3), {{{3, "tenant-c", 30}}, {{4, std::nullopt, 40}}});
    ASSERT_NE(nullptr, second);

    const auto before = snapshot_active_rowsets();
    const auto after = snapshot_active_rowsets();
    EXPECT_EQ(before, after);
    ASSERT_EQ(3, before.size()); // Includes the initial [0,1] rowset.
    EXPECT_EQ(Version(2, 2), before[1].version);
    EXPECT_EQ(2, before[1].num_rows);
    EXPECT_EQ(1, before[1].num_segments);
    EXPECT_EQ(Version(3, 3), before[2].version);
    EXPECT_EQ(2, before[2].num_rows);
    EXPECT_EQ(2, before[2].num_segments);
}

TEST_F(TenantTtlCompactionTestBase, BuildsEmptyAndNonNullableTenantLayouts) {
    ASSERT_NE(nullptr, create_tablet(false));
    auto empty = add_rowset(Version(2, 2), {});
    ASSERT_NE(nullptr, empty);
    EXPECT_EQ(0, empty->num_rows());
    EXPECT_EQ(0, empty->num_segments());

    auto populated = add_rowset(Version(3, 3), {{{1, "", 1}, {2, "tenant-a", 2}}});
    ASSERT_NE(nullptr, populated);
    EXPECT_EQ(2, populated->num_rows());
}

} // namespace starrocks

int main(int argc, char** argv) {
    return starrocks::init_test_env(argc, argv);
}
