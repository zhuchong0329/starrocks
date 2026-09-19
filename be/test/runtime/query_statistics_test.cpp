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

#include "runtime/query_statistics.h"

#include <gtest/gtest.h>

#include "gen_cpp/data.pb.h"

namespace starrocks {

class QueryStatisticsTest : public ::testing::Test {};

TEST_F(QueryStatisticsTest, pb) {
    QueryStatistics s;
    s.set_returned_rows(1);
    s.add_scan_stats(2, 3);
    s.add_cpu_costs(4);
    s.add_mem_costs(5);
    s.add_spill_bytes(6);
    s.add_read_stats(7, 8);
    s.add_transmitted_bytes(9);

    PQueryStatistics ps;
    s.to_pb(&ps);
    ASSERT_EQ(ps.returned_rows(), 1);
    ASSERT_EQ(ps.scan_rows(), 2);
    ASSERT_EQ(ps.scan_bytes(), 3);
    ASSERT_EQ(ps.cpu_cost_ns(), 4);
    ASSERT_EQ(ps.mem_cost_bytes(), 5);
    ASSERT_EQ(ps.spill_bytes(), 6);
    ASSERT_EQ(ps.read_local_cnt(), 7);
    ASSERT_EQ(ps.read_remote_cnt(), 8);
    ASSERT_EQ(ps.transmitted_bytes(), 9);

    s.merge_pb(ps);
    ASSERT_EQ(s.get_scan_rows(), 4);
    ASSERT_EQ(s.get_mem_bytes(), 5);
    ASSERT_EQ(s.get_transmitted_bytes(), 18);
    ASSERT_EQ(s.get_cpu_ns(), 8);
    ASSERT_EQ(s.get_read_local_cnt(), 14);
    ASSERT_EQ(s.get_read_remote_cnt(), 16);
}

TEST_F(QueryStatisticsTest, basic) {
    QueryStatistics s;
    s.set_returned_rows(1);
    s.add_scan_stats(2, 3);
    s.add_cpu_costs(4);
    s.add_mem_costs(5);
    s.add_spill_bytes(6);
    s.add_read_stats(7, 8);
    s.add_transmitted_bytes(9);

    QueryStatistics s2;
    s2.merge(0, s);
    ASSERT_EQ(s2.get_scan_rows(), 2);
    ASSERT_EQ(s2.get_mem_bytes(), 5);
    ASSERT_EQ(s2.get_transmitted_bytes(), 9);
    ASSERT_EQ(s2.get_cpu_ns(), 4);
    ASSERT_EQ(s2.get_read_local_cnt(), 7);
    ASSERT_EQ(s2.get_read_remote_cnt(), 8);

    s2.clear();
    ASSERT_EQ(s2.get_scan_rows(), 0);
    ASSERT_EQ(s2.get_mem_bytes(), 0);
    ASSERT_EQ(s2.get_transmitted_bytes(), 0);
    ASSERT_EQ(s2.get_cpu_ns(), 0);
    ASSERT_EQ(s2.get_read_local_cnt(), 0);
    ASSERT_EQ(s2.get_read_remote_cnt(), 0);
}

TEST_F(QueryStatisticsTest, corruption_is_optional_and_sticky_not_a_consumed_delta) {
    QueryStatistics healthy;
    PQueryStatistics empty;
    healthy.to_pb(&empty);
    ASSERT_FALSE(empty.has_query_corruption_detected());

    QueryStatistics damaged;
    damaged.set_query_corruption_detected(true);
    damaged.set_query_corruption_detected(false);
    PQueryStatistics wire;
    damaged.to_pb(&wire);
    ASSERT_TRUE(wire.query_corruption_detected());

    QueryStatistics first;
    QueryStatistics second;
    first.merge(0, damaged);
    second.merge(0, damaged);
    ASSERT_TRUE(first.query_corruption_detected());
    ASSERT_TRUE(second.query_corruption_detected());
    first.merge_pb(empty);
    ASSERT_TRUE(first.query_corruption_detected());

    QueryStatisticsRecvr receiver;
    receiver.insert(wire, 0);
    receiver.insert(empty, 0);
    QueryStatistics aggregate1;
    QueryStatistics aggregate2;
    receiver.aggregate(&aggregate1);
    receiver.aggregate(&aggregate2);
    ASSERT_TRUE(aggregate1.query_corruption_detected());
    ASSERT_TRUE(aggregate2.query_corruption_detected());
    first.clear();
    ASSERT_FALSE(first.query_corruption_detected());
    // Reusing a protobuf destination must not leak a previous query's diagnostic.
    first.to_pb(&wire);
    ASSERT_FALSE(wire.has_query_corruption_detected());
    ASSERT_FALSE(wire.query_corruption_detected());
}

} // namespace starrocks
