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

#include "storage/tenant_ttl_compaction_types.h"

#include <gtest/gtest.h>

#include "testutil/assert.h"

namespace starrocks {
namespace {

TenantTtlCompactionRequest valid_request() {
    TenantTtlCompactionRequest request;
    request.task_id = 11;
    request.tablet_id = 22;
    request.partition_id = 33;
    request.tenant_column_unique_id = 7;
    request.filter.mode = TenantFilterMode::DELETE_LIST;
    request.filter.tenants = {"tenant-b", "tenant-a"};
    request.policy_watermark = {.dictionary_id = 44, .dictionary_txn_id = 55, .evaluation_time_epoch_seconds = 1};
    request.expected_schema = {.schema_id = 66, .schema_version = 0};
    return request;
}

TEST(TenantTtlCompactionTypesTest, NormalizeUsesExactByteIdentity) {
    auto request = valid_request();
    request.filter.tenants = {"2", " 2", "A", "a", "2", "", "\xC3\xA9", "e\xCC\x81"};

    ASSERT_OK(normalize_and_validate_tenant_ttl_request(&request));
    EXPECT_EQ((std::vector<std::string>{"", " 2", "2", "A", "a", "e\xCC\x81", "\xC3\xA9"}), request.filter.tenants);
}

TEST(TenantTtlCompactionTypesTest, EmptyListSemantics) {
    auto delete_request = valid_request();
    delete_request.filter.tenants.clear();
    ASSERT_OK(normalize_and_validate_tenant_ttl_request(&delete_request));

    auto keep_request = valid_request();
    keep_request.filter.mode = TenantFilterMode::KEEP_LIST;
    keep_request.filter.tenants.clear();
    EXPECT_TRUE(normalize_and_validate_tenant_ttl_request(&keep_request).is_invalid_argument());
}

TEST(TenantTtlCompactionTypesTest, ValidatesIdentityAndWatermarkBoundaries) {
    EXPECT_TRUE(normalize_and_validate_tenant_ttl_request(nullptr).is_invalid_argument());

    auto request = valid_request();
    request.protocol_version = kTenantTtlProtocolVersion + 1;
    EXPECT_TRUE(normalize_and_validate_tenant_ttl_request(&request).is_invalid_argument());

    request = valid_request();
    request.task_id = 0;
    EXPECT_TRUE(normalize_and_validate_tenant_ttl_request(&request).is_invalid_argument());

    request = valid_request();
    request.tablet_id = 0;
    EXPECT_TRUE(normalize_and_validate_tenant_ttl_request(&request).is_invalid_argument());

    request = valid_request();
    request.partition_id = 0;
    EXPECT_TRUE(normalize_and_validate_tenant_ttl_request(&request).is_invalid_argument());

    request = valid_request();
    request.tenant_column_unique_id = -1;
    EXPECT_TRUE(normalize_and_validate_tenant_ttl_request(&request).is_invalid_argument());

    request = valid_request();
    request.filter.mode = static_cast<TenantFilterMode>(255);
    EXPECT_TRUE(normalize_and_validate_tenant_ttl_request(&request).is_invalid_argument());

    request = valid_request();
    request.policy_watermark.dictionary_id = 0;
    EXPECT_TRUE(normalize_and_validate_tenant_ttl_request(&request).is_invalid_argument());

    request = valid_request();
    request.policy_watermark.dictionary_txn_id = 0;
    EXPECT_TRUE(normalize_and_validate_tenant_ttl_request(&request).is_invalid_argument());

    request = valid_request();
    request.policy_watermark.evaluation_time_epoch_seconds = 0;
    EXPECT_TRUE(normalize_and_validate_tenant_ttl_request(&request).is_invalid_argument());

    request = valid_request();
    request.expected_schema.schema_id = 0;
    EXPECT_TRUE(normalize_and_validate_tenant_ttl_request(&request).is_invalid_argument());

    request = valid_request();
    request.expected_schema.schema_version = -1;
    EXPECT_TRUE(normalize_and_validate_tenant_ttl_request(&request).is_invalid_argument());

    request = valid_request();
    request.fe_observed_max_version = -1;
    EXPECT_TRUE(normalize_and_validate_tenant_ttl_request(&request).is_invalid_argument());
}

TEST(TenantTtlCompactionTypesTest, StableEnumStringsAndRetryability) {
    EXPECT_EQ("DELETE_LIST", tenant_filter_mode_to_string(TenantFilterMode::DELETE_LIST));
    EXPECT_EQ("KEEP_LIST", tenant_filter_mode_to_string(TenantFilterMode::KEEP_LIST));
    EXPECT_EQ("NOOP_VERIFIED", tenant_ttl_task_code_to_string(TenantTtlTaskCode::NOOP_VERIFIED));
    EXPECT_EQ("REWRITE", tenant_ttl_rowset_action_to_string(TenantTtlRowsetAction::REWRITE));
    EXPECT_EQ("DROP", segment_filter_action_to_string(SegmentFilterAction::DROP));
    EXPECT_TRUE(tenant_ttl_task_code_is_retryable(TenantTtlTaskCode::TABLET_BUSY));
    EXPECT_TRUE(tenant_ttl_task_code_is_retryable(TenantTtlTaskCode::STALE_ROWSET));
    EXPECT_FALSE(tenant_ttl_task_code_is_retryable(TenantTtlTaskCode::INVALID_ARGUMENT));
    EXPECT_FALSE(tenant_ttl_task_code_is_retryable(TenantTtlTaskCode::SUCCESS));
}

TEST(TenantTtlCompactionTypesTest, ContractDoesNotNeedSchemaHashOrDigest) {
    // Construction of a complete request is the compile-time contract check: the
    // request intentionally contains neither schema_hash nor request/predicate digest.
    auto request = valid_request();
    ASSERT_OK(normalize_and_validate_tenant_ttl_request(&request));
    EXPECT_EQ(66, request.expected_schema.schema_id);
    EXPECT_EQ(0, request.expected_schema.schema_version);
}

} // namespace
} // namespace starrocks
