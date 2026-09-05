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

#include "http/action/tenant_ttl_compaction_action.h"

#include <event2/buffer.h>
#include <event2/http.h>
#include <event2/http_struct.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <algorithm>
#include <string>
#include <vector>

#include "fmt/format.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "storage/tenant_ttl_compaction_test_util.h"
#include "testutil/init_test_env.h"

namespace starrocks {

extern void (*s_injected_send_reply)(HttpRequest*, HttpStatus, std::string_view);

namespace {

HttpStatus g_reply_status = HttpStatus::INTERNAL_SERVER_ERROR;
std::string g_reply_body;

void capture_reply(HttpRequest*, HttpStatus status, std::string_view body) {
    g_reply_status = status;
    g_reply_body.assign(body);
}

std::string legal_request_json(std::string_view mode = "DELETE_LIST", std::string_view tenants = R"(["b","a","a"])",
                               int64_t task_id = 101, int64_t tablet_id = 102, int64_t partition_id = 103,
                               int32_t tenant_column_unique_id = 2, int64_t schema_id = 104,
                               int32_t schema_version = 0) {
    return fmt::format(
            R"({{"protocol_version":1,"task_id":{},"tablet_id":{},"partition_id":{},"tenant_column_unique_id":{},"filter":{{"mode":"{}","tenants":{}}},"policy_watermark":{{"dictionary_id":201,"dictionary_txn_id":202,"evaluation_time_epoch_seconds":203}},"expected_schema":{{"schema_id":{},"schema_version":{}}},"fe_observed_max_version":1}})",
            task_id, tablet_id, partition_id, tenant_column_unique_id, mode, tenants, schema_id, schema_version);
}

} // namespace

TEST(TenantTtlCompactionActionContractTest, ParsesAndNormalizesBothModes) {
    TenantTtlCompactionRequest request;
    ASSERT_OK(TenantTtlCompactionAction::parse_request(legal_request_json(), &request));
    EXPECT_EQ(TenantFilterMode::DELETE_LIST, request.filter.mode);
    EXPECT_EQ(std::vector<std::string>({"a", "b"}), request.filter.tenants);
    ASSERT_TRUE(request.fe_observed_max_version.has_value());
    EXPECT_EQ(1, request.fe_observed_max_version.value());

    ASSERT_OK(TenantTtlCompactionAction::parse_request(legal_request_json("KEEP_LIST", R"(["kept"] )"), &request));
    EXPECT_EQ(TenantFilterMode::KEEP_LIST, request.filter.mode);
    EXPECT_EQ(std::vector<std::string>({"kept"}), request.filter.tenants);
}

TEST(TenantTtlCompactionActionContractTest, RejectsMalformedMissingUnknownAndBypassFields) {
    TenantTtlCompactionRequest request;
    EXPECT_TRUE(TenantTtlCompactionAction::parse_request("", &request).is_invalid_argument());
    EXPECT_TRUE(TenantTtlCompactionAction::parse_request("{", &request).is_invalid_argument());
    EXPECT_TRUE(TenantTtlCompactionAction::parse_request(R"({"protocol_version":1})", &request).is_invalid_argument());

    for (const auto& field :
         {"schema_hash", "rowset_ids", "segment_ids", "skip_lock", "skip_rowset_id_check", "force"}) {
        std::string body = legal_request_json();
        body.insert(body.size() - 1, fmt::format(R"(,"{}":1)", field));
        EXPECT_TRUE(TenantTtlCompactionAction::parse_request(body, &request).is_invalid_argument()) << field;
    }
    const std::string duplicate =
            R"({"protocol_version":1,"protocol_version":1,"task_id":1,"tablet_id":1,"partition_id":1,"tenant_column_unique_id":2,"filter":{"mode":"DELETE_LIST","tenants":[]},"policy_watermark":{"dictionary_id":1,"dictionary_txn_id":1,"evaluation_time_epoch_seconds":1},"expected_schema":{"schema_id":1,"schema_version":0}})";
    EXPECT_TRUE(TenantTtlCompactionAction::parse_request(duplicate, &request).is_invalid_argument());

    EXPECT_TRUE(TenantTtlCompactionAction::parse_request(legal_request_json("KEEP_LIST", "[]"), &request)
                        .is_invalid_argument());
    EXPECT_TRUE(TenantTtlCompactionAction::parse_request(legal_request_json("DELETE_AND_KEEP"), &request)
                        .is_invalid_argument());
}

TEST(TenantTtlCompactionActionContractTest, MapsAllStableResultCodes) {
    EXPECT_EQ(HttpStatus::OK, TenantTtlCompactionAction::http_status_for_code(TenantTtlTaskCode::SUCCESS));
    EXPECT_EQ(HttpStatus::OK, TenantTtlCompactionAction::http_status_for_code(TenantTtlTaskCode::NOOP_VERIFIED));
    EXPECT_EQ(HttpStatus::BAD_REQUEST,
              TenantTtlCompactionAction::http_status_for_code(TenantTtlTaskCode::INVALID_ARGUMENT));
    EXPECT_EQ(HttpStatus::BAD_REQUEST,
              TenantTtlCompactionAction::http_status_for_code(TenantTtlTaskCode::DATA_INVARIANT_VIOLATION));
    EXPECT_EQ(HttpStatus::NOT_FOUND,
              TenantTtlCompactionAction::http_status_for_code(TenantTtlTaskCode::TABLET_NOT_FOUND));
    EXPECT_EQ(HttpStatus::NOT_IMPLEMENTED,
              TenantTtlCompactionAction::http_status_for_code(TenantTtlTaskCode::NOT_SUPPORTED));
    EXPECT_EQ(HttpStatus::CONFLICT, TenantTtlCompactionAction::http_status_for_code(TenantTtlTaskCode::SCHEMA_CHANGED));
    EXPECT_EQ(HttpStatus::CONFLICT, TenantTtlCompactionAction::http_status_for_code(TenantTtlTaskCode::STALE_ROWSET));
    EXPECT_EQ(HttpStatus::SERVICE_UNAVAILABLE,
              TenantTtlCompactionAction::http_status_for_code(TenantTtlTaskCode::TABLET_BUSY));
    EXPECT_EQ(HttpStatus::SERVICE_UNAVAILABLE,
              TenantTtlCompactionAction::http_status_for_code(TenantTtlTaskCode::TTL_ALREADY_RUNNING));
    EXPECT_EQ(HttpStatus::SERVICE_UNAVAILABLE,
              TenantTtlCompactionAction::http_status_for_code(TenantTtlTaskCode::REPLICA_NOT_CAUGHT_UP));
    EXPECT_EQ(HttpStatus::SERVICE_UNAVAILABLE,
              TenantTtlCompactionAction::http_status_for_code(TenantTtlTaskCode::CANCELLED));
    EXPECT_EQ(HttpStatus::INTERNAL_SERVER_ERROR,
              TenantTtlCompactionAction::http_status_for_code(TenantTtlTaskCode::INTERNAL_ERROR));
}

TEST(TenantTtlCompactionActionContractTest, SerializesStableResultAndRowsetFields) {
    RowsetId source;
    source.init(11);
    RowsetId output;
    output.init(12);
    TenantTtlCompactionResult result{.code = TenantTtlTaskCode::SUCCESS,
                                     .detail_status = Status::OK(),
                                     .retryable = false,
                                     .task_id = 1,
                                     .tablet_id = 2,
                                     .partition_id = 3,
                                     .snapshot_end_version = 7,
                                     .processed_through_version = 7,
                                     .coverage_digest = "digest",
                                     .rowsets = {{.source_version = Version(4, 4),
                                                  .source_rowset_id = source,
                                                  .output_rowset_id = output,
                                                  .action = TenantTtlRowsetAction::REWRITE,
                                                  .source_rows = 3,
                                                  .kept_rows = 2,
                                                  .deleted_rows = 1,
                                                  .source_segments = 1,
                                                  .rewritten_segments = 1}},
                                     .scanned_rows = 3,
                                     .kept_rows = 2,
                                     .deleted_rows = 1,
                                     .rewritten_bytes = 99};
    rapidjson::Document json;
    json.Parse(TenantTtlCompactionAction::serialize_result(result).c_str());
    ASSERT_FALSE(json.HasParseError());
    EXPECT_STREQ("SUCCESS", json["code"].GetString());
    EXPECT_EQ(7, json["processed_through_version"].GetInt64());
    ASSERT_EQ(1, json["rowsets"].Size());
    EXPECT_STREQ("REWRITE", json["rowsets"][0]["action"].GetString());
    EXPECT_STREQ("11", json["rowsets"][0]["source_rowset_id"].GetString());
    EXPECT_STREQ("12", json["rowsets"][0]["output_rowset_id"].GetString());
}

class TenantTtlCompactionActionExecutionTest : public TenantTtlCompactionTestBase {
protected:
    static void SetUpTestSuite() { s_injected_send_reply = capture_reply; }
    static void TearDownTestSuite() { s_injected_send_reply = nullptr; }

    rapidjson::Document post(std::string body) {
        g_reply_body.clear();
        g_reply_status = HttpStatus::INTERNAL_SERVER_ERROR;
        evhttp_request* ev_request = evhttp_request_new(nullptr, nullptr);
        EXPECT_NE(nullptr, ev_request);
        EXPECT_EQ(0, evbuffer_add(evhttp_request_get_input_buffer(ev_request), body.data(), body.size()));
        HttpRequest request(ev_request);
        request.set_method(HttpMethod::POST);
        TenantTtlCompactionAction action;
        action.handle(&request);
        evhttp_request_free(ev_request);

        rapidjson::Document json;
        json.Parse(g_reply_body.c_str());
        EXPECT_FALSE(json.HasParseError()) << g_reply_body;
        return json;
    }
};

TEST_F(TenantTtlCompactionActionExecutionTest, ExecutesMultiRowsetKeepDropRewriteAndIdempotentNoop) {
    ASSERT_NE(nullptr, create_tablet());
    ASSERT_NE(nullptr, add_rowset(Version(2, 2), {{{1, "b", 10}, {2, "c", 20}, {3, std::nullopt, 30}}}));
    ASSERT_NE(nullptr, add_rowset(Version(3, 3), {{{4, "a", 40}, {5, "a", 50}}}));
    ASSERT_NE(nullptr, add_rowset(Version(4, 4), {{{6, "a", 60}, {7, "b", 70}, {8, std::nullopt, 80}}}));

    auto first = post(legal_request_json("DELETE_LIST", R"(["a"])", 1001, _tablet_id, _partition_id,
                                         kTenantColumnUniqueId, kSchemaId, kSchemaVersion));
    ASSERT_EQ(HttpStatus::OK, g_reply_status);
    ASSERT_TRUE(first.IsObject());
    EXPECT_STREQ("SUCCESS", first["code"].GetString());
    EXPECT_EQ(3, first["deleted_rows"].GetInt64());
    std::vector<std::string> actions;
    for (const auto& rowset : first["rowsets"].GetArray()) {
        actions.emplace_back(rowset["action"].GetString());
    }
    EXPECT_NE(actions.end(), std::find(actions.begin(), actions.end(), "VERIFIED_NO_CHANGE"));
    EXPECT_NE(actions.end(), std::find(actions.begin(), actions.end(), "DROP"));
    EXPECT_NE(actions.end(), std::find(actions.begin(), actions.end(), "REWRITE"));

    const auto after_first = snapshot_active_rowsets();
    auto second = post(legal_request_json("DELETE_LIST", R"(["a"])", 1002, _tablet_id, _partition_id,
                                          kTenantColumnUniqueId, kSchemaId, kSchemaVersion));
    ASSERT_EQ(HttpStatus::OK, g_reply_status);
    EXPECT_STREQ("NOOP_VERIFIED", second["code"].GetString());
    EXPECT_EQ(0, second["deleted_rows"].GetInt64());
    EXPECT_EQ(after_first, snapshot_active_rowsets());
}

TEST(TenantTtlCompactionActionHttpTest, ReturnsStableBadRequestBody) {
    s_injected_send_reply = capture_reply;
    evhttp_request* ev_request = evhttp_request_new(nullptr, nullptr);
    const std::string body = R"({"force":true})";
    ASSERT_EQ(0, evbuffer_add(evhttp_request_get_input_buffer(ev_request), body.data(), body.size()));
    HttpRequest request(ev_request);
    TenantTtlCompactionAction action;
    action.handle(&request);
    evhttp_request_free(ev_request);
    s_injected_send_reply = nullptr;

    EXPECT_EQ(HttpStatus::BAD_REQUEST, g_reply_status);
    rapidjson::Document json;
    json.Parse(g_reply_body.c_str());
    ASSERT_FALSE(json.HasParseError());
    EXPECT_STREQ("INVALID_ARGUMENT", json["code"].GetString());
    EXPECT_FALSE(json["retryable"].GetBool());
}

} // namespace starrocks

int main(int argc, char** argv) {
    return starrocks::init_test_env(argc, argv);
}
