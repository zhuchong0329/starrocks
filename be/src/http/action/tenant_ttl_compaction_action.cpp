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

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <limits>
#include <string>
#include <unordered_set>

#include "fmt/format.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "storage/task/engine_tenant_ttl_compaction_task.h"

namespace starrocks {
namespace {

constexpr size_t kMaxManualRequestBytes = 1024 * 1024;

Status validate_members(const rapidjson::Value& object, std::string_view object_name,
                        std::initializer_list<std::string_view> allowed) {
    if (!object.IsObject()) {
        return Status::InvalidArgument(fmt::format("{} must be a JSON object", object_name));
    }
    std::unordered_set<std::string> seen;
    for (auto it = object.MemberBegin(); it != object.MemberEnd(); ++it) {
        const std::string name(it->name.GetString(), it->name.GetStringLength());
        if (!seen.emplace(name).second) {
            return Status::InvalidArgument(fmt::format("duplicate field {}.{}", object_name, name));
        }
        if (std::find(allowed.begin(), allowed.end(), name) == allowed.end()) {
            return Status::InvalidArgument(fmt::format("unknown field {}.{}", object_name, name));
        }
    }
    return Status::OK();
}

Status get_required_int64(const rapidjson::Value& object, std::string_view object_name, const char* field,
                          int64_t* value) {
    const auto it = object.FindMember(field);
    if (it == object.MemberEnd()) {
        return Status::InvalidArgument(fmt::format("missing field {}.{}", object_name, field));
    }
    if (!it->value.IsInt64()) {
        return Status::InvalidArgument(fmt::format("{}.{} must be an int64", object_name, field));
    }
    *value = it->value.GetInt64();
    return Status::OK();
}

Status get_required_object(const rapidjson::Value& object, std::string_view object_name, const char* field,
                           const rapidjson::Value** value) {
    const auto it = object.FindMember(field);
    if (it == object.MemberEnd()) {
        return Status::InvalidArgument(fmt::format("missing field {}.{}", object_name, field));
    }
    if (!it->value.IsObject()) {
        return Status::InvalidArgument(fmt::format("{}.{} must be a JSON object", object_name, field));
    }
    *value = &it->value;
    return Status::OK();
}

template <typename Writer>
void write_string(Writer* writer, std::string_view value) {
    writer->String(value.data(), static_cast<rapidjson::SizeType>(value.size()));
}

TenantTtlCompactionResult invalid_result(const Status& status) {
    return TenantTtlCompactionResult{
            .code = TenantTtlTaskCode::INVALID_ARGUMENT, .detail_status = status, .retryable = false};
}

} // namespace

Status TenantTtlCompactionAction::parse_request(std::string_view body, TenantTtlCompactionRequest* request) {
    if (request == nullptr) {
        return Status::InvalidArgument("Tenant-TTL HTTP request output must not be null");
    }
    if (body.empty()) {
        return Status::InvalidArgument("Tenant-TTL HTTP request body must not be empty");
    }
    if (body.size() > kMaxManualRequestBytes) {
        return Status::InvalidArgument("Tenant-TTL HTTP request body exceeds 1 MiB");
    }

    rapidjson::Document root;
    root.Parse(body.data(), body.size());
    if (root.HasParseError()) {
        return Status::InvalidArgument(fmt::format("invalid JSON at offset {}: {}", root.GetErrorOffset(),
                                                   rapidjson::GetParseError_En(root.GetParseError())));
    }
    RETURN_IF_ERROR(
            validate_members(root, "request",
                             {"protocol_version", "task_id", "tablet_id", "partition_id", "tenant_column_unique_id",
                              "filter", "policy_watermark", "expected_schema", "fe_observed_max_version"}));

    TenantTtlCompactionRequest parsed;
    int64_t numeric = 0;
    RETURN_IF_ERROR(get_required_int64(root, "request", "protocol_version", &numeric));
    if (numeric < 0 || numeric > std::numeric_limits<uint32_t>::max()) {
        return Status::InvalidArgument("request.protocol_version is outside uint32 range");
    }
    parsed.protocol_version = static_cast<uint32_t>(numeric);
    RETURN_IF_ERROR(get_required_int64(root, "request", "task_id", &parsed.task_id));
    RETURN_IF_ERROR(get_required_int64(root, "request", "tablet_id", &parsed.tablet_id));
    RETURN_IF_ERROR(get_required_int64(root, "request", "partition_id", &parsed.partition_id));
    RETURN_IF_ERROR(get_required_int64(root, "request", "tenant_column_unique_id", &numeric));
    if (numeric < std::numeric_limits<int32_t>::min() || numeric > std::numeric_limits<int32_t>::max()) {
        return Status::InvalidArgument("request.tenant_column_unique_id is outside int32 range");
    }
    parsed.tenant_column_unique_id = static_cast<int32_t>(numeric);

    const rapidjson::Value* filter = nullptr;
    RETURN_IF_ERROR(get_required_object(root, "request", "filter", &filter));
    RETURN_IF_ERROR(validate_members(*filter, "request.filter", {"mode", "tenants"}));
    const auto mode = filter->FindMember("mode");
    if (mode == filter->MemberEnd() || !mode->value.IsString()) {
        return Status::InvalidArgument("request.filter.mode must be a string");
    }
    const std::string_view mode_value(mode->value.GetString(), mode->value.GetStringLength());
    if (mode_value == "DELETE_LIST") {
        parsed.filter.mode = TenantFilterMode::DELETE_LIST;
    } else if (mode_value == "KEEP_LIST") {
        parsed.filter.mode = TenantFilterMode::KEEP_LIST;
    } else {
        return Status::InvalidArgument("request.filter.mode must be DELETE_LIST or KEEP_LIST");
    }
    const auto tenants = filter->FindMember("tenants");
    if (tenants == filter->MemberEnd() || !tenants->value.IsArray()) {
        return Status::InvalidArgument("request.filter.tenants must be an array");
    }
    parsed.filter.tenants.reserve(tenants->value.Size());
    for (const auto& tenant : tenants->value.GetArray()) {
        if (!tenant.IsString()) {
            return Status::InvalidArgument("request.filter.tenants elements must be strings");
        }
        parsed.filter.tenants.emplace_back(tenant.GetString(), tenant.GetStringLength());
    }

    const rapidjson::Value* watermark = nullptr;
    RETURN_IF_ERROR(get_required_object(root, "request", "policy_watermark", &watermark));
    RETURN_IF_ERROR(validate_members(*watermark, "request.policy_watermark",
                                     {"dictionary_id", "dictionary_txn_id", "evaluation_time_epoch_seconds"}));
    RETURN_IF_ERROR(get_required_int64(*watermark, "request.policy_watermark", "dictionary_id",
                                       &parsed.policy_watermark.dictionary_id));
    RETURN_IF_ERROR(get_required_int64(*watermark, "request.policy_watermark", "dictionary_txn_id",
                                       &parsed.policy_watermark.dictionary_txn_id));
    RETURN_IF_ERROR(get_required_int64(*watermark, "request.policy_watermark", "evaluation_time_epoch_seconds",
                                       &parsed.policy_watermark.evaluation_time_epoch_seconds));

    const rapidjson::Value* schema = nullptr;
    RETURN_IF_ERROR(get_required_object(root, "request", "expected_schema", &schema));
    RETURN_IF_ERROR(validate_members(*schema, "request.expected_schema", {"schema_id", "schema_version"}));
    RETURN_IF_ERROR(
            get_required_int64(*schema, "request.expected_schema", "schema_id", &parsed.expected_schema.schema_id));
    RETURN_IF_ERROR(get_required_int64(*schema, "request.expected_schema", "schema_version", &numeric));
    if (numeric < std::numeric_limits<int32_t>::min() || numeric > std::numeric_limits<int32_t>::max()) {
        return Status::InvalidArgument("request.expected_schema.schema_version is outside int32 range");
    }
    parsed.expected_schema.schema_version = static_cast<int32_t>(numeric);

    const auto observed = root.FindMember("fe_observed_max_version");
    if (observed != root.MemberEnd()) {
        if (!observed->value.IsInt64()) {
            return Status::InvalidArgument("request.fe_observed_max_version must be an int64");
        }
        parsed.fe_observed_max_version = observed->value.GetInt64();
    }

    RETURN_IF_ERROR(normalize_and_validate_tenant_ttl_request(&parsed));
    *request = std::move(parsed);
    return Status::OK();
}

HttpStatus TenantTtlCompactionAction::http_status_for_code(TenantTtlTaskCode code) {
    switch (code) {
    case TenantTtlTaskCode::SUCCESS:
    case TenantTtlTaskCode::NOOP_VERIFIED:
        return HttpStatus::OK;
    case TenantTtlTaskCode::INVALID_ARGUMENT:
    case TenantTtlTaskCode::DATA_INVARIANT_VIOLATION:
        return HttpStatus::BAD_REQUEST;
    case TenantTtlTaskCode::TABLET_NOT_FOUND:
        return HttpStatus::NOT_FOUND;
    case TenantTtlTaskCode::NOT_SUPPORTED:
        return HttpStatus::NOT_IMPLEMENTED;
    case TenantTtlTaskCode::SCHEMA_CHANGED:
    case TenantTtlTaskCode::STALE_ROWSET:
        return HttpStatus::CONFLICT;
    case TenantTtlTaskCode::TABLET_BUSY:
    case TenantTtlTaskCode::TTL_ALREADY_RUNNING:
    case TenantTtlTaskCode::REPLICA_NOT_CAUGHT_UP:
    case TenantTtlTaskCode::CANCELLED:
        return HttpStatus::SERVICE_UNAVAILABLE;
    case TenantTtlTaskCode::INTERNAL_ERROR:
        return HttpStatus::INTERNAL_SERVER_ERROR;
    }
    return HttpStatus::INTERNAL_SERVER_ERROR;
}

std::string TenantTtlCompactionAction::serialize_result(const TenantTtlCompactionResult& result) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("code");
    write_string(&writer, tenant_ttl_task_code_to_string(result.code));
    writer.Key("retryable");
    writer.Bool(result.retryable);
    writer.Key("message");
    write_string(&writer, result.detail_status.ok() ? std::string_view() : result.detail_status.message());
    writer.Key("task_id");
    writer.Int64(result.task_id);
    writer.Key("tablet_id");
    writer.Int64(result.tablet_id);
    writer.Key("partition_id");
    writer.Int64(result.partition_id);
    writer.Key("snapshot_end_version");
    writer.Int64(result.snapshot_end_version);
    writer.Key("processed_through_version");
    writer.Int64(result.processed_through_version);
    writer.Key("coverage_digest");
    write_string(&writer, result.coverage_digest);
    writer.Key("scanned_rows");
    writer.Int64(result.scanned_rows);
    writer.Key("kept_rows");
    writer.Int64(result.kept_rows);
    writer.Key("deleted_rows");
    writer.Int64(result.deleted_rows);
    writer.Key("tenant_rows_read");
    writer.Int64(result.tenant_rows_read);
    writer.Key("rows_pruned_by_segment_zonemap");
    writer.Int64(result.rows_pruned_by_segment_zonemap);
    writer.Key("rows_pruned_by_page_zonemap");
    writer.Int64(result.rows_pruned_by_page_zonemap);
    writer.Key("linked_bytes");
    writer.Int64(result.linked_bytes);
    writer.Key("rewritten_bytes");
    writer.Int64(result.rewritten_bytes);
    writer.Key("rowsets");
    writer.StartArray();
    for (const auto& rowset : result.rowsets) {
        writer.StartObject();
        writer.Key("source_version_start");
        writer.Int64(rowset.source_version.first);
        writer.Key("source_version_end");
        writer.Int64(rowset.source_version.second);
        writer.Key("source_rowset_id");
        write_string(&writer, rowset.source_rowset_id.to_string());
        writer.Key("output_rowset_id");
        if (rowset.output_rowset_id.has_value()) {
            write_string(&writer, rowset.output_rowset_id->to_string());
        } else {
            writer.Null();
        }
        writer.Key("action");
        write_string(&writer, tenant_ttl_rowset_action_to_string(rowset.action));
        writer.Key("source_rows");
        writer.Int64(rowset.source_rows);
        writer.Key("kept_rows");
        writer.Int64(rowset.kept_rows);
        writer.Key("deleted_rows");
        writer.Int64(rowset.deleted_rows);
        writer.Key("source_segments");
        writer.Int(rowset.source_segments);
        writer.Key("linked_segments");
        writer.Int(rowset.linked_segments);
        writer.Key("dropped_segments");
        writer.Int(rowset.dropped_segments);
        writer.Key("rewritten_segments");
        writer.Int(rowset.rewritten_segments);
        writer.EndObject();
    }
    writer.EndArray();
    writer.EndObject();
    return std::string(buffer.GetString(), buffer.GetSize());
}

void TenantTtlCompactionAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "application/json");
    TenantTtlCompactionRequest request;
    const Status parse_status = parse_request(req->get_request_body(), &request);
    if (!parse_status.ok()) {
        const auto result = invalid_result(parse_status);
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, serialize_result(result));
        return;
    }

    EngineTenantTtlCompactionTask task(std::move(request));
    (void)task.execute();
    const auto& result = task.result();
    HttpChannel::send_reply(req, http_status_for_code(result.code), serialize_result(result));
}

} // namespace starrocks
