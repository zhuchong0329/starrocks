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

#include <algorithm>

namespace starrocks {

Status normalize_and_validate_tenant_ttl_request(TenantTtlCompactionRequest* request) {
    if (request == nullptr) {
        return Status::InvalidArgument("tenant ttl request must not be null");
    }
    if (request->protocol_version != kTenantTtlProtocolVersion) {
        return Status::InvalidArgument("unsupported tenant ttl protocol version: " +
                                       std::to_string(request->protocol_version));
    }
    if (request->task_id <= 0) {
        return Status::InvalidArgument("tenant ttl task_id must be positive");
    }
    if (request->tablet_id <= 0) {
        return Status::InvalidArgument("tenant ttl tablet_id must be positive");
    }
    if (request->partition_id <= 0) {
        return Status::InvalidArgument("tenant ttl partition_id must be positive");
    }
    if (request->tenant_column_unique_id < 0) {
        return Status::InvalidArgument("tenant ttl tenant_column_unique_id must be non-negative");
    }
    switch (request->filter.mode) {
    case TenantFilterMode::DELETE_LIST:
        break;
    case TenantFilterMode::KEEP_LIST:
        if (request->filter.tenants.empty()) {
            return Status::InvalidArgument("tenant ttl KEEP_LIST must not be empty");
        }
        break;
    default:
        return Status::InvalidArgument("invalid tenant ttl filter mode");
    }
    if (request->policy_watermark.dictionary_id <= 0) {
        return Status::InvalidArgument("tenant ttl dictionary_id must be positive");
    }
    if (request->policy_watermark.dictionary_txn_id <= 0) {
        return Status::InvalidArgument("tenant ttl dictionary_txn_id must be positive");
    }
    if (request->policy_watermark.evaluation_time_epoch_seconds <= 0) {
        return Status::InvalidArgument("tenant ttl evaluation_time_epoch_seconds must be positive");
    }
    if (request->expected_schema.schema_id <= 0) {
        return Status::InvalidArgument("tenant ttl expected schema_id must be positive");
    }
    if (request->expected_schema.schema_version < 0) {
        return Status::InvalidArgument("tenant ttl expected schema_version must be non-negative");
    }
    if (request->fe_observed_max_version.has_value() && request->fe_observed_max_version.value() < 0) {
        return Status::InvalidArgument("tenant ttl fe_observed_max_version must be non-negative");
    }

    auto& tenants = request->filter.tenants;
    std::sort(tenants.begin(), tenants.end());
    tenants.erase(std::unique(tenants.begin(), tenants.end()), tenants.end());
    return Status::OK();
}

bool tenant_ttl_task_code_is_retryable(TenantTtlTaskCode code) {
    switch (code) {
    case TenantTtlTaskCode::TABLET_BUSY:
    case TenantTtlTaskCode::TTL_ALREADY_RUNNING:
    case TenantTtlTaskCode::REPLICA_NOT_CAUGHT_UP:
    case TenantTtlTaskCode::SCHEMA_CHANGED:
    case TenantTtlTaskCode::STALE_ROWSET:
    case TenantTtlTaskCode::CANCELLED:
        return true;
    case TenantTtlTaskCode::SUCCESS:
    case TenantTtlTaskCode::NOOP_VERIFIED:
    case TenantTtlTaskCode::INVALID_ARGUMENT:
    case TenantTtlTaskCode::TABLET_NOT_FOUND:
    case TenantTtlTaskCode::NOT_SUPPORTED:
    case TenantTtlTaskCode::DATA_INVARIANT_VIOLATION:
    case TenantTtlTaskCode::INTERNAL_ERROR:
        return false;
    }
    return false;
}

std::string_view tenant_filter_mode_to_string(TenantFilterMode mode) {
    switch (mode) {
    case TenantFilterMode::DELETE_LIST:
        return "DELETE_LIST";
    case TenantFilterMode::KEEP_LIST:
        return "KEEP_LIST";
    }
    return "UNKNOWN";
}

std::string_view tenant_ttl_state_to_string(TenantTtlState state) {
    switch (state) {
    case TenantTtlState::IDLE:
        return "IDLE";
    case TenantTtlState::PENDING:
        return "PENDING";
    case TenantTtlState::RUNNING:
        return "RUNNING";
    }
    return "UNKNOWN";
}

std::string_view tenant_ttl_task_code_to_string(TenantTtlTaskCode code) {
    switch (code) {
    case TenantTtlTaskCode::SUCCESS:
        return "SUCCESS";
    case TenantTtlTaskCode::NOOP_VERIFIED:
        return "NOOP_VERIFIED";
    case TenantTtlTaskCode::INVALID_ARGUMENT:
        return "INVALID_ARGUMENT";
    case TenantTtlTaskCode::TABLET_NOT_FOUND:
        return "TABLET_NOT_FOUND";
    case TenantTtlTaskCode::NOT_SUPPORTED:
        return "NOT_SUPPORTED";
    case TenantTtlTaskCode::TABLET_BUSY:
        return "TABLET_BUSY";
    case TenantTtlTaskCode::TTL_ALREADY_RUNNING:
        return "TTL_ALREADY_RUNNING";
    case TenantTtlTaskCode::REPLICA_NOT_CAUGHT_UP:
        return "REPLICA_NOT_CAUGHT_UP";
    case TenantTtlTaskCode::DATA_INVARIANT_VIOLATION:
        return "DATA_INVARIANT_VIOLATION";
    case TenantTtlTaskCode::SCHEMA_CHANGED:
        return "SCHEMA_CHANGED";
    case TenantTtlTaskCode::STALE_ROWSET:
        return "STALE_ROWSET";
    case TenantTtlTaskCode::CANCELLED:
        return "CANCELLED";
    case TenantTtlTaskCode::INTERNAL_ERROR:
        return "INTERNAL_ERROR";
    }
    return "UNKNOWN";
}

std::string_view tenant_ttl_rowset_action_to_string(TenantTtlRowsetAction action) {
    switch (action) {
    case TenantTtlRowsetAction::VERIFIED_NO_CHANGE:
        return "VERIFIED_NO_CHANGE";
    case TenantTtlRowsetAction::DROP:
        return "DROP";
    case TenantTtlRowsetAction::REWRITE:
        return "REWRITE";
    }
    return "UNKNOWN";
}

std::string_view segment_filter_action_to_string(SegmentFilterAction action) {
    switch (action) {
    case SegmentFilterAction::KEEP:
        return "KEEP";
    case SegmentFilterAction::DROP:
        return "DROP";
    case SegmentFilterAction::REWRITE:
        return "REWRITE";
    }
    return "UNKNOWN";
}

} // namespace starrocks
