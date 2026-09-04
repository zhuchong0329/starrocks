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

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/olap_common.h"
#include "storage/range.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet_schema.h"

namespace starrocks {

inline constexpr uint32_t kTenantTtlProtocolVersion = 1;

enum class TenantFilterMode : uint8_t {
    DELETE_LIST = 0,
    KEEP_LIST = 1,
};

struct TenantFilter {
    TenantFilterMode mode{TenantFilterMode::DELETE_LIST};
    std::vector<std::string> tenants;
};

struct TenantTtlPolicyWatermark {
    int64_t dictionary_id{0};
    int64_t dictionary_txn_id{0};
    int64_t evaluation_time_epoch_seconds{0};

    bool operator==(const TenantTtlPolicyWatermark& rhs) const {
        return dictionary_id == rhs.dictionary_id && dictionary_txn_id == rhs.dictionary_txn_id &&
               evaluation_time_epoch_seconds == rhs.evaluation_time_epoch_seconds;
    }
};

struct TenantTtlSchemaExpectation {
    int64_t schema_id{0};
    int32_t schema_version{-1};

    bool operator==(const TenantTtlSchemaExpectation& rhs) const {
        return schema_id == rhs.schema_id && schema_version == rhs.schema_version;
    }
};

struct TenantTtlCompactionRequest {
    uint32_t protocol_version{kTenantTtlProtocolVersion};
    int64_t task_id{0};
    int64_t tablet_id{0};
    int64_t partition_id{0};
    int32_t tenant_column_unique_id{-1};
    TenantFilter filter;
    TenantTtlPolicyWatermark policy_watermark;
    TenantTtlSchemaExpectation expected_schema;
    std::optional<int64_t> fe_observed_max_version;
};

enum class TenantTtlTaskCode : uint8_t {
    SUCCESS = 0,
    NOOP_VERIFIED,
    INVALID_ARGUMENT,
    TABLET_NOT_FOUND,
    NOT_SUPPORTED,
    TABLET_BUSY,
    TTL_ALREADY_RUNNING,
    REPLICA_NOT_CAUGHT_UP,
    DATA_INVARIANT_VIOLATION,
    SCHEMA_CHANGED,
    STALE_ROWSET,
    CANCELLED,
    INTERNAL_ERROR,
};

enum class TenantTtlRowsetAction : uint8_t {
    VERIFIED_NO_CHANGE = 0,
    DROP,
    REWRITE,
};

enum class SegmentFilterAction : uint8_t {
    KEEP = 0,
    DROP,
    REWRITE,
};

struct TenantTtlRowsetResult {
    Version source_version;
    RowsetId source_rowset_id;
    std::optional<RowsetId> output_rowset_id;
    TenantTtlRowsetAction action{TenantTtlRowsetAction::VERIFIED_NO_CHANGE};
    int64_t source_rows{0};
    int64_t kept_rows{0};
    int64_t deleted_rows{0};
    int32_t source_segments{0};
    int32_t linked_segments{0};
    int32_t dropped_segments{0};
    int32_t rewritten_segments{0};
};

struct TenantTtlCompactionResult {
    TenantTtlTaskCode code{TenantTtlTaskCode::INTERNAL_ERROR};
    Status detail_status;
    bool retryable{false};
    int64_t task_id{0};
    int64_t tablet_id{0};
    int64_t partition_id{0};
    int64_t snapshot_end_version{-1};
    int64_t processed_through_version{-1};
    std::string coverage_digest;
    std::vector<TenantTtlRowsetResult> rowsets;
    int64_t scanned_rows{0};
    int64_t kept_rows{0};
    int64_t deleted_rows{0};
    int64_t linked_bytes{0};
    int64_t rewritten_bytes{0};
};

struct TenantTtlSchemaIdentity {
    TabletSchemaCSPtr captured_schema;
    int64_t schema_id{0};
    int32_t schema_version{-1};
};

struct TenantTtlCoverageEntry {
    Version version;
    RowsetId expected_rowset_id;
    RowsetSharedPtr source;
};

struct TenantTtlCoverage {
    int64_t snapshot_end_version{-1};
    TenantTtlSchemaIdentity schema_identity;
    std::vector<TenantTtlCoverageEntry> entries;
    std::string coverage_digest;
};

struct SegmentFilterPlan {
    uint32_t src_segment_id{0};
    SegmentFilterAction action{SegmentFilterAction::KEEP};
    SparseRangePtr keep_row_ranges;
    uint32_t source_rows{0};
    uint32_t kept_rows{0};
    uint32_t deleted_rows{0};
};

struct SegmentRuntimeStats {
    int64_t num_rows{0};
    int64_t total_row_size{0};
    int64_t accounted_data_size{0};
    int64_t accounted_index_size{0};
    int64_t physical_artifact_size{0};
    std::string encryption_meta;
};

struct SegmentArtifact {
    std::string source_path;
    std::string destination_path;
    int64_t size{0};
};

struct SegmentBuildResult {
    uint32_t src_segment_id{0};
    uint32_t dst_segment_id{0};
    SegmentRuntimeStats stats;
    std::vector<SegmentArtifact> artifacts;
};

Status normalize_and_validate_tenant_ttl_request(TenantTtlCompactionRequest* request);

bool tenant_ttl_task_code_is_retryable(TenantTtlTaskCode code);

std::string_view tenant_filter_mode_to_string(TenantFilterMode mode);
std::string_view tenant_ttl_task_code_to_string(TenantTtlTaskCode code);
std::string_view tenant_ttl_rowset_action_to_string(TenantTtlRowsetAction action);
std::string_view segment_filter_action_to_string(SegmentFilterAction action);

} // namespace starrocks
