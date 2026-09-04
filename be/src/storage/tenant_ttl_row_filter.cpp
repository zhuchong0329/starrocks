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

#include <algorithm>
#include <limits>
#include <string_view>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "fs/fs.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/rowset/segment_options.h"
#include "util/defer_op.h"

namespace starrocks {

TenantTtlRowFilter::TenantTtlRowFilter(TabletSchemaCSPtr tablet_schema, int32_t tenant_column_unique_id,
                                       TenantFilter filter, size_t chunk_size, MemTracker* mem_tracker,
                                       const std::atomic<bool>* is_cancelled)
        : _tablet_schema(std::move(tablet_schema)),
          _tenant_column_unique_id(tenant_column_unique_id),
          _filter(std::move(filter)),
          _chunk_size(chunk_size),
          _mem_tracker(mem_tracker),
          _is_cancelled(is_cancelled) {
    if (_tablet_schema != nullptr) {
        _tenant_column_index = _tablet_schema->field_index(_tenant_column_unique_id);
    }
    std::sort(_filter.tenants.begin(), _filter.tenants.end());
    _filter.tenants.erase(std::unique(_filter.tenants.begin(), _filter.tenants.end()), _filter.tenants.end());
}

Status TenantTtlRowFilter::validate() const {
    if (_tablet_schema == nullptr) {
        return Status::InvalidArgument("Tenant-TTL tablet schema is null");
    }
    if (_tenant_column_index < 0) {
        return Status::InvalidArgument("Tenant-TTL tenant column unique id does not exist");
    }
    if (_tablet_schema->column(_tenant_column_index).type() != TYPE_VARCHAR) {
        return Status::InvalidArgument("Tenant-TTL tenant column must be VARCHAR");
    }
    if (_filter.mode != TenantFilterMode::DELETE_LIST && _filter.mode != TenantFilterMode::KEEP_LIST) {
        return Status::InvalidArgument("Tenant-TTL filter mode is invalid");
    }
    if (_filter.mode == TenantFilterMode::KEEP_LIST && _filter.tenants.empty()) {
        return Status::InvalidArgument("Tenant-TTL KEEP_LIST must not be empty");
    }
    if (_chunk_size == 0 || _chunk_size > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return Status::InvalidArgument("Tenant-TTL chunk size is invalid");
    }
    return Status::OK();
}

Status TenantTtlRowFilter::_check_resource_state() const {
    if (_is_cancelled != nullptr && _is_cancelled->load(std::memory_order_acquire)) {
        return Status::Cancelled("Tenant-TTL row filter was cancelled");
    }
    if (_mem_tracker != nullptr) {
        return _mem_tracker->check_mem_limit("Tenant-TTL row filter");
    }
    return Status::OK();
}

bool TenantTtlRowFilter::_is_listed(const Slice& tenant) const {
    const std::string_view value(tenant.data, tenant.size);
    const auto it = std::lower_bound(_filter.tenants.begin(), _filter.tenants.end(), value,
                                     [](const std::string& lhs, std::string_view rhs) { return lhs < rhs; });
    return it != _filter.tenants.end() && std::string_view(*it) == value;
}

bool TenantTtlRowFilter::_should_keep(const Slice& tenant) const {
    const bool listed = _is_listed(tenant);
    return _filter.mode == TenantFilterMode::DELETE_LIST ? !listed : listed;
}

StatusOr<SegmentFilterPlan> TenantTtlRowFilter::plan_segment(const SegmentSharedPtr& segment,
                                                             uint32_t src_segment_id) const {
    RETURN_IF_ERROR(validate());
    RETURN_IF_ERROR(_check_resource_state());
    if (segment == nullptr) {
        return Status::InvalidArgument("Tenant-TTL source segment is null");
    }

    SegmentFilterPlan plan;
    plan.src_segment_id = src_segment_id;
    plan.source_rows = segment->num_rows();
    plan.keep_row_ranges = std::make_shared<SparseRange<>>();
    if (plan.source_rows == 0) {
        plan.action = SegmentFilterAction::KEEP;
        return plan;
    }

    const std::vector<ColumnId> tenant_column_ids{static_cast<ColumnId>(_tenant_column_index)};
    const Schema tenant_schema = ChunkHelper::convert_schema(_tablet_schema, tenant_column_ids);
    SegmentReadOptions read_options;
    ASSIGN_OR_RETURN(read_options.fs, FileSystem::CreateSharedFromString(segment->file_name()));
    OlapReaderStatistics reader_stats;
    read_options.stats = &reader_stats;
    read_options.reader_type = READER_CUMULATIVE_COMPACTION;
    read_options.chunk_size = static_cast<int>(_chunk_size);
    read_options.is_cancelled = _is_cancelled;
    read_options.tablet_schema = _tablet_schema;

    auto iterator_or = segment->new_iterator(tenant_schema, read_options);
    if (iterator_or.status().is_end_of_file()) {
        return Status::Corruption("Tenant-TTL non-empty segment unexpectedly returned EOF while opening iterator");
    }
    ASSIGN_OR_RETURN(auto iterator, std::move(iterator_or));
    if (iterator == nullptr) {
        return Status::Corruption("Tenant-TTL segment returned a null iterator");
    }
    ColumnIdToGlobalDictMap empty_global_dicts;
    RETURN_IF_ERROR(iterator->init_encoded_schema(empty_global_dicts));
    DeferOp close_iterator([&iterator]() { iterator->close(); });

    auto chunk = ChunkHelper::new_chunk(tenant_schema, _chunk_size);
    uint32_t row_ordinal = 0;
    uint32_t keep_run_begin = 0;
    bool in_keep_run = false;
    while (true) {
        RETURN_IF_ERROR(_check_resource_state());
        chunk->reset();
        const Status status = iterator->get_next(chunk.get());
        if (status.is_end_of_file()) {
            break;
        }
        RETURN_IF_ERROR(status);
        if (chunk->num_columns() != 1) {
            return Status::Corruption("Tenant-TTL tenant iterator returned an unexpected column count");
        }
        const auto& tenant_column = chunk->get_column_by_index(0);
        for (size_t i = 0; i < chunk->num_rows(); ++i) {
            if (row_ordinal >= plan.source_rows) {
                return Status::Corruption("Tenant-TTL tenant iterator returned too many rows");
            }
            const Datum tenant = tenant_column->get(i);
            // SQL NULL has no tenant identity and is therefore always retained.
            const bool keep = tenant.is_null() || _should_keep(tenant.get_slice());
            if (keep && !in_keep_run) {
                keep_run_begin = row_ordinal;
                in_keep_run = true;
            } else if (!keep && in_keep_run) {
                plan.keep_row_ranges->add(Range<rowid_t>(keep_run_begin, row_ordinal));
                in_keep_run = false;
            }
            plan.kept_rows += keep;
            ++row_ordinal;
        }
    }
    if (in_keep_run) {
        plan.keep_row_ranges->add(Range<rowid_t>(keep_run_begin, row_ordinal));
    }
    if (row_ordinal != plan.source_rows) {
        return Status::Corruption("Tenant-TTL tenant iterator row count does not match segment metadata");
    }

    plan.deleted_rows = plan.source_rows - plan.kept_rows;
    if (plan.deleted_rows == 0) {
        plan.action = SegmentFilterAction::KEEP;
        plan.keep_row_ranges.reset();
    } else if (plan.kept_rows == 0) {
        plan.action = SegmentFilterAction::DROP;
        plan.keep_row_ranges.reset();
    } else {
        plan.action = SegmentFilterAction::REWRITE;
    }
    return plan;
}

} // namespace starrocks
