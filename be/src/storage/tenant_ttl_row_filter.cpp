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
#include <optional>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "common/object_pool.h"
#include "fs/fs.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/column_predicate.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/predicate_tree/predicate_tree.h"
#include "storage/rowset/segment_options.h"
#include "testutil/sync_point.h"
#include "util/defer_op.h"

namespace starrocks {
namespace {

Status finalize_segment_plan(const SparseRange<>& matching_rows, TenantFilterMode mode, SegmentFilterPlan* plan) {
    const rowid_t matching_row_count = matching_rows.span_size();
    if (matching_row_count > plan->source_rows) {
        return Status::Corruption("Tenant-TTL predicate returned more rowids than the Segment contains");
    }

    if (mode == TenantFilterMode::KEEP_LIST) {
        plan->kept_rows = matching_row_count;
        plan->keep_row_ranges = std::make_shared<SparseRange<>>(matching_rows);
    } else {
        plan->kept_rows = plan->source_rows - matching_row_count;
        plan->keep_row_ranges = std::make_shared<SparseRange<>>();
        rowid_t cursor = 0;
        for (size_t i = 0; i < matching_rows.size(); ++i) {
            const auto& drop_range = matching_rows[i];
            plan->keep_row_ranges->add(Range<rowid_t>(cursor, drop_range.begin()));
            cursor = drop_range.end();
        }
        plan->keep_row_ranges->add(Range<rowid_t>(cursor, plan->source_rows));
    }

    plan->deleted_rows = plan->source_rows - plan->kept_rows;
    if (plan->deleted_rows == 0) {
        plan->action = SegmentFilterAction::KEEP;
        plan->keep_row_ranges.reset();
    } else if (plan->kept_rows == 0) {
        plan->action = SegmentFilterAction::DROP;
        plan->keep_row_ranges.reset();
    } else {
        plan->action = SegmentFilterAction::REWRITE;
    }
    return Status::OK();
}

} // namespace

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
    if (_filter.tenants.empty()) {
        return Status::InvalidArgument("Tenant-TTL row filter requires a non-empty tenant list");
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

    ObjectPool predicate_pool;
    const auto type_info = get_type_info(_tablet_schema->column(_tenant_column_index));
    auto* in_predicate = predicate_pool.add(
            new_column_in_predicate(type_info, static_cast<ColumnId>(_tenant_column_index), _filter.tenants));
    PredicateAndNode predicate_root;
    if (_filter.mode == TenantFilterMode::KEEP_LIST &&
        _tablet_schema->column(_tenant_column_index).is_nullable()) {
        PredicateOrNode keep_or_null;
        keep_or_null.add_child(PredicateColumnNode{in_predicate});
        auto* null_predicate = predicate_pool.add(
                new_column_null_predicate(type_info, static_cast<ColumnId>(_tenant_column_index), true));
        keep_or_null.add_child(PredicateColumnNode{null_predicate});
        predicate_root.add_child(std::move(keep_or_null));
    } else {
        predicate_root.add_child(PredicateColumnNode{in_predicate});
    }
    read_options.pred_tree = PredicateTree::create(std::move(predicate_root));
    RETURN_IF_ERROR(ZonemapPredicatesRewriter::rewrite_predicate_tree(
            &predicate_pool, read_options.pred_tree, read_options.pred_tree_for_zone_map));

    DeferOp collect_reader_stats([this, &reader_stats]() {
        _stats.tenant_rows_read += reader_stats.raw_rows_read;
        _stats.rows_pruned_by_segment_zonemap += reader_stats.segment_stats_filtered;
        _stats.rows_pruned_by_page_zonemap += reader_stats.rows_stats_filtered;
    });

    auto iterator_or = segment->new_iterator(tenant_schema, read_options);
    if (iterator_or.status().is_end_of_file()) {
        const SparseRange<> no_matching_rows;
        RETURN_IF_ERROR(finalize_segment_plan(no_matching_rows, _filter.mode, &plan));
        return plan;
    }
    ASSIGN_OR_RETURN(auto iterator, std::move(iterator_or));
    if (iterator == nullptr) {
        return Status::Corruption("Tenant-TTL segment returned a null iterator");
    }
    ColumnIdToGlobalDictMap empty_global_dicts;
    RETURN_IF_ERROR(iterator->init_encoded_schema(empty_global_dicts));
    DeferOp close_iterator([&iterator]() { iterator->close(); });

    auto chunk = ChunkHelper::new_chunk(tenant_schema, _chunk_size);
    SparseRange<> matching_rows;
    std::optional<rowid_t> last_matching_rowid;
    rowid_t matching_run_begin = 0;
    while (true) {
        RETURN_IF_ERROR(_check_resource_state());
        chunk->reset();
        std::vector<rowid_t> rowids;
        const Status status = iterator->get_next(chunk.get(), &rowids);
        if (status.is_end_of_file()) {
            break;
        }
        RETURN_IF_ERROR(status);
        if (chunk->num_columns() != 1) {
            return Status::Corruption("Tenant-TTL tenant iterator returned an unexpected column count");
        }
        TEST_SYNC_POINT_CALLBACK("TenantTtlRowFilter::plan_segment:matching_rowids", &rowids);
        if (rowids.size() != chunk->num_rows()) {
            return Status::Corruption("Tenant-TTL tenant iterator returned a mismatched rowid count");
        }
        for (const rowid_t rowid : rowids) {
            if (rowid >= plan.source_rows) {
                return Status::Corruption("Tenant-TTL tenant iterator returned an out-of-range rowid");
            }
            if (last_matching_rowid.has_value() && rowid <= last_matching_rowid.value()) {
                return Status::Corruption("Tenant-TTL tenant iterator returned duplicate or unordered rowids");
            }
            if (!last_matching_rowid.has_value()) {
                matching_run_begin = rowid;
            } else if (rowid != last_matching_rowid.value() + 1) {
                matching_rows.add(Range<rowid_t>(matching_run_begin, last_matching_rowid.value() + 1));
                matching_run_begin = rowid;
            }
            last_matching_rowid = rowid;
        }
    }
    if (last_matching_rowid.has_value()) {
        matching_rows.add(Range<rowid_t>(matching_run_begin, last_matching_rowid.value() + 1));
    }

    RETURN_IF_ERROR(finalize_segment_plan(matching_rows, _filter.mode, &plan));
    return plan;
}

} // namespace starrocks
