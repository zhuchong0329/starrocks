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

#include "storage/vertical_segment_rewriter.h"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <numeric>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "common/config.h"
#include "fs/fs.h"
#include "fs/key_cache.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/compaction_utils.h"
#include "storage/index/index_descriptor.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "util/defer_op.h"

namespace starrocks {
namespace {

Status check_resource_state(MemTracker* mem_tracker, const std::atomic<bool>* is_cancelled) {
    if (is_cancelled != nullptr && is_cancelled->load(std::memory_order_acquire)) {
        return Status::Cancelled("Tenant-TTL segment rewrite was cancelled");
    }
    if (mem_tracker != nullptr) {
        return mem_tracker->check_mem_limit("Tenant-TTL segment rewrite");
    }
    return Status::OK();
}

StatusOr<int64_t> artifact_size(const std::string& path) {
    std::error_code error;
    if (std::filesystem::is_directory(path, error)) {
        int64_t size = 0;
        for (std::filesystem::recursive_directory_iterator it(path, error), end; !error && it != end;
             it.increment(error)) {
            if (it->is_regular_file(error)) {
                size += static_cast<int64_t>(it->file_size(error));
            }
        }
        if (error) {
            return Status::IOError("Failed to inspect Tenant-TTL index directory: " + error.message());
        }
        return size;
    }
    if (error) {
        return Status::IOError("Failed to inspect Tenant-TTL artifact: " + error.message());
    }
    const auto size = std::filesystem::file_size(path, error);
    if (error) {
        return Status::IOError("Failed to get Tenant-TTL artifact size: " + error.message());
    }
    return static_cast<int64_t>(size);
}

Status collect_output_artifacts(const RowsetWriterContext& context, uint32_t dst_segment_id,
                                std::vector<SegmentArtifact>* artifacts, int64_t* physical_size) {
    const std::string segment_path =
            Rowset::segment_file_path(context.rowset_path_prefix, context.rowset_id, dst_segment_id);
    ASSIGN_OR_RETURN(const int64_t segment_size, artifact_size(segment_path));
    artifacts->push_back({.destination_path = segment_path, .size = segment_size});
    *physical_size += segment_size;

    for (const auto& index : *context.tablet_schema->indexes()) {
        std::string path;
        if (index.index_type() == GIN) {
            path = IndexDescriptor::inverted_index_file_path(context.rowset_path_prefix, context.rowset_id.to_string(),
                                                             dst_segment_id, index.index_id());
        } else if (index.index_type() == VECTOR) {
            path = IndexDescriptor::vector_index_file_path(context.rowset_path_prefix, context.rowset_id.to_string(),
                                                           dst_segment_id, index.index_id());
        } else {
            continue;
        }
        ASSIGN_OR_RETURN(const int64_t size, artifact_size(path));
        artifacts->push_back({.destination_path = path, .size = size});
        *physical_size += size;
    }
    return Status::OK();
}

} // namespace

StatusOr<SegmentBuildResult> VerticalSegmentRewriter::rewrite(
        const SegmentSharedPtr& source, const SegmentFilterPlan& plan, const RowsetWriterContext& context,
        uint32_t dst_segment_id, size_t chunk_size, MemTracker* mem_tracker, const std::atomic<bool>* is_cancelled) {
    RETURN_IF_ERROR(check_resource_state(mem_tracker, is_cancelled));
    if (source == nullptr || context.tablet_schema == nullptr) {
        return Status::InvalidArgument("Tenant-TTL segment rewrite source or schema is null");
    }
    if (plan.action != SegmentFilterAction::REWRITE || plan.keep_row_ranges == nullptr ||
        plan.keep_row_ranges->empty()) {
        return Status::InvalidArgument("Tenant-TTL segment rewrite requires non-empty REWRITE ranges");
    }
    if (plan.source_rows != source->num_rows() || plan.kept_rows != plan.keep_row_ranges->span_size() ||
        plan.kept_rows + plan.deleted_rows != plan.source_rows || plan.kept_rows == 0 || plan.deleted_rows == 0) {
        return Status::InvalidArgument("Tenant-TTL segment rewrite plan is inconsistent");
    }
    if (chunk_size == 0 || chunk_size > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return Status::InvalidArgument("Tenant-TTL segment rewrite chunk size is invalid");
    }

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(context.rowset_path_prefix));
    WritableFileOptions writable_options;
    SegmentWriterOptions writer_options;
    writer_options.is_compaction = true;
    writer_options.segment_file_mark.rowset_path_prefix = context.rowset_path_prefix;
    writer_options.segment_file_mark.rowset_id = context.rowset_id.to_string();
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        writable_options.encryption_info = pair.info;
        writer_options.encryption_meta = std::move(pair.encryption_meta);
    }
    const std::string destination_path =
            Rowset::segment_file_path(context.rowset_path_prefix, context.rowset_id, dst_segment_id);
    ASSIGN_OR_RETURN(auto writable_file, fs->new_writable_file(writable_options, destination_path));
    SegmentWriter writer(std::move(writable_file), dst_segment_id, context.tablet_schema, writer_options);

    std::vector<ColumnId> sort_columns = context.tablet_schema->sort_key_idxes();
    if (sort_columns.empty()) {
        sort_columns.resize(context.tablet_schema->num_key_columns());
        std::iota(sort_columns.begin(), sort_columns.end(), 0);
    }
    std::vector<std::vector<uint32_t>> column_groups;
    CompactionUtils::split_column_into_groups(context.tablet_schema->num_columns(), sort_columns,
                                              config::vertical_compaction_max_columns_per_group, &column_groups);
    column_groups.erase(
            std::remove_if(column_groups.begin(), column_groups.end(), [](const auto& group) { return group.empty(); }),
            column_groups.end());
    if (column_groups.empty()) {
        return Status::InvalidArgument("Tenant-TTL segment rewrite schema has no columns");
    }

    SegmentBuildResult result;
    result.src_segment_id = plan.src_segment_id;
    result.dst_segment_id = dst_segment_id;
    result.stats.num_rows = plan.kept_rows;
    result.stats.encryption_meta = writer_options.encryption_meta;
    uint64_t index_size = 0;
    for (size_t group_index = 0; group_index < column_groups.size(); ++group_index) {
        RETURN_IF_ERROR(check_resource_state(mem_tracker, is_cancelled));
        const auto& column_group = column_groups[group_index];
        RETURN_IF_ERROR(writer.init(column_group, group_index == 0));
        const Schema schema = ChunkHelper::convert_schema(context.tablet_schema, column_group);
        SegmentReadOptions read_options;
        read_options.fs = fs;
        OlapReaderStatistics reader_stats;
        read_options.stats = &reader_stats;
        read_options.reader_type = READER_CUMULATIVE_COMPACTION;
        read_options.chunk_size = static_cast<int>(chunk_size);
        read_options.rowid_range_option = plan.keep_row_ranges;
        read_options.is_cancelled = is_cancelled;
        read_options.tablet_schema = context.tablet_schema;

        ASSIGN_OR_RETURN(auto iterator, source->new_iterator(schema, read_options));
        if (iterator == nullptr) {
            return Status::Corruption("Tenant-TTL segment rewrite received a null iterator");
        }
        ColumnIdToGlobalDictMap empty_global_dicts;
        RETURN_IF_ERROR(iterator->init_encoded_schema(empty_global_dicts));
        DeferOp close_iterator([&iterator]() { iterator->close(); });
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        const auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);
        uint64_t rows_written = 0;
        while (true) {
            RETURN_IF_ERROR(check_resource_state(mem_tracker, is_cancelled));
            chunk->reset();
            const Status status = iterator->get_next(chunk.get());
            if (status.is_end_of_file()) {
                break;
            }
            RETURN_IF_ERROR(status);
            ChunkHelper::padding_char_columns(char_field_indexes, schema, context.tablet_schema, chunk.get());
            RETURN_IF_ERROR(writer.append_chunk(*chunk));
            rows_written += chunk->num_rows();
            result.stats.total_row_size += static_cast<int64_t>(chunk->bytes_usage());
        }
        if (rows_written != plan.kept_rows) {
            return Status::Corruption("Tenant-TTL segment rewrite column group row count is inconsistent");
        }
        RETURN_IF_ERROR(writer.finalize_columns(&index_size));
    }

    uint64_t segment_file_size = 0;
    RETURN_IF_ERROR(writer.finalize_footer(&segment_file_size));
    RETURN_IF_ERROR(
            collect_output_artifacts(context, dst_segment_id, &result.artifacts, &result.stats.physical_artifact_size));
    if (index_size > static_cast<uint64_t>(result.stats.physical_artifact_size)) {
        return Status::Corruption("Tenant-TTL rewritten Segment index size exceeds physical artifacts");
    }
    result.stats.accounted_index_size = static_cast<int64_t>(index_size);
    result.stats.accounted_data_size = result.stats.physical_artifact_size - result.stats.accounted_index_size;
    return result;
}

} // namespace starrocks
