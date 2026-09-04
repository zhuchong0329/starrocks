// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/filtered_rowset_writer.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <set>
#include <system_error>
#include <utility>

#include "common/logging.h"
#include "fs/fs.h"
#include "storage/index/index_descriptor.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/segment.h"
#include "storage/storage_engine.h"
#include "storage/vertical_segment_rewriter.h"

namespace starrocks {
namespace {

Status hard_link_file(const std::string& source, const std::string& destination) {
    std::error_code error;
    std::filesystem::create_hard_link(source, destination, error);
    if (error) {
        return Status::IOError("Failed to hard-link Tenant-TTL artifact from " + source + " to " + destination + ": " +
                               error.message());
    }
    return Status::OK();
}

Status hard_link_directory(const std::string& source, const std::string& destination) {
    std::error_code error;
    std::filesystem::create_directories(destination, error);
    if (error) {
        return Status::IOError("Failed to create Tenant-TTL index directory: " + error.message());
    }
    for (std::filesystem::recursive_directory_iterator it(source, error), end; !error && it != end;
         it.increment(error)) {
        const auto relative = std::filesystem::relative(it->path(), source, error);
        if (error) {
            break;
        }
        const auto target = std::filesystem::path(destination) / relative;
        if (it->is_directory(error)) {
            std::filesystem::create_directories(target, error);
        } else if (it->is_regular_file(error)) {
            std::filesystem::create_directories(target.parent_path(), error);
            if (!error) {
                std::filesystem::create_hard_link(it->path(), target, error);
            }
        }
        if (error) {
            break;
        }
    }
    if (error) {
        return Status::IOError("Failed to hard-link Tenant-TTL index directory: " + error.message());
    }
    return Status::OK();
}

StatusOr<int64_t> path_size(const std::string& path) {
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
            return Status::IOError("Failed to inspect Tenant-TTL directory size: " + error.message());
        }
        return size;
    }
    if (error) {
        return Status::IOError("Failed to inspect Tenant-TTL artifact: " + error.message());
    }
    const auto size = std::filesystem::file_size(path, error);
    if (error) {
        return Status::IOError("Failed to inspect Tenant-TTL artifact size: " + error.message());
    }
    return static_cast<int64_t>(size);
}

void add_page_pointer(const PagePointerPB& page, std::set<std::pair<uint64_t, uint64_t>>* pages) {
    pages->emplace(page.offset(), page.size());
}

void collect_indexed_column_pages(const IndexedColumnMetaPB& meta, std::set<std::pair<uint64_t, uint64_t>>* pages) {
    if (meta.has_ordinal_index_meta()) {
        add_page_pointer(meta.ordinal_index_meta().root_page(), pages);
    }
    if (meta.has_value_index_meta()) {
        add_page_pointer(meta.value_index_meta().root_page(), pages);
    }
}

void collect_column_index_pages(const ColumnMetaPB& column, std::set<std::pair<uint64_t, uint64_t>>* pages) {
    for (const auto& index : column.indexes()) {
        if (index.type() == ORDINAL_INDEX) {
            if (!index.ordinal_index().root_page().is_root_data_page()) {
                add_page_pointer(index.ordinal_index().root_page().root_page(), pages);
            }
        } else if (index.type() == ZONE_MAP_INDEX) {
            collect_indexed_column_pages(index.zone_map_index().page_zone_maps(), pages);
        } else if (index.type() == BITMAP_INDEX) {
            collect_indexed_column_pages(index.bitmap_index().dict_column(), pages);
            collect_indexed_column_pages(index.bitmap_index().bitmap_column(), pages);
        } else if (index.type() == BLOOM_FILTER_INDEX) {
            collect_indexed_column_pages(index.bloom_filter_index().bloom_filter(), pages);
        }
    }
    for (const auto& child : column.children_columns()) {
        collect_column_index_pages(child, pages);
    }
}

StatusOr<SegmentRuntimeStats> collect_linked_segment_stats(const RowsetSharedPtr& source_rowset,
                                                           const SegmentSharedPtr& segment, uint32_t src_segment_id) {
    SegmentRuntimeStats stats;
    stats.num_rows = segment->num_rows();
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(segment->file_name()));
    ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(segment->file_info()));
    SegmentFooterPB footer;
    size_t footer_hint = 4096;
    ASSIGN_OR_RETURN(const size_t footer_and_trailer_size,
                     Segment::parse_segment_footer(read_file.get(), &footer, &footer_hint, nullptr));
    if (footer.num_rows() != segment->num_rows()) {
        return Status::Corruption("Tenant-TTL linked Segment footer row count is inconsistent");
    }
    for (const auto& column : footer.columns()) {
        stats.total_row_size += static_cast<int64_t>(column.total_mem_footprint());
    }

    std::set<std::pair<uint64_t, uint64_t>> embedded_index_pages;
    if (footer.has_short_key_index_page()) {
        add_page_pointer(footer.short_key_index_page(), &embedded_index_pages);
    }
    for (const auto& column : footer.columns()) {
        collect_column_index_pages(column, &embedded_index_pages);
    }
    int64_t embedded_index_size = 0;
    for (const auto& [offset, size] : embedded_index_pages) {
        static_cast<void>(offset);
        embedded_index_size += static_cast<int64_t>(size);
    }

    ASSIGN_OR_RETURN(const int64_t segment_size, path_size(segment->file_name()));
    stats.physical_artifact_size = segment_size;
    int64_t external_index_size = 0;
    for (const auto& index : *source_rowset->schema()->indexes()) {
        std::string path;
        if (index.index_type() == GIN) {
            path = IndexDescriptor::inverted_index_file_path(source_rowset->rowset_path(),
                                                             source_rowset->rowset_id().to_string(), src_segment_id,
                                                             index.index_id());
        } else if (index.index_type() == VECTOR) {
            path = IndexDescriptor::vector_index_file_path(source_rowset->rowset_path(),
                                                           source_rowset->rowset_id().to_string(), src_segment_id,
                                                           index.index_id());
        } else {
            continue;
        }
        ASSIGN_OR_RETURN(const int64_t size, path_size(path));
        external_index_size += size;
        stats.physical_artifact_size += size;
    }
    stats.accounted_index_size = embedded_index_size + external_index_size;
    stats.accounted_data_size = stats.physical_artifact_size - stats.accounted_index_size;
    if (stats.accounted_data_size < static_cast<int64_t>(footer_and_trailer_size)) {
        return Status::Corruption("Tenant-TTL linked Segment accounting is inconsistent");
    }

    const auto& meta = source_rowset->rowset_meta()->get_meta_pb_without_schema();
    if (meta.segment_encryption_metas_size() != 0) {
        if (meta.segment_encryption_metas_size() != source_rowset->num_segments()) {
            return Status::Corruption("Tenant-TTL source Segment encryption metadata count is inconsistent");
        }
        stats.encryption_meta = meta.segment_encryption_metas(src_segment_id);
    }
    return stats;
}

} // namespace

FilteredRowsetWriter::FilteredRowsetWriter(const RowsetWriterContext& context, size_t chunk_size,
                                           MemTracker* mem_tracker, const std::atomic<bool>* is_cancelled)
        : RowsetWriter(context), _chunk_size(chunk_size), _mem_tracker(mem_tracker), _is_cancelled(is_cancelled) {}

FilteredRowsetWriter::~FilteredRowsetWriter() {
    if (!_already_built && !_cleanup_done) {
        const auto status = cleanup();
        LOG_IF(WARNING, !status.ok()) << "Failed to clean unfinished Tenant-TTL Rowset: " << status;
    }
}

Status FilteredRowsetWriter::init() {
    if (_initialized) {
        return Status::InvalidArgument("Tenant-TTL filtered Rowset writer is already initialized");
    }
    RETURN_IF_ERROR(RowsetWriter::init());
    _initialized = true;
    return Status::OK();
}

Status FilteredRowsetWriter::_register_destination_artifacts(uint32_t dst_segment_id) {
    _created_files.emplace_back(
            Rowset::segment_file_path(_context.rowset_path_prefix, _context.rowset_id, dst_segment_id));
    for (const auto& index : *_context.tablet_schema->indexes()) {
        if (index.index_type() == GIN) {
            _created_directories.emplace_back(IndexDescriptor::inverted_index_file_path(
                    _context.rowset_path_prefix, _context.rowset_id.to_string(), dst_segment_id, index.index_id()));
        } else if (index.index_type() == VECTOR) {
            _created_files.emplace_back(IndexDescriptor::vector_index_file_path(
                    _context.rowset_path_prefix, _context.rowset_id.to_string(), dst_segment_id, index.index_id()));
        }
    }
    return Status::OK();
}

StatusOr<SegmentBuildResult> FilteredRowsetWriter::_link_segment(const RowsetSharedPtr& source_rowset,
                                                                 const SegmentSharedPtr& source_segment,
                                                                 const SegmentFilterPlan& plan,
                                                                 uint32_t dst_segment_id) {
    SegmentBuildResult result;
    result.src_segment_id = plan.src_segment_id;
    result.dst_segment_id = dst_segment_id;
    ASSIGN_OR_RETURN(result.stats, collect_linked_segment_stats(source_rowset, source_segment, plan.src_segment_id));

    const std::string source_path = source_segment->file_name();
    const std::string destination_path =
            Rowset::segment_file_path(_context.rowset_path_prefix, _context.rowset_id, dst_segment_id);
    RETURN_IF_ERROR(hard_link_file(source_path, destination_path));
    ASSIGN_OR_RETURN(const int64_t segment_size, path_size(destination_path));
    result.artifacts.push_back(
            {.source_path = source_path, .destination_path = destination_path, .size = segment_size});

    for (const auto& index : *_context.tablet_schema->indexes()) {
        std::string source_index_path;
        std::string destination_index_path;
        if (index.index_type() == GIN) {
            source_index_path = IndexDescriptor::inverted_index_file_path(source_rowset->rowset_path(),
                                                                          source_rowset->rowset_id().to_string(),
                                                                          plan.src_segment_id, index.index_id());
            destination_index_path = IndexDescriptor::inverted_index_file_path(
                    _context.rowset_path_prefix, _context.rowset_id.to_string(), dst_segment_id, index.index_id());
            RETURN_IF_ERROR(hard_link_directory(source_index_path, destination_index_path));
        } else if (index.index_type() == VECTOR) {
            source_index_path = IndexDescriptor::vector_index_file_path(source_rowset->rowset_path(),
                                                                        source_rowset->rowset_id().to_string(),
                                                                        plan.src_segment_id, index.index_id());
            destination_index_path = IndexDescriptor::vector_index_file_path(
                    _context.rowset_path_prefix, _context.rowset_id.to_string(), dst_segment_id, index.index_id());
            RETURN_IF_ERROR(hard_link_file(source_index_path, destination_index_path));
        } else {
            continue;
        }
        ASSIGN_OR_RETURN(const int64_t size, path_size(destination_index_path));
        result.artifacts.push_back(
                {.source_path = source_index_path, .destination_path = destination_index_path, .size = size});
    }
    return result;
}

Status FilteredRowsetWriter::_account_segment(SegmentBuildResult result) {
    if (result.dst_segment_id != static_cast<uint32_t>(_num_segment) || result.stats.num_rows < 0 ||
        result.stats.total_row_size < 0 || result.stats.accounted_data_size < 0 ||
        result.stats.accounted_index_size < 0) {
        return Status::Corruption("Tenant-TTL output Segment accounting is invalid");
    }
    _num_rows_written += result.stats.num_rows;
    _total_row_size += result.stats.total_row_size;
    _total_data_size += result.stats.accounted_data_size;
    _total_index_size += result.stats.accounted_index_size;
    if (_segment_encryption_metas.size() != static_cast<size_t>(_num_segment)) {
        return Status::Corruption("Tenant-TTL output Segment encryption metadata ordinal is invalid");
    }
    _segment_encryption_metas.emplace_back(result.stats.encryption_meta);
    ++_num_segment;
    _segment_results.emplace_back(std::move(result));
    return Status::OK();
}

Status FilteredRowsetWriter::add_segment(const RowsetSharedPtr& source_rowset, const SegmentFilterPlan& plan) {
    if (!_initialized || _already_built || _cleanup_done) {
        return Status::InvalidArgument("Tenant-TTL filtered Rowset writer is not writable");
    }
    if (source_rowset == nullptr || plan.src_segment_id >= source_rowset->num_segments()) {
        return Status::InvalidArgument("Tenant-TTL source Rowset or Segment ordinal is invalid");
    }
    RETURN_IF_ERROR(source_rowset->load());
    if (plan.src_segment_id >= source_rowset->segments().size() ||
        source_rowset->segments()[plan.src_segment_id] == nullptr) {
        return Status::Corruption("Tenant-TTL source Segment is unavailable");
    }
    const auto& source_segment = source_rowset->segments()[plan.src_segment_id];
    if (plan.source_rows != source_segment->num_rows() || plan.kept_rows + plan.deleted_rows != plan.source_rows) {
        return Status::InvalidArgument("Tenant-TTL Segment plan row counts are inconsistent");
    }
    if (plan.action == SegmentFilterAction::DROP) {
        if (plan.kept_rows != 0 || plan.deleted_rows != plan.source_rows) {
            return Status::InvalidArgument("Tenant-TTL DROP plan row counts are inconsistent");
        }
        return Status::OK();
    }

    const uint32_t dst_segment_id = _num_segment;
    RETURN_IF_ERROR(_register_destination_artifacts(dst_segment_id));
    if (plan.action == SegmentFilterAction::KEEP) {
        if (plan.deleted_rows != 0 || plan.kept_rows != plan.source_rows || plan.keep_row_ranges != nullptr) {
            return Status::InvalidArgument("Tenant-TTL KEEP plan is inconsistent");
        }
        ASSIGN_OR_RETURN(auto result, _link_segment(source_rowset, source_segment, plan, dst_segment_id));
        return _account_segment(std::move(result));
    }
    if (plan.action != SegmentFilterAction::REWRITE) {
        return Status::InvalidArgument("Tenant-TTL Segment plan action is invalid");
    }
    ASSIGN_OR_RETURN(auto result, VerticalSegmentRewriter::rewrite(source_segment, plan, _context, dst_segment_id,
                                                                   _chunk_size, _mem_tracker, _is_cancelled));
    return _account_segment(std::move(result));
}

StatusOr<RowsetSharedPtr> FilteredRowsetWriter::build() {
    if (!_initialized || _cleanup_done) {
        return Status::InvalidArgument("Tenant-TTL filtered Rowset writer is not buildable");
    }
    ASSIGN_OR_RETURN(auto rowset, RowsetWriter::build());
    Status status = rowset->load();
    if (status.ok()) {
        status = rowset->verify();
    }
    if (!status.ok()) {
        rowset.reset();
        const Status cleanup_status = cleanup();
        if (!cleanup_status.ok()) {
            return status.clone_and_append("; cleanup failed: " + cleanup_status.to_string());
        }
        return status;
    }
    return rowset;
}

Status FilteredRowsetWriter::cleanup() {
    if (_cleanup_done) {
        return Status::OK();
    }
    Status first_error;
    for (auto it = _created_files.rbegin(); it != _created_files.rend(); ++it) {
        const Status status = _fs == nullptr ? Status::OK() : _fs->delete_file(*it);
        if (!status.ok() && !status.is_not_found() && first_error.ok()) {
            first_error = status;
        }
    }
    for (auto it = _created_directories.rbegin(); it != _created_directories.rend(); ++it) {
        const Status status = _fs == nullptr ? Status::OK() : _fs->delete_dir_recursive(*it);
        if (!status.ok() && !status.is_not_found() && first_error.ok()) {
            first_error = status;
        }
    }
    if (!_rowset_id_released) {
        StorageEngine::instance()->release_rowset_id(_context.rowset_id);
        _rowset_id_released = true;
    }
    _cleanup_done = true;
    return first_error;
}

} // namespace starrocks
