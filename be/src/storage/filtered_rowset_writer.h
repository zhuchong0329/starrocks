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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "storage/rowset/rowset_writer.h"
#include "storage/tenant_ttl_compaction_types.h"

namespace starrocks {

class MemTracker;

// Builds one replacement Rowset while preserving the one-source-Segment to
// zero-or-one-output-Segment invariant and compacting destination ordinals.
class FilteredRowsetWriter final : public RowsetWriter {
public:
    FilteredRowsetWriter(const RowsetWriterContext& context, size_t chunk_size = 4096,
                         MemTracker* mem_tracker = nullptr, const std::atomic<bool>* is_cancelled = nullptr);
    ~FilteredRowsetWriter() override;

    Status init() override;
    Status add_segment(const RowsetSharedPtr& source_rowset, const SegmentFilterPlan& plan);
    StatusOr<RowsetSharedPtr> build() override;

    // Removes all staged artifacts and releases the output Rowset ID. It is
    // valid both before and after build, but must not be called after commit.
    Status cleanup();

    const std::vector<SegmentBuildResult>& segment_results() const { return _segment_results; }

private:
    Status _register_destination_artifacts(uint32_t dst_segment_id);
    StatusOr<SegmentBuildResult> _link_segment(const RowsetSharedPtr& source_rowset,
                                               const SegmentSharedPtr& source_segment, const SegmentFilterPlan& plan,
                                               uint32_t dst_segment_id);
    Status _account_segment(SegmentBuildResult result);

    size_t _chunk_size;
    MemTracker* _mem_tracker;
    const std::atomic<bool>* _is_cancelled;
    bool _initialized{false};
    bool _cleanup_done{false};
    bool _rowset_id_released{false};
    std::vector<std::string> _created_files;
    std::vector<std::string> _created_directories;
    std::vector<SegmentBuildResult> _segment_results;
};

} // namespace starrocks
