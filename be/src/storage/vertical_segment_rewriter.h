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

#include "common/statusor.h"
#include "storage/tablet_schema.h"
#include "storage/tenant_ttl_compaction_types.h"

namespace starrocks {

class MemTracker;
class RowsetWriterContext;
class Segment;
using SegmentSharedPtr = std::shared_ptr<Segment>;

// Rewrites one source Segment into exactly one destination Segment. Every
// column group replays the same physical row-id ranges, so columns cannot
// become misaligned when the ranges are discontiguous.
class VerticalSegmentRewriter {
public:
    static StatusOr<SegmentBuildResult> rewrite(const SegmentSharedPtr& source, const SegmentFilterPlan& plan,
                                                const RowsetWriterContext& context, uint32_t dst_segment_id,
                                                size_t chunk_size = 4096, MemTracker* mem_tracker = nullptr,
                                                const std::atomic<bool>* is_cancelled = nullptr);
};

} // namespace starrocks
