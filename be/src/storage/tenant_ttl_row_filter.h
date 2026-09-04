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
#include "storage/rowset/segment.h"
#include "storage/tenant_ttl_compaction_types.h"
#include "util/slice.h"

namespace starrocks {

class MemTracker;

// Scans exactly one VARCHAR tenant column in physical row-id order and builds
// the rows that must be retained. It deliberately does not use predicates,
// zonemaps, short keys, generated columns, or recordTimestamp.
class TenantTtlRowFilter {
public:
    TenantTtlRowFilter(TabletSchemaCSPtr tablet_schema, int32_t tenant_column_unique_id, TenantFilter filter,
                       size_t chunk_size = 4096, MemTracker* mem_tracker = nullptr,
                       const std::atomic<bool>* is_cancelled = nullptr);

    Status validate() const;
    StatusOr<SegmentFilterPlan> plan_segment(const SegmentSharedPtr& segment, uint32_t src_segment_id) const;

private:
    Status _check_resource_state() const;
    bool _is_listed(const Slice& tenant) const;
    bool _should_keep(const Slice& tenant) const;

    TabletSchemaCSPtr _tablet_schema;
    int32_t _tenant_column_unique_id;
    int32_t _tenant_column_index{-1};
    TenantFilter _filter;
    size_t _chunk_size;
    MemTracker* _mem_tracker;
    const std::atomic<bool>* _is_cancelled;
};

} // namespace starrocks
