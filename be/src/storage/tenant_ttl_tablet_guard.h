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

#include <memory>
#include <shared_mutex>

#include "storage/tenant_ttl_compaction_types.h"

namespace starrocks {

class Tablet;
using TabletSharedPtr = std::shared_ptr<Tablet>;

// Owns one process-local Tenant-TTL admission, the migration shared lock, and
// both compaction locks. The acquisition order is admission ->
// migration(shared) -> base(unique) -> cumulative(unique).
class TenantTtlTabletGuard {
public:
    explicit TenantTtlTabletGuard(TabletSharedPtr tablet);
    ~TenantTtlTabletGuard();

    TenantTtlTabletGuard(const TenantTtlTabletGuard&) = delete;
    TenantTtlTabletGuard& operator=(const TenantTtlTabletGuard&) = delete;

    TenantTtlTaskCode try_acquire(const TenantTtlCompactionRequest& request, Status* detail_status);
    void release();

    bool owns_admission() const { return _owns_admission; }
    uint64_t generation() const { return _generation; }

private:
    TabletSharedPtr _tablet;
    std::shared_lock<std::shared_mutex> _migration_lock;
    std::unique_lock<std::shared_mutex> _base_lock;
    std::unique_lock<std::shared_mutex> _cumulative_lock;
    uint64_t _generation{kInvalidTenantTtlGeneration};
    bool _owns_admission{false};
};

// Owns the reader references and is_compacting flags acquired during coverage
// capture. It must be destroyed before TenantTtlTabletGuard.
class TenantTtlCoverageGuard {
public:
    explicit TenantTtlCoverageGuard(TabletSharedPtr tablet);
    ~TenantTtlCoverageGuard();

    TenantTtlCoverageGuard(const TenantTtlCoverageGuard&) = delete;
    TenantTtlCoverageGuard& operator=(const TenantTtlCoverageGuard&) = delete;

    TenantTtlTaskCode capture(const TenantTtlCompactionRequest& request, Status* detail_status);
    void release();

    const TenantTtlCoverage& coverage() const { return _coverage; }
    bool owns_coverage() const { return _owns_coverage; }

private:
    TabletSharedPtr _tablet;
    TenantTtlCoverage _coverage;
    bool _owns_coverage{false};
};

} // namespace starrocks
