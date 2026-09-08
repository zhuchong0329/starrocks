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

#include "storage/tenant_ttl_tablet_guard.h"

#include <utility>

#include "storage/tablet.h"

namespace starrocks {

TenantTtlTabletGuard::TenantTtlTabletGuard(TabletSharedPtr tablet) : _tablet(std::move(tablet)) {}

TenantTtlTabletGuard::~TenantTtlTabletGuard() {
    release();
}

TenantTtlTaskCode TenantTtlTabletGuard::try_acquire(const TenantTtlCompactionRequest& request, Status* detail_status) {
    if (detail_status == nullptr || _tablet == nullptr || _owns_admission) {
        if (detail_status != nullptr) {
            *detail_status = Status::InvalidArgument("invalid tenant ttl tablet guard acquisition");
        }
        return TenantTtlTaskCode::INVALID_ARGUMENT;
    }

    const auto admission = _tablet->try_begin_tenant_ttl(request.task_id, request.policy_watermark);
    if (admission.code != TenantTtlTaskCode::SUCCESS) {
        *detail_status = Status::ResourceBusy(admission.code == TenantTtlTaskCode::TTL_ALREADY_RUNNING
                                                      ? "tenant ttl task is already running on this tablet"
                                                      : "tablet already has a compaction task");
        return admission.code;
    }
    _generation = admission.generation;
    _owns_admission = true;

    // Storage migration copies Rowsets without holding its lock and verifies
    // only the max version before switching Tablets. Keep the shared lock for
    // the complete Tenant-TTL task so a same-version Rowset replacement cannot
    // be lost during migration.
    _migration_lock = std::shared_lock<std::shared_mutex>(_tablet->get_migration_lock(), std::try_to_lock);
    if (!_migration_lock.owns_lock()) {
        *detail_status = Status::ResourceBusy("tenant ttl failed to acquire migration lock");
        release();
        return TenantTtlTaskCode::TABLET_BUSY;
    }
    if (Tablet::check_migrate(_tablet)) {
        *detail_status = Status::ResourceBusy("tenant ttl tablet is migrating or has been replaced");
        release();
        return TenantTtlTaskCode::TABLET_BUSY;
    }

    // Ordinary compaction takes a shared lock on exactly one of these locks.
    // Taking both exclusively in this fixed order prevents either kind from
    // starting while the full Tablet coverage is being rewritten.
    _base_lock = std::unique_lock<std::shared_mutex>(_tablet->get_base_lock(), std::try_to_lock);
    if (!_base_lock.owns_lock()) {
        *detail_status = Status::ResourceBusy("tenant ttl failed to acquire base compaction lock");
        release();
        return TenantTtlTaskCode::TABLET_BUSY;
    }
    _cumulative_lock = std::unique_lock<std::shared_mutex>(_tablet->get_cumulative_lock(), std::try_to_lock);
    if (!_cumulative_lock.owns_lock()) {
        *detail_status = Status::ResourceBusy("tenant ttl failed to acquire cumulative compaction lock");
        release();
        return TenantTtlTaskCode::TABLET_BUSY;
    }
    if (!_tablet->mark_tenant_ttl_running(_generation)) {
        *detail_status = Status::Aborted("tenant ttl admission owner changed while acquiring locks");
        release();
        return TenantTtlTaskCode::INTERNAL_ERROR;
    }

    *detail_status = Status::OK();
    return TenantTtlTaskCode::SUCCESS;
}

void TenantTtlTabletGuard::release() {
    if (_cumulative_lock.owns_lock()) {
        _cumulative_lock.unlock();
    }
    if (_base_lock.owns_lock()) {
        _base_lock.unlock();
    }
    if (_migration_lock.owns_lock()) {
        _migration_lock.unlock();
    }
    if (_owns_admission) {
        _tablet->finish_tenant_ttl(_generation);
        _owns_admission = false;
    }
}

TenantTtlCoverageGuard::TenantTtlCoverageGuard(TabletSharedPtr tablet) : _tablet(std::move(tablet)) {}

TenantTtlCoverageGuard::~TenantTtlCoverageGuard() {
    release();
}

TenantTtlTaskCode TenantTtlCoverageGuard::capture(const TenantTtlCompactionRequest& request, Status* detail_status) {
    if (_tablet == nullptr || _owns_coverage) {
        if (detail_status != nullptr) {
            *detail_status = Status::InvalidArgument("invalid tenant ttl coverage guard capture");
        }
        return TenantTtlTaskCode::INVALID_ARGUMENT;
    }
    const auto code = _tablet->capture_tenant_ttl_coverage(request, &_coverage, detail_status);
    _owns_coverage = code == TenantTtlTaskCode::SUCCESS;
    return code;
}

void TenantTtlCoverageGuard::release() {
    if (_owns_coverage) {
        _tablet->release_tenant_ttl_coverage(_coverage);
        _owns_coverage = false;
    }
}

} // namespace starrocks
