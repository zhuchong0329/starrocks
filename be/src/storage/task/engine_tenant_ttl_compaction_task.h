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
#include <memory>

#include "storage/task/engine_task.h"
#include "storage/tenant_ttl_compaction_types.h"

namespace starrocks {

class MemTracker;

class EngineTenantTtlCompactionTask final : public EngineTask {
public:
    EngineTenantTtlCompactionTask(TenantTtlCompactionRequest request, MemTracker* parent_mem_tracker = nullptr,
                                  const std::atomic<bool>* is_cancelled = nullptr);
    ~EngineTenantTtlCompactionTask() override = default;

    Status execute() override;
    const TenantTtlCompactionResult& result() const { return _result; }

private:
    Status _finish(TenantTtlTaskCode code, Status detail_status);
    Status _check_cancelled() const;

    TenantTtlCompactionRequest _request;
    TenantTtlCompactionResult _result;
    std::unique_ptr<MemTracker> _mem_tracker;
    const std::atomic<bool>* _is_cancelled;
};

} // namespace starrocks
