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

#include <string>
#include <string_view>

#include "common/status.h"
#include "http/http_handler.h"
#include "http/http_status.h"
#include "storage/tenant_ttl_compaction_types.h"

namespace starrocks {

// This handler is compiled only when ENABLE_TENANT_TTL_MANUAL_TEST_ENDPOINT is ON.
// It is a synchronous test seam for the BE Engine task, not a production FE protocol.
class TenantTtlCompactionAction final : public HttpHandler {
public:
    void handle(HttpRequest* req) override;

    static Status parse_request(std::string_view body, TenantTtlCompactionRequest* request);
    static HttpStatus http_status_for_code(TenantTtlTaskCode code);
    static std::string serialize_result(const TenantTtlCompactionResult& result);
};

} // namespace starrocks
