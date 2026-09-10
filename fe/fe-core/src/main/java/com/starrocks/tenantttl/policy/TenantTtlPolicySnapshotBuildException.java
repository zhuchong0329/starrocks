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

package com.starrocks.tenantttl.policy;

/** A stable, programmatic failure returned while constructing a Tenant-TTL policy snapshot. */
public class TenantTtlPolicySnapshotBuildException extends Exception {
    public enum Classification {
        DETERMINISTIC,
        RETRYABLE
    }

    public enum ErrorCode {
        TENANT_TTL_POLICY_RPC_ERROR,
        TENANT_TTL_POLICY_VERSION_MISMATCH,
        TENANT_TTL_POLICY_CACHE_NOT_FOUND,
        TENANT_TTL_POLICY_SCHEMA_MISMATCH,
        TENANT_TTL_POLICY_LIMIT_EXCEEDED,
        TENANT_TTL_POLICY_EXPORT_INTERNAL_ERROR,
        TENANT_TTL_POLICY_UNSUPPORTED_PROTOCOL,
        TENANT_TTL_POLICY_RESPONSE_INTEGRITY,
        TENANT_TTL_POLICY_DUPLICATE_KEY,
        TENANT_TTL_POLICY_INVALID_ROW
    }

    private final Classification classification;
    private final ErrorCode errorCode;

    TenantTtlPolicySnapshotBuildException(Classification classification, ErrorCode errorCode, String message) {
        super(message);
        this.classification = classification;
        this.errorCode = errorCode;
    }

    TenantTtlPolicySnapshotBuildException(Classification classification, ErrorCode errorCode, String message,
                                          Throwable cause) {
        super(message, cause);
        this.classification = classification;
        this.errorCode = errorCode;
    }

    public Classification getClassification() {
        return classification;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
