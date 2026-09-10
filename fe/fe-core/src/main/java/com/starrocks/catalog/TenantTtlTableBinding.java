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

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

/** Persistent table-layout identity captured when Tenant-TTL is enabled. */
public class TenantTtlTableBinding {
    public static final int CURRENT_FORMAT_VERSION = 1;
    public static final int NO_LIST_TIME_COMPONENT = -1;

    @SerializedName(value = "formatVersion")
    private int formatVersion = CURRENT_FORMAT_VERSION;
    @SerializedName(value = "tenantColumnId")
    private String tenantColumnId;
    @SerializedName(value = "tenantColumnUniqueId")
    private int tenantColumnUniqueId;
    @SerializedName(value = "timeColumnId")
    private String timeColumnId;
    @SerializedName(value = "timeColumnUniqueId")
    private int timeColumnUniqueId;
    @SerializedName(value = "partitionExpressionType")
    private String partitionExpressionType;
    @SerializedName(value = "listTimeComponentIndex")
    private int listTimeComponentIndex = NO_LIST_TIME_COMPONENT;
    @SerializedName(value = "normalizedTimeZone")
    private String normalizedTimeZone;
    @SerializedName(value = "partitionExpressionFingerprint")
    private String partitionExpressionFingerprint;

    private TenantTtlTableBinding() {
    }

    public TenantTtlTableBinding(String tenantColumnId, int tenantColumnUniqueId,
                                 String timeColumnId, int timeColumnUniqueId,
                                 String partitionExpressionType, int listTimeComponentIndex,
                                 String normalizedTimeZone, String partitionExpressionFingerprint) {
        this(CURRENT_FORMAT_VERSION, tenantColumnId, tenantColumnUniqueId, timeColumnId, timeColumnUniqueId,
                partitionExpressionType, listTimeComponentIndex, normalizedTimeZone, partitionExpressionFingerprint);
    }

    public TenantTtlTableBinding(int formatVersion, String tenantColumnId, int tenantColumnUniqueId,
                                 String timeColumnId, int timeColumnUniqueId,
                                 String partitionExpressionType, int listTimeComponentIndex,
                                 String normalizedTimeZone, String partitionExpressionFingerprint) {
        this.formatVersion = formatVersion;
        this.tenantColumnId = tenantColumnId;
        this.tenantColumnUniqueId = tenantColumnUniqueId;
        this.timeColumnId = timeColumnId;
        this.timeColumnUniqueId = timeColumnUniqueId;
        this.partitionExpressionType = partitionExpressionType;
        this.listTimeComponentIndex = listTimeComponentIndex;
        this.normalizedTimeZone = normalizedTimeZone;
        this.partitionExpressionFingerprint = partitionExpressionFingerprint;
    }

    public TenantTtlTableBinding(TenantTtlTableBinding other) {
        this(other.formatVersion, other.tenantColumnId, other.tenantColumnUniqueId,
                other.timeColumnId, other.timeColumnUniqueId, other.partitionExpressionType,
                other.listTimeComponentIndex, other.normalizedTimeZone, other.partitionExpressionFingerprint);
    }

    public int getFormatVersion() {
        return formatVersion;
    }

    public String getTenantColumnId() {
        return tenantColumnId;
    }

    public int getTenantColumnUniqueId() {
        return tenantColumnUniqueId;
    }

    public String getTimeColumnId() {
        return timeColumnId;
    }

    public int getTimeColumnUniqueId() {
        return timeColumnUniqueId;
    }

    public String getPartitionExpressionType() {
        return partitionExpressionType;
    }

    public int getListTimeComponentIndex() {
        return listTimeComponentIndex;
    }

    public String getNormalizedTimeZone() {
        return normalizedTimeZone;
    }

    public String getPartitionExpressionFingerprint() {
        return partitionExpressionFingerprint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TenantTtlTableBinding)) {
            return false;
        }
        TenantTtlTableBinding that = (TenantTtlTableBinding) o;
        return formatVersion == that.formatVersion && tenantColumnUniqueId == that.tenantColumnUniqueId &&
                timeColumnUniqueId == that.timeColumnUniqueId &&
                listTimeComponentIndex == that.listTimeComponentIndex &&
                Objects.equals(tenantColumnId, that.tenantColumnId) &&
                Objects.equals(timeColumnId, that.timeColumnId) &&
                Objects.equals(partitionExpressionType, that.partitionExpressionType) &&
                Objects.equals(normalizedTimeZone, that.normalizedTimeZone) &&
                Objects.equals(partitionExpressionFingerprint, that.partitionExpressionFingerprint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(formatVersion, tenantColumnId, tenantColumnUniqueId, timeColumnId, timeColumnUniqueId,
                partitionExpressionType, listTimeComponentIndex, normalizedTimeZone, partitionExpressionFingerprint);
    }
}
