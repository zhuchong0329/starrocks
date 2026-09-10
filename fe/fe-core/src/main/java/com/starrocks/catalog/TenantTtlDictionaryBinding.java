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

/**
 * Persistent identity of the Dictionary used by a Tenant-TTL enabled table.
 *
 * <p>The dictionary id is authoritative. The name is retained for SHOW CREATE TABLE and diagnostics only.
 */
public class TenantTtlDictionaryBinding {
    public static final int CURRENT_FORMAT_VERSION = 1;

    @SerializedName(value = "formatVersion")
    private int formatVersion = CURRENT_FORMAT_VERSION;
    @SerializedName(value = "dictionaryId")
    private long dictionaryId;
    @SerializedName(value = "dictionaryName")
    private String dictionaryName;
    @SerializedName(value = "tableKey")
    private String tableKey;
    @SerializedName(value = "defaultDays")
    private int defaultDays;

    private TenantTtlDictionaryBinding() {
    }

    public TenantTtlDictionaryBinding(long dictionaryId, String dictionaryName, String tableKey, int defaultDays) {
        this(CURRENT_FORMAT_VERSION, dictionaryId, dictionaryName, tableKey, defaultDays);
    }

    public TenantTtlDictionaryBinding(int formatVersion, long dictionaryId, String dictionaryName,
                                      String tableKey, int defaultDays) {
        this.formatVersion = formatVersion;
        this.dictionaryId = dictionaryId;
        this.dictionaryName = dictionaryName;
        this.tableKey = tableKey;
        this.defaultDays = defaultDays;
    }

    public TenantTtlDictionaryBinding(TenantTtlDictionaryBinding other) {
        this(other.formatVersion, other.dictionaryId, other.dictionaryName, other.tableKey, other.defaultDays);
    }

    public int getFormatVersion() {
        return formatVersion;
    }

    public long getDictionaryId() {
        return dictionaryId;
    }

    public String getDictionaryName() {
        return dictionaryName;
    }

    public String getTableKey() {
        return tableKey;
    }

    public int getDefaultDays() {
        return defaultDays;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TenantTtlDictionaryBinding)) {
            return false;
        }
        TenantTtlDictionaryBinding that = (TenantTtlDictionaryBinding) o;
        return formatVersion == that.formatVersion && dictionaryId == that.dictionaryId &&
                defaultDays == that.defaultDays && Objects.equals(dictionaryName, that.dictionaryName) &&
                Objects.equals(tableKey, that.tableKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(formatVersion, dictionaryId, dictionaryName, tableKey, defaultDays);
    }
}
