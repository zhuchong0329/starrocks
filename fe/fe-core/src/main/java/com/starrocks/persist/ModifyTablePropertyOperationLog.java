// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.TenantTtlDictionaryBinding;
import com.starrocks.catalog.TenantTtlTableBinding;
import com.starrocks.common.io.Writable;

import java.util.HashMap;
import java.util.Map;

public class ModifyTablePropertyOperationLog implements Writable {

    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "properties")
    private Map<String, String> properties = new HashMap<>();
    @SerializedName(value = "comment")
    private String comment;
    @SerializedName(value = "tenantTtlDictionaryBinding")
    private TenantTtlDictionaryBinding tenantTtlDictionaryBinding;
    @SerializedName(value = "tenantTtlTableBinding")
    private TenantTtlTableBinding tenantTtlTableBinding;

    public ModifyTablePropertyOperationLog(long dbId, long tableId) {
        this.dbId = dbId;
        this.tableId = tableId;
    }

    public ModifyTablePropertyOperationLog(long dbId, long tableId, Map<String, String> properties) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.properties = properties;
    }

    public ModifyTablePropertyOperationLog(long dbId, long tableId, Map<String, String> properties,
                                           TenantTtlDictionaryBinding tenantTtlDictionaryBinding,
                                           TenantTtlTableBinding tenantTtlTableBinding) {
        this(dbId, tableId, properties);
        this.tenantTtlDictionaryBinding = tenantTtlDictionaryBinding;
        this.tenantTtlTableBinding = tenantTtlTableBinding;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getComment() {
        return comment;
    }

    public TenantTtlDictionaryBinding getTenantTtlDictionaryBinding() {
        return tenantTtlDictionaryBinding;
    }

    public TenantTtlTableBinding getTenantTtlTableBinding() {
        return tenantTtlTableBinding;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

}
