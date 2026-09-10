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

import com.starrocks.catalog.TenantTtlDictionaryBinding;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Owns the runtime state derived from persisted Tenant-TTL table bindings.
 *
 * <p>This first implementation establishes only the idempotent reference index and generation fencing. Snapshot
 * export and publication are added in the policy-snapshot implementation rounds.
 */
public class TenantTtlPolicySnapshotManager {
    private final Map<Long, Set<TableRef>> references = new HashMap<>();
    private final Map<Long, Long> generations = new HashMap<>();

    public synchronized void updateBinding(long dbId, long tableId, TenantTtlDictionaryBinding oldBinding,
                                           TenantTtlDictionaryBinding newBinding) {
        TableRef tableRef = new TableRef(dbId, tableId);
        if (oldBinding != null && (newBinding == null ||
                oldBinding.getDictionaryId() != newBinding.getDictionaryId())) {
            removeReference(oldBinding.getDictionaryId(), tableRef);
        }
        if (newBinding != null) {
            references.computeIfAbsent(newBinding.getDictionaryId(), ignored -> new HashSet<>()).add(tableRef);
        }
    }

    public synchronized void removeTable(long dbId, long tableId, TenantTtlDictionaryBinding binding) {
        if (binding != null) {
            removeReference(binding.getDictionaryId(), new TableRef(dbId, tableId));
        }
    }

    public synchronized void onDictionaryDropped(long dictionaryId) {
        generations.merge(dictionaryId, 1L, Long::sum);
    }

    public synchronized Set<TableRef> getReferencedTables(long dictionaryId) {
        Set<TableRef> tableRefs = references.get(dictionaryId);
        if (tableRefs == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(new HashSet<>(tableRefs));
    }

    public synchronized boolean isReferenced(long dictionaryId) {
        Set<TableRef> tableRefs = references.get(dictionaryId);
        return tableRefs != null && !tableRefs.isEmpty();
    }

    public synchronized long getGeneration(long dictionaryId) {
        return generations.getOrDefault(dictionaryId, 0L);
    }

    public synchronized void clearReferencesForRebuild() {
        references.clear();
        generations.clear();
    }

    private void removeReference(long dictionaryId, TableRef tableRef) {
        Set<TableRef> tableRefs = references.get(dictionaryId);
        if (tableRefs == null || !tableRefs.remove(tableRef)) {
            return;
        }
        if (tableRefs.isEmpty()) {
            references.remove(dictionaryId);
            generations.merge(dictionaryId, 1L, Long::sum);
        }
    }

    public static final class TableRef {
        private final long dbId;
        private final long tableId;

        public TableRef(long dbId, long tableId) {
            this.dbId = dbId;
            this.tableId = tableId;
        }

        public long getDbId() {
            return dbId;
        }

        public long getTableId() {
            return tableId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TableRef)) {
                return false;
            }
            TableRef tableRef = (TableRef) o;
            return dbId == tableRef.dbId && tableId == tableRef.tableId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbId, tableId);
        }
    }
}
