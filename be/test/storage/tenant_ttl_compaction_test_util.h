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

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "common/config.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "testutil/assert.h"

namespace starrocks {

struct TenantTtlTestRow {
    int64_t event_id{0};
    std::optional<std::string> tenant;
    int32_t payload{0};
};

struct TenantTtlRowsetSnapshot {
    Version version;
    RowsetId rowset_id;
    int64_t num_rows{0};
    int64_t num_segments{0};
    int64_t total_row_size{0};
    size_t total_disk_size{0};
    size_t data_disk_size{0};
    size_t index_disk_size{0};
    bool empty{false};
    std::vector<std::pair<std::string, uintmax_t>> artifacts;

    bool operator==(const TenantTtlRowsetSnapshot& rhs) const {
        return version == rhs.version && rowset_id == rhs.rowset_id && num_rows == rhs.num_rows &&
               num_segments == rhs.num_segments && total_row_size == rhs.total_row_size &&
               total_disk_size == rhs.total_disk_size && data_disk_size == rhs.data_disk_size &&
               index_disk_size == rhs.index_disk_size && empty == rhs.empty && artifacts == rhs.artifacts;
    }
};

// Shared fixture for Tenant-TTL tests. It intentionally uses a non-tenant sort
// key so correctness tests cannot accidentally depend on tenant sort order.
class TenantTtlCompactionTestBase : public testing::Test {
protected:
    static constexpr int64_t kTabletId = 91901;
    static constexpr int64_t kPartitionId = 91902;
    static constexpr int32_t kSchemaHash = 91903;
    static constexpr int64_t kSchemaId = 91904;
    static constexpr int32_t kSchemaVersion = 0;
    static constexpr int32_t kTenantColumnUniqueId = 2;

    void SetUp() override {
        static std::atomic<int64_t> sequence{0};
        const int64_t suffix = sequence.fetch_add(1, std::memory_order_relaxed);
        _tablet_id = kTabletId + suffix;
        _partition_id = kPartitionId + suffix;
        _old_storage_flood_stage_usage_percent = config::storage_flood_stage_usage_percent;
        _old_storage_flood_stage_left_capacity_bytes = config::storage_flood_stage_left_capacity_bytes;
        // Unit-test build artifacts may leave less than the production flood-stage
        // reserve in the container. The test Tablet data itself is tiny.
        config::storage_flood_stage_usage_percent = 101;
        config::storage_flood_stage_left_capacity_bytes = -1;
        _engine = StorageEngine::instance();
        ASSERT_NE(nullptr, _engine);
    }

    void TearDown() override {
        _tablet.reset();
        if (_engine != nullptr && _engine->tablet_manager()->get_tablet(_tablet_id, false) != nullptr) {
            ASSERT_OK(_engine->tablet_manager()->drop_tablet(_tablet_id, kDeleteFiles));
        }
        _engine = nullptr;
        config::storage_flood_stage_usage_percent = _old_storage_flood_stage_usage_percent;
        config::storage_flood_stage_left_capacity_bytes = _old_storage_flood_stage_left_capacity_bytes;
    }

    TabletSharedPtr create_tablet(bool tenant_nullable = true) {
        TCreateTabletReq request;
        request.tablet_id = _tablet_id;
        request.__set_partition_id(_partition_id);
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = kSchemaHash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.__set_id(kSchemaId);
        request.tablet_schema.__set_schema_version(kSchemaVersion);
        request.tablet_schema.keys_type = TKeysType::DUP_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn event_id;
        event_id.__set_column_name("event_id");
        event_id.__set_col_unique_id(1);
        event_id.__set_is_key(true);
        event_id.__set_is_allow_null(false);
        event_id.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.emplace_back(std::move(event_id));

        TColumn tenant;
        tenant.__set_column_name("tenant");
        tenant.__set_col_unique_id(kTenantColumnUniqueId);
        tenant.__set_is_key(false);
        tenant.__set_is_allow_null(tenant_nullable);
        tenant.column_type.type = TPrimitiveType::VARCHAR;
        tenant.column_type.__set_len(128);
        tenant.__set_aggregation_type(TAggregationType::NONE);
        request.tablet_schema.columns.emplace_back(std::move(tenant));

        TColumn payload;
        payload.__set_column_name("payload");
        payload.__set_col_unique_id(3);
        payload.__set_is_key(false);
        payload.__set_is_allow_null(false);
        payload.column_type.type = TPrimitiveType::INT;
        payload.__set_aggregation_type(TAggregationType::NONE);
        request.tablet_schema.columns.emplace_back(std::move(payload));

        EXPECT_OK(_engine->create_tablet(request));
        _tablet = _engine->tablet_manager()->get_tablet(_tablet_id, false);
        EXPECT_NE(nullptr, _tablet);
        return _tablet;
    }

    RowsetSharedPtr add_rowset(const Version& version, const std::vector<std::vector<TenantTtlTestRow>>& segments) {
        EXPECT_NE(nullptr, _tablet);
        RowsetWriterContext context;
        context.rowset_id = _engine->next_rowset_id();
        context.tablet_uid = _tablet->tablet_uid();
        context.tablet_id = _tablet->tablet_id();
        context.tablet_schema_hash = _tablet->schema_hash();
        context.partition_id = _tablet->partition_id();
        context.rowset_path_prefix = _tablet->schema_hash_path();
        context.rowset_state = VISIBLE;
        context.tablet_schema = _tablet->tablet_schema();
        context.version = version;
        context.segments_overlap = NONOVERLAPPING;

        std::unique_ptr<RowsetWriter> writer;
        EXPECT_OK(RowsetFactory::create_rowset_writer(context, &writer));
        const auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
        for (const auto& segment : segments) {
            auto chunk = ChunkHelper::new_chunk(schema, segment.size());
            for (const auto& row : segment) {
                chunk->get_column_by_index(0)->append_datum(Datum(row.event_id));
                if (row.tenant.has_value()) {
                    chunk->get_column_by_index(1)->append_datum(Datum(Slice(row.tenant.value())));
                } else {
                    EXPECT_TRUE(chunk->get_column_by_index(1)->append_nulls(1));
                }
                chunk->get_column_by_index(2)->append_datum(Datum(row.payload));
            }
            EXPECT_OK(writer->add_chunk(*chunk));
            EXPECT_OK(writer->flush());
        }
        if (segments.empty()) {
            EXPECT_OK(writer->flush());
        }
        auto rowset_or = writer->build();
        EXPECT_OK(rowset_or.status());
        if (!rowset_or.ok()) {
            return nullptr;
        }
        RowsetSharedPtr rowset = std::move(rowset_or).value();
        EXPECT_OK(_tablet->add_rowset(rowset, false));
        return rowset;
    }

    std::vector<TenantTtlRowsetSnapshot> snapshot_active_rowsets() const {
        std::vector<TenantTtlRowsetSnapshot> snapshot;
        std::shared_lock lock(_tablet->get_header_lock());
        auto max_rowset = _tablet->rowset_with_max_version();
        if (max_rowset == nullptr) {
            return snapshot;
        }
        std::vector<RowsetSharedPtr> rowsets;
        EXPECT_OK(_tablet->capture_consistent_rowsets(Version(0, max_rowset->end_version()), &rowsets));
        for (const auto& rowset : rowsets) {
            TenantTtlRowsetSnapshot rowset_snapshot{.version = rowset->version(),
                                                    .rowset_id = rowset->rowset_id(),
                                                    .num_rows = rowset->num_rows(),
                                                    .num_segments = rowset->num_segments(),
                                                    .total_row_size = rowset->rowset_meta()->total_row_size(),
                                                    .total_disk_size = rowset->rowset_meta()->total_disk_size(),
                                                    .data_disk_size = rowset->rowset_meta()->data_disk_size(),
                                                    .index_disk_size = rowset->rowset_meta()->index_disk_size(),
                                                    .empty = rowset->rowset_meta()->empty()};
            const auto rowset_id_prefix = rowset->rowset_id().to_string() + "_";
            for (const auto& entry : std::filesystem::directory_iterator(_tablet->schema_hash_path())) {
                const auto filename = entry.path().filename().string();
                if (filename.starts_with(rowset_id_prefix)) {
                    rowset_snapshot.artifacts.emplace_back(filename, entry.is_regular_file() ? entry.file_size() : 0);
                }
            }
            std::sort(rowset_snapshot.artifacts.begin(), rowset_snapshot.artifacts.end());
            snapshot.emplace_back(std::move(rowset_snapshot));
        }
        return snapshot;
    }

    StorageEngine* _engine{nullptr};
    TabletSharedPtr _tablet;
    int64_t _tablet_id{0};
    int64_t _partition_id{0};
    int32_t _old_storage_flood_stage_usage_percent{0};
    int64_t _old_storage_flood_stage_left_capacity_bytes{0};
};

} // namespace starrocks
