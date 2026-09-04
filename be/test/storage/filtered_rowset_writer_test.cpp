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

#include "storage/filtered_rowset_writer.h"

#include <sys/stat.h>

#include <filesystem>

#include "fs/fs.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/tenant_ttl_compaction_test_util.h"
#include "testutil/init_test_env.h"

namespace starrocks {

class FilteredRowsetWriterTest : public TenantTtlCompactionTestBase {
protected:
    RowsetWriterContext output_context(const RowsetSharedPtr& source) {
        RowsetWriterContext context;
        context.rowset_id = _engine->next_rowset_id();
        context.tablet_uid = _tablet->tablet_uid();
        context.tablet_id = _tablet->tablet_id();
        context.tablet_schema_hash = _tablet->schema_hash();
        context.partition_id = _tablet->partition_id();
        context.rowset_path_prefix = _tablet->schema_hash_path();
        context.rowset_state = VISIBLE;
        context.tablet_schema = _tablet->tablet_schema();
        context.version = source->version();
        context.segments_overlap = NONOVERLAPPING;
        context.is_compaction = true;
        return context;
    }

    static SegmentFilterPlan keep_plan(uint32_t segment_id, uint32_t rows) {
        return {.src_segment_id = segment_id,
                .action = SegmentFilterAction::KEEP,
                .source_rows = rows,
                .kept_rows = rows,
                .deleted_rows = 0};
    }

    static SegmentFilterPlan drop_plan(uint32_t segment_id, uint32_t rows) {
        return {.src_segment_id = segment_id,
                .action = SegmentFilterAction::DROP,
                .source_rows = rows,
                .kept_rows = 0,
                .deleted_rows = rows};
    }

    static SegmentFilterPlan rewrite_plan(uint32_t segment_id, uint32_t rows,
                                          std::initializer_list<Range<rowid_t>> ranges) {
        auto keep_ranges = std::make_shared<SparseRange<>>(ranges);
        const auto kept_rows = keep_ranges->span_size();
        return {.src_segment_id = segment_id,
                .action = SegmentFilterAction::REWRITE,
                .keep_row_ranges = std::move(keep_ranges),
                .source_rows = rows,
                .kept_rows = kept_rows,
                .deleted_rows = rows - kept_rows};
    }

    std::vector<TenantTtlTestRow> read_rows(const RowsetSharedPtr& rowset) {
        EXPECT_OK(rowset->load());
        const Schema schema = ChunkHelper::convert_schema(rowset->schema());
        auto fs_or = FileSystem::CreateSharedFromString(rowset->rowset_path());
        EXPECT_OK(fs_or.status());
        if (!fs_or.ok()) {
            return {};
        }
        std::vector<TenantTtlTestRow> rows;
        for (const auto& segment : rowset->segments()) {
            SegmentReadOptions options;
            options.fs = fs_or.value();
            OlapReaderStatistics stats;
            options.stats = &stats;
            options.tablet_schema = rowset->schema();
            auto iterator_or = segment->new_iterator(schema, options);
            EXPECT_OK(iterator_or.status());
            if (!iterator_or.ok()) {
                return {};
            }
            auto iterator = iterator_or.value();
            EXPECT_OK(iterator->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
            auto chunk = ChunkHelper::new_chunk(schema, 2);
            while (true) {
                chunk->reset();
                const Status status = iterator->get_next(chunk.get());
                if (status.is_end_of_file()) {
                    break;
                }
                EXPECT_OK(status);
                if (!status.ok()) {
                    break;
                }
                for (size_t i = 0; i < chunk->num_rows(); ++i) {
                    const Datum tenant = chunk->get_column_by_index(1)->get(i);
                    rows.push_back({.event_id = chunk->get_column_by_index(0)->get(i).get_int64(),
                                    .tenant = tenant.is_null()
                                                      ? std::nullopt
                                                      : std::optional<std::string>(tenant.get_slice().to_string()),
                                    .payload = chunk->get_column_by_index(2)->get(i).get_int32()});
                }
            }
            iterator->close();
        }
        return rows;
    }
};

TEST_F(FilteredRowsetWriterTest, KeepsDropsAndRewritesWithCompactDestinationOrdinals) {
    ASSERT_NE(nullptr, create_tablet());
    auto source = add_rowset(Version(2, 2), {{{1, "a", 10}, {2, "b", 20}},
                                             {{3, "drop", 30}, {4, "drop", 40}},
                                             {{5, "x", 50}, {6, std::nullopt, 60}, {7, "y", 70}}});
    ASSERT_NE(nullptr, source);
    ASSERT_OK(source->load());

    const auto context = output_context(source);
    FilteredRowsetWriter writer(context, 1);
    ASSERT_OK(writer.init());
    ASSERT_OK(writer.add_segment(source, keep_plan(0, 2)));
    ASSERT_OK(writer.add_segment(source, drop_plan(1, 2)));
    ASSERT_OK(writer.add_segment(source, rewrite_plan(2, 3, {Range<rowid_t>(0, 2)})));
    auto output_or = writer.build();
    ASSERT_OK(output_or.status());
    auto output = output_or.value();

    ASSERT_EQ(2, output->num_segments());
    EXPECT_EQ(4, output->num_rows());
    EXPECT_EQ(source->version(), output->version());
    EXPECT_EQ(output->rowset_meta()->total_disk_size(),
              output->rowset_meta()->data_disk_size() + output->rowset_meta()->index_disk_size());
    ASSERT_EQ(2, writer.segment_results().size());
    EXPECT_EQ(0, writer.segment_results()[0].src_segment_id);
    EXPECT_EQ(0, writer.segment_results()[0].dst_segment_id);
    EXPECT_EQ(2, writer.segment_results()[1].src_segment_id);
    EXPECT_EQ(1, writer.segment_results()[1].dst_segment_id);

    struct stat source_stat {};
    struct stat linked_stat {};
    ASSERT_EQ(0,
              ::stat(Rowset::segment_file_path(source->rowset_path(), source->rowset_id(), 0).c_str(), &source_stat));
    ASSERT_EQ(0,
              ::stat(Rowset::segment_file_path(output->rowset_path(), output->rowset_id(), 0).c_str(), &linked_stat));
    EXPECT_EQ(source_stat.st_ino, linked_stat.st_ino);
    EXPECT_FALSE(std::filesystem::exists(Rowset::segment_file_path(output->rowset_path(), output->rowset_id(), 2)));

    const auto rows = read_rows(output);
    ASSERT_EQ(4, rows.size());
    EXPECT_EQ(1, rows[0].event_id);
    EXPECT_EQ(2, rows[1].event_id);
    EXPECT_EQ(5, rows[2].event_id);
    EXPECT_EQ(6, rows[3].event_id);
    EXPECT_EQ(std::nullopt, rows[3].tenant);
    EXPECT_EQ(60, rows[3].payload);

    output.reset();
    ASSERT_OK(writer.cleanup());
}

TEST_F(FilteredRowsetWriterTest, RewritesDiscontiguousRangesAcrossAllColumnGroups) {
    ASSERT_NE(nullptr, create_tablet());
    auto source = add_rowset(Version(2, 2), {{{1, "a", 10}, {2, "b", 20}, {3, "c", 30}, {4, "d", 40}, {5, "e", 50}}});
    ASSERT_NE(nullptr, source);
    const auto context = output_context(source);
    FilteredRowsetWriter writer(context, 1);
    ASSERT_OK(writer.init());
    ASSERT_OK(writer.add_segment(source, rewrite_plan(0, 5, {Range<rowid_t>(0, 1), Range<rowid_t>(2, 4)})));
    auto output_or = writer.build();
    ASSERT_OK(output_or.status());
    auto output = output_or.value();

    const auto rows = read_rows(output);
    ASSERT_EQ(3, rows.size());
    EXPECT_EQ(1, rows[0].event_id);
    EXPECT_EQ("a", rows[0].tenant);
    EXPECT_EQ(10, rows[0].payload);
    EXPECT_EQ(3, rows[1].event_id);
    EXPECT_EQ("c", rows[1].tenant);
    EXPECT_EQ(30, rows[1].payload);
    EXPECT_EQ(4, rows[2].event_id);
    EXPECT_EQ("d", rows[2].tenant);
    EXPECT_EQ(40, rows[2].payload);

    output.reset();
    ASSERT_OK(writer.cleanup());
}

TEST_F(FilteredRowsetWriterTest, AllDroppedBuildsVerifiedEmptySameVersionRowset) {
    ASSERT_NE(nullptr, create_tablet(false));
    auto source = add_rowset(Version(2, 2), {{{1, "a", 10}}, {{2, "b", 20}}});
    ASSERT_NE(nullptr, source);
    const auto context = output_context(source);
    FilteredRowsetWriter writer(context);
    ASSERT_OK(writer.init());
    ASSERT_OK(writer.add_segment(source, drop_plan(0, 1)));
    ASSERT_OK(writer.add_segment(source, drop_plan(1, 1)));
    auto output_or = writer.build();
    ASSERT_OK(output_or.status());
    auto output = output_or.value();

    EXPECT_EQ(source->version(), output->version());
    EXPECT_EQ(0, output->num_rows());
    EXPECT_EQ(0, output->num_segments());
    EXPECT_TRUE(output->rowset_meta()->empty());
    EXPECT_TRUE(writer.segment_results().empty());
    ASSERT_OK(output->verify());

    output.reset();
    ASSERT_OK(writer.cleanup());
}

} // namespace starrocks

int main(int argc, char** argv) {
    return starrocks::init_test_env(argc, argv);
}
