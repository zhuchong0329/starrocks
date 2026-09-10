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

#include "storage/dictionary_cache_manager.h"

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <snappy/snappy.h>

#include <fstream>
#include <set>
#include <tuple>

#include "exec/tablet_info.h"
#include "exprs/dictionary_get_expr.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/descriptor_helper.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "testutil/assert.h"
#include "testutil/column_test_helper.h"
#include "testutil/exprs_test_helper.h"
#include "testutil/sync_point.h"
#include "util/crc32c.h"

namespace starrocks {

class DictionaryCacheManagerTest : public testing::Test {
public:
    ~DictionaryCacheManagerTest() override = default;

    void SetUp() override {}

    void TearDown() override {
        if (test_tablet) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(test_tablet->tablet_id());
            test_tablet.reset();
            test_tablet = nullptr;
        }
    }

    static TCreateTabletReq get_create_tablet_request(int64_t tablet_id, int64_t schema_hash,
                                                      const std::vector<TColumn>* tcolumns = nullptr) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "k1";
        k1.__set_is_key(true);
        k1.__set_default_value("1");
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "k2";
        k2.__set_is_key(true);
        k2.__set_default_value("2");
        k2.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "k3";
        k3.__set_is_key(true);
        k3.__set_default_value("3");
        k3.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k3);

        if (tcolumns != nullptr) {
            for (auto tcolumn : *tcolumns) {
                request.tablet_schema.columns.push_back(tcolumn);
            }
        }

        return request;
    }

    static void create_new_dictionary_cache(starrocks::DictionaryCacheManager* dictionary_cache_manager, int64_t dict,
                                            int64_t txn_id, TabletSharedPtr tablet,
                                            const std::vector<TColumn>* tcolumns = nullptr) {
        auto schema = ChunkHelper::convert_schema(tablet->thread_safe_get_tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, 0);
        chunk->reset_slot_id_to_index();
        for (size_t i = 0; i < chunk->num_columns(); ++i) {
            chunk->set_slot_id_to_index(i + 1, i);
            if (i < 3) {
                down_cast<Int64Column*>(chunk->get_column_by_index(i).get())->append(i);
            } else {
                std::string s(60000, 'a');
                down_cast<BinaryColumnBase<uint32_t>*>(chunk->get_column_by_index(i).get())->append_string(s);
            }
        }

        std::unique_ptr<ChunkPB> pchunk = std::make_unique<ChunkPB>();
        DictionaryCacheWriter::ChunkUtil::compress_and_serialize_chunk(chunk.get(), pchunk.get());

        TOlapTableSchemaParam tschema;
        tschema.db_id = 1;
        tschema.table_id = 1;
        tschema.version = 0;

        // descriptor
        {
            TDescriptorTableBuilder dtb;
            TTupleDescriptorBuilder tuple_builder;

            tuple_builder.add_slot(
                    TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("k1").column_pos(1).id(1).build());
            tuple_builder.add_slot(
                    TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("k2").column_pos(2).id(2).build());
            tuple_builder.add_slot(
                    TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("k3").column_pos(3).id(3).build());

            if (tcolumns != nullptr) {
                for (int i = 0; i < tcolumns->size(); ++i) {
                    std::string column_name = "large_column_" + std::to_string(i);
                    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                                   .string_type(60000)
                                                   .column_name(column_name)
                                                   .column_pos(i + 4)
                                                   .id(i + 4)
                                                   .build());
                }
            }

            tuple_builder.build(&dtb);

            auto desc_tbl = dtb.desc_tbl();
            tschema.slot_descs = desc_tbl.slotDescriptors;
            tschema.tuple_desc = desc_tbl.tupleDescriptors[0];
        }
        // index
        tschema.indexes.resize(1);
        tschema.indexes[0].id = 4;
        tschema.indexes[0].columns = {"k1", "k2", "k3"};
        if (tcolumns != nullptr) {
            for (auto tcolumn : *tcolumns) {
                tschema.indexes[0].columns.emplace_back(tcolumn.column_name);
            }
        }
        auto req = get_create_tablet_request(0, 0, tcolumns);
        TOlapTableColumnParam column_param;
        column_param.columns = (req.tablet_schema).columns;
        tschema.indexes[0].__set_column_param(column_param);

        OlapTableSchemaParam olap_schema;
        olap_schema.init(tschema);
        auto pschema = std::make_unique<POlapTableSchemaParam>();
        olap_schema.to_protobuf(pschema.get());

        PProcessDictionaryCacheRequest request;
        request.set_allocated_chunk(pchunk.get());
        request.set_dict_id(dict);
        request.set_txn_id(txn_id);
        request.set_allocated_schema(pschema.get());
        int64_t memory_limit = 1;
        memory_limit *= 1024;
        memory_limit *= 1024;
        memory_limit *= 1024;
        memory_limit *= 1024;

        request.set_memory_limit(memory_limit);
        request.set_key_size(1);
        request.set_type(PProcessDictionaryCacheRequestType::REFRESH);

        ASSERT_TRUE(dictionary_cache_manager->begin(&request).ok());
        ASSERT_TRUE(dictionary_cache_manager->refresh(&request).ok());
        ASSERT_TRUE(dictionary_cache_manager->commit(&request).ok());

        request.release_chunk();
        request.release_schema();
    }

    static TabletSharedPtr create_tablet(int64_t tablet_id, int64_t schema_hash,
                                         const std::vector<TColumn>* tcolumns = nullptr) {
        auto st = StorageEngine::instance()->create_tablet(get_create_tablet_request(tablet_id, schema_hash, tcolumns));
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    static void read_dictionary(starrocks::DictionaryCacheManager* dictionary_cache_manager, TabletSharedPtr tablet,
                                int64_t dict_id, int64_t txn_id) {
        auto res = dictionary_cache_manager->get_dictionary_by_version(dict_id, txn_id);
        ASSERT_TRUE(res.ok());
        DictionaryCachePtr dictionary = std::move(res.value());
        ASSERT_TRUE(dictionary.get() != nullptr);

        auto schema = dictionary_cache_manager->get_dictionary_schema_by_id(dict_id);
        auto chunk = ChunkHelper::new_chunk(*schema, 0);
        chunk->reset_slot_id_to_index();
        for (size_t i = 0; i < chunk->num_columns(); ++i) {
            chunk->set_slot_id_to_index(i + 1, i);
            if (i < 3) {
                down_cast<Int64Column*>(chunk->get_column_by_index(i).get())->append(i);
            } else {
                std::string s(60000, 'a');
                down_cast<BinaryColumnBase<uint32_t>*>(chunk->get_column_by_index(i).get())->append_string(s);
            }
        }
        std::vector<ColumnId> kids{0};
        std::vector<ColumnId> vids;
        for (ColumnId id = 1; id < chunk->num_columns(); id++) {
            vids.emplace_back(id);
        }

        ChunkPtr key_chunk = ChunkHelper::new_chunk(Schema(schema.get(), kids), 0);
        key_chunk->get_column_by_index(0)->append(*chunk->get_column_by_index(0));

        ChunkPtr value_chunk = ChunkHelper::new_chunk(Schema(schema.get(), vids), 0);
        auto st = DictionaryCacheManager::probe_given_dictionary_cache(
                *key_chunk->schema().get(), *value_chunk->schema().get(), dictionary, key_chunk, value_chunk, nullptr);
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(value_chunk->num_rows() == 1);
        for (int i = 0; i < value_chunk->num_columns(); ++i) {
            auto column_1 = value_chunk->get_column_by_index(i);
            auto column_2 = chunk->get_column_by_index(i + 1);
            ASSERT_TRUE(column_1->equals(0, *column_2, 0));
        }
    }

    static MockExpr* new_mock_expr(ColumnPtr value, const LogicalType& type, ObjectPool& objpool) {
        return new_mock_expr(std::move(value), TypeDescriptor(type), objpool);
    }

    static MockExpr* new_mock_expr(ColumnPtr value, const TypeDescriptor& type, ObjectPool& objpool) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::INT_LITERAL);
        node.__set_num_children(0);
        node.__set_type(type.to_thrift());
        MockExpr* e = objpool.add(new MockExpr(node, std::move(value)));
        return e;
    }

    using TenantTtlEntry = std::tuple<std::string, std::string, int32_t>;

    static void create_tenant_ttl_dictionary_cache(DictionaryCacheManager* manager, int64_t dict_id, int64_t txn_id,
                                                   const std::vector<TenantTtlEntry>& entries,
                                                   const std::string& table_name_column = "table_name") {
        Fields fields{
                std::make_shared<Field>(0, "tenant", TYPE_VARCHAR, false),
                std::make_shared<Field>(1, table_name_column, TYPE_VARCHAR, false),
                std::make_shared<Field>(2, "retention_days", TYPE_INT, false),
        };
        Schema chunk_schema(fields);
        auto chunk = ChunkHelper::new_chunk(chunk_schema, entries.size());
        chunk->reset_slot_id_to_index();
        for (size_t i = 0; i < chunk->num_columns(); ++i) {
            chunk->set_slot_id_to_index(i + 1, i);
        }
        for (const auto& [tenant, table_name, retention_days] : entries) {
            down_cast<BinaryColumn*>(chunk->get_column_by_index(0).get())->append(Slice(tenant));
            down_cast<BinaryColumn*>(chunk->get_column_by_index(1).get())->append(Slice(table_name));
            down_cast<Int32Column*>(chunk->get_column_by_index(2).get())->append(retention_days);
        }

        auto pchunk = std::make_unique<ChunkPB>();
        ASSERT_OK(DictionaryCacheWriter::ChunkUtil::compress_and_serialize_chunk(chunk.get(), pchunk.get()));

        TDescriptorTableBuilder descriptor_builder;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(128)
                                       .column_name("tenant")
                                       .column_pos(1)
                                       .id(1)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(256)
                                       .column_name(table_name_column)
                                       .column_pos(2)
                                       .id(2)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_INT)
                                       .column_name("retention_days")
                                       .column_pos(3)
                                       .id(3)
                                       .build());
        tuple_builder.build(&descriptor_builder);

        TOlapTableSchemaParam thrift_schema;
        thrift_schema.db_id = 1;
        thrift_schema.table_id = 1;
        thrift_schema.version = 0;
        auto descriptor_table = descriptor_builder.desc_tbl();
        thrift_schema.slot_descs = descriptor_table.slotDescriptors;
        thrift_schema.tuple_desc = descriptor_table.tupleDescriptors[0];
        thrift_schema.indexes.resize(1);
        thrift_schema.indexes[0].id = 1;
        thrift_schema.indexes[0].columns = {"tenant", table_name_column, "retention_days"};

        TColumn tenant_column;
        tenant_column.column_name = "tenant";
        tenant_column.__set_is_key(true);
        tenant_column.column_type.type = TPrimitiveType::VARCHAR;
        tenant_column.column_type.__set_len(128);
        TColumn table_name_column_descriptor;
        table_name_column_descriptor.column_name = table_name_column;
        table_name_column_descriptor.__set_is_key(true);
        table_name_column_descriptor.column_type.type = TPrimitiveType::VARCHAR;
        table_name_column_descriptor.column_type.__set_len(256);
        TColumn retention_days_column;
        retention_days_column.column_name = "retention_days";
        retention_days_column.__set_is_key(false);
        retention_days_column.column_type.type = TPrimitiveType::INT;
        TOlapTableColumnParam column_param;
        column_param.columns = {tenant_column, table_name_column_descriptor, retention_days_column};
        thrift_schema.indexes[0].__set_column_param(column_param);

        OlapTableSchemaParam schema;
        ASSERT_OK(schema.init(thrift_schema));
        auto protobuf_schema = std::make_unique<POlapTableSchemaParam>();
        schema.to_protobuf(protobuf_schema.get());

        PProcessDictionaryCacheRequest request;
        request.set_allocated_chunk(pchunk.get());
        request.set_dict_id(dict_id);
        request.set_txn_id(txn_id);
        request.set_allocated_schema(protobuf_schema.get());
        request.set_memory_limit(1024L * 1024 * 1024);
        request.set_key_size(2);
        request.set_type(PProcessDictionaryCacheRequestType::REFRESH);
        ASSERT_OK(manager->begin(&request));
        ASSERT_OK(manager->refresh(&request));
        ASSERT_OK(manager->commit(&request));
        request.release_chunk();
        request.release_schema();
    }

    static std::vector<TenantTtlEntry> decode_export(const PExportDictionaryCacheResult& response) {
        std::vector<TenantTtlEntry> entries;
        uint32_t content_crc = 0;
        for (int i = 0; i < response.batches_size(); ++i) {
            const auto& compressed_batch = response.batches(i);
            EXPECT_EQ(i, compressed_batch.sequence());
            std::string uncompressed;
            if (compressed_batch.compression_type() == CompressionTypePB::SNAPPY) {
                uncompressed.resize(compressed_batch.uncompressed_size());
                EXPECT_TRUE(snappy::RawUncompress(compressed_batch.payload().data(), compressed_batch.payload().size(),
                                                  uncompressed.data()));
            } else {
                uncompressed = compressed_batch.payload();
            }
            EXPECT_EQ(compressed_batch.uncompressed_size(), uncompressed.size());
            EXPECT_EQ(compressed_batch.uncompressed_crc32c(), crc32c::Value(uncompressed.data(), uncompressed.size()));
            content_crc = crc32c::Extend(content_crc, uncompressed.data(), uncompressed.size());
            PTenantTtlPolicyBatchPB batch;
            EXPECT_TRUE(batch.ParseFromString(uncompressed));
            EXPECT_EQ(compressed_batch.row_count(), batch.entries_size());
            for (const auto& entry : batch.entries()) {
                entries.emplace_back(entry.tenant(), entry.table_name(), entry.retention_days());
            }
        }
        EXPECT_EQ(response.content_crc32c(), content_crc);
        return entries;
    }

    starrocks::DictionaryCacheManager* dictionary_cache_manager = StorageEngine::instance()->dictionary_cache_manager();
    TabletSharedPtr test_tablet = nullptr;
};

// NOLINTNEXTLINE
TEST_F(DictionaryCacheManagerTest, concurrent_refresh_and_read) {
    auto test_tablet = create_tablet(9143, 6543);

    int N = 100;
    std::vector<std::thread> pool;
    for (int i = 0; i < N; ++i) {
        pool.emplace_back(create_new_dictionary_cache, dictionary_cache_manager, i + N, 1, test_tablet, nullptr);
    }

    for (int i = 0; i < N; ++i) {
        pool[i].join();
    }

    std::vector<std::thread> read_pool;
    for (int i = 0; i < 1; ++i) {
        read_pool.emplace_back(read_dictionary, dictionary_cache_manager, test_tablet, i + N, 1);
    }

    for (int i = 0; i < 1; ++i) {
        read_pool[i].join();
    }
}

// NOLINTNEXTLINE
TEST_F(DictionaryCacheManagerTest, large_column_refresh_and_read) {
    int N = 100;
    std::vector<TColumn> large_string_column;
    for (size_t i = 0; i < N; i++) {
        TColumn k;
        k.column_name = "large_column_" + std::to_string(i);
        k.__set_is_key(true);
        k.__set_default_value("");
        k.column_type.type = TPrimitiveType::VARCHAR;
        large_string_column.push_back(k);
    }
    auto test_tablet = create_tablet(9144, 6544, &large_string_column);
    create_new_dictionary_cache(dictionary_cache_manager, 300, 301, test_tablet, &large_string_column);
    read_dictionary(dictionary_cache_manager, test_tablet, 300, 301);
}

// NOLINTNEXTLINE
TEST_F(DictionaryCacheManagerTest, dictionary_get_expr_test) {
    auto test_tablet = create_tablet(9145, 6545);
    create_new_dictionary_cache(dictionary_cache_manager, 400, 1, test_tablet);
    ObjectPool objpool;

    TypeDescriptor type;
    type.type = LogicalType::TYPE_STRUCT;
    type.field_names.emplace_back("k2");
    type.field_names.emplace_back("k3");
    type.children.emplace_back();
    type.children.back().type = LogicalType::TYPE_BIGINT;
    type.children.emplace_back();
    type.children.back().type = LogicalType::TYPE_BIGINT;

    TDictionaryGetExpr t_dictionary_get_expr;
    t_dictionary_get_expr.__set_dict_id(400);
    t_dictionary_get_expr.__set_txn_id(1);
    t_dictionary_get_expr.__set_key_size(1);

    TExprNode node;
    node.__set_node_type(TExprNodeType::DICTIONARY_GET_EXPR);
    node.__set_is_nullable(false);
    node.__set_type(type.to_thrift());
    node.__set_num_children(0);
    node.__set_dictionary_get_expr(t_dictionary_get_expr);

    auto dictionary_get_expr = std::make_unique<DictionaryGetExpr>(node);

    TypeDescriptor type_varchar(LogicalType::TYPE_VARCHAR);
    type_varchar.len = 100;
    std::string dictionary_name("dictionary_name");
    Slice slice(dictionary_name);
    dictionary_get_expr->add_child(
            new_mock_expr(ColumnTestHelper::build_column<Slice>({slice}), type_varchar, objpool));

    dictionary_get_expr->add_child(
            new_mock_expr(ColumnTestHelper::build_column<long>({0}), LogicalType::TYPE_BIGINT, objpool));

    ASSERT_TRUE(dictionary_get_expr->prepare(nullptr, nullptr).ok());
    auto res = dictionary_get_expr->evaluate_checked(nullptr, nullptr);
    ASSERT_TRUE(res.ok());

    auto res_column = std::move(res.value());
    ASSERT_TRUE(res_column->size() == 1);
    auto struct_column = down_cast<StructColumn*>(down_cast<NullableColumn*>(res_column.get())->data_column().get());
    ASSERT_TRUE(struct_column->fields_column().size() == 2);
}

// NOLINTNEXTLINE
TEST_F(DictionaryCacheManagerTest, tenant_ttl_export_exact_snapshot_and_limits) {
    DictionaryCacheManager manager;
    std::vector<TenantTtlEntry> original{{std::string("a\0b", 3), "business.http_log", 30},
                                         {"租户", "业务.日志", 180}};
    create_tenant_ttl_dictionary_cache(&manager, 500, 501, original);

    PExportDictionaryCacheRequest request;
    request.set_protocol_version(1);
    request.set_dictionary_id(500);
    request.set_expected_txn_id(501);
    request.set_max_rows(100000);
    request.set_max_uncompressed_bytes(64L * 1024 * 1024);
    request.set_max_response_bytes(64L * 1024 * 1024);
    PExportDictionaryCacheResult response;
    ASSERT_OK(manager.export_cache(&request, &response));
    ASSERT_EQ(PDictionaryCacheExportOutcome::EXPORT_OK, response.outcome());
    ASSERT_TRUE(response.complete());
    ASSERT_EQ(2, response.total_row_count());
    ASSERT_EQ(response.batch_count(), response.batches_size());
    auto decoded = decode_export(response);
    std::set<TenantTtlEntry> expected(original.begin(), original.end());
    ASSERT_EQ(expected, std::set<TenantTtlEntry>(decoded.begin(), decoded.end()));

    request.set_expected_txn_id(500);
    ASSERT_OK(manager.export_cache(&request, &response));
    ASSERT_EQ(PDictionaryCacheExportOutcome::VERSION_MISMATCH, response.outcome());
    ASSERT_EQ(501, response.actual_txn_id());
    ASSERT_EQ(0, response.batches_size());

    request.set_expected_txn_id(501);
    request.set_max_rows(1);
    ASSERT_OK(manager.export_cache(&request, &response));
    ASSERT_EQ(PDictionaryCacheExportOutcome::LIMIT_EXCEEDED, response.outcome());
    ASSERT_EQ(0, response.batches_size());

    request.set_max_rows(100000);
    request.set_max_uncompressed_bytes(1);
    ASSERT_OK(manager.export_cache(&request, &response));
    ASSERT_EQ(PDictionaryCacheExportOutcome::LIMIT_EXCEEDED, response.outcome());
    ASSERT_EQ(0, response.batches_size());

    request.set_max_uncompressed_bytes(64L * 1024 * 1024);
    request.set_max_response_bytes(1);
    ASSERT_OK(manager.export_cache(&request, &response));
    ASSERT_EQ(PDictionaryCacheExportOutcome::LIMIT_EXCEEDED, response.outcome());
    ASSERT_EQ(0, response.batches_size());

    request.set_max_response_bytes(64L * 1024 * 1024);
    request.set_dictionary_id(999);
    ASSERT_OK(manager.export_cache(&request, &response));
    ASSERT_EQ(PDictionaryCacheExportOutcome::CACHE_NOT_FOUND, response.outcome());

    create_tenant_ttl_dictionary_cache(&manager, 550, 551, {});
    request.set_dictionary_id(550);
    request.set_expected_txn_id(551);
    ASSERT_OK(manager.export_cache(&request, &response));
    ASSERT_EQ(PDictionaryCacheExportOutcome::EXPORT_OK, response.outcome());
    ASSERT_TRUE(response.complete());
    ASSERT_EQ(0, response.total_row_count());
    ASSERT_EQ(0, response.batches_size());

    create_tenant_ttl_dictionary_cache(&manager, 600, 601, original, "wrong_table_name");
    request.set_dictionary_id(600);
    request.set_expected_txn_id(601);
    request.set_max_rows(100000);
    ASSERT_OK(manager.export_cache(&request, &response));
    ASSERT_EQ(PDictionaryCacheExportOutcome::SCHEMA_MISMATCH, response.outcome());
}

// NOLINTNEXTLINE
TEST_F(DictionaryCacheManagerTest, tenant_ttl_export_holds_captured_cache) {
    DictionaryCacheManager manager;
    std::vector<TenantTtlEntry> old_entries{{"old", "business.http_log", 30}};
    std::vector<TenantTtlEntry> new_entries{{"new", "business.http_log", 90}};
    create_tenant_ttl_dictionary_cache(&manager, 700, 701, old_entries);

    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("DictionaryCacheManager::export_cache:after_capture", [&](void*) {
        create_tenant_ttl_dictionary_cache(&manager, 700, 702, new_entries);
    });
    PExportDictionaryCacheRequest request;
    request.set_dictionary_id(700);
    request.set_expected_txn_id(701);
    PExportDictionaryCacheResult response;
    Status export_status = manager.export_cache(&request, &response);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    ASSERT_OK(export_status);
    ASSERT_EQ(PDictionaryCacheExportOutcome::EXPORT_OK, response.outcome());
    ASSERT_EQ(701, response.actual_txn_id());
    ASSERT_EQ(old_entries, decode_export(response));
    ASSERT_TRUE(manager.get_dictionary_by_version(700, 702).ok());
}

} // namespace starrocks
