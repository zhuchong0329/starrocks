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

#include <boost/algorithm/string/predicate.hpp>
#include <google/protobuf/io/coded_stream.h>
#include <snappy/snappy.h>

#include <optional>

#include "column/binary_column.h"
#include "common/config.h"
#include "exec/tablet_info.h"
#include "testutil/sync_point.h"
#include "util/crc32c.h"

namespace {

constexpr int32_t kTenantTtlExportProtocolVersion = 1;
constexpr int64_t kTenantTtlExportBatchBytes = 1024 * 1024;
constexpr size_t kTenantTtlDecodeBatchRows = 1024;

int64_t effective_limit(bool has_request_limit, int64_t request_limit, int64_t local_limit) {
    return has_request_limit ? std::min(request_limit, local_limit) : local_limit;
}

} // namespace

namespace starrocks {

Status DictionaryCacheManager::begin(const PProcessDictionaryCacheRequest* request) {
    auto dict_id = request->dict_id();
    auto txn_id = request->txn_id();
    std::unique_lock wlock(_refresh_lock);
    if (LIKELY(_mutable_dict_caches.find(dict_id) == _mutable_dict_caches.end())) {
        _mutable_dict_caches[dict_id] = std::make_shared<OrderedMutableDictionaryCache>();
    } // It is ok, if dictionary id is duplicated

    if (LIKELY(_mutable_dict_caches[dict_id]->find(txn_id) == _mutable_dict_caches[dict_id]->end())) {
        (*_mutable_dict_caches[dict_id])[txn_id] = nullptr;
    } else {
        return Status::InternalError(
                fmt::format("duplicated dictionary cache refresh task, duplicated txn id: {}", txn_id));
    }

    return Status::OK();
}

Status DictionaryCacheManager::refresh(const PProcessDictionaryCacheRequest* request) {
    const auto& dict_id = request->dict_id();
    const auto& txn_id = request->txn_id();
    const auto& pchunk = request->chunk();
    const auto& pschema = request->schema();
    const auto& memory_limit = request->memory_limit();

    // 1. uncompress and deserialize chunk
    faststring uncompressed_buffer;
    auto schema = std::make_shared<OlapTableSchemaParam>();
    RETURN_IF_ERROR(schema->init(pschema));
    auto chunk = std::make_unique<Chunk>();
    RETURN_IF_ERROR(DictionaryCacheWriter::ChunkUtil::uncompress_and_deserialize_chunk(
            pchunk, *chunk.get(), &uncompressed_buffer, schema.get()));
    RETURN_IF_ERROR(DictionaryCacheWriter::ChunkUtil::check_chunk_has_null(*chunk.get()));

    // 2. split into key chunk and value chunk in ordered
    std::vector<std::string_view> col_names;
    std::vector<SlotId> key_slot_ids;
    std::vector<SlotId> value_slot_ids;
    for (int i = 0; i < schema->tuple_desc()->slots().size(); ++i) {
        const string& name = schema->tuple_desc()->slots()[i]->col_name();
        col_names.emplace_back(name.data(), name.size());

        if (i < request->key_size()) {
            key_slot_ids.emplace_back(schema->tuple_desc()->slots()[i]->id());
        } else {
            value_slot_ids.emplace_back(schema->tuple_desc()->slots()[i]->id());
        }
    }

    // dictionary definition: col2 key, col3 key, col1 value, col4 value
    // OlapTableSchemaParam->tuple_desc()->slots(): col2 key, col3 key, col1 value, col4 value (with nullable attribute)
    // OlapTableSchemaParam->indexes()[0]->column_param->columns: col1, col2, col3, col4 (with nullable attribute)
    // chunk schema: col1, col2, col3, col4 (with nullable attribute)
    // dictionary_schema: col2, col3, col1, col4 (without nullable attribute)
    SchemaPtr dictionary_schema = ChunkHelper::convert_schema((schema->indexes()[0])->column_param->columns, col_names);
    DCHECK(dictionary_schema != nullptr);
    std::vector<int> keys(dictionary_schema->fields().size(), 1);
    // remove the nullable attribute if necessary
    dictionary_schema = ChunkHelper::get_non_nullable_schema(dictionary_schema, &keys);

    std::vector<ColumnId> key_col_ids(request->key_size());
    std::vector<ColumnId> value_col_ids(dictionary_schema->fields().size() - request->key_size());
    std::iota(key_col_ids.begin(), key_col_ids.end(), 0);
    std::iota(value_col_ids.begin(), value_col_ids.end(), request->key_size());

    SchemaPtr key_schema = std::make_shared<Schema>(dictionary_schema.get(), key_col_ids);
    SchemaPtr value_schema = std::make_shared<Schema>(dictionary_schema.get(), value_col_ids);

    ChunkPtr key_chunk = ChunkHelper::new_chunk(*key_schema.get(), chunk->num_rows());
    ChunkPtr value_chunk = ChunkHelper::new_chunk(*value_schema.get(), chunk->num_rows());

    for (size_t i = 0; i < key_slot_ids.size(); ++i) {
        const auto& key_slot_id = key_slot_ids[i];
        auto ori_key_column = chunk->get_column_by_slot_id(key_slot_id);
        if (ori_key_column->is_constant()) {
            ori_key_column = ColumnHelper::unpack_and_duplicate_const_column(ori_key_column->size(), ori_key_column);
        }
        if (ori_key_column->is_nullable()) {
            ori_key_column = ColumnHelper::update_column_nullable(false, ori_key_column, ori_key_column->size());
        }
        key_chunk->get_column_by_index(i).swap(ori_key_column);
    }
    for (size_t i = 0; i < value_slot_ids.size(); ++i) {
        const auto& value_slot_id = value_slot_ids[i];
        auto ori_value_column = chunk->get_column_by_slot_id(value_slot_id);
        if (ori_value_column->is_constant()) {
            ori_value_column =
                    ColumnHelper::unpack_and_duplicate_const_column(ori_value_column->size(), ori_value_column);
        }
        if (ori_value_column->is_nullable()) {
            ori_value_column = ColumnHelper::update_column_nullable(false, ori_value_column, ori_value_column->size());
        }
        value_chunk->get_column_by_index(i).swap(ori_value_column);
    }

    // 3. encode key / value chunk without any nullable attribute
    auto encoded_key_column = DictionaryCacheUtil::encode_columns(*key_schema.get(), key_chunk.get());
    if (encoded_key_column == nullptr) {
        return Status::InternalError(
                fmt::format("encode key chunk failed when refreshing dictionary cache, dictionary id: {}, txn id: {}",
                            dict_id, txn_id));
    }

    std::vector<uint8_t> value_encode_flags(chunk->num_rows(), 1);
    if (DictionaryCacheUtil::get_encoded_type(*value_schema.get()) == TYPE_VARCHAR) {
        DictionaryCacheUtil::precheck_value_encode(value_chunk.get(), value_encode_flags);
    }

    auto encoded_value_column = DictionaryCacheUtil::encode_columns(*value_schema.get(), value_chunk.get());
    if (encoded_value_column == nullptr) {
        return Status::InternalError(
                fmt::format("encode value chunk failed when refreshing dictionary cache, dictionary id: {}, txn id: {}",
                            dict_id, txn_id));
    }

    // release memory after get the encoded column
    key_chunk.reset();
    value_chunk.reset();
    chunk.reset();

    // 4. refresh
    return _refresh_encoded_chunk(dict_id, txn_id, encoded_key_column.get(), encoded_value_column.get(),
                                  dictionary_schema, DictionaryCacheUtil::get_encoded_type(*key_schema.get()),
                                  DictionaryCacheUtil::get_encoded_type(*value_schema.get()), memory_limit,
                                  value_encode_flags);
}

Status DictionaryCacheManager::_refresh_encoded_chunk(DictionaryId dict_id, DictionaryCacheTxnId txn_id,
                                                      const Column* encoded_key_column,
                                                      const Column* encoded_value_column, const SchemaPtr& schema,
                                                      LogicalType key_encoded_type, LogicalType value_encoded_type,
                                                      long memory_limit,
                                                      const std::vector<uint8_t>& value_encode_flags) {
    DCHECK(key_encoded_type != TYPE_NONE);
    DCHECK(value_encoded_type != TYPE_NONE);

    // 1. fill the dictionary schema if necessary
    {
        DCHECK(schema != nullptr);
        std::unique_lock wlock(_schema_lock);
        if (UNLIKELY(_dict_cache_schema.find(dict_id) == _dict_cache_schema.end())) {
            _dict_cache_schema[dict_id] = schema;
        }
    }

    // 2. get and init mutable cache using <dict_id, txn_id>
    DictionaryCachePtr mutable_cache = nullptr;
    {
        std::unique_lock rlock(_refresh_lock);
        if (LIKELY(_mutable_dict_caches.find(dict_id) != _mutable_dict_caches.end() &&
                   _mutable_dict_caches[dict_id]->find(txn_id) != _mutable_dict_caches[dict_id]->end())) {
            if ((*_mutable_dict_caches[dict_id])[txn_id] == nullptr) {
                DictionaryCachePtr p = DictionaryCacheUtil::create_dictionary_cache(
                        std::pair<LogicalType, LogicalType>(key_encoded_type, value_encoded_type));
                if (p == nullptr) {
                    return Status::InternalError("Invalid dictionary type");
                }
                (*_mutable_dict_caches[dict_id])[txn_id] = p;
            }
            mutable_cache = (*_mutable_dict_caches[dict_id])[txn_id];
        } else {
            if (_dict_cancel.find(dict_id) != _dict_cancel.end()) {
                _dict_cancel.erase(dict_id);
                return Status::InternalError(fmt::format("cancel refreshing dictionary: {}", dict_id));
            }
            return Status::NotFound(fmt::format(
                    "refresh buffer for refreshing dictionary does not exist, dictionary id: {}, txn id: {}", dict_id,
                    txn_id)); /* happen when BE crash during refreshing dictionary */
        }
    }

    DCHECK(mutable_cache != nullptr);

    // 3. update mutable cache
    DCHECK(encoded_key_column != nullptr);
    DCHECK(encoded_value_column != nullptr);
    DCHECK(encoded_key_column->size() == encoded_value_column->size());

    std::lock_guard lg(mutable_cache->lock());
    for (size_t i = 0; i < encoded_key_column->size(); ++i) {
        auto key = encoded_key_column->get(i);
        auto value = encoded_value_column->get(i);
        RETURN_IF_ERROR(mutable_cache->insert(key, value, value_encode_flags[i]));
    }

    if (mutable_cache->memory_usage() > memory_limit) {
        // hit the memory limit, we should clear the cache as soon as possible
        this->clear(dict_id);
        return Status::InternalError(
                fmt::format("Reach the memory limit: {} bytes for dictionary, id: {}", memory_limit, dict_id));
    }

    return Status::OK();
}

Status DictionaryCacheManager::commit(const PProcessDictionaryCacheRequest* request) {
    auto dict_id = request->dict_id();
    auto txn_id = request->txn_id();
    std::unique_lock wlock1(_lock);
    std::unique_lock wlock2(_refresh_lock);

    if (UNLIKELY(_mutable_dict_caches.find(dict_id) == _mutable_dict_caches.end() ||
                 _mutable_dict_caches[dict_id]->find(txn_id) == _mutable_dict_caches[dict_id]->end())) {
        if (_dict_cancel.find(dict_id) != _dict_cancel.end()) {
            _dict_cancel.erase(dict_id);
            return Status::InternalError(fmt::format("cancel refreshing dictionary: {}", dict_id));
        }
        return Status::InternalError(
                fmt::format("commit failed the dictionary cache task: current txn id: {}", txn_id));
    } // This may happen if the current BE node crash just before commit

    auto ordered_mutable_cache = _mutable_dict_caches[dict_id];
    if (ordered_mutable_cache->crbegin()->first != txn_id) {
        return Status::InternalError(fmt::format(
                "The commit refresh dictionary cache task is not the latest one, current txn id: {}, max txn id: {}",
                txn_id, ordered_mutable_cache->crbegin()->first));
    }

    _dict_cache[dict_id] = nullptr;
    _dict_cache[dict_id].swap((*ordered_mutable_cache)[txn_id]);
    // The successful txn id will be used as the version number
    _dict_cache_versions[dict_id] = txn_id;

    _mutable_dict_caches.erase(dict_id);
    return Status::OK();
}

Status DictionaryCacheManager::export_cache(const PExportDictionaryCacheRequest* request,
                                             PExportDictionaryCacheResult* response) {
    auto initialize_response = [&](PDictionaryCacheExportOutcome outcome, std::optional<int64_t> actual_txn_id) {
        response->Clear();
        Status::OK().to_protobuf(response->mutable_status());
        response->set_outcome(outcome);
        response->set_protocol_version(kTenantTtlExportProtocolVersion);
        response->set_dictionary_id(request->dictionary_id());
        response->set_expected_txn_id(request->expected_txn_id());
        if (actual_txn_id.has_value()) {
            response->set_actual_txn_id(*actual_txn_id);
        }
        response->set_complete(false);
    };

    if (request->protocol_version() != kTenantTtlExportProtocolVersion || !request->has_dictionary_id() ||
        !request->has_expected_txn_id()) {
        initialize_response(PDictionaryCacheExportOutcome::EXPORT_INTERNAL_ERROR, std::nullopt);
        return Status::InvalidArgument("invalid Tenant-TTL Dictionary export request");
    }

    int64_t max_rows = effective_limit(request->has_max_rows(), request->max_rows(),
                                       config::tenant_ttl_policy_export_max_rows);
    int64_t max_uncompressed_bytes = effective_limit(request->has_max_uncompressed_bytes(),
                                                      request->max_uncompressed_bytes(),
                                                      config::tenant_ttl_policy_export_max_uncompressed_bytes);
    int64_t max_response_bytes = effective_limit(request->has_max_response_bytes(), request->max_response_bytes(),
                                                  config::tenant_ttl_policy_export_max_response_bytes);
    if (max_rows <= 0 || max_uncompressed_bytes <= 0 || max_response_bytes <= 0) {
        initialize_response(PDictionaryCacheExportOutcome::EXPORT_INTERNAL_ERROR, std::nullopt);
        return Status::InvalidArgument("Tenant-TTL Dictionary export limits must be positive");
    }

    DictionaryCachePtr cache;
    SchemaPtr schema;
    int64_t actual_txn_id = 0;
    {
        std::shared_lock cache_lock(_lock);
        auto version_it = _dict_cache_versions.find(request->dictionary_id());
        auto cache_it = _dict_cache.find(request->dictionary_id());
        if (version_it == _dict_cache_versions.end() || cache_it == _dict_cache.end() || cache_it->second == nullptr) {
            initialize_response(PDictionaryCacheExportOutcome::CACHE_NOT_FOUND, std::nullopt);
            return Status::OK();
        }
        actual_txn_id = version_it->second;
        if (actual_txn_id != request->expected_txn_id()) {
            initialize_response(PDictionaryCacheExportOutcome::VERSION_MISMATCH, actual_txn_id);
            return Status::OK();
        }
        cache = cache_it->second;

        std::shared_lock schema_lock(_schema_lock);
        auto schema_it = _dict_cache_schema.find(request->dictionary_id());
        if (schema_it != _dict_cache_schema.end()) {
            schema = schema_it->second;
        }
    }
    TEST_SYNC_POINT("DictionaryCacheManager::export_cache:after_capture");

    auto schema_matches = [&]() {
        return schema != nullptr && schema->num_fields() == 3 &&
               boost::iequals(schema->field(0)->name(), "tenant") &&
               schema->field(0)->type()->type() == TYPE_VARCHAR &&
               boost::iequals(schema->field(1)->name(), "table_name") &&
               schema->field(1)->type()->type() == TYPE_VARCHAR &&
               boost::iequals(schema->field(2)->name(), "retention_days") &&
               schema->field(2)->type()->type() == TYPE_INT;
    };
    using TenantTtlCache = DictionaryCacheImpl<TYPE_VARCHAR, TYPE_INT>;
    auto tenant_ttl_cache = std::dynamic_pointer_cast<TenantTtlCache>(cache);
    if (!schema_matches() || tenant_ttl_cache == nullptr) {
        initialize_response(PDictionaryCacheExportOutcome::SCHEMA_MISMATCH, actual_txn_id);
        return Status::OK();
    }

    PExportDictionaryCacheResult candidate;
    Status::OK().to_protobuf(candidate.mutable_status());
    candidate.set_outcome(PDictionaryCacheExportOutcome::EXPORT_OK);
    candidate.set_protocol_version(kTenantTtlExportProtocolVersion);
    candidate.set_dictionary_id(request->dictionary_id());
    candidate.set_expected_txn_id(request->expected_txn_id());
    candidate.set_actual_txn_id(actual_txn_id);

    int64_t total_rows = 0;
    int64_t total_uncompressed_bytes = 0;
    int64_t total_payload_bytes = 0;
    uint32_t content_crc32c = 0;
    PTenantTtlPolicyBatchPB policy_batch;
    size_t policy_batch_bytes = 0;
    bool limit_exceeded = false;

    auto finalize_policy_batch = [&]() -> Status {
        if (policy_batch.entries_size() == 0) {
            return Status::OK();
        }
        std::string uncompressed;
        if (!policy_batch.SerializeToString(&uncompressed)) {
            return Status::InternalError("failed to serialize Tenant-TTL policy batch");
        }
        if (uncompressed.size() > kTenantTtlExportBatchBytes ||
            total_uncompressed_bytes > max_uncompressed_bytes - static_cast<int64_t>(uncompressed.size())) {
            limit_exceeded = true;
            return Status::CapacityLimitExceed("Tenant-TTL Dictionary export exceeds uncompressed byte limit");
        }

        std::string compressed;
        compressed.resize(snappy::MaxCompressedLength(uncompressed.size()));
        size_t compressed_size = 0;
        snappy::RawCompress(uncompressed.data(), uncompressed.size(), compressed.data(), &compressed_size);
        compressed.resize(compressed_size);
        bool use_snappy = compressed.size() * 11 < uncompressed.size() * 10;

        auto* output_batch = candidate.add_batches();
        output_batch->set_sequence(candidate.batches_size() - 1);
        output_batch->set_row_count(policy_batch.entries_size());
        output_batch->set_compression_type(use_snappy ? CompressionTypePB::SNAPPY
                                                       : CompressionTypePB::NO_COMPRESSION);
        output_batch->set_uncompressed_size(uncompressed.size());
        output_batch->set_uncompressed_crc32c(crc32c::Value(uncompressed.data(), uncompressed.size()));
        if (use_snappy) {
            output_batch->set_payload(std::move(compressed));
        } else {
            output_batch->set_payload(uncompressed);
        }
        total_uncompressed_bytes += uncompressed.size();
        total_payload_bytes += output_batch->payload().size();
        content_crc32c = crc32c::Extend(content_crc32c, uncompressed.data(), uncompressed.size());
        policy_batch.Clear();
        policy_batch_bytes = 0;
        return Status::OK();
    };

    std::vector<ColumnId> key_column_ids{0, 1};
    Schema key_schema(schema.get(), key_column_ids);
    auto encoded_keys = BinaryColumn::create();
    std::vector<int32_t> retention_days;
    retention_days.reserve(kTenantTtlDecodeBatchRows);

    auto append_decoded_rows = [&]() -> Status {
        if (encoded_keys->empty()) {
            return Status::OK();
        }
        auto decoded_keys = ChunkHelper::new_chunk(key_schema, encoded_keys->size());
        RETURN_IF_ERROR(DictionaryCacheUtil::decode_columns(key_schema, encoded_keys.get(), decoded_keys.get(), nullptr));
        for (size_t i = 0; i < retention_days.size(); ++i) {
            Slice tenant = decoded_keys->get_column_by_index(0)->get(i).get_slice();
            Slice table_name = decoded_keys->get_column_by_index(1)->get(i).get_slice();
            PTenantTtlPolicyEntryPB entry;
            entry.set_tenant(tenant.data, tenant.size);
            entry.set_table_name(table_name.data, table_name.size);
            entry.set_retention_days(retention_days[i]);
            size_t entry_size = entry.ByteSizeLong();
            size_t framed_size = 1 + google::protobuf::io::CodedOutputStream::VarintSize32(entry_size) + entry_size;
            if (framed_size > kTenantTtlExportBatchBytes) {
                limit_exceeded = true;
                return Status::CapacityLimitExceed("one Tenant-TTL policy entry exceeds the batch limit");
            }
            if (policy_batch_bytes + framed_size > kTenantTtlExportBatchBytes) {
                RETURN_IF_ERROR(finalize_policy_batch());
            }
            policy_batch.add_entries()->Swap(&entry);
            policy_batch_bytes += framed_size;
        }
        encoded_keys = BinaryColumn::create();
        retention_days.clear();
        return Status::OK();
    };

    Status visit_status = tenant_ttl_cache->visit_entries([&](const Slice& encoded_key, const int32_t& value) {
        if (total_rows >= max_rows) {
            limit_exceeded = true;
            return Status::CapacityLimitExceed("Tenant-TTL Dictionary export exceeds row limit");
        }
        ++total_rows;
        encoded_keys->append(encoded_key);
        retention_days.emplace_back(value);
        if (retention_days.size() >= kTenantTtlDecodeBatchRows) {
            return append_decoded_rows();
        }
        return Status::OK();
    });
    if (visit_status.ok()) {
        visit_status = append_decoded_rows();
    }
    if (visit_status.ok()) {
        visit_status = finalize_policy_batch();
    }
    if (!visit_status.ok()) {
        if (limit_exceeded) {
            initialize_response(PDictionaryCacheExportOutcome::LIMIT_EXCEEDED, actual_txn_id);
            return Status::OK();
        }
        initialize_response(PDictionaryCacheExportOutcome::EXPORT_INTERNAL_ERROR, actual_txn_id);
        return visit_status;
    }

    candidate.set_total_row_count(total_rows);
    candidate.set_batch_count(candidate.batches_size());
    candidate.set_total_uncompressed_bytes(total_uncompressed_bytes);
    candidate.set_total_payload_bytes(total_payload_bytes);
    candidate.set_content_crc32c(content_crc32c);
    candidate.set_complete(true);
    if (candidate.ByteSizeLong() > max_response_bytes) {
        initialize_response(PDictionaryCacheExportOutcome::LIMIT_EXCEEDED, actual_txn_id);
        return Status::OK();
    }
    response->Swap(&candidate);
    return Status::OK();
}

void DictionaryCacheManager::clear(DictionaryId dict_id, bool is_cancel) {
    std::unique_lock wlock1(_lock);
    std::unique_lock wlock2(_schema_lock);
    std::unique_lock wlock3(_refresh_lock);

    if (_mutable_dict_caches.find(dict_id) != _mutable_dict_caches.end()) {
        _mutable_dict_caches.erase(_mutable_dict_caches.find(dict_id));
    }

    if (is_cancel) {
        _dict_cancel.insert(dict_id);
        return;
    }

    if (_dict_cache.find(dict_id) != _dict_cache.end()) {
        _dict_cache.erase(_dict_cache.find(dict_id));
    }

    if (_dict_cache_versions.find(dict_id) != _dict_cache_versions.end()) {
        _dict_cache_versions.erase(_dict_cache_versions.find(dict_id));
    }

    if (_dict_cache_schema.find(dict_id) != _dict_cache_schema.end()) {
        _dict_cache_schema.erase(_dict_cache_schema.find(dict_id));
    }
}

void DictionaryCacheManager::get_info(DictionaryId dict_id, PProcessDictionaryCacheResult& response) {
    std::shared_lock wlock1(_lock);

    long dictionary_memory_usage = 0;
    if (_dict_cache.find(dict_id) != _dict_cache.end() && _dict_cache[dict_id] != nullptr) {
        dictionary_memory_usage = _dict_cache[dict_id]->memory_usage();
    }

    response.set_dictionary_memory_usage(dictionary_memory_usage);
}

SchemaPtr DictionaryCacheManager::get_dictionary_schema_by_id(const DictionaryId& dict_id) {
    std::shared_lock rlock(_schema_lock);
    if (UNLIKELY(_dict_cache_schema.find(dict_id) == _dict_cache_schema.end())) {
        return nullptr;
    }
    return _dict_cache_schema[dict_id];
}

StatusOr<DictionaryCachePtr> DictionaryCacheManager::get_dictionary_by_version(const DictionaryId& dict_id,
                                                                               const DictionaryCacheTxnId& txn_id) {
    DictionaryCachePtr dictionary = nullptr;
    {
        std::shared_lock wlock(_lock);
        if (UNLIKELY(_dict_cache_versions.find(dict_id) == _dict_cache_versions.end() ||
                     _dict_cache_versions[dict_id] != txn_id)) {
            return Status::NotFound(fmt::format(
                    "can not found dictionary by verison: {}, dictionary maybe dropped or updated, dictionary id: {}",
                    txn_id, dict_id));
        }
        DCHECK(_dict_cache.find(dict_id) != _dict_cache.end());
        dictionary = _dict_cache[dict_id];
    }
    if (dictionary == nullptr) {
        return Status::InternalError(fmt::format("empty dictionary cache, id: {}", dict_id));
    }

    return dictionary;
}

} // namespace starrocks
