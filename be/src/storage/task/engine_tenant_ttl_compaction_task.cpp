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

#include "storage/task/engine_tenant_ttl_compaction_task.h"

#include <fmt/format.h>

#include <algorithm>
#include <limits>
#include <utility>

#include "common/logging.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "storage/delta_column_group.h"
#include "storage/filtered_rowset_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tenant_ttl_row_filter.h"
#include "storage/tenant_ttl_tablet_guard.h"
#include "testutil/sync_point.h"
#include "util/defer_op.h"
#include "util/starrocks_metrics.h"
#include "util/stopwatch.hpp"
#include "util/trace.h"

namespace starrocks {
namespace {

class TenantTtlCompactionMetrics {
public:
    TenantTtlCompactionMetrics() {
        auto* registry = StarRocksMetrics::instance()->metrics();
        registry->register_metric("tenant_ttl_compaction_requests_total", MetricLabels().add("result", "success"),
                                  &requests_success);
        registry->register_metric("tenant_ttl_compaction_requests_total", MetricLabels().add("result", "noop"),
                                  &requests_noop);
        registry->register_metric("tenant_ttl_compaction_requests_total", MetricLabels().add("result", "busy"),
                                  &requests_busy);
        registry->register_metric("tenant_ttl_compaction_requests_total", MetricLabels().add("result", "conflict"),
                                  &requests_conflict);
        registry->register_metric("tenant_ttl_compaction_requests_total", MetricLabels().add("result", "failed"),
                                  &requests_failed);
        registry->register_metric("tenant_ttl_compaction_rows_scanned_total", &rows_scanned);
        registry->register_metric("tenant_ttl_compaction_rows_deleted_total", &rows_deleted);
        registry->register_metric("tenant_ttl_compaction_rowsets_total", MetricLabels().add("action", "keep"),
                                  &rowsets_keep);
        registry->register_metric("tenant_ttl_compaction_rowsets_total", MetricLabels().add("action", "drop"),
                                  &rowsets_drop);
        registry->register_metric("tenant_ttl_compaction_rowsets_total", MetricLabels().add("action", "rewrite"),
                                  &rowsets_rewrite);
        registry->register_metric("tenant_ttl_compaction_segments_total", MetricLabels().add("action", "keep"),
                                  &segments_keep);
        registry->register_metric("tenant_ttl_compaction_segments_total", MetricLabels().add("action", "drop"),
                                  &segments_drop);
        registry->register_metric("tenant_ttl_compaction_segments_total", MetricLabels().add("action", "rewrite"),
                                  &segments_rewrite);
        registry->register_metric("tenant_ttl_compaction_linked_bytes_total", &linked_bytes);
        registry->register_metric("tenant_ttl_compaction_rewritten_bytes_total", &rewritten_bytes);
        registry->register_metric("tenant_ttl_compaction_duration_us", &duration_us);
        registry->register_metric("tenant_ttl_compaction_running", &running);
    }

    IntCounter requests_success{MetricUnit::REQUESTS};
    IntCounter requests_noop{MetricUnit::REQUESTS};
    IntCounter requests_busy{MetricUnit::REQUESTS};
    IntCounter requests_conflict{MetricUnit::REQUESTS};
    IntCounter requests_failed{MetricUnit::REQUESTS};
    IntCounter rows_scanned{MetricUnit::ROWS};
    IntCounter rows_deleted{MetricUnit::ROWS};
    IntCounter rowsets_keep{MetricUnit::ROWSETS};
    IntCounter rowsets_drop{MetricUnit::ROWSETS};
    IntCounter rowsets_rewrite{MetricUnit::ROWSETS};
    IntCounter segments_keep{MetricUnit::OPERATIONS};
    IntCounter segments_drop{MetricUnit::OPERATIONS};
    IntCounter segments_rewrite{MetricUnit::OPERATIONS};
    IntCounter linked_bytes{MetricUnit::BYTES};
    IntCounter rewritten_bytes{MetricUnit::BYTES};
    IntCounter duration_us{MetricUnit::MICROSECONDS};
    IntGauge running{MetricUnit::NOUNIT};
};

TenantTtlCompactionMetrics& tenant_ttl_compaction_metrics() {
    static TenantTtlCompactionMetrics metrics;
    return metrics;
}

void record_tenant_ttl_result(const TenantTtlCompactionRequest& request, const TenantTtlCompactionResult& result,
                              int64_t duration_us) {
    auto& metrics = tenant_ttl_compaction_metrics();
    metrics.running.increment(-1);
    metrics.duration_us.increment(duration_us);
    metrics.rows_scanned.increment(result.scanned_rows);
    metrics.rows_deleted.increment(result.deleted_rows);
    metrics.linked_bytes.increment(result.linked_bytes);
    metrics.rewritten_bytes.increment(result.rewritten_bytes);

    int64_t rowsets_keep = 0;
    int64_t rowsets_drop = 0;
    int64_t rowsets_rewrite = 0;
    int64_t segments_keep = 0;
    int64_t segments_drop = 0;
    int64_t segments_rewrite = 0;
    for (const auto& rowset : result.rowsets) {
        switch (rowset.action) {
        case TenantTtlRowsetAction::VERIFIED_NO_CHANGE:
            ++rowsets_keep;
            break;
        case TenantTtlRowsetAction::DROP:
            ++rowsets_drop;
            break;
        case TenantTtlRowsetAction::REWRITE:
            ++rowsets_rewrite;
            break;
        }
        segments_drop += rowset.dropped_segments;
        segments_rewrite += rowset.rewritten_segments;
        segments_keep += rowset.source_segments - rowset.dropped_segments - rowset.rewritten_segments;
    }
    metrics.rowsets_keep.increment(rowsets_keep);
    metrics.rowsets_drop.increment(rowsets_drop);
    metrics.rowsets_rewrite.increment(rowsets_rewrite);
    metrics.segments_keep.increment(segments_keep);
    metrics.segments_drop.increment(segments_drop);
    metrics.segments_rewrite.increment(segments_rewrite);

    switch (result.code) {
    case TenantTtlTaskCode::SUCCESS:
        metrics.requests_success.increment(1);
        break;
    case TenantTtlTaskCode::NOOP_VERIFIED:
        metrics.requests_noop.increment(1);
        break;
    case TenantTtlTaskCode::TABLET_BUSY:
    case TenantTtlTaskCode::TTL_ALREADY_RUNNING:
        metrics.requests_busy.increment(1);
        break;
    case TenantTtlTaskCode::REPLICA_NOT_CAUGHT_UP:
    case TenantTtlTaskCode::SCHEMA_CHANGED:
    case TenantTtlTaskCode::STALE_ROWSET:
        metrics.requests_conflict.increment(1);
        break;
    default:
        metrics.requests_failed.increment(1);
        break;
    }

    TRACE_COUNTER_INCREMENT("tenant_ttl_rows_scanned", result.scanned_rows);
    TRACE_COUNTER_INCREMENT("tenant_ttl_rows_deleted", result.deleted_rows);
    TRACE_COUNTER_INCREMENT("tenant_ttl_rowsets_keep", rowsets_keep);
    TRACE_COUNTER_INCREMENT("tenant_ttl_rowsets_drop", rowsets_drop);
    TRACE_COUNTER_INCREMENT("tenant_ttl_rowsets_rewrite", rowsets_rewrite);
    TRACE_COUNTER_INCREMENT("tenant_ttl_segments_keep", segments_keep);
    TRACE_COUNTER_INCREMENT("tenant_ttl_segments_drop", segments_drop);
    TRACE_COUNTER_INCREMENT("tenant_ttl_segments_rewrite", segments_rewrite);
    TRACE_COUNTER_INCREMENT("tenant_ttl_linked_bytes", result.linked_bytes);
    TRACE_COUNTER_INCREMENT("tenant_ttl_rewritten_bytes", result.rewritten_bytes);
    TRACE_COUNTER_INCREMENT("tenant_ttl_duration_us", duration_us);

    LOG(INFO) << "Tenant-TTL task finished. task_id=" << request.task_id << " tablet_id=" << request.tablet_id
              << " partition_id=" << request.partition_id << " dictionary_id=" << request.policy_watermark.dictionary_id
              << " dictionary_txn_id=" << request.policy_watermark.dictionary_txn_id
              << " evaluation_time_epoch_seconds=" << request.policy_watermark.evaluation_time_epoch_seconds
              << " expected_schema_id=" << request.expected_schema.schema_id
              << " expected_schema_version=" << request.expected_schema.schema_version
              << " snapshot_end_version=" << result.snapshot_end_version
              << " processed_through_version=" << result.processed_through_version
              << " coverage_digest=" << result.coverage_digest << " tenant_count=" << request.filter.tenants.size()
              << " rowsets_keep=" << rowsets_keep << " rowsets_drop=" << rowsets_drop
              << " rowsets_rewrite=" << rowsets_rewrite << " segments_keep=" << segments_keep
              << " segments_drop=" << segments_drop << " segments_rewrite=" << segments_rewrite
              << " scanned_rows=" << result.scanned_rows << " kept_rows=" << result.kept_rows
              << " deleted_rows=" << result.deleted_rows << " linked_bytes=" << result.linked_bytes
              << " rewritten_bytes=" << result.rewritten_bytes << " duration_us=" << duration_us
              << " code=" << tenant_ttl_task_code_to_string(result.code) << " detail_status=" << result.detail_status;
}

class StagedTenantTtlOutputs {
public:
    ~StagedTenantTtlOutputs() {
        if (_committed) {
            return;
        }
        for (auto it = _entries.rbegin(); it != _entries.rend(); ++it) {
            it->output.reset();
            const Status status = it->writer->cleanup();
            LOG_IF(WARNING, !status.ok()) << "Failed to clean staged Tenant-TTL Rowset: " << status;
        }
    }

    void add(std::unique_ptr<FilteredRowsetWriter> writer, RowsetSharedPtr output) {
        _entries.emplace_back(Entry{.writer = std::move(writer), .output = std::move(output)});
    }

    void mark_committed() { _committed = true; }

private:
    struct Entry {
        std::unique_ptr<FilteredRowsetWriter> writer;
        RowsetSharedPtr output;
    };

    std::vector<Entry> _entries;
    bool _committed{false};
};

Status validate_tablet_and_tenant_column(const TabletSharedPtr& tablet, const TenantTtlCompactionRequest& request) {
    if (tablet->partition_id() != request.partition_id) {
        return Status::InvalidArgument("Tenant-TTL request partition does not match tablet");
    }
    if (tablet->tablet_state() != TABLET_RUNNING) {
        return Status::NotSupported("Tenant-TTL requires a running tablet");
    }
    if (tablet->belonged_to_cloud_native() || tablet->updates() != nullptr) {
        return Status::NotSupported("Tenant-TTL only supports local shared-nothing non-PK tablets");
    }
    if (tablet->keys_type() != DUP_KEYS) {
        return Status::NotSupported("Tenant-TTL only supports DUP_KEYS tablets");
    }

    const auto schema = tablet->tablet_schema();
    const int32_t tenant_column_index = schema->field_index(request.tenant_column_unique_id);
    if (tenant_column_index < 0) {
        return Status::Corruption("Tenant-TTL tenant column unique ID does not exist");
    }
    if (schema->column(tenant_column_index).type() != TYPE_VARCHAR) {
        return Status::NotSupported("Tenant-TTL tenant column must be nullable or non-nullable VARCHAR");
    }
    return Status::OK();
}

Status validate_no_delta_column_groups(const TabletSharedPtr& tablet, const TenantTtlCoverage& coverage) {
    for (const auto& entry : coverage.entries) {
        for (uint32_t segment_id = 0; segment_id < entry.source->num_segments(); ++segment_id) {
            DeltaColumnGroupList dcgs;
            RETURN_IF_ERROR(StorageEngine::instance()->get_delta_column_group(
                    tablet->data_dir()->get_meta(), tablet->tablet_id(), entry.expected_rowset_id, segment_id,
                    std::numeric_limits<int64_t>::max(), &dcgs));
            if (!dcgs.empty()) {
                return Status::NotSupported("Tenant-TTL coverage contains a Delta Column Group");
            }
        }
    }
    return Status::OK();
}

RowsetWriterContext make_output_context(const TabletSharedPtr& tablet, const TenantTtlCoverage& coverage,
                                        const RowsetSharedPtr& source) {
    RowsetWriterContext context;
    context.rowset_id = StorageEngine::instance()->next_rowset_id();
    context.tablet_uid = tablet->tablet_uid();
    context.tablet_id = tablet->tablet_id();
    context.tablet_schema_hash = tablet->schema_hash();
    context.partition_id = tablet->partition_id();
    context.rowset_path_prefix = tablet->schema_hash_path();
    context.rowset_state = VISIBLE;
    context.tablet_schema = coverage.schema_identity.captured_schema;
    context.version = source->version();
    context.segments_overlap = NONOVERLAPPING;
    context.gtid = source->rowset_meta()->gtid();
    context.is_compaction = true;
    return context;
}

} // namespace

EngineTenantTtlCompactionTask::EngineTenantTtlCompactionTask(TenantTtlCompactionRequest request,
                                                             MemTracker* parent_mem_tracker,
                                                             const std::atomic<bool>* is_cancelled)
        : _request(std::move(request)), _is_cancelled(is_cancelled) {
    _mem_tracker = std::make_unique<MemTracker>(
            MemTrackerType::COMPACTION_TASK, -1,
            fmt::format("tenant_ttl_compaction_task_{}_tablet_{}", _request.task_id, _request.tablet_id),
            parent_mem_tracker);
}

Status EngineTenantTtlCompactionTask::_finish(TenantTtlTaskCode code, Status detail_status) {
    _result.code = code;
    _result.detail_status = std::move(detail_status);
    _result.retryable = tenant_ttl_task_code_is_retryable(code);
    if (code == TenantTtlTaskCode::SUCCESS || code == TenantTtlTaskCode::NOOP_VERIFIED) {
        return Status::OK();
    }
    if (_result.detail_status.ok()) {
        _result.detail_status = Status::InternalError("Tenant-TTL task failed without a detail status");
    }
    return _result.detail_status;
}

Status EngineTenantTtlCompactionTask::_check_cancelled() const {
    if (_is_cancelled != nullptr && _is_cancelled->load(std::memory_order_acquire)) {
        return Status::Cancelled("Tenant-TTL task was cancelled");
    }
    return _mem_tracker->check_mem_limit("Tenant-TTL compaction task");
}

Status EngineTenantTtlCompactionTask::execute() {
    MonotonicStopWatch watch;
    watch.start();
    auto& metrics = tenant_ttl_compaction_metrics();
    metrics.running.increment(1);
    DeferOp metrics_and_log([&] { record_tenant_ttl_result(_request, _result, watch.elapsed_time() / 1000); });

    _result = TenantTtlCompactionResult();
    _result.task_id = _request.task_id;
    _result.tablet_id = _request.tablet_id;
    _result.partition_id = _request.partition_id;
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker.get());

    Status status = normalize_and_validate_tenant_ttl_request(&_request);
    if (!status.ok()) {
        return _finish(TenantTtlTaskCode::INVALID_ARGUMENT, std::move(status));
    }
    status = _check_cancelled();
    if (!status.ok()) {
        return _finish(status.is_cancelled() ? TenantTtlTaskCode::CANCELLED : TenantTtlTaskCode::INTERNAL_ERROR,
                       std::move(status));
    }

    auto tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_request.tablet_id, false);
    if (tablet == nullptr) {
        return _finish(TenantTtlTaskCode::TABLET_NOT_FOUND,
                       Status::NotFound(fmt::format("Tenant-TTL tablet {} was not found", _request.tablet_id)));
    }
    status = validate_tablet_and_tenant_column(tablet, _request);
    if (!status.ok()) {
        const auto code = status.is_not_supported() ? TenantTtlTaskCode::NOT_SUPPORTED
                          : status.is_corruption()  ? TenantTtlTaskCode::DATA_INVARIANT_VIOLATION
                                                    : TenantTtlTaskCode::INVALID_ARGUMENT;
        return _finish(code, std::move(status));
    }

    TenantTtlTabletGuard tablet_guard(tablet);
    TenantTtlTaskCode code = tablet_guard.try_acquire(_request, &status);
    if (code != TenantTtlTaskCode::SUCCESS) {
        return _finish(code, std::move(status));
    }
    TenantTtlCoverageGuard coverage_guard(tablet);
    code = coverage_guard.capture(_request, &status);
    if (code != TenantTtlTaskCode::SUCCESS) {
        return _finish(code, std::move(status));
    }
    const auto& coverage = coverage_guard.coverage();
    _result.snapshot_end_version = coverage.snapshot_end_version;
    _result.coverage_digest = coverage.coverage_digest;
    TEST_SYNC_POINT("EngineTenantTtlCompactionTask::coverage_captured");

    if (_request.filter.mode == TenantFilterMode::DELETE_LIST && _request.filter.tenants.empty()) {
        if (!tablet->tenant_ttl_owner_matches(_request.task_id, tablet_guard.generation(), _request.policy_watermark)) {
            return _finish(TenantTtlTaskCode::INTERNAL_ERROR,
                           Status::Aborted("Tenant-TTL admission owner changed before empty-list validation"));
        }
        code = tablet->validate_tenant_ttl_coverage(coverage, &status);
        if (code != TenantTtlTaskCode::SUCCESS) {
            return _finish(code, std::move(status));
        }
        for (const auto& entry : coverage.entries) {
            _result.rowsets.emplace_back(
                    TenantTtlRowsetResult{.source_version = entry.version,
                                          .source_rowset_id = entry.expected_rowset_id,
                                          .action = TenantTtlRowsetAction::VERIFIED_NO_CHANGE,
                                          .source_rows = entry.source->num_rows(),
                                          .kept_rows = entry.source->num_rows(),
                                          .source_segments = static_cast<int32_t>(entry.source->num_segments())});
            _result.kept_rows += entry.source->num_rows();
        }
        _result.processed_through_version = coverage.snapshot_end_version;
        return _finish(TenantTtlTaskCode::NOOP_VERIFIED, Status::OK());
    }

    status = validate_no_delta_column_groups(tablet, coverage);
    if (!status.ok()) {
        return _finish(status.is_not_supported() ? TenantTtlTaskCode::NOT_SUPPORTED : TenantTtlTaskCode::INTERNAL_ERROR,
                       std::move(status));
    }

    TenantTtlRowFilter filter(coverage.schema_identity.captured_schema, _request.tenant_column_unique_id,
                              _request.filter, 4096, _mem_tracker.get(), _is_cancelled);
    status = filter.validate();
    if (!status.ok()) {
        return _finish(status.is_invalid_argument() ? TenantTtlTaskCode::DATA_INVARIANT_VIOLATION
                                                    : TenantTtlTaskCode::NOT_SUPPORTED,
                       std::move(status));
    }

    StagedTenantTtlOutputs staged_outputs;
    std::vector<TenantTtlReplacement> replacements;
    replacements.reserve(coverage.entries.size());
    _result.rowsets.reserve(coverage.entries.size());
    for (const auto& entry : coverage.entries) {
        status = _check_cancelled();
        if (!status.ok()) {
            return _finish(status.is_cancelled() ? TenantTtlTaskCode::CANCELLED : TenantTtlTaskCode::INTERNAL_ERROR,
                           std::move(status));
        }

        TenantTtlRowsetResult rowset_result{.source_version = entry.version,
                                            .source_rowset_id = entry.expected_rowset_id,
                                            .source_rows = entry.source->num_rows(),
                                            .source_segments = static_cast<int32_t>(entry.source->num_segments())};
        std::vector<SegmentFilterPlan> plans;
        plans.reserve(entry.source->num_segments());
        for (uint32_t segment_id = 0; segment_id < entry.source->num_segments(); ++segment_id) {
            auto plan_or = filter.plan_segment(entry.source->segments()[segment_id], segment_id);
            if (!plan_or.ok()) {
                const auto failure_code = plan_or.status().is_cancelled() ? TenantTtlTaskCode::CANCELLED
                                                                          : TenantTtlTaskCode::INTERNAL_ERROR;
                return _finish(failure_code, plan_or.status());
            }
            auto plan = std::move(plan_or).value();
            rowset_result.kept_rows += plan.kept_rows;
            rowset_result.deleted_rows += plan.deleted_rows;
            rowset_result.linked_segments += plan.action == SegmentFilterAction::KEEP;
            rowset_result.dropped_segments += plan.action == SegmentFilterAction::DROP;
            rowset_result.rewritten_segments += plan.action == SegmentFilterAction::REWRITE;
            plans.emplace_back(std::move(plan));
        }
        TEST_SYNC_POINT("EngineTenantTtlCompactionTask::rowset_planned");
        if (rowset_result.source_rows != rowset_result.kept_rows + rowset_result.deleted_rows) {
            return _finish(TenantTtlTaskCode::DATA_INVARIANT_VIOLATION,
                           Status::Corruption("Tenant-TTL Rowset row accounting does not balance"));
        }
        _result.scanned_rows += rowset_result.source_rows;
        _result.kept_rows += rowset_result.kept_rows;
        _result.deleted_rows += rowset_result.deleted_rows;

        if (rowset_result.deleted_rows == 0) {
            rowset_result.action = TenantTtlRowsetAction::VERIFIED_NO_CHANGE;
            rowset_result.linked_segments = 0;
            _result.rowsets.emplace_back(std::move(rowset_result));
            continue;
        }

        auto writer = std::make_unique<FilteredRowsetWriter>(make_output_context(tablet, coverage, entry.source), 4096,
                                                             _mem_tracker.get(), _is_cancelled);
        status = writer->init();
        if (!status.ok()) {
            return _finish(TenantTtlTaskCode::INTERNAL_ERROR, std::move(status));
        }
        for (const auto& plan : plans) {
            status = writer->add_segment(entry.source, plan);
            if (!status.ok()) {
                return _finish(status.is_cancelled() ? TenantTtlTaskCode::CANCELLED : TenantTtlTaskCode::INTERNAL_ERROR,
                               std::move(status));
            }
        }
        Status injected_status;
        TEST_SYNC_POINT_CALLBACK("EngineTenantTtlCompactionTask::before_output_build", &injected_status);
        if (!injected_status.ok()) {
            return _finish(TenantTtlTaskCode::INTERNAL_ERROR, std::move(injected_status));
        }
        auto output_or = writer->build();
        if (!output_or.ok()) {
            return _finish(output_or.status().is_cancelled() ? TenantTtlTaskCode::CANCELLED
                                                             : TenantTtlTaskCode::INTERNAL_ERROR,
                           output_or.status());
        }
        auto output = std::move(output_or).value();
        if (output->num_rows() != rowset_result.kept_rows) {
            output.reset();
            const Status cleanup_status = writer->cleanup();
            return _finish(TenantTtlTaskCode::DATA_INVARIANT_VIOLATION,
                           cleanup_status.ok()
                                   ? Status::Corruption("Tenant-TTL replacement Rowset row count is incorrect")
                                   : Status::Corruption("Tenant-TTL replacement Rowset row count is incorrect; " +
                                                        cleanup_status.to_string()));
        }

        for (const auto& segment_result : writer->segment_results()) {
            const auto plan = std::find_if(plans.begin(), plans.end(), [&](const SegmentFilterPlan& candidate) {
                return candidate.src_segment_id == segment_result.src_segment_id;
            });
            DCHECK(plan != plans.end());
            if (plan->action == SegmentFilterAction::KEEP) {
                _result.linked_bytes += segment_result.stats.physical_artifact_size;
            } else {
                _result.rewritten_bytes += segment_result.stats.physical_artifact_size;
            }
        }

        rowset_result.output_rowset_id = output->rowset_id();
        rowset_result.action =
                rowset_result.kept_rows == 0 ? TenantTtlRowsetAction::DROP : TenantTtlRowsetAction::REWRITE;
        replacements.emplace_back(TenantTtlReplacement{.source_version = entry.version,
                                                       .expected_source_rowset_id = entry.expected_rowset_id,
                                                       .output = output});
        staged_outputs.add(std::move(writer), std::move(output));
        _result.rowsets.emplace_back(std::move(rowset_result));
        TEST_SYNC_POINT("EngineTenantTtlCompactionTask::output_staged");
    }

    if (_result.scanned_rows != _result.kept_rows + _result.deleted_rows) {
        return _finish(TenantTtlTaskCode::DATA_INVARIANT_VIOLATION,
                       Status::Corruption("Tenant-TTL coverage row accounting does not balance"));
    }
    status = _check_cancelled();
    if (!status.ok()) {
        return _finish(status.is_cancelled() ? TenantTtlTaskCode::CANCELLED : TenantTtlTaskCode::INTERNAL_ERROR,
                       std::move(status));
    }
    if (!tablet->tenant_ttl_owner_matches(_request.task_id, tablet_guard.generation(), _request.policy_watermark)) {
        return _finish(TenantTtlTaskCode::INTERNAL_ERROR,
                       Status::Aborted("Tenant-TTL admission owner changed before commit"));
    }

    if (_result.deleted_rows == 0) {
        code = tablet->validate_tenant_ttl_coverage(coverage, &status);
        if (code != TenantTtlTaskCode::SUCCESS) {
            return _finish(code, std::move(status));
        }
        _result.processed_through_version = coverage.snapshot_end_version;
        return _finish(TenantTtlTaskCode::NOOP_VERIFIED, Status::OK());
    }
    if (replacements.empty()) {
        return _finish(TenantTtlTaskCode::DATA_INVARIANT_VIOLATION,
                       Status::Corruption("Tenant-TTL deleted rows without producing replacements"));
    }

    TEST_SYNC_POINT("EngineTenantTtlCompactionTask::before_commit");
    std::vector<RowsetSharedPtr> replaced_stale_rowsets;
    code = tablet->commit_tenant_ttl_rowsets(coverage, replacements, &replaced_stale_rowsets, &status);
    if (code != TenantTtlTaskCode::SUCCESS) {
        return _finish(code, std::move(status));
    }
    staged_outputs.mark_committed();
    for (const auto& stale : replaced_stale_rowsets) {
        StorageEngine::instance()->add_unused_rowset(stale);
    }
    _result.processed_through_version = coverage.snapshot_end_version;
    return _finish(TenantTtlTaskCode::SUCCESS, Status::OK());
}

} // namespace starrocks
