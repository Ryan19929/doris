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

#pragma once

#include <memory>

#include "olap/base_tablet.h"
#include "olap/partial_update_info.h"

namespace doris {

class CloudStorageEngine;

struct SyncRowsetStats {
    int64_t get_remote_rowsets_num {0};
    int64_t get_remote_rowsets_rpc_ns {0};

    int64_t get_local_delete_bitmap_rowsets_num {0};
    int64_t get_remote_delete_bitmap_rowsets_num {0};
    int64_t get_remote_delete_bitmap_key_count {0};
    int64_t get_remote_delete_bitmap_bytes {0};
    int64_t get_remote_delete_bitmap_rpc_ns {0};

    int64_t get_remote_tablet_meta_rpc_ns {0};
    int64_t tablet_meta_cache_hit {0};
    int64_t tablet_meta_cache_miss {0};
};

struct SyncOptions {
    bool warmup_delta_data = false;
    bool sync_delete_bitmap = true;
    bool full_sync = false;
    bool merge_schema = false;
    int64_t query_version = -1;
};

class CloudTablet final : public BaseTablet {
public:
    CloudTablet(CloudStorageEngine& engine, TabletMetaSharedPtr tablet_meta);

    ~CloudTablet() override;

    bool exceed_version_limit(int32_t limit) override;

    Result<std::unique_ptr<RowsetWriter>> create_rowset_writer(RowsetWriterContext& context,
                                                               bool vertical) override;

    Status capture_rs_readers(const Version& spec_version, std::vector<RowSetSplits>* rs_splits,
                              bool skip_missing_version) override;

    Status capture_consistent_rowsets_unlocked(
            const Version& spec_version, std::vector<RowsetSharedPtr>* rowsets) const override;

    size_t tablet_footprint() override {
        return _approximate_data_size.load(std::memory_order_relaxed);
    }

    std::string tablet_path() const override;

    // clang-format off
    int64_t fetch_add_approximate_num_rowsets (int64_t x) { return _approximate_num_rowsets .fetch_add(x, std::memory_order_relaxed); }
    int64_t fetch_add_approximate_num_segments(int64_t x) { return _approximate_num_segments.fetch_add(x, std::memory_order_relaxed); }
    int64_t fetch_add_approximate_num_rows    (int64_t x) { return _approximate_num_rows    .fetch_add(x, std::memory_order_relaxed); }
    int64_t fetch_add_approximate_data_size   (int64_t x) { return _approximate_data_size   .fetch_add(x, std::memory_order_relaxed); }
    int64_t fetch_add_approximate_cumu_num_rowsets (int64_t x) { return _approximate_cumu_num_rowsets.fetch_add(x, std::memory_order_relaxed); }
    int64_t fetch_add_approximate_cumu_num_deltas   (int64_t x) { return _approximate_cumu_num_deltas.fetch_add(x, std::memory_order_relaxed); }
    // clang-format on

    // meta lock must be held when calling this function
    void reset_approximate_stats(int64_t num_rowsets, int64_t num_segments, int64_t num_rows,
                                 int64_t data_size);

    // return a json string to show the compaction status of this tablet
    void get_compaction_status(std::string* json_result);

    // Synchronize the rowsets from meta service.
    // If tablet state is not `TABLET_RUNNING`, sync tablet meta and all visible rowsets.
    // If `query_version` > 0 and local max_version of the tablet >= `query_version`, do nothing.
    // If 'need_download_data_async' is true, it means that we need to download the new version
    // rowsets datum async.
    Status sync_rowsets(const SyncOptions& options = {}, SyncRowsetStats* stats = nullptr);

    // Synchronize the tablet meta from meta service.
    Status sync_meta();

    // If `version_overlap` is true, function will delete rowsets with overlapped version in this tablet.
    // If 'warmup_delta_data' is true, download the new version rowset data in background.
    // MUST hold EXCLUSIVE `_meta_lock`.
    // If 'need_download_data_async' is true, it means that we need to download the new version
    // rowsets datum async.
    void add_rowsets(std::vector<RowsetSharedPtr> to_add, bool version_overlap,
                     std::unique_lock<std::shared_mutex>& meta_lock,
                     bool warmup_delta_data = false);

    // MUST hold EXCLUSIVE `_meta_lock`.
    void delete_rowsets(const std::vector<RowsetSharedPtr>& to_delete,
                        std::unique_lock<std::shared_mutex>& meta_lock);

    // When the tablet is dropped, we need to recycle cached data:
    // 1. The data in file cache
    // 2. The memory in tablet cache
    void clear_cache() override;

    // Return number of deleted stale rowsets
    uint64_t delete_expired_stale_rowsets();

    bool has_stale_rowsets() const { return !_stale_rs_version_map.empty(); }

    int64_t get_cloud_base_compaction_score() const;
    int64_t get_cloud_cumu_compaction_score() const;

    int64_t max_version_unlocked() const override { return _max_version; }
    int64_t base_compaction_cnt() const { return _base_compaction_cnt; }
    int64_t cumulative_compaction_cnt() const { return _cumulative_compaction_cnt; }
    int64_t cumulative_layer_point() const {
        return _cumulative_point.load(std::memory_order_relaxed);
    }

    void set_base_compaction_cnt(int64_t cnt) { _base_compaction_cnt = cnt; }
    void set_cumulative_compaction_cnt(int64_t cnt) { _cumulative_compaction_cnt = cnt; }
    void set_cumulative_layer_point(int64_t new_point);

    int64_t last_cumu_compaction_failure_time() { return _last_cumu_compaction_failure_millis; }
    void set_last_cumu_compaction_failure_time(int64_t millis) {
        _last_cumu_compaction_failure_millis = millis;
    }

    int64_t last_base_compaction_failure_time() { return _last_base_compaction_failure_millis; }
    void set_last_base_compaction_failure_time(int64_t millis) {
        _last_base_compaction_failure_millis = millis;
    }

    int64_t last_full_compaction_failure_time() { return _last_full_compaction_failure_millis; }
    void set_last_full_compaction_failure_time(int64_t millis) {
        _last_full_compaction_failure_millis = millis;
    }

    int64_t last_cumu_compaction_success_time() { return _last_cumu_compaction_success_millis; }
    void set_last_cumu_compaction_success_time(int64_t millis) {
        _last_cumu_compaction_success_millis = millis;
    }

    int64_t last_base_compaction_success_time() { return _last_base_compaction_success_millis; }
    void set_last_base_compaction_success_time(int64_t millis) {
        _last_base_compaction_success_millis = millis;
    }

    int64_t last_full_compaction_success_time() { return _last_full_compaction_success_millis; }
    void set_last_full_compaction_success_time(int64_t millis) {
        _last_full_compaction_success_millis = millis;
    }

    int64_t last_cumu_compaction_schedule_time() { return _last_cumu_compaction_schedule_millis; }
    void set_last_cumu_compaction_schedule_time(int64_t millis) {
        _last_cumu_compaction_schedule_millis = millis;
    }

    int64_t last_base_compaction_schedule_time() { return _last_base_compaction_schedule_millis; }
    void set_last_base_compaction_schedule_time(int64_t millis) {
        _last_base_compaction_schedule_millis = millis;
    }

    int64_t last_full_compaction_schedule_time() { return _last_full_compaction_schedule_millis; }
    void set_last_full_compaction_schedule_time(int64_t millis) {
        _last_full_compaction_schedule_millis = millis;
    }

    void set_last_cumu_compaction_status(std::string status) {
        _last_cumu_compaction_status = std::move(status);
    }

    std::string get_last_cumu_compaction_status() { return _last_cumu_compaction_status; }

    void set_last_base_compaction_status(std::string status) {
        _last_base_compaction_status = std::move(status);
    }

    std::string get_last_base_compaction_status() { return _last_base_compaction_status; }

    void set_last_full_compaction_status(std::string status) {
        _last_full_compaction_status = std::move(status);
    }

    std::string get_last_full_compaction_status() { return _last_full_compaction_status; }

    int64_t alter_version() const { return _alter_version; }
    void set_alter_version(int64_t alter_version) { _alter_version = alter_version; }

    std::vector<RowsetSharedPtr> pick_candidate_rowsets_to_base_compaction();

    inline Version max_version() const {
        std::shared_lock rdlock(_meta_lock);
        return _tablet_meta->max_version();
    }

    int64_t base_size() const { return _base_size; }

    std::vector<RowsetSharedPtr> pick_candidate_rowsets_to_full_compaction();

    std::mutex& get_base_compaction_lock() { return _base_compaction_lock; }
    std::mutex& get_cumulative_compaction_lock() { return _cumulative_compaction_lock; }

    Result<std::unique_ptr<RowsetWriter>> create_transient_rowset_writer(
            const Rowset& rowset, std::shared_ptr<PartialUpdateInfo> partial_update_info,
            int64_t txn_expiration = 0) override;

    CalcDeleteBitmapExecutor* calc_delete_bitmap_executor() override;

    Status save_delete_bitmap(const TabletTxnInfo* txn_info, int64_t txn_id,
                              DeleteBitmapPtr delete_bitmap, RowsetWriter* rowset_writer,
                              const RowsetIdUnorderedSet& cur_rowset_ids, int64_t lock_id = -1,
                              int64_t next_visible_version = -1) override;

    Status save_delete_bitmap_to_ms(int64_t cur_version, int64_t txn_id,
                                    DeleteBitmapPtr delete_bitmap, int64_t lock_id,
                                    int64_t next_visible_version);

    Status calc_delete_bitmap_for_compaction(const std::vector<RowsetSharedPtr>& input_rowsets,
                                             const RowsetSharedPtr& output_rowset,
                                             const RowIdConversion& rowid_conversion,
                                             ReaderType compaction_type, int64_t merged_rows,
                                             int64_t filtered_rows, int64_t initiator,
                                             DeleteBitmapPtr& output_rowset_delete_bitmap,
                                             bool allow_delete_in_cumu_compaction,
                                             int64_t& get_delete_bitmap_lock_start_time);

    // Find the missed versions until the spec_version.
    //
    // for example:
    //     [0-4][5-5][8-8][9-9][14-14]
    // if spec_version = 12, it will return [6-7],[10-12]
    Versions calc_missed_versions(int64_t spec_version, Versions existing_versions) const override;

    std::mutex& get_rowset_update_lock() { return _rowset_update_lock; }

    bthread::Mutex& get_sync_meta_lock() { return _sync_meta_lock; }

    const auto& rowset_map() const { return _rs_version_map; }

    // Merge all rowset schemas within a CloudTablet
    Status merge_rowsets_schema();

    int64_t last_sync_time_s = 0;
    int64_t last_load_time_ms = 0;
    int64_t last_base_compaction_success_time_ms = 0;
    int64_t last_cumu_compaction_success_time_ms = 0;
    int64_t last_cumu_no_suitable_version_ms = 0;
    int64_t last_access_time_ms = 0;

    std::atomic<int64_t> local_read_time_us = 0;
    std::atomic<int64_t> remote_read_time_us = 0;
    std::atomic<int64_t> exec_compaction_time_us = 0;

    // Return merged extended schema
    TabletSchemaSPtr merged_tablet_schema() const override;

    void build_tablet_report_info(TTabletInfo* tablet_info);

    static void recycle_cached_data(const std::vector<RowsetSharedPtr>& rowsets);

    // check that if the delete bitmap in delete bitmap cache has the same cardinality with the expected_delete_bitmap's
    Status check_delete_bitmap_cache(int64_t txn_id, DeleteBitmap* expected_delete_bitmap) override;

    void agg_delete_bitmap_for_compaction(int64_t start_version, int64_t end_version,
                                          const std::vector<RowsetSharedPtr>& pre_rowsets,
                                          DeleteBitmapPtr& new_delete_bitmap,
                                          std::map<std::string, int64_t>& pre_rowset_to_versions);

    bool need_remove_unused_rowsets();

    void add_unused_rowsets(const std::vector<RowsetSharedPtr>& rowsets);
    void remove_unused_rowsets();

private:
    // FIXME(plat1ko): No need to record base size if rowsets are ordered by version
    void update_base_size(const Rowset& rs);

    Status sync_if_not_running(SyncRowsetStats* stats = nullptr);

    CloudStorageEngine& _engine;

    // this mutex MUST ONLY be used when sync meta
    bthread::Mutex _sync_meta_lock;
    // ATTENTION: lock order should be: _sync_meta_lock -> _meta_lock

    std::atomic<int64_t> _cumulative_point {-1};
    std::atomic<int64_t> _approximate_num_rowsets {-1};
    std::atomic<int64_t> _approximate_num_segments {-1};
    std::atomic<int64_t> _approximate_num_rows {-1};
    std::atomic<int64_t> _approximate_data_size {-1};
    std::atomic<int64_t> _approximate_cumu_num_rowsets {-1};
    // Number of sorted arrays (e.g. for rowset with N segments, if rowset is overlapping, delta is N, otherwise 1) after cumu point
    std::atomic<int64_t> _approximate_cumu_num_deltas {-1};

    // timestamp of last cumu compaction failure
    std::atomic<int64_t> _last_cumu_compaction_failure_millis;
    // timestamp of last base compaction failure
    std::atomic<int64_t> _last_base_compaction_failure_millis;
    // timestamp of last full compaction failure
    std::atomic<int64_t> _last_full_compaction_failure_millis;
    // timestamp of last cumu compaction success
    std::atomic<int64_t> _last_cumu_compaction_success_millis;
    // timestamp of last base compaction success
    std::atomic<int64_t> _last_base_compaction_success_millis;
    // timestamp of last full compaction success
    std::atomic<int64_t> _last_full_compaction_success_millis;
    // timestamp of last cumu compaction schedule time
    std::atomic<int64_t> _last_cumu_compaction_schedule_millis;
    // timestamp of last base compaction schedule time
    std::atomic<int64_t> _last_base_compaction_schedule_millis;
    // timestamp of last full compaction schedule time
    std::atomic<int64_t> _last_full_compaction_schedule_millis;

    std::string _last_cumu_compaction_status;
    std::string _last_base_compaction_status;
    std::string _last_full_compaction_status;

    int64_t _base_compaction_cnt = 0;
    int64_t _cumulative_compaction_cnt = 0;
    int64_t _max_version = -1;
    int64_t _base_size = 0;
    int64_t _alter_version = -1;

    std::mutex _base_compaction_lock;
    std::mutex _cumulative_compaction_lock;

    // To avoid multiple calc delete bitmap tasks on same (txn_id, tablet_id) with different
    // signatures being executed concurrently, we use _rowset_update_lock to serialize them
    mutable std::mutex _rowset_update_lock;

    // Schema will be merged from all rowsets when sync_rowsets
    TabletSchemaSPtr _merged_tablet_schema;

    // unused_rowsets, [start_version, end_version]
    std::mutex _gc_mutex;
    std::unordered_map<RowsetId, RowsetSharedPtr> _unused_rowsets;
    std::vector<std::pair<std::vector<RowsetId>, DeleteBitmapKeyRanges>> _unused_delete_bitmap;
};

using CloudTabletSPtr = std::shared_ptr<CloudTablet>;

} // namespace doris
