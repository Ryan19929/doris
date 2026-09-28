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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Types_types.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/tablet/tablet_fwd.h"

namespace doris {
namespace io {
class RemoteFileSystem;
} // namespace io

class DataDir;
class TRemoteTabletSnapshot;
class StorageEngine;

struct FileStat {
    std::string name;
    std::string md5;
    int64_t size;
};
class ExecEnv;

// Compute the md5 and the SHA-256 of a local file by reading it once. Both are lower case hex strings.
// Pass nullptr to skip one of them. If size is not nullptr, it is set to the number of bytes read.
Status compute_file_digests(const std::string& path, std::string* md5, std::string* sha256,
                            int64_t* size = nullptr);

struct ManifestCheckResult {
    // The manifest is valid and the local tablet snapshot matches it.
    bool checked = false;
    // The SHA-256 of all the files except the tablet meta file are checked.
    bool digest_checked = false;
};

// Check the files of a downloaded local tablet snapshot against the manifest of the source tablet
// snapshot, which is recorded at backup time:
//  - the set of files must be the same, no more and no less;
//  - the size of each file must be the same;
//  - if check_digest is true, the SHA-256 of each file must be the same, if the manifest has it. The
//    tablet meta file is excluded, it is rewritten in restore.
// The names in the manifest are those of the source tablet, the tablet meta file "<source tablet id>.hdr"
// is expected as "<local_tablet_id>.hdr", as it is renamed when downloading.
//
// Returns a Corruption status with the first mismatched file, the expected and the actual value, if the
// local tablet snapshot does not match. An invalid manifest is not checked: returns OK with
// result->checked = false.
Status check_tablet_snapshot_manifest(const std::string& local_path, int64_t local_tablet_id,
                                      const TTabletManifest& manifest, bool check_digest,
                                      ManifestCheckResult* result);

class BaseSnapshotLoader {
public:
    BaseSnapshotLoader(ExecEnv* env, int64_t job_id, int64_t task_id,
                       const TNetworkAddress& broker_addr = {},
                       const std::map<std::string, std::string>& broker_prop = {});

    virtual ~BaseSnapshotLoader() = default;

    Status init(TStorageBackendType::type type, const std::string& location);

    virtual Status upload(const std::map<std::string, std::string>& src_to_dest_path,
                          std::map<int64_t, std::vector<std::string>>* tablet_files) = 0;

    virtual Status download(const std::map<std::string, std::string>& src_to_dest_path,
                            std::vector<int64_t>* downloaded_tablet_ids) = 0;

    std::shared_ptr<ResourceContext> resource_ctx() { return _resource_ctx; }

protected:
    Status _get_tablet_id_from_remote_path(const std::string& remote_path, int64_t* tablet_id);

    Status _report_every(int report_threshold, int* counter, int finished_num, int total_num,
                         TTaskType::type type);

    Status _list_with_checksum(const std::string& dir, std::map<std::string, FileStat>* md5_files);

protected:
    ExecEnv* _env = nullptr;
    int64_t _job_id;
    int64_t _task_id;
    const TNetworkAddress _broker_addr;
    const std::map<std::string, std::string> _prop;
    std::shared_ptr<io::RemoteFileSystem> _remote_fs;
    std::shared_ptr<ResourceContext> _resource_ctx;
};

/*
 * Upload:
 * upload() will upload the specified snapshot
 * to remote storage via broker.
 * Each call of upload() is responsible for several tablet snapshots.
 *
 * It will try to get the existing files in remote storage,
 * and only upload the incremental part of files.
 *
 * Download:
 * download() will download the remote tablet snapshot files
 * to local snapshot dir via broker.
 * It will also only download files which does not exist in local dir.
 *
 * Move:
 * move() is the final step of restore process. it will replace the
 * old tablet data dir with the newly downloaded snapshot dir.
 * and reload the tablet header to take this tablet on line.
 *
 */
class SnapshotLoader : public BaseSnapshotLoader {
    friend class SnapshotHttpDownloader;

public:
    SnapshotLoader(StorageEngine& engine, ExecEnv* env, int64_t job_id, int64_t task_id,
                   const TNetworkAddress& broker_addr = {},
                   const std::map<std::string, std::string>& broker_prop = {});
    ~SnapshotLoader() override {};

    Status upload(const std::map<std::string, std::string>& src_to_dest_path,
                  std::map<int64_t, std::vector<std::string>>* tablet_files) override;

    Status download(const std::map<std::string, std::string>& src_to_dest_path,
                    std::vector<int64_t>* downloaded_tablet_ids) override;

    Status remote_http_download(const std::vector<TRemoteTabletSnapshot>& remote_tablets,
                                std::vector<int64_t>* downloaded_tablet_ids);

    Status move(const std::string& snapshot_path, TabletSharedPtr tablet, bool overwrite);

    int64_t get_http_download_files_num() const { return _http_download_files_num; }

    // The expected files of each tablet to download, the key is the remote path, the same as the key
    // of src_to_dest_path of download().
    void set_expected_files(std::map<std::string, TTabletManifest> expected_files) {
        _expected_files = std::move(expected_files);
    }

    // The size and SHA-256 of the files of each tablet, collected by upload().
    const std::map<int64_t, std::vector<TSnapshotFileStat>>& tablet_file_stats() const {
        return _tablet_file_stats;
    }

    // The local tablets whose downloaded snapshot has been checked against the manifest.
    const std::vector<int64_t>& manifest_verified_tablets() const {
        return _manifest_verified_tablets;
    }

    // Whether the digests are checked for all the tablets in manifest_verified_tablets().
    bool manifest_digest_checked() const {
        return !_manifest_verified_tablets.empty() &&
               _manifest_digest_checked_num == _manifest_verified_tablets.size();
    }

private:
    // Download the files of a tablet from the remote path to the local path, see download().
    Status _download_tablet_from_remote(const std::string& remote_path,
                                        const std::string& local_path, int64_t local_tablet_id,
                                        int64_t remote_tablet_id, int* report_counter,
                                        int finished_num, int total_num);

    // Delete all the files in a local tablet snapshot dir, so that it can be downloaded again
    // without reusing any local file.
    Status _clear_local_snapshot_files(const std::string& local_path);

    void _add_manifest_verified_tablet(int64_t tablet_id, bool digest_checked);

    Status _replace_tablet_id(const std::string& file_name, int64_t tablet_id,
                              std::string* new_file_name);

    Status _check_snapshot_path_on_broken_storage(const std::string& path);

    Status _check_local_snapshot_paths(const std::map<std::string, std::string>& src_to_dest_path,
                                       bool check_src);

    Status _get_tablet_id_and_schema_hash_from_file_path(const std::string& src_path,
                                                         int64_t* tablet_id, int32_t* schema_hash);

    Status _get_existing_files_from_local(const std::string& local_path,
                                          std::vector<std::string>* local_files);

    void _set_http_download_files_num(int64_t num) { _http_download_files_num = num; }

private:
    StorageEngine& _engine;
    // for test remote_http_download
    size_t _http_download_files_num;

    std::map<std::string, TTabletManifest> _expected_files;
    std::map<int64_t, std::vector<TSnapshotFileStat>> _tablet_file_stats;
    std::vector<int64_t> _manifest_verified_tablets;
    size_t _manifest_digest_checked_num = 0;
};

} // end namespace doris
