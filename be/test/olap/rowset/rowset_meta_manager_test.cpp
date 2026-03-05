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

#include "olap/rowset/rowset_meta_manager.h"

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <boost/algorithm/string/replace.hpp>
#include <filesystem>
#include <fstream>
#include <new>
#include <string>

#include "common/config.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/binlog.h"
#include "olap/olap_define.h"
#include "olap/olap_meta.h"
#include "olap/options.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "util/uid_util.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

const std::string rowset_meta_path = "./be/test/olap/test_data/rowset_meta.json";

class RowsetMetaManagerTest : public testing::Test {
public:
    virtual void SetUp() {
        LOG(INFO) << "SetUp";

        std::string meta_path = "./meta";
        EXPECT_TRUE(std::filesystem::create_directory(meta_path));
        _meta = new (std::nothrow) OlapMeta(meta_path);
        EXPECT_NE(nullptr, _meta);
        Status st = _meta->init();
        EXPECT_TRUE(st == Status::OK());
        EXPECT_TRUE(std::filesystem::exists("./meta"));

        std::ifstream infile(rowset_meta_path);
        char buffer[1024];
        while (!infile.eof()) {
            infile.getline(buffer, 1024);
            _json_rowset_meta = _json_rowset_meta + buffer + "\n";
        }
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);
        boost::replace_all(_json_rowset_meta, "\r", "");
        _tablet_uid = TabletUid(10, 10);
    }

    virtual void TearDown() {
        SAFE_DELETE(_meta);
        EXPECT_TRUE(std::filesystem::remove_all("./meta"));
        LOG(INFO) << "TearDown";
    }

private:
    OlapMeta* _meta;
    std::string _json_rowset_meta;
    TabletUid _tablet_uid {0, 0};
};

TEST_F(RowsetMetaManagerTest, IngestBinlogMetasThenGetBinlogInfo) {
    TabletUid tablet_uid(100, 200);
    int64_t tablet_id = 12345;
    int64_t version = 5;
    std::string rowset_id_str = "020000000000000100000000000000020000000000000003";
    int64_t num_segments = 3;

    BinlogMetaEntryPB binlog_meta_entry;
    binlog_meta_entry.set_version(version);
    binlog_meta_entry.set_tablet_id(tablet_id);
    binlog_meta_entry.set_num_segments(num_segments);
    binlog_meta_entry.set_rowset_id_v2(rowset_id_str);
    std::string meta_value;
    binlog_meta_entry.SerializeToString(&meta_value);

    RowsetBinlogMetasPB metas_pb;
    auto* rowset_binlog_meta = metas_pb.add_rowset_binlog_metas();
    rowset_binlog_meta->set_rowset_id(rowset_id_str);
    rowset_binlog_meta->set_version(version);
    rowset_binlog_meta->set_num_segments(num_segments);
    rowset_binlog_meta->set_meta(meta_value);
    rowset_binlog_meta->set_data("binlog_data_placeholder");

    Status st = RowsetMetaManager::ingest_binlog_metas(_meta, tablet_uid, &metas_pb);
    ASSERT_TRUE(st.ok()) << st;

    auto [got_rowset_id, got_num_segments] =
            RowsetMetaManager::get_binlog_info(_meta, tablet_uid, std::to_string(version));
    EXPECT_EQ(rowset_id_str, got_rowset_id);
    EXPECT_EQ(num_segments, got_num_segments);
}

TEST_F(RowsetMetaManagerTest, GetBinlogInfoReturnsEmptyForMissingMeta) {
    TabletUid tablet_uid(300, 400);
    auto [rowset_id, num_segments] =
            RowsetMetaManager::get_binlog_info(_meta, tablet_uid, "999");
    EXPECT_TRUE(rowset_id.empty());
    EXPECT_EQ(-1, num_segments);
}

TEST_F(RowsetMetaManagerTest, RemoveBinlogThenGetBinlogInfoReturnsEmpty) {
    TabletUid tablet_uid(500, 600);
    int64_t version = 10;
    std::string rowset_id_str = "020000000000000100000000000000020000000000000099";

    BinlogMetaEntryPB binlog_meta_entry;
    binlog_meta_entry.set_version(version);
    binlog_meta_entry.set_tablet_id(99999);
    binlog_meta_entry.set_num_segments(2);
    binlog_meta_entry.set_rowset_id_v2(rowset_id_str);
    std::string meta_value;
    binlog_meta_entry.SerializeToString(&meta_value);

    RowsetBinlogMetasPB metas_pb;
    auto* rowset_binlog_meta = metas_pb.add_rowset_binlog_metas();
    rowset_binlog_meta->set_rowset_id(rowset_id_str);
    rowset_binlog_meta->set_version(version);
    rowset_binlog_meta->set_num_segments(2);
    rowset_binlog_meta->set_meta(meta_value);
    rowset_binlog_meta->set_data("binlog_data_placeholder");

    ASSERT_TRUE(RowsetMetaManager::ingest_binlog_metas(_meta, tablet_uid, &metas_pb).ok());

    auto [rowset_id, num_segments] =
            RowsetMetaManager::get_binlog_info(_meta, tablet_uid, std::to_string(version));
    ASSERT_EQ(rowset_id_str, rowset_id);

    std::string suffix = fmt::format("{}_{:020d}_{}", tablet_uid.to_string(), version, rowset_id_str);
    ASSERT_TRUE(RowsetMetaManager::remove_binlog(_meta, suffix).ok());

    auto [rowset_id2, num_segments2] =
            RowsetMetaManager::get_binlog_info(_meta, tablet_uid, std::to_string(version));
    EXPECT_TRUE(rowset_id2.empty());
    EXPECT_EQ(-1, num_segments2);
}

} // namespace doris
