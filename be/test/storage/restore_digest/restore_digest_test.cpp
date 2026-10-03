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

#include "storage/restore_digest/restore_digest.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>
#include <unistd.h>
#include <xxh3.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <shared_mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "agent/task_worker_pool.h"
#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/field.h"
#include "core/value/vdatetime_value.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/data_dir.h"
#include "storage/delete/delete_handler.h"
#include "storage/merger.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/rowid_conversion.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/mow/mow_transform_test_base.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "testutil/creators.h"
#include "util/defer_op.h"
#include "util/jsonb_parser_simd.h"
#include "util/jsonb_writer.h"
#include "util/sha.h"

namespace doris {
using namespace ErrorCode;

namespace restore_digest_ut {

struct Cell {
    bool null = false;
    std::string b; // raw bytes for fixed width types, content for strings
    std::optional<Field> field; // ARRAY / MAP / STRUCT values
};
using Row = std::vector<Cell>;

Cell N() {
    Cell c;
    c.null = true;
    return c;
}
template <typename T>
Cell V(T v) {
    Cell c;
    c.b.assign(reinterpret_cast<const char*>(&v), sizeof(T));
    return c;
}
Cell S(std::string s) {
    Cell c;
    c.b = std::move(s);
    return c;
}
// the JSONB binary of a JSON text
std::string jsonb(const std::string& json) {
    JsonbWriter writer;
    Status st = JsonbParser::parse(json.data(), json.size(), writer);
    EXPECT_TRUE(st.ok()) << st;
    return std::string(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
}
Cell F(Field f) {
    Cell c;
    c.field = std::move(f);
    return c;
}
// DATE / DATETIME v1 (the in-memory packed VecDateTimeValue)
Cell D1(int y, int mo, int d, int h = 0, int mi = 0, int sec = 0, bool datetime = false) {
    VecDateTimeValue v;
    v.unchecked_set_time(y, mo, d, h, mi, sec);
    v.set_type(datetime ? TIME_DATETIME : TIME_DATE);
    Cell c;
    c.b.assign(reinterpret_cast<const char*>(&v), sizeof(v));
    return c;
}
// nested values
Field FI(int32_t v) {
    return Field::create_field<TYPE_INT>(v);
}
Field FS(std::string v) {
    return Field::create_field<TYPE_STRING>(std::move(v));
}
Field FNull() {
    return Field();
}
Field FArr(Array a) {
    return Field::create_field<TYPE_ARRAY>(std::move(a));
}
Field FMap(Array keys, Array values) {
    Map m;
    m.push_back(FArr(std::move(keys)));
    m.push_back(FArr(std::move(values)));
    return Field::create_field<TYPE_MAP>(std::move(m));
}
Field FStruct(Struct st) {
    return Field::create_field<TYPE_STRUCT>(std::move(st));
}

struct ColSpec {
    ColSpec(std::string name_, std::string type_, bool key_ = false, bool nullable_ = true,
            int length_ = 4, int precision_ = 0, int frac_ = 0)
            : name(std::move(name_)),
              type(std::move(type_)),
              key(key_),
              nullable(nullable_),
              length(length_),
              precision(precision_),
              frac(frac_) {}
    std::string name;
    std::string type;
    bool key;
    bool nullable;
    int length;
    int precision;
    int frac;
    std::string aggregation;
    std::string default_value;
    std::vector<ColSpec> children; // ARRAY: element, MAP: key and value, STRUCT: fields
};

int32_t key_of(const Row& r) {
    int32_t k = 0;
    std::memcpy(&k, r[0].b.data(), sizeof(k));
    return k;
}

} // namespace restore_digest_ut

using namespace restore_digest_ut;

static StorageEngine* rd_engine_ref = nullptr;

class RestoreDigestTest : public ::testing::Test {
protected:
    void SetUp() override {
        char buffer[1024];
        ASSERT_NE(getcwd(buffer, sizeof(buffer)), nullptr);
        _dir = std::string(buffer) + "/ut_dir/restore_digest_test";
        auto st = io::global_local_filesystem()->delete_directory(_dir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(_dir);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_dir + "/tablet_path").ok());

        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        rd_engine_ref = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        _data_dir = new DataDir(*rd_engine_ref, _dir, 100000000);
        static_cast<void>(_data_dir->init());
    }
    void TearDown() override {
        SAFE_DELETE(_data_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_dir).ok());
        rd_engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    // ---------------------------------------------------------------- schema
    TabletSchemaSPtr make_schema(KeysType keys_type, const std::vector<ColSpec>& cols,
                                 int sequence_idx = -1,
                                 const std::vector<uint32_t>& cluster_key_uids = {},
                                 int schema_version = 0) {
        auto schema = std::make_shared<TabletSchema>();
        TabletSchemaPB pb;
        pb.set_keys_type(keys_type);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_NONE);
        pb.set_next_column_unique_id(static_cast<int32_t>(cols.size()) + 1);
        pb.set_schema_version(schema_version);
        if (sequence_idx >= 0) {
            pb.set_sequence_col_idx(sequence_idx);
        }
        for (uint32_t uid : cluster_key_uids) {
            pb.add_cluster_key_uids(uid);
        }
        int uid = 1;
        std::function<void(ColumnPB*, const ColSpec&, int)> fill = [&](ColumnPB* c,
                                                                       const ColSpec& spec,
                                                                       int unique_id) {
            c->set_unique_id(unique_id);
            c->set_name(spec.name);
            c->set_type(spec.type);
            c->set_is_key(spec.key);
            c->set_length(spec.length);
            c->set_index_length(spec.length);
            c->set_is_nullable(spec.nullable);
            c->set_is_bf_column(false);
            if (spec.precision > 0 || spec.frac > 0) {
                c->set_precision(spec.precision);
                c->set_frac(spec.frac);
            }
            if (!spec.aggregation.empty()) {
                c->set_aggregation(spec.aggregation);
            }
            if (!spec.default_value.empty()) {
                c->set_default_value(spec.default_value);
            }
            for (const auto& child : spec.children) {
                fill(c->add_children_columns(), child, -1);
            }
        };
        for (const auto& spec : cols) {
            fill(pb.add_column(), spec, uid++);
        }
        schema->init_from_pb(pb);
        return schema;
    }

    static std::vector<ColSpec> wide_cols() {
        return {{"k", "INT", true, false, 4},
                {"c_bool", "BOOLEAN", false, true, 1},
                {"c_i8", "TINYINT", false, true, 1},
                {"c_i16", "SMALLINT", false, true, 2},
                {"c_i64", "BIGINT", false, true, 8},
                {"c_li", "LARGEINT", false, true, 16},
                {"c_f", "FLOAT", false, true, 4},
                {"c_d", "DOUBLE", false, true, 8},
                {"c_dec32", "DECIMAL32", false, true, 4, 9, 2},
                {"c_dec64", "DECIMAL64", false, true, 8, 18, 4},
                {"c_dec128", "DECIMAL128I", false, true, 16, 38, 10},
                {"c_dec256", "DECIMAL256", false, true, 32, 76, 20},
                {"c_date", "DATEV2", false, true, 4},
                {"c_dt", "DATETIMEV2", false, true, 8, 0, 3},
                {"c_ip4", "IPV4", false, true, 4},
                {"c_ip6", "IPV6", false, true, 16},
                {"c_vc", "VARCHAR", false, true, 32},
                {"c_ch", "CHAR", false, true, 8},
                {"c_str", "STRING", false, true, 64}};
    }

    // Deterministic row for a wide schema; every nullable column is sometimes NULL.
    static Row wide_row(int i) {
        auto nul = [&](int c) { return (i + c) % (c + 3) == 0; };
        uint64_t m = static_cast<uint64_t>(i) * 2654435761ULL + 12345;
        Row r;
        r.push_back(V<int32_t>(i));
        r.push_back(nul(1) ? N() : V<uint8_t>(i % 2));
        r.push_back(nul(2) ? N() : V<int8_t>(static_cast<int8_t>(m % 100)));
        r.push_back(nul(3) ? N() : V<int16_t>(static_cast<int16_t>(m % 30000)));
        r.push_back(nul(4) ? N() : V<int64_t>(static_cast<int64_t>(m) * 7));
        r.push_back(nul(5) ? N() : V<__int128>((static_cast<__int128>(m) << 40) + i));
        r.push_back(nul(6) ? N() : V<float>(static_cast<float>(i) * 0.25F));
        r.push_back(nul(7) ? N() : V<double>(static_cast<double>(i) * 0.125));
        r.push_back(nul(8) ? N() : V<int32_t>(static_cast<int32_t>(m % 1000000)));
        r.push_back(nul(9) ? N() : V<int64_t>(static_cast<int64_t>(m % 100000000)));
        r.push_back(nul(10) ? N() : V<__int128>(static_cast<__int128>(m) * 1000003));
        {
            Cell c;
            if (nul(11)) {
                c.null = true;
            } else {
                c.b.assign(32, '\0');
                std::memcpy(c.b.data(), &m, sizeof(m));
                c.b[16] = static_cast<char>(i);
            }
            r.push_back(std::move(c));
        }
        r.push_back(nul(12) ? N() : V<uint32_t>(0x2F0000U + (i % 28)));
        r.push_back(nul(13) ? N() : V<uint64_t>(0x1F00000000ULL + static_cast<uint64_t>(i) * 1000));
        r.push_back(nul(14) ? N() : V<uint32_t>(0x0A000000U + i));
        r.push_back(nul(15) ? N()
                            : V<unsigned __int128>((static_cast<unsigned __int128>(1) << 100) + i));
        r.push_back(nul(16) ? N() : S(std::string("vc") + std::to_string(i % 97)));
        r.push_back(nul(17) ? N() : S(std::string("ab").substr(0, i % 3) + std::to_string(i % 10)));
        r.push_back(nul(18) ? N() : S(std::string(static_cast<size_t>(i % 5), 'z') + "s"));
        return r;
    }

    // ---------------------------------------------------------------- rowsets
    RowsetWriterContext make_writer_context(const TabletSchemaSPtr& schema, const Version& version,
                                            bool overlapping) {
        RowsetWriterContext ctx;
        RowsetId id;
        id.init(_next_rowset_id++);
        ctx.rowset_id = id;
        ctx.rowset_type = BETA_ROWSET;
        ctx.data_dir = _data_dir;
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = schema;
        ctx.tablet_path = _dir + "/tablet_path";
        ctx.version = version;
        ctx.segments_overlap = overlapping ? OVERLAPPING : NONOVERLAPPING;
        ctx.max_rows_per_segment = UINT32_MAX;
        return ctx;
    }

    Status append_block(RowsetWriter* writer, const TabletSchemaSPtr& schema,
                        const std::vector<Row>& rows) {
        Block block = schema->create_storage_block();
        auto columns = std::move(block).mutate_columns();
        for (const Row& r : rows) {
            for (size_t c = 0; c < r.size(); ++c) {
                if (r[c].null) {
                    columns[c]->insert_data(nullptr, 0);
                } else if (r[c].field.has_value()) {
                    columns[c]->insert(*r[c].field);
                } else {
                    columns[c]->insert_data(r[c].b.data(), r[c].b.size());
                }
            }
        }
        block.set_columns(std::move(columns));
        RETURN_IF_ERROR(writer->add_block(&block));
        return writer->flush();
    }

    // Writes `rows` as one rowset of version `version`. `rows_per_segment` decides the segment
    // split; with `overlapping` the rows are dealt round-robin to the segments, otherwise the
    // rowset is globally sorted by key and cut into consecutive segments.
    RowsetSharedPtr write_rowset(const TabletSchemaSPtr& schema, std::vector<Row> rows,
                                 int64_t version, size_t rows_per_segment, bool overlapping,
                                 int64_t start_version = -1) {
        auto by_key = [](const Row& a, const Row& b) { return key_of(a) < key_of(b); };
        std::vector<std::vector<Row>> segments;
        if (overlapping) {
            size_t n = std::max<size_t>(1, (rows.size() + rows_per_segment - 1) / rows_per_segment);
            segments.resize(n);
            for (size_t i = 0; i < rows.size(); ++i) {
                segments[i % n].push_back(std::move(rows[i]));
            }
        } else {
            std::stable_sort(rows.begin(), rows.end(), by_key);
            for (size_t i = 0; i < rows.size(); i += rows_per_segment) {
                size_t end = std::min(rows.size(), i + rows_per_segment);
                segments.emplace_back(std::make_move_iterator(rows.begin() + i),
                                      std::make_move_iterator(rows.begin() + end));
            }
        }
        auto ctx = make_writer_context(
                schema, Version(start_version < 0 ? version : start_version, version), overlapping);
        auto res = RowsetFactory::create_rowset_writer(*rd_engine_ref, ctx, true);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();
        for (auto& seg : segments) {
            std::stable_sort(seg.begin(), seg.end(), by_key);
            Status st = append_block(writer.get(), schema, seg);
            EXPECT_TRUE(st.ok()) << st;
        }
        RowsetSharedPtr rowset;
        EXPECT_EQ(Status::OK(), writer->build(rowset));
        return rowset;
    }

    // ---------------------------------------------------------------- digest
    RestoreDigest digest_of(const TabletSchemaSPtr& schema, std::vector<RowsetSharedPtr> rowsets,
                            KeysType keys_type = DUP_KEYS, bool mow = false,
                            DeleteBitmapPtr bitmap = nullptr, int64_t version = 100) {
        RestoreDigestInput in;
        in.rowsets = std::move(rowsets);
        in.schema = schema;
        in.keys_type = keys_type;
        in.enable_mow = mow;
        in.delete_bitmap = std::move(bitmap);
        in.version = version;
        RestoreDigest d;
        Status st = compute_restore_digest(in, &d);
        EXPECT_TRUE(st.ok()) << st;
        return d;
    }

    static bool same_digest(const RestoreDigest& a, const RestoreDigest& b) {
        if (a.root != b.root || a.rows != b.rows) {
            return false;
        }
        for (size_t i = 0; i < RestoreDigest::kNumBuckets; ++i) {
            if (a.buckets[i].sum != b.buckets[i].sum || a.buckets[i].count != b.buckets[i].count) {
                return false;
            }
        }
        return true;
    }

    // digest of rows written as one rowset
    RestoreDigest digest_rows(const TabletSchemaSPtr& schema, const std::vector<Row>& rows) {
        return digest_of(schema, {write_rowset(schema, rows, 2, 1000, false)});
    }


    // ---------------------------------------------------------------- helpers P2
    static ColSpec with_children(ColSpec c, std::vector<ColSpec> children) {
        c.children = std::move(children);
        return c;
    }

    // A tablet object for the Merger and the MoR reader; only its meta (keys type) matters.
    TabletSharedPtr make_meta_tablet(KeysType keys_type, const TabletSchemaSPtr& schema) {
        std::vector<TColumn> tcols;
        std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id;
        for (size_t i = 0; i < schema->num_columns(); ++i) {
            TColumn col;
            col.column_type.type = TPrimitiveType::INT;
            col.__set_column_name(schema->column(i).name());
            col.__set_is_key(schema->column(i).is_key());
            tcols.push_back(col);
            col_ordinal_to_unique_id[i] = schema->column(i).unique_id();
        }
        TTabletSchema t_schema;
        t_schema.__set_short_key_column_count(1);
        t_schema.__set_schema_hash(3333);
        t_schema.__set_keys_type(keys_type == UNIQUE_KEYS ? TKeysType::UNIQUE_KEYS
                                                          : TKeysType::DUP_KEYS);
        t_schema.__set_storage_type(TStorageType::COLUMN);
        t_schema.__set_columns(tcols);
        TabletMetaSharedPtr tablet_meta(new TabletMeta(
                2, 2, 2, 2, 2, 2, t_schema, 2, col_ordinal_to_unique_id, UniqueId(1, 2),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, false));
        TabletSharedPtr tablet(new Tablet(*rd_engine_ref, tablet_meta, _data_dir));
        static_cast<void>(tablet->init());
        return tablet;
    }

    // Merges `inputs` into one rowset of version `out`, the way a compaction does (vertical
    // merge: the delete predicates among the inputs are applied, unique keys are merged).
    RowsetSharedPtr compact(const TabletSchemaSPtr& schema, const TabletSharedPtr& tablet,
                            const std::vector<RowsetSharedPtr>& inputs, Version out,
                            ReaderType reader_type = ReaderType::READER_BASE_COMPACTION) {
        std::vector<RowsetReaderSharedPtr> readers;
        for (const auto& rs : inputs) {
            RowsetReaderSharedPtr reader;
            EXPECT_TRUE(rs->create_reader(&reader).ok());
            readers.push_back(std::move(reader));
        }
        auto ctx = make_writer_context(schema, out, false);
        auto res = RowsetFactory::create_rowset_writer(*rd_engine_ref, ctx, true);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();
        Merger::Statistics stats;
        RowIdConversion rowid_conversion;
        stats.rowid_conversion = &rowid_conversion;
        Status st = Merger::vertical_merge_rowsets(tablet, reader_type, *schema, readers,
                                                   writer.get(), 100, 2, &stats);
        EXPECT_TRUE(st.ok()) << st;
        RowsetSharedPtr merged;
        EXPECT_EQ(Status::OK(), writer->build(merged));
        return merged;
    }

    static TCondition cond(const std::string& column, const std::string& op,
                           std::vector<std::string> values) {
        TCondition c;
        c.column_name = column;
        c.condition_op = op;
        c.condition_values = std::move(values);
        return c;
    }

    // A rowset without rows which carries a DELETE condition (what DELETE FROM ... WHERE writes).
    RowsetSharedPtr write_delete_rowset(const TabletSchemaSPtr& schema, int64_t version,
                                        const std::vector<TCondition>& conditions) {
        DeletePredicatePB pred;
        Status st = DeleteHandler::generate_delete_predicate(*schema, conditions, &pred);
        EXPECT_TRUE(st.ok()) << st;
        auto ctx = make_writer_context(schema, Version(version, version), false);
        auto res = RowsetFactory::create_rowset_writer(*rd_engine_ref, ctx, true);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();
        RowsetSharedPtr rowset;
        EXPECT_EQ(Status::OK(), writer->build(rowset));
        rowset->rowset_meta()->set_delete_predicate(std::move(pred));
        return rowset;
    }

    RestoreDigest mor_digest(const TabletSchemaSPtr& schema, std::vector<RowsetSharedPtr> rowsets,
                             const TabletSharedPtr& tablet, int64_t version = 100) {
        RestoreDigestInput in;
        in.rowsets = std::move(rowsets);
        in.schema = schema;
        in.keys_type = UNIQUE_KEYS;
        in.enable_mow = false;
        in.version = version;
        in.tablet = tablet;
        RestoreDigest d;
        Status st = compute_restore_digest(in, &d);
        EXPECT_TRUE(st.ok()) << st;
        return d;
    }

    RestoreDigest threaded_digest(const TabletSchemaSPtr& schema,
                                  std::vector<RowsetSharedPtr> rowsets, KeysType keys_type,
                                  bool mow, DeleteBitmapPtr bitmap, int64_t version, int threads) {
        RestoreDigestInput in;
        in.rowsets = std::move(rowsets);
        in.schema = schema;
        in.keys_type = keys_type;
        in.enable_mow = mow;
        in.delete_bitmap = std::move(bitmap);
        in.version = version;
        in.threads = threads;
        RestoreDigest d;
        Status st = compute_restore_digest(in, &d);
        EXPECT_TRUE(st.ok()) << st;
        return d;
    }

    std::string _dir;
    DataDir* _data_dir = nullptr;
    int64_t _next_rowset_id = 10000;
};

// ===== 1. layout independence ===============================================================

TEST_F(RestoreDigestTest, LayoutIndependence) {
    auto schema = make_schema(DUP_KEYS, wide_cols());
    std::vector<Row> rows;
    for (int i = 0; i < 400; ++i) {
        rows.push_back(wide_row(i));
    }
    // exact duplicate rows: multiplicity must be preserved by every layout
    for (int i = 0; i < 20; ++i) {
        rows.push_back(wide_row(i * 3));
    }
    RestoreDigest base = digest_of(schema, {write_rowset(schema, rows, 2, 100000, false)});
    ASSERT_EQ(rows.size(), base.rows);
    ASSERT_EQ(rows.size(), base.rows_scanned);

    std::mt19937 rng(42);
    // layout B: three rowsets, shuffled arrival, overlapping segments of different sizes
    {
        auto shuffled = rows;
        std::shuffle(shuffled.begin(), shuffled.end(), rng);
        size_t third = shuffled.size() / 3;
        std::vector<Row> p1(shuffled.begin(), shuffled.begin() + third);
        std::vector<Row> p2(shuffled.begin() + third, shuffled.begin() + 2 * third);
        std::vector<Row> p3(shuffled.begin() + 2 * third, shuffled.end());
        auto b = digest_of(schema, {write_rowset(schema, p3, 4, 37, true),
                                    write_rowset(schema, p1, 2, 50, true),
                                    write_rowset(schema, p2, 3, 17, false)});
        EXPECT_TRUE(same_digest(base, b));
        EXPECT_EQ(base.schema_sig, b.schema_sig);
    }
    // layout C: many small rowsets and tiny segments
    {
        auto shuffled = rows;
        std::shuffle(shuffled.begin(), shuffled.end(), rng);
        std::vector<RowsetSharedPtr> rowsets;
        int64_t version = 2;
        for (size_t i = 0; i < shuffled.size(); i += 21) {
            size_t end = std::min(shuffled.size(), i + 21);
            std::vector<Row> part(shuffled.begin() + i, shuffled.begin() + end);
            const bool overlapping = version % 2 == 0;
            rowsets.push_back(write_rowset(schema, part, version, 5, overlapping));
            ++version;
        }
        auto c = digest_of(schema, rowsets);
        EXPECT_TRUE(same_digest(base, c));
    }
    // the digest is read only and repeatable
    EXPECT_TRUE(same_digest(base, digest_of(schema, {write_rowset(schema, rows, 2, 64, true)})));
}

TEST_F(RestoreDigestTest, CompactionKeepsDigest) {
    // (k INT key, v INT) duplicate table, the same shape the vertical compaction test uses
    std::vector<ColSpec> cols = {{"c1", "INT", true, false, 4}, {"c2", "INT", false, false, 4}};
    auto schema = make_schema(DUP_KEYS, cols);
    std::vector<Row> r1;
    std::vector<Row> r2;
    for (int i = 0; i < 3000; ++i) {
        r1.push_back({V<int32_t>(i * 2), V<int32_t>(i)});
        r2.push_back({V<int32_t>(i * 3), V<int32_t>(i % 11)});
    }
    for (int i = 0; i < 50; ++i) { // identical duplicates across the inputs
        r2.push_back({V<int32_t>(i * 2), V<int32_t>(i)});
    }
    std::vector<RowsetSharedPtr> inputs = {write_rowset(schema, r1, 2, 700, false),
                                           write_rowset(schema, r2, 3, 900, true)};
    RestoreDigest before = digest_of(schema, inputs);
    ASSERT_EQ(r1.size() + r2.size(), before.rows);

    // compact the two rowsets into one
    std::vector<TColumn> tcols;
    std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id;
    for (size_t i = 0; i < schema->num_columns(); ++i) {
        TColumn col;
        col.column_type.type = TPrimitiveType::INT;
        col.__set_column_name(schema->column(i).name());
        col.__set_is_key(schema->column(i).is_key());
        tcols.push_back(col);
        col_ordinal_to_unique_id[i] = schema->column(i).unique_id();
    }
    TTabletSchema t_schema;
    t_schema.__set_short_key_column_count(1);
    t_schema.__set_schema_hash(3333);
    t_schema.__set_keys_type(TKeysType::DUP_KEYS);
    t_schema.__set_storage_type(TStorageType::COLUMN);
    t_schema.__set_columns(tcols);
    TabletMetaSharedPtr tablet_meta(
            new TabletMeta(2, 2, 2, 2, 2, 2, t_schema, 2, col_ordinal_to_unique_id, UniqueId(1, 2),
                           TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, false));
    TabletSharedPtr tablet(new Tablet(*rd_engine_ref, tablet_meta, _data_dir));
    static_cast<void>(tablet->init());

    std::vector<RowsetReaderSharedPtr> readers;
    for (auto& rs : inputs) {
        RowsetReaderSharedPtr reader;
        ASSERT_TRUE(rs->create_reader(&reader).ok());
        readers.push_back(std::move(reader));
    }
    auto ctx = make_writer_context(schema, Version(0, 3), false);
    auto res = RowsetFactory::create_rowset_writer(*rd_engine_ref, ctx, true);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto out_writer = std::move(res).value();
    Merger::Statistics stats;
    RowIdConversion rowid_conversion;
    stats.rowid_conversion = &rowid_conversion;
    Status st = Merger::vertical_merge_rowsets(tablet, ReaderType::READER_BASE_COMPACTION, *schema,
                                               readers, out_writer.get(), 100, 2, &stats);
    ASSERT_TRUE(st.ok()) << st;
    RowsetSharedPtr merged;
    ASSERT_EQ(Status::OK(), out_writer->build(merged));
    ASSERT_EQ(before.rows, merged->num_rows());

    RestoreDigest after = digest_of(schema, {merged});
    EXPECT_TRUE(same_digest(before, after));
}

// ===== 2. any change shows ===================================================================

TEST_F(RestoreDigestTest, AnyChangeChangesDigest) {
    auto schema = make_schema(DUP_KEYS, wide_cols());
    std::vector<Row> rows;
    for (int i = 0; i < 60; ++i) {
        rows.push_back(wide_row(i));
    }
    RestoreDigest base = digest_rows(schema, rows);
    std::vector<std::string> roots {base.root};

    // change every non-key column of one row, one at a time
    const size_t target = 7;
    for (size_t c = 1; c < rows[target].size(); ++c) {
        auto mutated = rows;
        // pick a replacement which is different from the original value
        const Cell& orig = rows[target][c];
        Cell repl;
        if (orig.null) {
            // NULL -> some non-NULL value of the same column
            for (int i = 0; i < 200 && (repl.null || repl.b.empty()); ++i) {
                repl = wide_row(i)[c];
                if (!repl.null && c < 16) {
                    break;
                }
                if (!repl.null && !repl.b.empty()) {
                    break;
                }
                repl = N();
            }
            ASSERT_FALSE(repl.null);
        } else {
            repl = orig;
            if (c >= 16) { // strings
                repl.b += "x";
            } else {
                repl.b[0] = static_cast<char>(repl.b[0] ^ 0x01);
            }
        }
        mutated[target][c] = repl;
        RestoreDigest d = digest_rows(schema, mutated);
        EXPECT_NE(base.root, d.root) << "column " << c << " changed but digest did not";
        roots.push_back(d.root);

        // and set to NULL when it was not NULL
        if (!orig.null) {
            auto nulled = rows;
            nulled[target][c] = N();
            RestoreDigest dn = digest_rows(schema, nulled);
            EXPECT_NE(base.root, dn.root) << "column " << c << " set to NULL";
            roots.push_back(dn.root);
        }
    }
    // delete one row
    {
        auto fewer = rows;
        fewer.erase(fewer.begin() + 13);
        RestoreDigest d = digest_rows(schema, fewer);
        EXPECT_NE(base.root, d.root);
        EXPECT_EQ(base.rows - 1, d.rows);
        roots.push_back(d.root);
    }
    // add one duplicate row
    {
        auto more = rows;
        more.push_back(rows[21]);
        RestoreDigest d = digest_rows(schema, more);
        EXPECT_NE(base.root, d.root);
        EXPECT_EQ(base.rows + 1, d.rows);
        roots.push_back(d.root);
    }
    // all of these digests are pairwise different
    std::set<std::string> uniq(roots.begin(), roots.end());
    EXPECT_EQ(roots.size(), uniq.size());

    // swapping the values of two rows (same multiset of column values) changes the digest
    {
        std::vector<ColSpec> cols = {{"k", "INT", true, false, 4}, {"v", "INT", false, false, 4}};
        auto s2 = make_schema(DUP_KEYS, cols);
        auto d1 =
                digest_rows(s2, {{V<int32_t>(1), V<int32_t>(10)}, {V<int32_t>(2), V<int32_t>(20)}});
        auto d2 =
                digest_rows(s2, {{V<int32_t>(1), V<int32_t>(20)}, {V<int32_t>(2), V<int32_t>(10)}});
        EXPECT_NE(d1.root, d2.root);
    }
}

// ===== 3. encoding is injective ================================================================

namespace {
void expect_pairwise_different(const std::vector<std::pair<std::string, std::string>>& cases) {
    std::set<std::string> seen;
    for (const auto& [name, root] : cases) {
        EXPECT_TRUE(seen.insert(root).second) << "digest collision at case " << name;
    }
}
} // namespace

TEST_F(RestoreDigestTest, EncodingIsInjective) {
    // two nullable strings
    {
        auto schema = make_schema(DUP_KEYS, {{"k", "INT", true, false, 4},
                                             {"a", "VARCHAR", false, true, 16},
                                             {"b", "VARCHAR", false, true, 16}});
        auto one = [&](Cell a, Cell b) {
            return digest_rows(schema, {{V<int32_t>(1), std::move(a), std::move(b)}}).root;
        };
        expect_pairwise_different({{"(a,bc)", one(S("a"), S("bc"))},
                                   {"(ab,c)", one(S("ab"), S("c"))},
                                   {"(NULL,'')", one(N(), S(""))},
                                   {"('',NULL)", one(S(""), N())},
                                   {"(NULL,NULL)", one(N(), N())},
                                   {"('','')", one(S(""), S(""))},
                                   {"(abc,NULL)", one(S("abc"), N())},
                                   {"(NULL,abc)", one(N(), S("abc"))},
                                   {"(abc,'')", one(S("abc"), S(""))},
                                   {"('',abc)", one(S(""), S("abc"))}});
    }
    // NULL vs 0, and which column is NULL
    {
        auto schema = make_schema(DUP_KEYS, {{"k", "INT", true, false, 4},
                                             {"a", "INT", false, true, 4},
                                             {"b", "INT", false, true, 4}});
        auto one = [&](Cell a, Cell b) {
            return digest_rows(schema, {{V<int32_t>(1), std::move(a), std::move(b)}}).root;
        };
        expect_pairwise_different({{"(NULL,NULL)", one(N(), N())},
                                   {"(0,0)", one(V<int32_t>(0), V<int32_t>(0))},
                                   {"(NULL,0)", one(N(), V<int32_t>(0))},
                                   {"(0,NULL)", one(V<int32_t>(0), N())},
                                   {"(NULL,1)", one(N(), V<int32_t>(1))},
                                   {"(1,NULL)", one(V<int32_t>(1), N())}});
    }
    // CHAR padding boundaries: padding is not part of the value, spaces are
    {
        auto schema = make_schema(DUP_KEYS,
                                  {{"k", "INT", true, false, 4}, {"c", "CHAR", false, true, 8}});
        auto one = [&](Cell c) {
            return digest_rows(schema, {{V<int32_t>(1), std::move(c)}}).root;
        };
        expect_pairwise_different({{"'a'", one(S("a"))},
                                   {"'a '", one(S("a "))},
                                   {"' a'", one(S(" a"))},
                                   {"''", one(S(""))},
                                   {"' '", one(S(" "))},
                                   {"NULL", one(N())},
                                   {"'ab'", one(S("ab"))}});
    }
    // float / double use the raw bits: -0 and +0 differ, NULL differs from both
    {
        auto schema = make_schema(DUP_KEYS, {{"k", "INT", true, false, 4},
                                             {"f", "FLOAT", false, true, 4},
                                             {"d", "DOUBLE", false, true, 8}});
        auto one = [&](Cell f, Cell d) {
            return digest_rows(schema, {{V<int32_t>(1), std::move(f), std::move(d)}}).root;
        };
        expect_pairwise_different({{"(0,0)", one(V<float>(0.0F), V<double>(0.0))},
                                   {"(-0,0)", one(V<float>(-0.0F), V<double>(0.0))},
                                   {"(0,-0)", one(V<float>(0.0F), V<double>(-0.0))},
                                   {"(NULL,0)", one(N(), V<double>(0.0))},
                                   {"(0,NULL)", one(V<float>(0.0F), N())}});
    }
    // boolean, wide integers, ipv6 corner values
    {
        auto schema = make_schema(DUP_KEYS, {{"k", "INT", true, false, 4},
                                             {"b", "BOOLEAN", false, true, 1},
                                             {"l", "LARGEINT", false, true, 16},
                                             {"ip", "IPV6", false, true, 16}});
        auto one = [&](Cell b, Cell l, Cell ip) {
            return digest_rows(schema, {{V<int32_t>(1), std::move(b), std::move(l), std::move(ip)}})
                    .root;
        };
        const __int128 big = static_cast<__int128>(1) << 100;
        expect_pairwise_different(
                {{"base", one(V<uint8_t>(0), V<__int128>(0), V<unsigned __int128>(0))},
                 {"bool1", one(V<uint8_t>(1), V<__int128>(0), V<unsigned __int128>(0))},
                 {"li=1", one(V<uint8_t>(0), V<__int128>(1), V<unsigned __int128>(0))},
                 {"li=-1", one(V<uint8_t>(0), V<__int128>(-1), V<unsigned __int128>(0))},
                 {"li=2^100", one(V<uint8_t>(0), V<__int128>(big), V<unsigned __int128>(0))},
                 {"ip=1", one(V<uint8_t>(0), V<__int128>(0), V<unsigned __int128>(1))},
                 {"ip=2^100",
                  one(V<uint8_t>(0), V<__int128>(0),
                      V<unsigned __int128>(static_cast<unsigned __int128>(1) << 100))}});
    }
}

TEST_F(RestoreDigestTest, EncodingSpecGolden) {
    // locks the algo_version=1 byte layout and the bucket / sum arithmetic
    auto schema = make_schema(DUP_KEYS,
                              {{"k", "INT", true, false, 4}, {"s", "VARCHAR", false, true, 16}});
    RestoreDigest d = digest_rows(schema, {{V<int32_t>(1), S("ab")}, {V<int32_t>(2), N()}});
    ASSERT_EQ(2, d.rows);

    auto hash_of = [](const std::string& bytes) {
        return XXH3_128bits_withSeed(bytes.data(), bytes.size(), 1);
    };
    std::string r1;
    r1 += '\x01';
    r1.append("\x01\x00\x00\x00", 4);
    r1 += '\x01';
    r1.append("\x02\x00\x00\x00", 4);
    r1 += "ab";
    std::string r2;
    r2 += '\x01';
    r2.append("\x02\x00\x00\x00", 4);
    r2 += '\x00';
    std::array<RestoreDigest::Bucket, RestoreDigest::kNumBuckets> expect {};
    for (const std::string& r : {r1, r2}) {
        XXH128_hash_t h = hash_of(r);
        auto& b = expect[h.high64 >> 56];
        b.sum += (static_cast<unsigned __int128>(h.high64) << 64) | h.low64;
        b.count++;
    }
    for (size_t i = 0; i < expect.size(); ++i) {
        EXPECT_TRUE(expect[i].sum == d.buckets[i].sum) << "bucket " << i;
        EXPECT_EQ(expect[i].count, d.buckets[i].count) << "bucket " << i;
    }
    EXPECT_EQ(64, d.root.size());
    EXPECT_EQ(64, d.schema_sig.size());
    std::string json = d.to_json();
    EXPECT_NE(std::string::npos, json.find("\"algo_version\":1"));
    EXPECT_NE(std::string::npos, json.find("\"rows\":2"));
}

TEST_F(RestoreDigestTest, HiddenColumnsAreExcluded) {
    // version / row store columns are neither compared nor allowed to break the digest
    auto schema = make_schema(DUP_KEYS, {{"k", "INT", true, false, 4},
                                         {"v", "INT", false, true, 4},
                                         {VERSION_COL, "BIGINT", false, false, 8},
                                         {"__DORIS_ROW_STORE_COL__", "STRING", false, true, 64}});
    std::vector<Row> a;
    std::vector<Row> b;
    for (int i = 0; i < 100; ++i) {
        a.push_back(
                {V<int32_t>(i), V<int32_t>(i * 3), V<int64_t>(5), S("row" + std::to_string(i))});
        b.push_back({V<int32_t>(i), V<int32_t>(i * 3), V<int64_t>(900 + i), S("other-writer")});
    }
    RestoreDigest da = digest_rows(schema, a);
    RestoreDigest db = digest_rows(schema, b);
    EXPECT_TRUE(same_digest(da, db));
    // a visible column still matters
    b[3][1] = V<int32_t>(-1);
    EXPECT_NE(da.root, digest_rows(schema, b).root);
}

// ===== 4. merge-on-write =====================================================================

class RestoreDigestMowTest : public RestoreDigestTest {
protected:
    static std::vector<ColSpec> mow_cols() {
        return {{"k", "INT", true, false, 4},
                {"v1", "INT", false, true, 4},
                {"v2", "VARCHAR", false, true, 16},
                {SEQUENCE_COL, "INT", false, false, 4},
                {DELETE_SIGN, "TINYINT", false, false, 1}};
    }
    static Row mow_row(int k, int v1, const std::string& v2, int seq, int del) {
        return {V<int32_t>(k), V<int32_t>(v1), S(v2), V<int32_t>(seq), V<int8_t>(del)};
    }
    RestoreDigest mow_digest(const TabletSchemaSPtr& schema, std::vector<RowsetSharedPtr> rowsets,
                             DeleteBitmapPtr bitmap, int64_t version) {
        return digest_of(schema, std::move(rowsets), UNIQUE_KEYS, true, std::move(bitmap), version);
    }
};

TEST_F(RestoreDigestMowTest, SameVisibleRowsSameDigest) {
    auto schema = make_schema(UNIQUE_KEYS, mow_cols(), 3);
    auto bitmap = std::make_shared<DeleteBitmap>(990001);
    auto mark = [&](const RowsetSharedPtr& rs, uint32_t row_id, int64_t version) {
        bitmap->add({rs->rowset_id(), 0, version}, row_id);
    };

    // v2: keys 1..10
    std::vector<Row> r2;
    for (int k = 1; k <= 10; ++k) {
        r2.push_back(mow_row(k, k * 10, "a" + std::to_string(k), 1, 0));
    }
    auto rs2 = write_rowset(schema, r2, 2, 1000, false);
    // v3: update keys 3 and 5
    auto rs3 = write_rowset(schema, {mow_row(3, 31, "b3", 2, 0), mow_row(5, 51, "b5", 2, 0)}, 3,
                            1000, false);
    mark(rs2, 2, 3); // key 3
    mark(rs2, 4, 3); // key 5
    // v4: delete key 7, update key 3 again, insert key 11
    auto rs4 = write_rowset(
            schema,
            {mow_row(3, 32, "c3", 4, 0), mow_row(7, 0, "", 3, 1), mow_row(11, 110, "n11", 3, 0)}, 4,
            1000, false);
    mark(rs2, 6, 4); // key 7
    mark(rs3, 0, 4); // key 3 of v3
    // v5: delete key 9 and bring key 7 back
    auto rs5 = write_rowset(schema, {mow_row(7, 70, "d7", 5, 0), mow_row(9, 0, "", 5, 1)}, 5, 1000,
                            false);
    mark(rs2, 8, 5); // key 9
    mark(rs4, 1, 5); // the delete row of key 7
    // a mark written by a later version must not leak into reads at version 5
    mark(rs2, 0, 6); // key 1

    auto expect_state = [&](std::vector<Row> visible, int64_t version,
                            std::vector<RowsetSharedPtr> rowsets) {
        // reference tablet: only the final visible rows, no bitmap marks, written in two rowsets
        std::vector<Row> first(visible.begin(), visible.begin() + visible.size() / 2);
        std::vector<Row> second(visible.begin() + visible.size() / 2, visible.end());
        auto ref = mow_digest(schema,
                              {write_rowset(schema, first, 2, 3, true),
                               write_rowset(schema, second, 3, 1000, false)},
                              std::make_shared<DeleteBitmap>(990002), 100);
        auto actual = mow_digest(schema, rowsets, bitmap, version);
        EXPECT_TRUE(same_digest(ref, actual)) << "version " << version;
        EXPECT_EQ(visible.size(), actual.rows) << "version " << version;
    };

    // version 5
    expect_state(
            {mow_row(1, 10, "a1", 1, 0), mow_row(2, 20, "a2", 1, 0), mow_row(3, 32, "c3", 4, 0),
             mow_row(4, 40, "a4", 1, 0), mow_row(5, 51, "b5", 2, 0), mow_row(6, 60, "a6", 1, 0),
             mow_row(7, 70, "d7", 5, 0), mow_row(8, 80, "a8", 1, 0), mow_row(10, 100, "a10", 1, 0),
             mow_row(11, 110, "n11", 3, 0)},
            5, {rs2, rs3, rs4, rs5});
    // version 4: key 7 deleted, key 9 still there
    expect_state(
            {mow_row(1, 10, "a1", 1, 0), mow_row(2, 20, "a2", 1, 0), mow_row(3, 32, "c3", 4, 0),
             mow_row(4, 40, "a4", 1, 0), mow_row(5, 51, "b5", 2, 0), mow_row(6, 60, "a6", 1, 0),
             mow_row(8, 80, "a8", 1, 0), mow_row(9, 90, "a9", 1, 0), mow_row(10, 100, "a10", 1, 0),
             mow_row(11, 110, "n11", 3, 0)},
            4, {rs2, rs3, rs4});
    // version 3
    expect_state(
            {mow_row(1, 10, "a1", 1, 0), mow_row(2, 20, "a2", 1, 0), mow_row(3, 31, "b3", 2, 0),
             mow_row(4, 40, "a4", 1, 0), mow_row(5, 51, "b5", 2, 0), mow_row(6, 60, "a6", 1, 0),
             mow_row(7, 70, "a7", 1, 0), mow_row(8, 80, "a8", 1, 0), mow_row(9, 90, "a9", 1, 0),
             mow_row(10, 100, "a10", 1, 0)},
            3, {rs2, rs3});

    // dropping a bitmap mark (a stale row becomes visible again) changes the digest
    auto full = mow_digest(schema, {rs2, rs3, rs4, rs5}, bitmap, 5);
    auto missing_mark = std::make_shared<DeleteBitmap>(990003);
    for (const auto& [k, bm] : bitmap->delete_bitmap) {
        for (uint32_t row : bm) {
            if (std::get<0>(k) == rs2->rowset_id() && row == 2) {
                continue;
            }
            missing_mark->add(k, row);
        }
    }
    EXPECT_NE(full.root, mow_digest(schema, {rs2, rs3, rs4, rs5}, missing_mark, 5).root);

    // the sequence column takes part in the digest
    auto seq_changed =
            mow_digest(schema, {write_rowset(schema, {mow_row(1, 10, "a1", 1, 0)}, 2, 10, false)},
                       std::make_shared<DeleteBitmap>(990004), 5);
    auto seq_other =
            mow_digest(schema, {write_rowset(schema, {mow_row(1, 10, "a1", 2, 0)}, 2, 10, false)},
                       std::make_shared<DeleteBitmap>(990005), 5);
    EXPECT_NE(seq_changed.root, seq_other.root);
}

// ===== 6. delete predicates =================================================================

namespace {
struct Rec {
    int k;
    std::optional<int> v;
    std::string s;
};
} // namespace

class RestoreDigestDeleteTest : public RestoreDigestTest {
protected:
    static std::vector<ColSpec> cols() {
        return {{"k", "INT", true, false, 4},
                {"v", "INT", false, true, 4},
                {"s", "VARCHAR", false, true, 16}};
    }
    static Row to_row(const Rec& r) {
        return {V<int32_t>(r.k), r.v.has_value() ? V<int32_t>(*r.v) : N(), S(r.s)};
    }
    static std::vector<Row> rows_of(const std::vector<Rec>& recs) {
        std::vector<Row> out;
        for (const Rec& r : recs) {
            out.push_back(to_row(r));
        }
        return out;
    }
    static std::vector<Rec> concat(std::vector<Rec> a, const std::vector<Rec>& b) {
        a.insert(a.end(), b.begin(), b.end());
        return a;
    }
};

TEST_F(RestoreDigestDeleteTest, AppliedByVersionAndStableAcrossCompaction) {
    auto schema = make_schema(DUP_KEYS, cols());
    std::vector<Rec> a;
    std::vector<Rec> b;
    std::vector<Rec> c;
    std::vector<Rec> e;
    for (int k = 0; k < 200; ++k) {
        a.push_back({k, k % 7 == 0 ? std::nullopt : std::optional<int>(k % 10), "a" + std::to_string(k)});
    }
    for (int k = 100; k < 260; ++k) { // keys 100..199 exist in `a` as well: duplicates stay
        b.push_back({k, (k * 3) % 10, "b" + std::to_string(k)});
    }
    for (int k = 300; k < 341; ++k) {
        c.push_back({k, 3, "c" + std::to_string(k)});
    }
    for (int k = 400; k < 411; ++k) {
        e.push_back({k, 7, "e" + std::to_string(k)});
    }
    // d4: DELETE WHERE k >= 10 AND v = 3 (version 4), d6: DELETE WHERE v = 7 (version 6)
    auto d4 = [](const Rec& r) { return r.k >= 10 && r.v.has_value() && *r.v == 3; };
    auto d6 = [](const Rec& r) { return r.v.has_value() && *r.v == 7; };
    auto survivors_at = [&](int ver) {
        std::vector<Rec> out;
        auto keep_old = [&](const Rec& r) { return !((ver >= 4 && d4(r)) || (ver >= 6 && d6(r))); };
        for (const Rec& r : a) {
            if (keep_old(r)) {
                out.push_back(r);
            }
        }
        for (const Rec& r : b) {
            if (ver >= 3 && keep_old(r)) {
                out.push_back(r);
            }
        }
        if (ver >= 5) {
            for (const Rec& r : c) {
                if (!(ver >= 6 && d6(r))) {
                    out.push_back(r);
                }
            }
        }
        if (ver >= 7) {
            out.insert(out.end(), e.begin(), e.end());
        }
        return out;
    };

    auto rs2 = write_rowset(schema, rows_of(a), 2, 50, false);
    auto rs3 = write_rowset(schema, rows_of(b), 3, 37, true);
    auto rs4 = write_delete_rowset(schema, 4, {cond("k", ">=", {"10"}), cond("v", "=", {"3"})});
    auto rs5 = write_rowset(schema, rows_of(c), 5, 20, false);
    auto rs6 = write_delete_rowset(schema, 6, {cond("v", "=", {"7"})});
    auto rs7 = write_rowset(schema, rows_of(e), 7, 20, false);
    ASSERT_TRUE(rs4->rowset_meta()->has_delete_predicate());
    ASSERT_EQ(0, rs4->num_rows());

    // every prefix of the version chain sees the conditions up to its version
    const std::vector<RowsetSharedPtr> all = {rs2, rs3, rs4, rs5, rs6, rs7};
    for (int ver = 3; ver <= 7; ++ver) {
        std::vector<RowsetSharedPtr> prefix(all.begin(), all.begin() + (ver - 1));
        RestoreDigest d = digest_of(schema, prefix, DUP_KEYS, false, nullptr, ver);
        auto expect = survivors_at(ver);
        RestoreDigest ref = digest_rows(schema, rows_of(expect));
        EXPECT_TRUE(same_digest(ref, d)) << "version " << ver;
        EXPECT_EQ(expect.size(), d.rows) << "version " << ver;
        EXPECT_EQ(ver >= 6 ? 2U : (ver >= 4 ? 1U : 0U), d.delete_predicates) << "version " << ver;
    }

    RestoreDigest with_pred = digest_of(schema, all, DUP_KEYS, false, nullptr, 7);
    // the same rowsets without the delete conditions give another digest
    RestoreDigest without_pred = digest_of(schema, {rs2, rs3, rs5, rs7}, DUP_KEYS, false, nullptr, 7);
    EXPECT_NE(with_pred.root, without_pred.root);
    EXPECT_GT(without_pred.rows, with_pred.rows);

    // layout: the same rows below the first condition cut into other rowsets and segments
    {
        auto ab = concat(a, b);
        std::mt19937 rng(7);
        std::shuffle(ab.begin(), ab.end(), rng);
        std::vector<Rec> h1(ab.begin(), ab.begin() + 170);
        std::vector<Rec> h2(ab.begin() + 170, ab.end());
        auto x2 = write_rowset(schema, rows_of(h1), 2, 11, true);
        auto x3 = write_rowset(schema, rows_of(h2), 3, 1000, false);
        RestoreDigest d = digest_of(schema, {x2, x3, rs4, rs5, rs6, rs7}, DUP_KEYS, false, nullptr, 7);
        EXPECT_TRUE(same_digest(with_pred, d));
    }

    // compaction physically drops the rows; the digest does not move
    auto tablet = make_meta_tablet(DUP_KEYS, schema);
    const size_t expect_rows = survivors_at(7).size();
    auto m24 = compact(schema, tablet, {rs2, rs3, rs4}, Version(2, 4));
    ASSERT_NE(nullptr, m24);
    EXPECT_EQ(survivors_at(4).size(), m24->num_rows()) << "compaction did not apply the condition";
    EXPECT_TRUE(same_digest(with_pred, digest_of(schema, {m24, rs5, rs6, rs7}, DUP_KEYS, false, nullptr, 7)));
    EXPECT_TRUE(same_digest(digest_rows(schema, rows_of(survivors_at(4))),
                            digest_of(schema, {m24}, DUP_KEYS, false, nullptr, 4)));

    // a cumulative compaction below the conditions keeps them effective for its rows
    auto m23 = compact(schema, tablet, {rs2, rs3}, Version(2, 3),
                       ReaderType::READER_CUMULATIVE_COMPACTION);
    ASSERT_NE(nullptr, m23);
    EXPECT_EQ(a.size() + b.size(), m23->num_rows());
    EXPECT_TRUE(same_digest(with_pred, digest_of(schema, {m23, rs4, rs5, rs6, rs7}, DUP_KEYS, false, nullptr, 7)));

    auto m27 = compact(schema, tablet, {rs2, rs3, rs4, rs5, rs6, rs7}, Version(2, 7));
    ASSERT_NE(nullptr, m27);
    EXPECT_EQ(expect_rows, m27->num_rows());
    RestoreDigest after = digest_of(schema, {m27}, DUP_KEYS, false, nullptr, 7);
    EXPECT_TRUE(same_digest(with_pred, after));
    EXPECT_EQ(0U, after.delete_predicates);
}

TEST_F(RestoreDigestDeleteTest, Operators) {
    auto schema = make_schema(DUP_KEYS, cols());
    std::vector<Rec> rows;
    for (int k = 0; k < 60; ++k) {
        rows.push_back({k, k % 4 == 0 ? std::nullopt : std::optional<int>(k % 6), "a" + std::to_string(k)});
    }
    struct Case {
        std::string name;
        std::vector<TCondition> conds;
        std::function<bool(const Rec&)> deleted;
    };
    auto is = [](const Rec& r, int x) { return r.v.has_value() && *r.v == x; };
    std::vector<Case> cases = {
            {"v = 3", {cond("v", "=", {"3"})}, [&](const Rec& r) { return is(r, 3); }},
            {"v != 3", {cond("v", "!=", {"3"})}, [&](const Rec& r) { return r.v.has_value() && *r.v != 3; }},
            {"v < 2", {cond("v", "<", {"2"})}, [&](const Rec& r) { return r.v.has_value() && *r.v < 2; }},
            {"v >= 4", {cond("v", ">=", {"4"})}, [&](const Rec& r) { return r.v.has_value() && *r.v >= 4; }},
            {"v IS NULL", {cond("v", "IS", {"NULL"})}, [&](const Rec& r) { return !r.v.has_value(); }},
            {"v IS NOT NULL", {cond("v", "IS", {"NOT NULL"})}, [&](const Rec& r) { return r.v.has_value(); }},
            {"v IN (1,2)", {cond("v", "*=", {"1", "2"})}, [&](const Rec& r) { return is(r, 1) || is(r, 2); }},
            {"v NOT IN (1,2)", {cond("v", "!*=", {"1", "2"})},
             [&](const Rec& r) { return r.v.has_value() && !is(r, 1) && !is(r, 2); }},
            {"k >= 10 AND k < 30 AND v = 1",
             {cond("k", ">=", {"10"}), cond("k", "<", {"30"}), cond("v", "=", {"1"})},
             [&](const Rec& r) { return r.k >= 10 && r.k < 30 && is(r, 1); }},
            {"s = a5", {cond("s", "=", {"a5"})}, [&](const Rec& r) { return r.s == "a5"; }},
    };
    std::vector<Rec> first(rows.begin(), rows.begin() + 25);
    std::vector<Rec> second(rows.begin() + 25, rows.end());
    for (const Case& cs : cases) {
        auto rs2 = write_rowset(schema, rows_of(first), 2, 7, true);
        auto rs3 = write_rowset(schema, rows_of(second), 3, 13, false);
        auto del = write_delete_rowset(schema, 4, cs.conds);
        std::vector<Rec> expect;
        for (const Rec& r : rows) {
            if (!cs.deleted(r)) {
                expect.push_back(r);
            }
        }
        RestoreDigest d = digest_of(schema, {rs2, rs3, del}, DUP_KEYS, false, nullptr, 4);
        EXPECT_TRUE(same_digest(digest_rows(schema, rows_of(expect)), d)) << cs.name;
        EXPECT_EQ(expect.size(), d.rows) << cs.name;
        EXPECT_LT(expect.size(), rows.size()) << cs.name << " matched nothing";
    }
}

TEST_F(RestoreDigestDeleteTest, MergeOnWriteWithBitmapAndCondition) {
    std::vector<ColSpec> cols = {{"k", "INT", true, false, 4},
                                 {"v1", "INT", false, true, 4},
                                 {DELETE_SIGN, "TINYINT", false, false, 1}};
    auto schema = make_schema(UNIQUE_KEYS, cols);
    auto row = [](int k, int v, int del = 0) { return Row {V<int32_t>(k), V<int32_t>(v), V<int8_t>(del)}; };
    std::vector<Row> r2;
    for (int k = 1; k <= 10; ++k) {
        r2.push_back(row(k, k * 10));
    }
    auto rs2 = write_rowset(schema, r2, 2, 1000, false);
    auto rs3 = write_rowset(schema, {row(3, 31)}, 3, 1000, false);
    auto rs4 = write_delete_rowset(schema, 4, {cond("v1", "=", {"50"})});
    auto rs5 = write_rowset(schema, {row(5, 50), row(8, 0, 1)}, 5, 1000, false);
    auto bitmap = std::make_shared<DeleteBitmap>(990010);
    bitmap->add({rs2->rowset_id(), 0, 3}, 2); // k=3 replaced at v3
    bitmap->add({rs2->rowset_id(), 0, 5}, 4); // k=5 rewritten at v5 (the old row is also hit by d4)
    bitmap->add({rs2->rowset_id(), 0, 5}, 7); // k=8 deleted at v5

    auto expect_at = [&](int ver) {
        std::vector<Row> rows;
        for (int k = 1; k <= 10; ++k) {
            int v = k * 10;
            if (k == 3 && ver >= 3) {
                v = 31;
            }
            if (k == 5) {
                if (ver >= 5) {
                    v = 50; // the new row of version 5 is not hit by the condition of version 4
                } else if (ver >= 4) {
                    continue; // deleted by v1 = 50
                }
            }
            if (k == 8 && ver >= 5) {
                continue;
            }
            rows.push_back(row(k, v));
        }
        return rows;
    };
    const std::vector<RowsetSharedPtr> all = {rs2, rs3, rs4, rs5};
    for (int ver = 3; ver <= 5; ++ver) {
        std::vector<RowsetSharedPtr> prefix(all.begin(), all.begin() + (ver - 1));
        RestoreDigest d = digest_of(schema, prefix, UNIQUE_KEYS, true, bitmap, ver);
        auto expect = expect_at(ver);
        RestoreDigest ref = digest_of(schema, {write_rowset(schema, expect, 2, 3, true)}, UNIQUE_KEYS,
                                      true, std::make_shared<DeleteBitmap>(990011), 100);
        EXPECT_TRUE(same_digest(ref, d)) << "version " << ver;
        EXPECT_EQ(expect.size(), d.rows) << "version " << ver;
    }
}

// ===== 7. unique merge-on-read ===============================================================

class RestoreDigestMorTest : public RestoreDigestTest {
protected:
    struct MRow {
        int k;
        int v1;
        std::string v2;
        int seq;
        int del;
    };
    static std::vector<ColSpec> mor_cols(bool has_seq) {
        std::vector<ColSpec> c = {{"k", "INT", true, false, 4},
                                  {"v1", "INT", false, true, 4},
                                  {"v2", "VARCHAR", false, true, 16}};
        if (has_seq) {
            c.push_back({SEQUENCE_COL, "INT", false, false, 4});
        }
        c.push_back({DELETE_SIGN, "TINYINT", false, false, 1});
        return c;
    }
    static Row to_row(bool has_seq, const MRow& r) {
        Row out = {V<int32_t>(r.k), V<int32_t>(r.v1), S(r.v2)};
        if (has_seq) {
            out.push_back(V<int32_t>(r.seq));
        }
        out.push_back(V<int8_t>(static_cast<int8_t>(r.del)));
        return out;
    }
    static std::vector<Row> rows_of(bool has_seq, const std::vector<MRow>& rs) {
        std::vector<Row> out;
        for (const auto& r : rs) {
            out.push_back(to_row(has_seq, r));
        }
        return out;
    }
    // the visible rows after the versions `1..n` of `versions` were applied
    static std::vector<MRow> visible(bool has_seq, const std::vector<std::vector<MRow>>& versions,
                                     size_t n) {
        std::map<int, MRow> state;
        for (size_t i = 0; i < n; ++i) {
            for (const MRow& r : versions[i]) {
                auto it = state.find(r.k);
                if (it == state.end() || !has_seq || r.seq >= it->second.seq) {
                    state[r.k] = r;
                }
            }
        }
        std::vector<MRow> out;
        for (const auto& [k, r] : state) {
            if (r.del == 0) {
                out.push_back(r);
            }
        }
        return out;
    }

    void run(bool has_seq) {
        auto schema = make_schema(UNIQUE_KEYS, mor_cols(has_seq), has_seq ? 3 : -1);
        auto tablet = make_meta_tablet(UNIQUE_KEYS, schema);
        std::vector<std::vector<MRow>> versions(5);
        for (int k = 1; k <= 10; ++k) {
            versions[0].push_back({k, k * 10, "a" + std::to_string(k), 1, 0});
        }
        versions[1] = {{3, 31, "b3", 2, 0}, {5, 51, "b5", 2, 0}};
        versions[2] = {{3, 32, "c3", 4, 0}, {7, 0, "", 3, 1}, {11, 110, "n11", 3, 0}};
        versions[3] = {{7, 70, "d7", 5, 0}, {9, 0, "", 5, 1}};
        // with a sequence column this row loses against seq 4, without one it is simply newer
        versions[4] = {{3, 99, "z3", 1, 0}};

        std::vector<RowsetSharedPtr> rowsets;
        for (size_t i = 0; i < versions.size(); ++i) {
            rowsets.push_back(write_rowset(schema, rows_of(has_seq, versions[i]),
                                           static_cast<int64_t>(i) + 2, 4, i % 2 == 0));
        }
        std::vector<RestoreDigest> expected;
        for (size_t n = 1; n <= versions.size(); ++n) {
            auto vis = visible(has_seq, versions, n);
            RestoreDigest ref =
                    mor_digest(schema, {write_rowset(schema, rows_of(has_seq, vis), 2, 1000, false)}, tablet);
            ASSERT_EQ(vis.size(), ref.rows);
            expected.push_back(ref);
            std::vector<RowsetSharedPtr> prefix(rowsets.begin(), rowsets.begin() + n);
            RestoreDigest d = mor_digest(schema, prefix, tablet, static_cast<int64_t>(n) + 1);
            EXPECT_TRUE(same_digest(ref, d)) << "has_seq=" << has_seq << " version " << n + 1;
            EXPECT_EQ(vis.size(), d.rows);
            EXPECT_EQ(1U, d.threads);
        }
        if (has_seq) {
            // key 3 keeps the row with the highest sequence value, v5 changed nothing
            EXPECT_TRUE(same_digest(expected[3], expected[4]));
        } else {
            EXPECT_FALSE(same_digest(expected[3], expected[4]));
        }

        // other layout: every version in one rowset per key range, tiny segments
        {
            std::vector<RowsetSharedPtr> other;
            for (size_t i = 0; i < versions.size(); ++i) {
                other.push_back(write_rowset(schema, rows_of(has_seq, versions[i]),
                                             static_cast<int64_t>(i) + 2, 1, i % 2 == 1));
            }
            EXPECT_TRUE(same_digest(expected[4], mor_digest(schema, other, tablet, 6)));
        }

        // compaction of a prefix, and of everything
        auto c24 = compact(schema, tablet, {rowsets[0], rowsets[1], rowsets[2]}, Version(2, 4));
        ASSERT_NE(nullptr, c24);
        EXPECT_TRUE(same_digest(expected[4],
                                mor_digest(schema, {c24, rowsets[3], rowsets[4]}, tablet, 6)));
        EXPECT_TRUE(same_digest(expected[2], mor_digest(schema, {c24}, tablet, 4)));
        auto c26 = compact(schema, tablet, rowsets, Version(2, 6));
        ASSERT_NE(nullptr, c26);
        EXPECT_TRUE(same_digest(expected[4], mor_digest(schema, {c26}, tablet, 6)));
        auto c46 = compact(schema, tablet, {rowsets[2], rowsets[3], rowsets[4]}, Version(4, 6),
                           ReaderType::READER_CUMULATIVE_COMPACTION);
        ASSERT_NE(nullptr, c46);
        EXPECT_TRUE(same_digest(expected[4], mor_digest(schema, {rowsets[0], rowsets[1], c46}, tablet, 6)));

        // any change shows: a value, a delete sign, a missing update
        auto mutate = [&](size_t version_idx, size_t row_idx, const std::function<void(MRow&)>& f) {
            auto copy = versions;
            f(copy[version_idx][row_idx]);
            std::vector<RowsetSharedPtr> rs;
            for (size_t i = 0; i < copy.size(); ++i) {
                rs.push_back(write_rowset(schema, rows_of(has_seq, copy[i]), static_cast<int64_t>(i) + 2, 4, false));
            }
            return mor_digest(schema, rs, tablet, 6).root;
        };
        std::set<std::string> roots {expected[4].root};
        roots.insert(mutate(0, 0, [](MRow& r) { r.v1 += 1; }));
        roots.insert(mutate(3, 0, [](MRow& r) { r.v2 = "d7x"; }));
        roots.insert(mutate(3, 1, [](MRow& r) { r.del = 0; })); // key 9 stays
        roots.insert(mutate(2, 1, [](MRow& r) { r.del = 0; })); // key 7 deleted -> not deleted: still replaced at v5
        roots.insert(mutate(1, 1, [](MRow& r) { r.v1 = 52; }));
        // the last one only matters when the version / sequence order decides
        EXPECT_GE(roots.size(), 5U);
    }
};

TEST_F(RestoreDigestMorTest, MultipleUpdatesEqualFinalRowsWithSequenceColumn) {
    run(true);
}

TEST_F(RestoreDigestMorTest, MultipleUpdatesEqualFinalRowsWithoutSequenceColumn) {
    run(false);
}

// ===== 8. types added in P2 ==================================================================

class RestoreDigestTypesTest : public RestoreDigestTest {
protected:
    static std::vector<ColSpec> type_cols() {
        return {{"k", "INT", true, false, 4},
                with_children({"c_arr", "ARRAY", false, true, 24}, {{"item", "INT", false, true, 4}}),
                with_children({"c_arr_s", "ARRAY", false, true, 24},
                              {{"item", "VARCHAR", false, true, 16}}),
                with_children({"c_arr2", "ARRAY", false, true, 24},
                              {with_children({"item", "ARRAY", false, true, 24},
                                             {{"item", "INT", false, true, 4}})}),
                with_children({"c_map", "MAP", false, true, 24},
                              {{"key", "VARCHAR", false, true, 16}, {"value", "INT", false, true, 4}}),
                with_children({"c_st", "STRUCT", false, true, 24},
                              {{"a", "INT", false, true, 4}, {"b", "VARCHAR", false, true, 16}}),
                {"c_json", "JSONB", false, true, 64},
                {"c_dec2", "DECIMAL", false, true, 16, 27, 9},
                {"c_date1", "DATE", false, true, 3},
                {"c_dt1", "DATETIME", false, true, 8}};
    }
    static Field opt(bool null, int v) { return null ? FNull() : FI(v); }

    // Deterministic row; every nullable column, and the elements, are sometimes NULL.
    static Row typed_row(int i) {
        auto nul = [&](int c) { return (i + c) % (c + 4) == 0; };
        Row r;
        r.push_back(V<int32_t>(i));
        {
            Array a;
            for (int j = 0; j < i % 4; ++j) {
                a.push_back(opt((i + j) % 5 == 0, i * 10 + j));
            }
            r.push_back(nul(1) ? N() : F(FArr(a)));
        }
        {
            Array a;
            for (int j = 0; j < i % 3; ++j) {
                a.push_back((i + j) % 4 == 0 ? FNull() : FS(std::string(static_cast<size_t>(j), 'q') + std::to_string(i)));
            }
            r.push_back(nul(2) ? N() : F(FArr(a)));
        }
        {
            Array outer;
            for (int j = 0; j < i % 3; ++j) {
                Array inner;
                for (int m = 0; m < (i + j) % 3; ++m) {
                    inner.push_back(opt(m == 1 && i % 2 == 0, j * 100 + m));
                }
                outer.push_back(FArr(inner));
            }
            r.push_back(nul(3) ? N() : F(FArr(outer)));
        }
        {
            Array keys;
            Array values;
            for (int j = 0; j < i % 3; ++j) {
                keys.push_back(FS("k" + std::to_string(j)));
                values.push_back(opt((i + j) % 4 == 0, i + j));
            }
            r.push_back(nul(4) ? N() : F(FMap(keys, values)));
        }
        {
            Struct st;
            st.push_back(opt(i % 3 == 0, i));
            st.push_back(i % 5 == 0 ? FNull() : FS("s" + std::to_string(i % 11)));
            r.push_back(nul(5) ? N() : F(FStruct(st)));
        }
        r.push_back(nul(6) ? N() : S(jsonb("{\"id\":" + std::to_string(i) + "}")));
        r.push_back(nul(7) ? N() : V<__int128>(static_cast<__int128>(i) * 1000000007LL));
        r.push_back(nul(8) ? N() : D1(2000 + i % 30, 1 + i % 12, 1 + i % 28));
        r.push_back(nul(9) ? N() : D1(2000 + i % 30, 1 + i % 12, 1 + i % 28, i % 24, i % 60, (i * 7) % 60, true));
        return r;
    }
};

TEST_F(RestoreDigestTypesTest, LayoutIndependenceAndCompaction) {
    auto schema = make_schema(DUP_KEYS, type_cols());
    std::vector<Row> rows;
    for (int i = 0; i < 300; ++i) {
        rows.push_back(typed_row(i));
    }
    for (int i = 0; i < 12; ++i) { // exact duplicates
        rows.push_back(typed_row(i * 5));
    }
    RestoreDigest base = digest_of(schema, {write_rowset(schema, rows, 2, 100000, false)});
    ASSERT_EQ(rows.size(), base.rows);

    std::mt19937 rng(11);
    auto shuffled = rows;
    std::shuffle(shuffled.begin(), shuffled.end(), rng);
    size_t half = shuffled.size() / 2;
    std::vector<Row> p1(shuffled.begin(), shuffled.begin() + half);
    std::vector<Row> p2(shuffled.begin() + half, shuffled.end());
    auto x2 = write_rowset(schema, p1, 2, 33, true);
    auto x3 = write_rowset(schema, p2, 3, 41, false);
    EXPECT_TRUE(same_digest(base, digest_of(schema, {x2, x3})));
    EXPECT_TRUE(same_digest(base, digest_of(schema, {write_rowset(schema, rows, 2, 7, true)})));

    // compaction (vertical merge) of the two rowsets
    auto tablet = make_meta_tablet(DUP_KEYS, schema);
    auto merged = compact(schema, tablet, {x2, x3}, Version(2, 3));
    ASSERT_NE(nullptr, merged);
    ASSERT_EQ(rows.size(), merged->num_rows());
    EXPECT_TRUE(same_digest(base, digest_of(schema, {merged})));
}

TEST_F(RestoreDigestTypesTest, AnyChangeChangesDigest) {
    auto schema = make_schema(DUP_KEYS, type_cols());
    auto arr = [](std::vector<Field> v) {
        Array a;
        for (auto& f : v) {
            a.push_back(f);
        }
        return F(FArr(a));
    };
    auto arr_s = arr;
    auto st = [](Field a, Field b) {
        Struct s;
        s.push_back(std::move(a));
        s.push_back(std::move(b));
        return F(FStruct(s));
    };
    auto mp = [](std::vector<std::pair<std::string, std::optional<int>>> kv) {
        Array keys;
        Array values;
        for (auto& [k, v] : kv) {
            keys.push_back(FS(k));
            values.push_back(v.has_value() ? FI(*v) : FNull());
        }
        return F(FMap(keys, values));
    };
    auto nested = [](std::vector<std::vector<std::optional<int>>> v) {
        Array outer;
        for (auto& in : v) {
            Array inner;
            for (auto& e : in) {
                inner.push_back(e.has_value() ? FI(*e) : FNull());
            }
            outer.push_back(FArr(inner));
        }
        return F(FArr(outer));
    };
    // base: col index -> value
    auto base_row = [&]() {
        Row r;
        r.push_back(V<int32_t>(1));
        r.push_back(arr({FI(1), FNull(), FI(3)}));
        r.push_back(arr_s({FS("a"), FS("bc")}));
        r.push_back(nested({{1}, {2, 3}}));
        r.push_back(mp({{"x", 1}, {"y", std::nullopt}}));
        r.push_back(st(FI(5), FS("s")));
        r.push_back(S(jsonb("{\"a\":1}")));
        r.push_back(V<__int128>(static_cast<__int128>(12345)));
        r.push_back(D1(2024, 3, 15));
        r.push_back(D1(2024, 3, 15, 10, 20, 30, true));
        return r;
    };
    std::vector<std::pair<std::string, Row>> cases;
    cases.push_back({"base", base_row()});
    auto add = [&](const std::string& name, size_t col, Cell c) {
        Row r = base_row();
        r[col] = std::move(c);
        cases.push_back({name, std::move(r)});
    };
    add("arr [1,NULL,4]", 1, arr({FI(1), FNull(), FI(4)}));
    add("arr [1,3]", 1, arr({FI(1), FI(3)}));
    add("arr [1,NULL,3,3]", 1, arr({FI(1), FNull(), FI(3), FI(3)}));
    add("arr [NULL,1,3]", 1, arr({FNull(), FI(1), FI(3)}));
    add("arr []", 1, arr({}));
    add("arr NULL", 1, N());
    add("arr [0]", 1, arr({FI(0)}));
    add("arr [NULL]", 1, arr({FNull()}));
    add("arr_s [ab,c]", 2, arr_s({FS("ab"), FS("c")}));
    add("arr_s [a,bc,'']", 2, arr_s({FS("a"), FS("bc"), FS("")}));
    add("arr_s [a,NULL]", 2, arr_s({FS("a"), FNull()}));
    add("arr_s ['']", 2, arr_s({FS("")}));
    add("arr_s []", 2, arr_s({}));
    add("arr2 [[1,2],[3]]", 3, nested({{1, 2}, {3}}));
    add("arr2 [[1],[2],[3]]", 3, nested({{1}, {2}, {3}}));
    add("arr2 [[1],[2,3],[]]", 3, nested({{1}, {2, 3}, {}}));
    add("arr2 [[1],[2,NULL]]", 3, nested({{1}, {2, std::nullopt}}));
    add("arr2 [[1,2,3]]", 3, nested({{1, 2, 3}}));
    add("arr2 []", 3, nested({}));
    add("map x:1,y:2", 4, mp({{"x", 1}, {"y", 2}}));
    add("map y:NULL,x:1 (order)", 4, mp({{"y", std::nullopt}, {"x", 1}}));
    add("map x:1", 4, mp({{"x", 1}}));
    add("map +z:0", 4, mp({{"x", 1}, {"y", std::nullopt}, {"z", 0}}));
    add("map x:NULL,y:1", 4, mp({{"x", std::nullopt}, {"y", 1}}));
    add("map xy:1", 4, mp({{"xy", 1}}));
    add("map {}", 4, mp({}));
    add("map NULL", 4, N());
    add("st (5,t)", 5, st(FI(5), FS("t")));
    add("st (6,s)", 5, st(FI(6), FS("s")));
    add("st (NULL,s)", 5, st(FNull(), FS("s")));
    add("st (5,NULL)", 5, st(FI(5), FNull()));
    add("st (NULL,NULL)", 5, st(FNull(), FNull()));
    add("st NULL", 5, N());
    add("json byte", 6, S(jsonb("{\"a\":2}")));
    add("json longer", 6, S(jsonb("{\"a\":1,\"b\":[1,2]}")));
    add("json empty obj", 6, S(jsonb("{}")));
    add("json NULL", 6, N());
    add("dec +1", 7, V<__int128>(static_cast<__int128>(12346)));
    add("dec NULL", 7, N());
    add("date +1 day", 8, D1(2024, 3, 16));
    add("date other month", 8, D1(2024, 4, 15));
    add("date NULL", 8, N());
    add("dt +1 sec", 9, D1(2024, 3, 15, 10, 20, 31, true));
    add("dt other hour", 9, D1(2024, 3, 15, 11, 20, 30, true));
    add("dt NULL", 9, N());

    std::vector<std::pair<std::string, std::string>> roots;
    for (const auto& [name, row] : cases) {
        roots.push_back({name, digest_rows(schema, {row}).root});
    }
    expect_pairwise_different(roots);
}

TEST_F(RestoreDigestTypesTest, EncodingSpecGolden) {
    // locks the byte layout of the types added after the first cut
    auto schema = make_schema(
            DUP_KEYS,
            {{"k", "INT", true, false, 4},
             with_children({"a", "ARRAY", false, true, 24}, {{"item", "INT", false, true, 4}}),
             with_children({"m", "MAP", false, true, 24},
                           {{"key", "VARCHAR", false, true, 16}, {"value", "INT", false, true, 4}}),
             with_children({"st", "STRUCT", false, true, 24},
                           {{"x", "INT", false, true, 4}, {"y", "VARCHAR", false, true, 16}}),
             {"d", "DATE", false, true, 3},
             {"dt", "DATETIME", false, true, 8},
             {"dec", "DECIMAL", false, true, 16, 27, 9},
             {"j", "JSONB", false, true, 64}});
    Array elems = {FI(1), FNull()};
    Struct fields;
    fields.push_back(FI(5));
    fields.push_back(FNull());
    const std::string json = jsonb("{\"q\":[1,null]}");
    Row row = {V<int32_t>(1),
               F(FArr(elems)),
               F(FMap({FS("x")}, {FI(7)})),
               F(FStruct(fields)),
               D1(2024, 3, 15),
               D1(2024, 3, 15, 10, 20, 30, true),
               V<__int128>(static_cast<__int128>(123456789012345678LL)),
               S(json)};
    RestoreDigest d = digest_rows(schema, {row});
    ASSERT_EQ(1, d.rows);

    std::string e;
    auto u32 = [&](uint32_t v) { e.append(reinterpret_cast<const char*>(&v), 4); };
    auto u64 = [&](uint64_t v) { e.append(reinterpret_cast<const char*>(&v), 8); };
    auto u16 = [&](uint16_t v) { e.append(reinterpret_cast<const char*>(&v), 2); };
    e += '\x01';
    u32(1); // k
    e += '\x01';
    u64(2); // a = [1, NULL]
    e += '\x01';
    u32(1);
    e += '\x00';
    e += '\x01';
    u64(1); // m = {"x": 7}
    e += '\x01';
    u32(1);
    e += 'x';
    e += '\x01';
    u32(7);
    e += '\x01'; // st = (5, NULL), no count
    e += '\x01';
    u32(5);
    e += '\x00';
    e += '\x01'; // d = 2024-03-15
    u16(2024);
    e += '\x03';
    e += '\x0f';
    e += '\x01'; // dt = 2024-03-15 10:20:30
    u16(2024);
    e += '\x03';
    e += '\x0f';
    e += '\x0a';
    e += '\x14';
    e += '\x1e';
    e += '\x01'; // dec as int128
    {
        __int128 v = 123456789012345678LL;
        e.append(reinterpret_cast<const char*>(&v), 16);
    }
    e += '\x01'; // j
    u32(static_cast<uint32_t>(json.size()));
    e += json;

    XXH128_hash_t h = XXH3_128bits_withSeed(e.data(), e.size(), 1);
    auto& expect_bucket = d.buckets[h.high64 >> 56];
    EXPECT_EQ(1, expect_bucket.count);
    EXPECT_TRUE(((static_cast<unsigned __int128>(h.high64) << 64) | h.low64) == expect_bucket.sum);
    EXPECT_EQ(e.size(), d.encoded_bytes);
}

// ===== 9. light schema change ================================================================

TEST_F(RestoreDigestTest, ColumnAddedByLightSchemaChangeReadsItsDefault) {
    // rowset 2 was written before the column `c` / `d` were added, rowset 3 after
    auto old_schema = make_schema(DUP_KEYS, {{"k", "INT", true, false, 4}, {"v", "INT", false, true, 4}});
    ColSpec c_col("c", "INT", false, true, 4);
    c_col.default_value = "7";
    auto new_schema = make_schema(DUP_KEYS,
                                  {{"k", "INT", true, false, 4},
                                   {"v", "INT", false, true, 4},
                                   c_col,
                                   {"d", "VARCHAR", false, true, 16}},
                                  -1, {}, 1);
    std::vector<Row> old_rows;
    std::vector<Row> old_rows_materialized;
    for (int i = 0; i < 50; ++i) {
        old_rows.push_back({V<int32_t>(i), V<int32_t>(i * 2)});
        old_rows_materialized.push_back({V<int32_t>(i), V<int32_t>(i * 2), V<int32_t>(7), N()});
    }
    std::vector<Row> new_rows;
    for (int i = 100; i < 130; ++i) {
        new_rows.push_back({V<int32_t>(i), V<int32_t>(i), V<int32_t>(i + 1), S("n")});
    }
    auto rs_old = write_rowset(old_schema, old_rows, 2, 20, false);
    auto rs_new = write_rowset(new_schema, new_rows, 3, 20, false);
    RestoreDigest lazy = digest_of(new_schema, {rs_old, rs_new});

    // the same data after a compaction (or a reload) has the default materialized
    auto all = old_rows_materialized;
    all.insert(all.end(), new_rows.begin(), new_rows.end());
    RestoreDigest materialized = digest_of(new_schema, {write_rowset(new_schema, all, 2, 1000, false)});
    EXPECT_TRUE(same_digest(lazy, materialized));
    EXPECT_EQ(80, lazy.rows);

    // a different default is a different tablet content
    ColSpec c_col8("c", "INT", false, true, 4);
    c_col8.default_value = "8";
    auto new_schema8 = make_schema(DUP_KEYS,
                                   {{"k", "INT", true, false, 4},
                                    {"v", "INT", false, true, 4},
                                    c_col8,
                                    {"d", "VARCHAR", false, true, 16}},
                                   -1, {}, 1);
    RestoreDigest other = digest_of(new_schema8, {rs_old, write_rowset(new_schema8, new_rows, 3, 20, false)});
    EXPECT_NE(lazy.root, other.root);
}

// ===== 10. parallel read =====================================================================

TEST_F(RestoreDigestTest, ThreadsDoNotChangeTheDigest) {
    auto schema = make_schema(DUP_KEYS, wide_cols());
    std::vector<Row> rows;
    for (int i = 0; i < 1500; ++i) {
        rows.push_back(wide_row(i));
    }
    std::vector<Row> p1(rows.begin(), rows.begin() + 900);
    std::vector<Row> p2(rows.begin() + 900, rows.end());
    auto rs2 = write_rowset(schema, p1, 2, 37, false); // ~25 segments
    auto rs3 = write_rowset(schema, p2, 3, 50, true);  // 12 overlapping segments
    auto rs4 = write_rowset(schema, {wide_row(9000)}, 4, 10, false);
    auto del = write_delete_rowset(schema, 5, {cond("k", ">=", {"600"}), cond("k", "<", {"900"})});
    auto rs6 = write_rowset(schema, {wide_row(600)}, 6, 10, false); // newer than the condition
    std::vector<RowsetSharedPtr> rowsets = {rs2, rs3, rs4, del, rs6};
    RestoreDigest one = threaded_digest(schema, rowsets, DUP_KEYS, false, nullptr, 6, 1);
    EXPECT_EQ(1U, one.threads);
    EXPECT_EQ(1500U + 2 - 300, one.rows);
    for (int threads : {2, 3, 8, 64}) {
        RestoreDigest d = threaded_digest(schema, rowsets, DUP_KEYS, false, nullptr, 6, threads);
        EXPECT_TRUE(same_digest(one, d)) << "threads " << threads;
        EXPECT_EQ(one.rows_scanned, d.rows_scanned);
        EXPECT_EQ(one.encoded_bytes, d.encoded_bytes);
        EXPECT_GT(d.threads, 1U);
    }
}

TEST_F(RestoreDigestMowTest, ThreadsDoNotChangeTheDigest) {
    auto schema = make_schema(UNIQUE_KEYS, mow_cols(), 3);
    auto bitmap = std::make_shared<DeleteBitmap>(990020);
    std::vector<Row> r2;
    for (int k = 1; k <= 2000; ++k) {
        r2.push_back(mow_row(k, k, "v" + std::to_string(k), 1, 0));
    }
    auto rs2 = write_rowset(schema, r2, 2, 100, false); // 20 segments, key k is at (k-1)/100, (k-1)%100
    std::vector<int> updated = {5, 150, 777, 1999, 2000};
    std::vector<Row> r3;
    std::vector<Row> final_rows = r2;
    for (int k : updated) {
        bitmap->add({rs2->rowset_id(), static_cast<uint32_t>((k - 1) / 100), 3},
                    static_cast<uint32_t>((k - 1) % 100));
        r3.push_back(mow_row(k, -k, "upd", 2, 0));
        final_rows[static_cast<size_t>(k - 1)] = mow_row(k, -k, "upd", 2, 0);
    }
    auto rs3 = write_rowset(schema, r3, 3, 2, false);
    // a deleted key
    bitmap->add({rs2->rowset_id(), 3, 4}, 0); // key 301
    auto rs4 = write_rowset(schema, {mow_row(301, 0, "", 3, 1)}, 4, 10, false);
    final_rows.erase(final_rows.begin() + 300);

    RestoreDigest ref = mow_digest(schema, {write_rowset(schema, final_rows, 2, 333, true)},
                                   std::make_shared<DeleteBitmap>(990021), 100);
    for (int threads : {1, 2, 5, 16}) {
        RestoreDigest d = threaded_digest(schema, {rs2, rs3, rs4}, UNIQUE_KEYS, true, bitmap, 4, threads);
        EXPECT_TRUE(same_digest(ref, d)) << "threads " << threads;
        EXPECT_EQ(final_rows.size(), d.rows);
    }
}

// ===== 5. not supported ======================================================================

TEST_F(RestoreDigestTest, UnsupportedModelsAndTypes) {
    auto expect_unsupported = [&](const TabletSchemaSPtr& schema, KeysType kt, bool mow,
                                  const std::string& what) {
        Status st = check_restore_digest_supported(*schema, kt, mow);
        EXPECT_TRUE(st.is<NOT_IMPLEMENTED_ERROR>()) << what << ": " << st;
        // and the compute entry never produces a digest
        RestoreDigestInput in;
        in.schema = schema;
        in.keys_type = kt;
        in.enable_mow = mow;
        in.delete_bitmap = std::make_shared<DeleteBitmap>(1);
        RestoreDigest d;
        EXPECT_TRUE(compute_restore_digest(in, &d).is<NOT_IMPLEMENTED_ERROR>()) << what;
    };
    std::vector<ColSpec> base = {{"k", "INT", true, false, 4}, {"v", "INT", false, true, 4}};

    // supported baselines
    EXPECT_TRUE(check_restore_digest_supported(*make_schema(DUP_KEYS, wide_cols()), DUP_KEYS, false)
                        .ok());
    EXPECT_TRUE(check_restore_digest_supported(
                        *make_schema(UNIQUE_KEYS, {{"k", "INT", true, false, 4},
                                                   {DELETE_SIGN, "TINYINT", false, false, 1}}),
                        UNIQUE_KEYS, true)
                        .ok());
    // merge on read is supported since P2
    EXPECT_TRUE(check_restore_digest_supported(
                        *make_schema(UNIQUE_KEYS, {{"k", "INT", true, false, 4},
                                                   {DELETE_SIGN, "TINYINT", false, false, 1}}),
                        UNIQUE_KEYS, false)
                        .ok());

    // models
    {
        auto cols = base;
        cols[1].aggregation = "SUM";
        expect_unsupported(make_schema(AGG_KEYS, cols), AGG_KEYS, false, "aggregate");
    }
    auto uniq_cols = base;
    uniq_cols.push_back({DELETE_SIGN, "TINYINT", false, false, 1});
    expect_unsupported(make_schema(UNIQUE_KEYS, base), UNIQUE_KEYS, true, "no delete sign");
    expect_unsupported(make_schema(UNIQUE_KEYS, base), UNIQUE_KEYS, false, "mor no delete sign");
    expect_unsupported(make_schema(UNIQUE_KEYS, uniq_cols, -1, {1}), UNIQUE_KEYS, true,
                       "cluster key");

    // types
    for (const char* type : {"HLL", "BITMAP", "QUANTILE_STATE", "VARIANT"}) {
        auto cols = base;
        cols.push_back({"x", type, false, true, 16});
        expect_unsupported(make_schema(DUP_KEYS, cols), DUP_KEYS, false, type);
    }
    // a nested element of an unsupported type
    {
        auto cols = base;
        cols.push_back(with_children({"m", "MAP", false, true, 24},
                                     {{"key", "VARCHAR", false, true, 16},
                                      {"value", "HLL", false, true, 16}}));
        expect_unsupported(make_schema(DUP_KEYS, cols), DUP_KEYS, false, "map<varchar,hll>");
    }
    // unknown hidden column, row binlog column
    {
        auto cols = base;
        cols.push_back({"__DORIS_SOMETHING_NEW__", "BIGINT", false, true, 8});
        expect_unsupported(make_schema(DUP_KEYS, cols), DUP_KEYS, false, "unknown hidden column");
    }
    {
        auto cols = base;
        cols.push_back({"__DORIS_BINLOG_OP__", "TINYINT", false, true, 1});
        expect_unsupported(make_schema(DUP_KEYS, cols), DUP_KEYS, false, "row binlog");
    }
}


// ===== 11. tablet level entry =================================================================

namespace {
struct TabRow {
    int32_t k;
    int32_t v;
    int8_t del = 0;
};

bool digests_equal(const RestoreDigest& a, const RestoreDigest& b) {
    if (a.root != b.root || a.rows != b.rows) {
        return false;
    }
    for (size_t i = 0; i < RestoreDigest::kNumBuckets; ++i) {
        if (a.buckets[i].sum != b.buckets[i].sum || a.buckets[i].count != b.buckets[i].count) {
            return false;
        }
    }
    return true;
}
} // namespace

// Real tablets created through the engine, rowsets added to them, and the tablet level entry
// (capture under the header lock, delete bitmap snapshot, schema choice) driven.
class RestoreDigestTabletTest : public MowTransformTestBase {
protected:
    // keys: DUP, UNIQUE (merge on write when `mow`, otherwise merge on read)
    TabletSharedPtr create_tablet(int64_t tablet_id, bool mow, bool unique = false) {
        unique = unique || mow;
        auto request = testutil::create_tablet_request(
                tablet_id, /*schema_hash=*/1, /*partition_id=*/10, /*short_key_column_count=*/1,
                unique ? TKeysType::UNIQUE_KEYS : TKeysType::DUP_KEYS,
                {{"k", TPrimitiveType::INT, true},
                 {"v", TPrimitiveType::INT, false, false, TAggregationType::NONE, true}});
        if (unique) {
            TColumn del;
            del.column_name = DELETE_SIGN;
            del.__set_is_key(false);
            del.__set_is_allow_null(false);
            del.column_type.type = TPrimitiveType::TINYINT;
            del.__set_default_value("0");
            request.tablet_schema.columns.push_back(del);
            request.tablet_schema.__set_delete_sign_idx(2);
            request.__set_enable_unique_key_merge_on_write(mow);
        }
        RuntimeProfile profile("restore_digest_ut");
        Status st = _engine->create_tablet(request, &profile);
        EXPECT_TRUE(st.ok()) << st;
        return _engine->tablet_manager()->get_tablet(tablet_id);
    }

    // Writes one rowset, one segment per entry of `segments`; every segment sorted by key.
    RowsetSharedPtr write(const TabletSharedPtr& tablet, const TabletSchemaSPtr& schema,
                          int64_t version, const std::vector<std::vector<TabRow>>& segments,
                          bool overlapping, bool add_to_tablet) {
        RowsetWriterContext ctx;
        RowsetId id;
        id.init(_next_id++);
        ctx.rowset_id = id;
        ctx.tablet_id = tablet->tablet_id();
        ctx.tablet_schema_hash = tablet->schema_hash();
        ctx.partition_id = tablet->partition_id();
        ctx.rowset_type = BETA_ROWSET;
        ctx.tablet_path = tablet->tablet_path();
        ctx.data_dir = tablet->data_dir();
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = schema;
        ctx.version = {version, version};
        ctx.enable_unique_key_merge_on_write = tablet->enable_unique_key_merge_on_write();
        ctx.write_type = DataWriteType::TYPE_DIRECT;
        ctx.tablet = tablet;
        ctx.segments_overlap = overlapping ? OVERLAPPING : NONOVERLAPPING;
        ctx.max_rows_per_segment = UINT32_MAX;
        ctx.enable_segcompaction = false;
        auto res = RowsetFactory::create_rowset_writer(*_engine, ctx, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();
        for (const auto& seg : segments) {
            Block block = schema->create_storage_block();
            auto cols = std::move(block).mutate_columns();
            for (const TabRow& r : seg) {
                size_t c = 0;
                cols[c++]->insert_data(reinterpret_cast<const char*>(&r.k), sizeof(r.k));
                cols[c++]->insert_data(reinterpret_cast<const char*>(&r.v), sizeof(r.v));
                if (c < cols.size()) {
                    cols[c++]->insert_data(reinterpret_cast<const char*>(&r.del), sizeof(r.del));
                }
            }
            block.set_columns(std::move(cols));
            EXPECT_TRUE(writer->add_block(&block).ok());
            EXPECT_TRUE(writer->flush().ok());
        }
        RowsetSharedPtr rowset;
        EXPECT_TRUE(writer->build(rowset).ok());
        if (add_to_tablet) {
            EXPECT_TRUE(tablet->add_rowset(rowset).ok());
        }
        return rowset;
    }

    RowsetSharedPtr write_delete(const TabletSharedPtr& tablet, int64_t version,
                                 const std::vector<TCondition>& conds) {
        auto schema = tablet->tablet_schema();
        DeletePredicatePB pred;
        Status st = DeleteHandler::generate_delete_predicate(*schema, conds, &pred);
        EXPECT_TRUE(st.ok()) << st;
        auto rowset = write(tablet, schema, version, {}, false, false);
        rowset->rowset_meta()->set_delete_predicate(std::move(pred));
        EXPECT_TRUE(tablet->add_rowset(rowset).ok());
        return rowset;
    }

    static TCondition cond(const std::string& column, const std::string& op,
                           const std::string& value) {
        TCondition c;
        c.column_name = column;
        c.condition_op = op;
        c.condition_values = {value};
        return c;
    }

    // digest of rows written as plain rowsets of a throw away layout, same schema and model
    RestoreDigest reference(const TabletSharedPtr& tablet, const TabletSchemaSPtr& schema,
                            const std::vector<TabRow>& rows) {
        RestoreDigestInput in;
        in.schema = schema;
        in.keys_type = tablet->keys_type();
        in.enable_mow = tablet->enable_unique_key_merge_on_write();
        if (in.enable_mow) {
            in.delete_bitmap = std::make_shared<DeleteBitmap>(tablet->tablet_id());
        }
        in.version = 100;
        in.tablet = tablet;
        std::vector<TabRow> sorted = rows;
        std::sort(sorted.begin(), sorted.end(), [](const TabRow& a, const TabRow& b) { return a.k < b.k; });
        in.rowsets.push_back(write(tablet, schema, 2, {sorted}, false, false));
        RestoreDigest d;
        Status st = compute_restore_digest(in, &d);
        EXPECT_TRUE(st.ok()) << st;
        return d;
    }

    RestoreDigest tablet_digest(int64_t tablet_id, int64_t version, int threads = 1) {
        RestoreDigest d;
        Status st = compute_tablet_restore_digest(*_engine, tablet_id, version, &d, threads);
        EXPECT_TRUE(st.ok()) << st;
        return d;
    }

    int64_t _next_id = 880000;
};

TEST_F(RestoreDigestTabletTest, MowPartialUpdateConflictRewriteNeedsTheBitmap) {
    auto tablet = create_tablet(7001, true);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    ASSERT_TRUE(tablet->enable_unique_key_merge_on_write());

    auto rs2 = write(tablet, schema, 2, {{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}, {6, 60}}}, false, true);
    // version 3: a partial column update which hit a concurrent change at publish time. Key 3 was
    // first written (segment 0) from the old row and then rewritten (segment 1): the same key
    // twice in one rowset, only the delete bitmap says which row is visible.
    auto rs3 = write(tablet, schema, 3, {{{3, 31}, {5, 51}}, {{3, 32}}}, true, true);
    auto& bitmap = tablet->tablet_meta()->delete_bitmap();
    bitmap.add({rs2->rowset_id(), 0, 3}, 2); // key 3 of version 2
    bitmap.add({rs2->rowset_id(), 0, 3}, 4); // key 5 of version 2
    bitmap.add({rs3->rowset_id(), 0, 3}, 0); // key 3, the first write of version 3
    // version 4: key 6 updated, key 1 deleted (delete sign)
    auto rs4 = write(tablet, schema, 4, {{{1, 0, 1}, {6, 61}}}, false, true);
    bitmap.add({rs2->rowset_id(), 0, 4}, 0);
    bitmap.add({rs2->rowset_id(), 0, 4}, 5);

    std::vector<TabRow> at3 = {{1, 10}, {2, 20}, {3, 32}, {4, 40}, {5, 51}, {6, 60}};
    std::vector<TabRow> at4 = {{2, 20}, {3, 32}, {4, 40}, {5, 51}, {6, 61}};
    RestoreDigest d3 = tablet_digest(7001, 3);
    RestoreDigest d4 = tablet_digest(7001, 4);
    EXPECT_TRUE(digests_equal(reference(tablet, schema, at3), d3));
    EXPECT_TRUE(digests_equal(reference(tablet, schema, at4), d4));
    EXPECT_EQ(at3.size(), d3.rows);
    EXPECT_EQ(at4.size(), d4.rows);
    EXPECT_NE(d3.root, d4.root);
    for (int threads : {2, 4}) {
        EXPECT_TRUE(digests_equal(d4, tablet_digest(7001, 4, threads))) << threads;
    }

    // what a reader without the bitmap would see differs: key 3 twice, stale rows included
    RestoreDigestInput no_bitmap;
    no_bitmap.rowsets = {rs2, rs3};
    no_bitmap.schema = schema;
    no_bitmap.keys_type = UNIQUE_KEYS;
    no_bitmap.enable_mow = true;
    no_bitmap.delete_bitmap = std::make_shared<DeleteBitmap>(7999); // get_agg caches by tablet id
    no_bitmap.version = 3;
    RestoreDigest stale;
    ASSERT_TRUE(compute_restore_digest(no_bitmap, &stale).ok());
    EXPECT_NE(d3.root, stale.root);
    EXPECT_GT(stale.rows, d3.rows);

    // a version which does not exist is an error, not a digest
    RestoreDigest none;
    EXPECT_FALSE(compute_tablet_restore_digest(*_engine, 7001, 9, &none).ok());
    EXPECT_TRUE(compute_tablet_restore_digest(*_engine, 987654, 3, &none).is<ErrorCode::NOT_FOUND>());
}

TEST_F(RestoreDigestTabletTest, MergeOnReadMergesVersionsAndDropsDeleteSign) {
    auto tablet = create_tablet(7004, /*mow=*/false, /*unique=*/true);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    ASSERT_EQ(UNIQUE_KEYS, tablet->keys_type());
    ASSERT_FALSE(tablet->enable_unique_key_merge_on_write());
    write(tablet, schema, 2, {{{1, 10}, {2, 20}, {3, 30}, {4, 40}}}, false, true);
    write(tablet, schema, 3, {{{2, 21}, {3, 31}}}, false, true);
    write(tablet, schema, 4, {{{1, 0, 1}, {4, 41}, {5, 50}}}, true, true);
    write(tablet, schema, 5, {{{1, 11}, {3, 0, 1}}}, false, true);
    // plus a condition on v: DELETE WHERE v = 41 removes the version 4 row of key 4 only
    write_delete(tablet, 6, {cond("v", "=", "41")});
    std::vector<std::vector<TabRow>> expect = {
            {{1, 10}, {2, 20}, {3, 30}, {4, 40}},
            {{1, 10}, {2, 21}, {3, 31}, {4, 40}},
            {{2, 21}, {3, 31}, {4, 41}, {5, 50}},
            {{1, 11}, {2, 21}, {4, 41}, {5, 50}},
            // the condition is applied to the rows before they are merged: key 4 falls back to
            // its older row, the way a query on a merge-on-read table reads it
            {{1, 11}, {2, 21}, {4, 40}, {5, 50}}};
    for (size_t i = 0; i < expect.size(); ++i) {
        RestoreDigest d = tablet_digest(7004, static_cast<int64_t>(i) + 2);
        EXPECT_EQ(expect[i].size(), d.rows) << "version " << i + 2;
        EXPECT_TRUE(digests_equal(reference(tablet, schema, expect[i]), d)) << "version " << i + 2;
        EXPECT_EQ(1U, d.threads);
    }
}

TEST_F(RestoreDigestTabletTest, DuplicateWithDeleteConditionBeforeAndAfterCompaction) {
    auto tablet = create_tablet(7002, false);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    std::vector<TabRow> a;
    std::vector<TabRow> b;
    for (int k = 0; k < 100; ++k) {
        a.push_back({k, k % 10});
    }
    for (int k = 50; k < 150; ++k) {
        b.push_back({k, (k * 7) % 10});
    }
    auto rs2 = write(tablet, schema, 2, {a}, false, true);
    auto rs3 = write(tablet, schema, 3, {std::vector<TabRow>(b.begin(), b.begin() + 40), std::vector<TabRow>(b.begin() + 40, b.end())}, false, true);
    write_delete(tablet, 4, {cond("v", "=", "3")});
    std::vector<TabRow> c = {{500, 3}, {501, 4}};
    write(tablet, schema, 5, {c}, false, true);

    std::vector<TabRow> expect;
    for (const auto& r : a) {
        if (r.v != 3) {
            expect.push_back(r);
        }
    }
    for (const auto& r : b) {
        if (r.v != 3) {
            expect.push_back(r);
        }
    }
    expect.insert(expect.end(), c.begin(), c.end());
    RestoreDigest before = tablet_digest(7002, 5);
    EXPECT_EQ(1U, before.delete_predicates);
    EXPECT_EQ(expect.size(), before.rows);
    EXPECT_TRUE(digests_equal(reference(tablet, schema, expect), before));
    // version 3 is below the condition: nothing deleted yet
    std::vector<TabRow> at3 = a;
    at3.insert(at3.end(), b.begin(), b.end());
    EXPECT_TRUE(digests_equal(reference(tablet, schema, at3), tablet_digest(7002, 3)));

    // the compaction of versions 2..4: rows are physically gone, the condition rowset too
    std::vector<RowsetReaderSharedPtr> readers;
    std::vector<RowsetSharedPtr> inputs;
    {
        std::shared_lock lock(tablet->get_header_lock());
        auto ret = tablet->capture_consistent_rowsets_unlocked(Version(0, 4), CaptureRowsetOps {});
        ASSERT_TRUE(ret.has_value());
        for (const auto& rs : ret->rowsets) {
            if (rs->version().first >= 2) {
                inputs.push_back(rs);
            }
        }
    }
    ASSERT_EQ(3U, inputs.size());
    for (const auto& rs : inputs) {
        RowsetReaderSharedPtr reader;
        ASSERT_TRUE(rs->create_reader(&reader).ok());
        readers.push_back(reader);
    }
    RowsetWriterContext ctx;
    RowsetId id;
    id.init(_next_id++);
    ctx.rowset_id = id;
    ctx.tablet_id = tablet->tablet_id();
    ctx.tablet_schema_hash = tablet->schema_hash();
    ctx.partition_id = tablet->partition_id();
    ctx.rowset_type = BETA_ROWSET;
    ctx.tablet_path = tablet->tablet_path();
    ctx.data_dir = tablet->data_dir();
    ctx.rowset_state = VISIBLE;
    ctx.tablet_schema = schema;
    ctx.version = {2, 4};
    ctx.segments_overlap = NONOVERLAPPING;
    ctx.max_rows_per_segment = UINT32_MAX;
    ctx.tablet = tablet;
    auto res = RowsetFactory::create_rowset_writer(*_engine, ctx, true);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto writer = std::move(res).value();
    Merger::Statistics stats;
    RowIdConversion conversion;
    stats.rowid_conversion = &conversion;
    ASSERT_TRUE(Merger::vertical_merge_rowsets(tablet, ReaderType::READER_BASE_COMPACTION, *schema,
                                               readers, writer.get(), 100, 2, &stats)
                        .ok());
    RowsetSharedPtr merged;
    ASSERT_TRUE(writer->build(merged).ok());
    EXPECT_EQ(a.size() + b.size() - 20, merged->num_rows()); // 10 rows with v = 3 in a, 10 in b
    ASSERT_TRUE(tablet->add_rowset(merged).ok());

    RestoreDigest after = tablet_digest(7002, 5);
    EXPECT_TRUE(digests_equal(before, after));
    EXPECT_EQ(0U, after.delete_predicates);
}

TEST_F(RestoreDigestTabletTest, ColumnAddedLaterIsReadAsItsDefault) {
    auto tablet = create_tablet(7003, false);
    ASSERT_NE(nullptr, tablet);
    auto old_schema = tablet->tablet_schema();
    std::vector<TabRow> rows;
    for (int k = 0; k < 40; ++k) {
        rows.push_back({k, k * 3});
    }
    write(tablet, old_schema, 2, {rows}, false, true);

    // light schema change: a column c INT DEFAULT 7 is added, the rowset is not rewritten
    TabletSchemaPB pb;
    old_schema->to_schema_pb(&pb);
    pb.set_schema_version(old_schema->schema_version() + 1);
    ColumnPB* c = pb.add_column();
    c->set_unique_id(pb.next_column_unique_id());
    pb.set_next_column_unique_id(pb.next_column_unique_id() + 1);
    c->set_name("c");
    c->set_type("INT");
    c->set_is_key(false);
    c->set_length(4);
    c->set_index_length(4);
    c->set_is_nullable(true);
    c->set_default_value("7");
    c->set_aggregation("NONE");
    auto new_schema = std::make_shared<TabletSchema>();
    new_schema->init_from_pb(pb);
    tablet->update_max_version_schema(new_schema);

    RestoreDigest d = tablet_digest(7003, 2);
    EXPECT_EQ(rows.size(), d.rows);

    // reference: the same rows with the default written out
    RestoreDigestInput ref_in;
    ref_in.schema = new_schema;
    ref_in.keys_type = DUP_KEYS;
    ref_in.version = 100;
    {
        RowsetWriterContext ctx;
        RowsetId id;
        id.init(_next_id++);
        ctx.rowset_id = id;
        ctx.tablet_id = tablet->tablet_id();
        ctx.tablet_schema_hash = tablet->schema_hash();
        ctx.partition_id = tablet->partition_id();
        ctx.rowset_type = BETA_ROWSET;
        ctx.tablet_path = tablet->tablet_path();
        ctx.data_dir = tablet->data_dir();
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = new_schema;
        ctx.version = {2, 2};
        ctx.segments_overlap = NONOVERLAPPING;
        ctx.max_rows_per_segment = UINT32_MAX;
        ctx.tablet = tablet;
        auto res = RowsetFactory::create_rowset_writer(*_engine, ctx, false);
        ASSERT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();
        Block block = new_schema->create_storage_block();
        auto cols = std::move(block).mutate_columns();
        const int32_t seven = 7;
        for (const TabRow& r : rows) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&r.k), 4);
            cols[1]->insert_data(reinterpret_cast<const char*>(&r.v), 4);
            cols[2]->insert_data(reinterpret_cast<const char*>(&seven), 4);
        }
        block.set_columns(std::move(cols));
        ASSERT_TRUE(writer->add_block(&block).ok());
        ASSERT_TRUE(writer->flush().ok());
        RowsetSharedPtr rs;
        ASSERT_TRUE(writer->build(rs).ok());
        ref_in.rowsets.push_back(rs);
    }
    RestoreDigest ref;
    ASSERT_TRUE(compute_restore_digest(ref_in, &ref).ok());
    EXPECT_TRUE(digests_equal(ref, d));
    EXPECT_EQ(ref.schema_sig, d.schema_sig);
}


// ---------------------------------------------------------------------------------------------
// Backup / restore side: the digest cache and the digest of the snapshot and RESTORE_DIGEST tasks.
// ---------------------------------------------------------------------------------------------

TEST_F(RestoreDigestTabletTest, LogicalDigestIsCachedByTabletVersionAndSchema) {
    auto tablet = create_tablet(7101, false);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    write(tablet, schema, 2, {{{1, 10}, {2, 20}, {3, 30}}}, false, true);
    write(tablet, schema, 3, {{{4, 40}, {5, 50}}}, false, true);

    RestoreDigestCache cache(/*capacity=*/16);
    LogicalDigestResult first;
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7101, 3, 2, &cache, &first).ok());
    EXPECT_FALSE(first.from_cache);
    EXPECT_EQ(5U, first.rows);
    EXPECT_EQ(RestoreDigest::kAlgoVersion, first.algo_version);
    EXPECT_EQ(1U, cache.size());
    EXPECT_EQ(0U, cache.hits());
    // same as the digest the debug endpoint computes
    RestoreDigest direct = tablet_digest(7101, 3);
    EXPECT_EQ(direct.root, first.root);
    EXPECT_EQ(direct.schema_sig, first.schema_sig);

    LogicalDigestResult second;
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7101, 3, 1, &cache, &second).ok());
    EXPECT_TRUE(second.from_cache);
    EXPECT_EQ(first.root, second.root);
    EXPECT_EQ(first.schema_sig, second.schema_sig);
    EXPECT_EQ(first.rows, second.rows);
    EXPECT_EQ(1U, cache.hits());

    // another version is another entry
    LogicalDigestResult v2;
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7101, 2, 1, &cache, &v2).ok());
    EXPECT_FALSE(v2.from_cache);
    EXPECT_EQ(3U, v2.rows);
    EXPECT_NE(first.root, v2.root);
    EXPECT_EQ(2U, cache.size());

    // no cache: always computed
    LogicalDigestResult nocache;
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7101, 3, 1, nullptr, &nocache).ok());
    EXPECT_FALSE(nocache.from_cache);
    EXPECT_EQ(first.root, nocache.root);
}

TEST(RestoreDigestCacheTest, LruEvictionAndKey) {
    RestoreDigestCache cache(/*capacity=*/2);
    auto key = [](int64_t tablet, int64_t version, uint32_t algo, const std::string& sig) {
        RestoreDigestCache::Key k;
        k.tablet_id = tablet;
        k.version = version;
        k.algo_version = algo;
        k.schema_sig = sig;
        return k;
    };
    auto value = [](const std::string& root) {
        LogicalDigestResult r;
        r.root = root;
        r.schema_sig = "sig";
        r.rows = 1;
        return r;
    };
    cache.insert(key(1, 5, 1, "a"), value("r1"));
    cache.insert(key(2, 5, 1, "a"), value("r2"));
    LogicalDigestResult out;
    // touch 1 so that 2 is the eviction victim
    ASSERT_TRUE(cache.lookup(key(1, 5, 1, "a"), &out));
    EXPECT_EQ("r1", out.root);
    EXPECT_TRUE(out.from_cache);
    cache.insert(key(3, 5, 1, "a"), value("r3"));
    EXPECT_EQ(2U, cache.size());
    EXPECT_FALSE(cache.lookup(key(2, 5, 1, "a"), &out));
    EXPECT_TRUE(cache.lookup(key(3, 5, 1, "a"), &out));
    // every field of the key matters
    EXPECT_FALSE(cache.lookup(key(1, 6, 1, "a"), &out));
    EXPECT_FALSE(cache.lookup(key(1, 5, 2, "a"), &out));
    EXPECT_FALSE(cache.lookup(key(1, 5, 1, "b"), &out));
    EXPECT_TRUE(cache.lookup(key(1, 5, 1, "a"), &out));
    // overwriting does not grow the cache
    cache.insert(key(1, 5, 1, "a"), value("r1x"));
    EXPECT_EQ(2U, cache.size());
    ASSERT_TRUE(cache.lookup(key(1, 5, 1, "a"), &out));
    EXPECT_EQ("r1x", out.root);
    cache.clear();
    EXPECT_EQ(0U, cache.size());
}

// What the snapshot task (compute_logical_digest) and the RESTORE_DIGEST task report.
TEST_F(RestoreDigestTabletTest, TaskReportsTheDigestAndHitsTheCache) {
    auto tablet = create_tablet(7102, false);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    write(tablet, schema, 2, {{{1, 10}, {2, 20}}}, false, true);
    write(tablet, schema, 3, {{{3, 30}}}, false, true);
    RestoreDigestCache::instance()->clear();
    const uint64_t hits_before = RestoreDigestCache::instance()->hits();

    TLogicalDigest d = compute_logical_digest_for_task(*_engine, 7102, 3, /*threads=*/0);
    EXPECT_EQ("OK", d.status_code);
    EXPECT_TRUE(d.__isset.root);
    EXPECT_EQ(3, d.rows);
    EXPECT_EQ(static_cast<int32_t>(RestoreDigest::kAlgoVersion), d.algo_version);
    RestoreDigest direct = tablet_digest(7102, 3);
    EXPECT_EQ(direct.root, d.root);
    EXPECT_EQ(direct.schema_sig, d.schema_sig);
    EXPECT_FALSE(d.__isset.status_msg);
    EXPECT_EQ(hits_before, RestoreDigestCache::instance()->hits());

    // the second request (e.g. the RESTORE_DIGEST task after the snapshot task) is a cache hit
    TLogicalDigest again = compute_logical_digest_for_task(*_engine, 7102, 3, /*threads=*/2);
    EXPECT_EQ("OK", again.status_code);
    EXPECT_EQ(d.root, again.root);
    EXPECT_EQ(hits_before + 1, RestoreDigestCache::instance()->hits());
}

TEST_F(RestoreDigestTabletTest, TaskReportsNotSupportedAndErrors) {
    // aggregate keys: NOT_SUPPORTED, no root
    auto request = testutil::create_tablet_request(
            7103, /*schema_hash=*/1, /*partition_id=*/10, /*short_key_column_count=*/1,
            TKeysType::AGG_KEYS,
            {{"k", TPrimitiveType::INT, true},
             {"v", TPrimitiveType::INT, false, false, TAggregationType::SUM, true}});
    RuntimeProfile profile("restore_digest_ut");
    ASSERT_TRUE(_engine->create_tablet(request, &profile).ok());
    ASSERT_NE(nullptr, _engine->tablet_manager()->get_tablet(7103));
    RestoreDigestCache::instance()->clear();
    TLogicalDigest d = compute_logical_digest_for_task(*_engine, 7103, 1, 0);
    EXPECT_EQ("NOT_SUPPORTED", d.status_code);
    EXPECT_FALSE(d.__isset.root);
    EXPECT_TRUE(d.__isset.status_msg);
    EXPECT_FALSE(d.status_msg.empty());
    // a failure is not cached
    EXPECT_EQ(0U, RestoreDigestCache::instance()->size());

    // unknown tablet: ERROR
    TLogicalDigest missing = compute_logical_digest_for_task(*_engine, 7199, 2, 0);
    EXPECT_EQ("ERROR", missing.status_code);
    EXPECT_FALSE(missing.__isset.root);
    EXPECT_FALSE(missing.status_msg.empty());

    // a version the tablet does not have: ERROR
    auto tablet = create_tablet(7104, false);
    ASSERT_NE(nullptr, tablet);
    write(tablet, tablet->tablet_schema(), 2, {{{1, 10}}}, false, true);
    TLogicalDigest bad_version = compute_logical_digest_for_task(*_engine, 7104, 9, 0);
    EXPECT_EQ("ERROR", bad_version.status_code);
    EXPECT_FALSE(bad_version.__isset.root);
}


// ===== 12. decomposed digest: compose(V) == the digest computed at V ==========================

namespace {
std::atomic<int64_t> g_prefix_bitmap_tablet_id {5000000};
int64_t next_bitmap_tablet_id() {
    return g_prefix_bitmap_tablet_id.fetch_add(1);
}
constexpr int64_t kPrefixTabletId = 777;
constexpr int64_t kNever = INT64_MAX;

struct MRow {
    int k;
    int v;
    int seq;
    int del;
};
struct MBatch {
    RowsetSharedPtr rs;
    int64_t start = 0;
    int64_t version = 0;
    std::vector<MRow> rows;
    std::vector<std::pair<int, int>> loc; // segment, row id of every row
    std::vector<int64_t> death;           // the lowest version at which the bitmap marks the row
};
} // namespace

class RestoreDigestPrefixTest : public RestoreDigestTest {
protected:
    void SetUp() override {
        RestoreDigestTest::SetUp();
        // The segment cache is keyed by the rowset id and outlives the test: never reuse the ids of
        // the other tests, whose files have other contents.
        static std::atomic<int64_t> next_base {20000000};
        _next_rowset_id = next_base.fetch_add(1000000);
    }

    static std::vector<ColSpec> dup_cols() {
        return {{"k", "INT", true, false, 4},
                {"v", "INT", false, true, 4},
                {"s", "VARCHAR", false, true, 16}};
    }
    static std::vector<ColSpec> mow_cols() {
        return {{"k", "INT", true, false, 4},
                {"v1", "INT", false, true, 4},
                {"v2", "VARCHAR", false, true, 16},
                {SEQUENCE_COL, "INT", false, false, 4},
                {DELETE_SIGN, "TINYINT", false, false, 1}};
    }
    static Row dup_row(int k, std::optional<int> v) {
        return {V<int32_t>(k), v.has_value() ? V<int32_t>(*v) : N(),
                S("s" + std::to_string(k % 13))};
    }
    static Row mow_row(const MRow& r) {
        return {V<int32_t>(r.k), V<int32_t>(r.v), S("m" + std::to_string(r.v % 7)),
                V<int32_t>(r.seq), V<int8_t>(r.del)};
    }

    RestoreDigestDecomposed decompose(const TabletSchemaSPtr& schema,
                                      const std::vector<RowsetSharedPtr>& chain, KeysType keys_type,
                                      bool mow, const DeleteBitmapPtr& bitmap, int threads = 1) {
        RestoreDigestInput in;
        in.rowsets = chain;
        in.schema = schema;
        in.keys_type = keys_type;
        in.enable_mow = mow;
        in.delete_bitmap = bitmap;
        in.version = chain.back()->end_version();
        in.threads = threads;
        RestoreDigestDecomposed d;
        Status st = decompose_restore_digest(in, kPrefixTabletId, &d);
        EXPECT_TRUE(st.ok()) << st;
        return d;
    }

    // compose(V) at every rowset boundary is bucket by bucket the digest computed at V; also through
    // a serialize / parse round trip.
    void expect_compose_equals_direct(const TabletSchemaSPtr& schema,
                                      const std::vector<RowsetSharedPtr>& chain, KeysType keys_type,
                                      bool mow, const DeleteBitmapPtr& bitmap,
                                      const RestoreDigestDecomposed& dec, const std::string& what,
                                      std::map<int64_t, RestoreDigest>* directs = nullptr) {
        ASSERT_EQ(chain.size(), dec.rowsets.size()) << what;
        std::string content = dec.serialize();
        RestoreDigestDecomposed parsed;
        Status pst = RestoreDigestDecomposed::parse(content, dec.file_root(), kPrefixTabletId, &parsed);
        ASSERT_TRUE(pst.ok()) << pst << " " << what;
        EXPECT_EQ(content, parsed.serialize()) << what;
        for (size_t i = 0; i < chain.size(); ++i) {
            const int64_t v = chain[i]->end_version();
            std::vector<RowsetSharedPtr> prefix(chain.begin(), chain.begin() + i + 1);
            RestoreDigest direct = digest_of(schema, prefix, keys_type, mow, bitmap, v);
            const RestoreDigestDecomposed* both[] = {&dec, &parsed};
            for (const RestoreDigestDecomposed* d : both) {
                ASSERT_TRUE(d->is_boundary(v)) << what << " version " << v;
                RestoreDigest composed;
                Status st = d->compose(v, &composed);
                ASSERT_TRUE(st.ok()) << st << " " << what << " version " << v;
                EXPECT_TRUE(same_digest(direct, composed)) << what << " version " << v;
                EXPECT_EQ(direct.root, composed.root) << what << " version " << v;
                EXPECT_EQ(direct.rows, composed.rows) << what << " version " << v;
                EXPECT_EQ(direct.schema_sig, composed.schema_sig) << what;
            }
            if (directs != nullptr) {
                (*directs)[v] = direct;
            }
        }
        // the digest recorded in the file is the one at the base version
        RestoreDigest base;
        ASSERT_TRUE(dec.compose(dec.base_version, &base).ok());
        EXPECT_EQ(base.root, dec.root) << what;
        EXPECT_EQ(base.rows, dec.rows) << what;
    }

    static std::vector<std::pair<int, int>> locate(const std::vector<int>& keys, size_t rps,
                                                   bool overlapping) {
        std::vector<std::pair<int, int>> loc(keys.size());
        if (keys.empty()) {
            return loc;
        }
        if (overlapping) {
            const size_t segs = std::max<size_t>(1, (keys.size() + rps - 1) / rps);
            std::vector<std::vector<size_t>> by_seg(segs);
            for (size_t i = 0; i < keys.size(); ++i) {
                by_seg[i % segs].push_back(i);
            }
            for (size_t s = 0; s < segs; ++s) {
                auto& idx = by_seg[s];
                std::stable_sort(idx.begin(), idx.end(),
                                 [&](size_t a, size_t b) { return keys[a] < keys[b]; });
                for (size_t r = 0; r < idx.size(); ++r) {
                    loc[idx[r]] = {static_cast<int>(s), static_cast<int>(r)};
                }
            }
        } else {
            std::vector<size_t> idx(keys.size());
            for (size_t i = 0; i < idx.size(); ++i) {
                idx[i] = i;
            }
            std::stable_sort(idx.begin(), idx.end(),
                             [&](size_t a, size_t b) { return keys[a] < keys[b]; });
            for (size_t p = 0; p < idx.size(); ++p) {
                loc[idx[p]] = {static_cast<int>(p / rps), static_cast<int>(p % rps)};
            }
        }
        return loc;
    }

    // ---- duplicate

    struct DupChain {
        TabletSchemaSPtr schema;
        std::vector<RowsetSharedPtr> chain;
        std::vector<bool> is_delete;
    };

    // `deletes`: also write DELETE conditions among the batches.
    DupChain make_dup_chain(unsigned seed, int batches, bool deletes) {
        std::mt19937 rng(seed);
        DupChain out;
        out.schema = make_schema(DUP_KEYS, dup_cols());
        int64_t version = 2;
        for (int b = 0; b < batches; ++b) {
            if (deletes && b > 0 && rng() % 3 == 0) {
                std::vector<TCondition> conds;
                switch (rng() % 3) {
                case 0:
                    conds = {cond("v", "=", {std::to_string(rng() % 6)})};
                    break;
                case 1:
                    conds = {cond("k", ">=", {std::to_string(20 + rng() % 20)}),
                             cond("v", "<=", {std::to_string(rng() % 6)})};
                    break;
                default:
                    conds = {cond("k", "<", {std::to_string(rng() % 8)})};
                    break;
                }
                out.chain.push_back(write_delete_rowset(out.schema, version++, conds));
                out.is_delete.push_back(true);
                continue;
            }
            const size_t n = rng() % 5 == 0 ? 0 : 1 + rng() % 60;
            std::vector<Row> rows;
            for (size_t i = 0; i < n; ++i) {
                std::optional<int> v;
                if (rng() % 7 != 0) {
                    v = static_cast<int>(rng() % 6);
                }
                rows.push_back(dup_row(static_cast<int>(rng() % 50), v));
            }
            static const size_t kSegRows[] = {3, 7, 1000};
            out.chain.push_back(write_rowset(out.schema, rows, version++, kSegRows[rng() % 3],
                                             rng() % 2 == 0));
            out.is_delete.push_back(false);
        }
        return out;
    }

    // ---- merge on write

    struct MowChain {
        TabletSchemaSPtr schema;
        std::vector<MBatch> batches;
        DeleteBitmapPtr bitmap;
        std::vector<RowsetSharedPtr> chain() const {
            std::vector<RowsetSharedPtr> c;
            for (const auto& b : batches) {
                c.push_back(b.rs);
            }
            return c;
        }
        // the number of visible rows at `version` by the model
        uint64_t visible_at(int64_t version) const {
            uint64_t n = 0;
            for (const auto& b : batches) {
                if (b.version > version) {
                    break;
                }
                for (size_t i = 0; i < b.rows.size(); ++i) {
                    n += b.rows[i].del == 0 && b.death[i] > version ? 1 : 0;
                }
            }
            return n;
        }
    };

    MowChain make_mow_chain(unsigned seed, int batches) {
        std::mt19937 rng(seed);
        MowChain out;
        out.schema = make_schema(UNIQUE_KEYS, mow_cols(), 3);
        out.bitmap = std::make_shared<DeleteBitmap>(next_bitmap_tablet_id());
        std::map<int, std::pair<size_t, size_t>> latest; // key -> (batch, row)
        int64_t version = 2;
        for (int b = 0; b < batches; ++b) {
            MBatch batch;
            batch.version = batch.start = version++;
            const size_t n = rng() % 6 == 0 ? 0 : 1 + rng() % 24;
            std::set<int> used;
            for (size_t i = 0; i < n; ++i) {
                int k = static_cast<int>(rng() % 30);
                if (!used.insert(k).second) {
                    continue;
                }
                batch.rows.push_back({k, static_cast<int>(rng() % 1000),
                                      static_cast<int>(batch.version), rng() % 5 == 0 ? 1 : 0});
            }
            // overlapping rowset with segments of two rows, and a key written twice in it: the
            // first write is marked at the version of the rowset itself
            const bool overlapping = batch.rows.size() >= 3 && rng() % 3 == 0;
            const size_t rps = overlapping ? 2 : (rng() % 2 == 0 ? 4 : 1000);
            int self_dup = -1;
            if (overlapping) {
                self_dup = static_cast<int>(rng() % (batch.rows.size() - 1));
                MRow again = batch.rows[static_cast<size_t>(self_dup)];
                again.v += 1;
                again.del = 0;
                batch.rows.insert(batch.rows.begin() + self_dup + 1, again);
            }
            std::vector<int> keys;
            for (const auto& r : batch.rows) {
                keys.push_back(r.k);
            }
            batch.loc = locate(keys, rps, overlapping);
            batch.death.assign(batch.rows.size(), kNever);

            const size_t bi = out.batches.size();
            std::vector<Row> rows;
            for (const auto& r : batch.rows) {
                rows.push_back(mow_row(r));
            }
            batch.rs = write_rowset(out.schema, rows, batch.version, rps, overlapping);
            // marks
            const int64_t mark_version = batch.version;
            auto mark = [&](MBatch& target, size_t row) {
                out.bitmap->add({target.rs->rowset_id(), static_cast<uint32_t>(target.loc[row].first),
                                 static_cast<uint64_t>(mark_version)},
                                static_cast<uint32_t>(target.loc[row].second));
                target.death[row] = std::min(target.death[row], mark_version);
            };
            out.batches.push_back(std::move(batch));
            MBatch& cur = out.batches.back();
            if (self_dup >= 0) {
                mark(cur, static_cast<size_t>(self_dup));
            }
            for (size_t i = 0; i < cur.rows.size(); ++i) {
                if (static_cast<int>(i) == self_dup) {
                    continue;
                }
                auto it = latest.find(cur.rows[i].k);
                if (it != latest.end()) {
                    MBatch& old = out.batches[it->second.first];
                    const size_t old_row = it->second.second;
                    // a delete sign row is hidden anyway, whether it is marked or not
                    if (old.rows[old_row].del == 0 || rng() % 4 != 0) {
                        mark(old, old_row);
                    }
                }
                latest[cur.rows[i].k] = {bi, i};
            }
            // noise: a row which is dead already is marked again at a later version
            if (bi > 1 && rng() % 6 == 0) {
                MBatch& old = out.batches[rng() % (bi - 1)];
                for (size_t i = 0; i < old.rows.size(); ++i) {
                    if (old.death[i] < mark_version) {
                        mark(old, i);
                        break;
                    }
                }
            }
        }
        return out;
    }

    // Merges the batches [a, b] into one rowset with the version range [start of a, end of b]: the
    // rows alive at the end version are kept (also the delete sign rows), the marks of later
    // versions follow them. Returns the chain after the compaction.
    MowChain compact_mow(const MowChain& in, size_t a, size_t b, unsigned seed) {
        std::mt19937 rng(seed);
        const int64_t end = in.batches[b].version;
        struct Kept {
            MRow row;
            int64_t death;
        };
        std::vector<Kept> kept;
        for (size_t i = a; i <= b; ++i) {
            for (size_t r = 0; r < in.batches[i].rows.size(); ++r) {
                if (in.batches[i].death[r] > end) {
                    kept.push_back({in.batches[i].rows[r], in.batches[i].death[r]});
                }
            }
        }
        const bool overlapping = false;
        const size_t rps = rng() % 2 == 0 ? 5 : 1000;
        std::vector<int> keys;
        for (const auto& k : kept) {
            keys.push_back(k.row.k);
        }
        MBatch merged;
        merged.start = in.batches[a].start;
        merged.version = end;
        merged.loc = locate(keys, rps, overlapping);
        std::vector<Row> rows;
        for (const auto& k : kept) {
            merged.rows.push_back(k.row);
            merged.death.push_back(k.death);
            rows.push_back(mow_row(k.row));
        }
        MowChain out;
        out.schema = in.schema;
        out.bitmap = std::make_shared<DeleteBitmap>(next_bitmap_tablet_id());
        merged.rs = write_rowset(in.schema, rows, end, rps, overlapping, merged.start);
        for (size_t i = 0; i < merged.rows.size(); ++i) {
            if (merged.death[i] != kNever) {
                out.bitmap->add({merged.rs->rowset_id(), static_cast<uint32_t>(merged.loc[i].first),
                                 merged.death[i]},
                                static_cast<uint32_t>(merged.loc[i].second));
            }
        }
        for (size_t i = 0; i < in.batches.size(); ++i) {
            if (i == a) {
                out.batches.push_back(merged);
            }
            if (i >= a && i <= b) {
                continue;
            }
            out.batches.push_back(in.batches[i]);
            for (size_t r = 0; r < in.batches[i].rows.size(); ++r) {
                if (in.batches[i].death[r] != kNever) {
                    out.bitmap->add({in.batches[i].rs->rowset_id(),
                                     static_cast<uint32_t>(in.batches[i].loc[r].first),
                                     in.batches[i].death[r]},
                                    static_cast<uint32_t>(in.batches[i].loc[r].second));
                }
            }
        }
        return out;
    }
};

TEST_F(RestoreDigestPrefixTest, DuplicateComposeEqualsDirectAtEveryBoundary) {
    for (unsigned seed : {1U, 2U, 3U, 4U, 5U, 6U}) {
        auto c = make_dup_chain(seed, 10, /*deletes=*/false);
        auto dec = decompose(c.schema, c.chain, DUP_KEYS, false, nullptr);
        ASSERT_EQ(10U, dec.rowsets.size());
        EXPECT_TRUE(dec.marks.empty()) << "a duplicate table without conditions has no marks";
        EXPECT_FALSE(dec.mow);
        expect_compose_equals_direct(c.schema, c.chain, DUP_KEYS, false, nullptr, dec,
                                     "seed " + std::to_string(seed));
        // the worker threads do not change a byte
        EXPECT_EQ(dec.serialize(),
                  decompose(c.schema, c.chain, DUP_KEYS, false, nullptr, 3).serialize());
        // not a boundary: nothing can be composed
        RestoreDigest d;
        for (int64_t v : {int64_t(0), int64_t(1), int64_t(100)}) {
            EXPECT_FALSE(dec.is_boundary(v));
            Status st = dec.compose(v, &d);
            EXPECT_TRUE(st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << st;
        }
    }
}

TEST_F(RestoreDigestPrefixTest, DuplicateWithDeleteConditions) {
    bool any_marks = false;
    for (unsigned seed : {11U, 12U, 13U, 14U, 15U, 16U, 17U, 18U}) {
        auto c = make_dup_chain(seed, 12, /*deletes=*/true);
        auto dec = decompose(c.schema, c.chain, DUP_KEYS, false, nullptr);
        any_marks = any_marks || !dec.marks.empty();
        // a mark version is the version of a DELETE condition
        for (const auto& mark : dec.marks) {
            bool is_condition = false;
            for (size_t i = 0; i < c.chain.size(); ++i) {
                is_condition = is_condition || (c.is_delete[i] && c.chain[i]->end_version() == mark.mark_version);
            }
            EXPECT_TRUE(is_condition) << "mark version " << mark.mark_version;
        }
        expect_compose_equals_direct(c.schema, c.chain, DUP_KEYS, false, nullptr, dec,
                                     "seed " + std::to_string(seed));
        EXPECT_EQ(dec.serialize(),
                  decompose(c.schema, c.chain, DUP_KEYS, false, nullptr, 4).serialize());
    }
    EXPECT_TRUE(any_marks) << "no DELETE condition matched a row, the test covers nothing";
}

TEST_F(RestoreDigestPrefixTest, DuplicateStaysComposableAfterCompaction) {
    for (bool deletes : {false, true}) {
        for (unsigned seed : {21U, 22U, 23U, 24U}) {
            auto c = make_dup_chain(seed, 9, deletes);
            auto schema = c.schema;
            auto tablet = make_meta_tablet(DUP_KEYS, schema);
            auto before = decompose(schema, c.chain, DUP_KEYS, false, nullptr);
            std::map<int64_t, RestoreDigest> directs;
            expect_compose_equals_direct(schema, c.chain, DUP_KEYS, false, nullptr, before, "before",
                                         &directs);
            // A base compaction covers the rowsets from the start of the chain (the DELETE
            // conditions inside are applied for good). A cumulative compaction takes a run of data
            // rowsets: the conditions after it stay effective for its rows.
            struct Run {
                size_t first;
                size_t last;
                ReaderType type;
            };
            std::vector<Run> runs = {{0, 4, ReaderType::READER_BASE_COMPACTION},
                                     {0, 7, ReaderType::READER_BASE_COMPACTION}};
            for (size_t i = 0; i + 1 < c.chain.size(); ++i) {
                if (!c.is_delete[i] && !c.is_delete[i + 1] && c.chain[i]->num_rows() > 0) {
                    size_t j = i + 1;
                    while (j + 1 < c.chain.size() && !c.is_delete[j + 1] && j - i < 3) {
                        ++j;
                    }
                    runs.push_back({i, j, ReaderType::READER_CUMULATIVE_COMPACTION});
                    break;
                }
            }
            for (const Run& run : runs) {
                std::vector<RowsetSharedPtr> inputs(c.chain.begin() + run.first,
                                                    c.chain.begin() + run.last + 1);
                auto merged = compact(schema, tablet, inputs,
                                      Version(inputs.front()->start_version(),
                                              inputs.back()->end_version()),
                                      run.type);
                ASSERT_NE(nullptr, merged);
                std::vector<RowsetSharedPtr> chain(c.chain.begin(), c.chain.begin() + run.first);
                chain.push_back(merged);
                chain.insert(chain.end(), c.chain.begin() + run.last + 1, c.chain.end());
                auto dec = decompose(schema, chain, DUP_KEYS, false, nullptr);
                const std::string what = std::string(deletes ? "deletes " : "") + "seed " +
                                         std::to_string(seed) + " run " + std::to_string(run.first) +
                                         "-" + std::to_string(run.last);
                expect_compose_equals_direct(schema, chain, DUP_KEYS, false, nullptr, dec, what);
                // the versions which are still boundaries give the digests of before the compaction
                for (const auto& rs : chain) {
                    RestoreDigest composed;
                    ASSERT_TRUE(dec.compose(rs->end_version(), &composed).ok());
                    EXPECT_TRUE(same_digest(directs.at(rs->end_version()), composed))
                            << what << " version " << rs->end_version();
                }
                // the versions inside the merged rowset are not
                for (int64_t v = merged->start_version(); v < merged->end_version(); ++v) {
                    EXPECT_FALSE(dec.is_boundary(v)) << what << " version " << v;
                    RestoreDigest composed;
                    EXPECT_TRUE(dec.compose(v, &composed).is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
                }
            }
        }
    }
}

TEST_F(RestoreDigestPrefixTest, MergeOnWriteComposeEqualsDirectAtEveryBoundary) {
    for (unsigned seed : {31U, 32U, 33U, 34U, 35U, 36U, 37U, 38U}) {
        auto c = make_mow_chain(seed, 12);
        auto chain = c.chain();
        auto dec = decompose(c.schema, chain, UNIQUE_KEYS, true, c.bitmap);
        EXPECT_TRUE(dec.mow);
        EXPECT_FALSE(dec.marks.empty());
        // the simulation itself: what the model says is visible is what the digest reads
        for (const auto& b : c.batches) {
            RestoreDigest composed;
            ASSERT_TRUE(dec.compose(b.version, &composed).ok());
            EXPECT_EQ(c.visible_at(b.version), composed.rows) << "seed " << seed << " version " << b.version;
        }
        expect_compose_equals_direct(c.schema, chain, UNIQUE_KEYS, true, c.bitmap, dec,
                                     "seed " + std::to_string(seed));
        EXPECT_EQ(dec.serialize(),
                  decompose(c.schema, chain, UNIQUE_KEYS, true, c.bitmap, 3).serialize());
        // marks only at versions of the chain, never at a version of the base or below
        for (const auto& mark : dec.marks) {
            EXPECT_GT(mark.mark_version, 2);
            EXPECT_LE(mark.mark_version, dec.base_version);
        }
    }
}

TEST_F(RestoreDigestPrefixTest, MergeOnWriteIgnoresMarksAboveTheBaseVersion) {
    auto c = make_mow_chain(41, 10);
    auto chain = c.chain();
    // the backup is taken at version 7: later marks must not leak into any part
    std::vector<RowsetSharedPtr> prefix;
    for (const auto& b : c.batches) {
        if (b.version <= 7) {
            prefix.push_back(b.rs);
        }
    }
    auto dec = decompose(c.schema, prefix, UNIQUE_KEYS, true, c.bitmap);
    EXPECT_EQ(7, dec.base_version);
    expect_compose_equals_direct(c.schema, prefix, UNIQUE_KEYS, true, c.bitmap, dec, "base 7");
    for (const auto& mark : dec.marks) {
        EXPECT_LE(mark.mark_version, 7);
    }
}

TEST_F(RestoreDigestPrefixTest, MergeOnWriteStaysComposableAfterCompaction) {
    for (unsigned seed : {51U, 52U, 53U, 54U, 55U, 56U}) {
        auto c = make_mow_chain(seed, 12);
        auto chain = c.chain();
        auto before = decompose(c.schema, chain, UNIQUE_KEYS, true, c.bitmap);
        std::map<int64_t, RestoreDigest> directs;
        expect_compose_equals_direct(c.schema, chain, UNIQUE_KEYS, true, c.bitmap, before, "before",
                                     &directs);
        // compact twice in a row: [2, 5] first, then the result with its successor
        MowChain once = compact_mow(c, 1, 4, seed);
        MowChain twice = compact_mow(once, 0, 1, seed + 1);
        for (const MowChain* after : {&once, &twice}) {
            auto after_chain = after->chain();
            auto dec = decompose(after->schema, after_chain, UNIQUE_KEYS, true, after->bitmap);
            const std::string what = "seed " + std::to_string(seed);
            expect_compose_equals_direct(after->schema, after_chain, UNIQUE_KEYS, true, after->bitmap,
                                         dec, what);
            for (const auto& b : after->batches) {
                RestoreDigest composed;
                ASSERT_TRUE(dec.compose(b.version, &composed).ok());
                EXPECT_TRUE(same_digest(directs.at(b.version), composed))
                        << what << " version " << b.version;
                EXPECT_EQ(after->visible_at(b.version), composed.rows) << what;
            }
            for (const auto& b : after->batches) {
                for (int64_t v = b.start; v < b.version; ++v) {
                    EXPECT_FALSE(dec.is_boundary(v)) << what << " version " << v;
                }
            }
        }
    }
}

TEST_F(RestoreDigestPrefixTest, DeleteSignRowsAndTheirMarksAreNotCounted) {
    auto schema = make_schema(UNIQUE_KEYS, mow_cols(), 3);
    auto bitmap = std::make_shared<DeleteBitmap>(next_bitmap_tablet_id());
    // v2: keys 1..4. v3: key 2 deleted (delete sign), key 3 updated. v4: key 2 written again, and
    // the delete sign row of v3 is marked (or not, both are legal), key 3 deleted.
    auto rs2 = write_rowset(schema,
                            {mow_row({1, 10, 1, 0}), mow_row({2, 20, 1, 0}), mow_row({3, 30, 1, 0}),
                             mow_row({4, 40, 1, 0})},
                            2, 1000, false);
    auto rs3 = write_rowset(schema, {mow_row({2, 0, 3, 1}), mow_row({3, 31, 3, 0})}, 3, 1000, false);
    bitmap->add({rs2->rowset_id(), 0, 3}, 1); // key 2
    bitmap->add({rs2->rowset_id(), 0, 3}, 2); // key 3
    auto rs4 = write_rowset(schema, {mow_row({2, 21, 4, 0}), mow_row({3, 0, 4, 1})}, 4, 1000, false);
    bitmap->add({rs3->rowset_id(), 0, 4}, 0); // the delete sign row of key 2
    bitmap->add({rs3->rowset_id(), 0, 4}, 1); // key 3 of v3
    std::vector<RowsetSharedPtr> chain = {rs2, rs3, rs4};
    auto dec = decompose(schema, chain, UNIQUE_KEYS, true, bitmap);
    // rowset parts hold the rows without the delete sign: 4 + 1 + 1
    uint64_t counted = 0;
    for (const auto& rs : dec.rowsets) {
        for (const auto& b : rs.buckets) {
            counted += b.count;
        }
    }
    EXPECT_EQ(6U, counted);
    // marks: v3 kills keys 2 and 3 of v2; v4 kills only key 3 of v3 (the delete sign row is not counted)
    ASSERT_EQ(2U, dec.marks.size());
    EXPECT_EQ(3, dec.marks[0].mark_version);
    EXPECT_EQ(4, dec.marks[1].mark_version);
    auto marked_rows = [](const RestoreDigestMarkPart& m) {
        uint64_t n = 0;
        for (const auto& b : m.buckets) {
            n += b.count;
        }
        return n;
    };
    EXPECT_EQ(2U, marked_rows(dec.marks[0]));
    EXPECT_EQ(1U, marked_rows(dec.marks[1]));
    expect_compose_equals_direct(schema, chain, UNIQUE_KEYS, true, bitmap, dec, "delete sign");
    RestoreDigest at4;
    ASSERT_TRUE(dec.compose(4, &at4).ok());
    EXPECT_EQ(3U, at4.rows); // keys 1 and 4 of v2, key 2 of v4
}

TEST_F(RestoreDigestPrefixTest, UnsupportedCasesAndBadVersions) {
    // unique merge on read
    {
        auto schema = make_schema(UNIQUE_KEYS, mow_cols(), 3);
        auto rs = write_rowset(schema, {mow_row({1, 1, 1, 0})}, 2, 100, false);
        RestoreDigestInput in;
        in.rowsets = {rs};
        in.schema = schema;
        in.keys_type = UNIQUE_KEYS;
        in.enable_mow = false;
        in.version = 2;
        RestoreDigestDecomposed d;
        Status st = decompose_restore_digest(in, kPrefixTabletId, &d);
        EXPECT_TRUE(st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << st;
        EXPECT_NE(std::string::npos, st.to_string().find("merge on read")) << st;
    }
    // unique merge on write with a DELETE condition
    {
        auto schema = make_schema(UNIQUE_KEYS, mow_cols(), 3);
        auto bitmap = std::make_shared<DeleteBitmap>(next_bitmap_tablet_id());
        auto rs2 = write_rowset(schema, {mow_row({1, 1, 1, 0}), mow_row({2, 2, 1, 0})}, 2, 100, false);
        auto rs3 = write_delete_rowset(schema, 3, {cond("v1", "=", {"1"})});
        RestoreDigestInput in;
        in.rowsets = {rs2, rs3};
        in.schema = schema;
        in.keys_type = UNIQUE_KEYS;
        in.enable_mow = true;
        in.delete_bitmap = bitmap;
        in.version = 3;
        RestoreDigestDecomposed d;
        Status st = decompose_restore_digest(in, kPrefixTabletId, &d);
        EXPECT_TRUE(st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << st;
        EXPECT_NE(std::string::npos, st.to_string().find("delete conditions")) << st;
    }
    // too many extra scans
    {
        auto c = make_dup_chain(61, 12, /*deletes=*/true);
        int deletes = 0;
        for (bool is_delete : c.is_delete) {
            deletes += is_delete ? 1 : 0;
        }
        ASSERT_GT(deletes, 0);
        const int32_t saved = config::restore_digest_prefix_max_scans;
        config::restore_digest_prefix_max_scans = 1;
        RestoreDigestInput in;
        in.rowsets = c.chain;
        in.schema = c.schema;
        in.keys_type = DUP_KEYS;
        in.version = c.chain.back()->end_version();
        RestoreDigestDecomposed d;
        Status st = decompose_restore_digest(in, kPrefixTabletId, &d);
        config::restore_digest_prefix_max_scans = saved;
        EXPECT_TRUE(st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << st;
        EXPECT_NE(std::string::npos, st.to_string().find("extra scans")) << st;
    }
    // the base version must be the end of the last rowset
    {
        auto c = make_dup_chain(62, 3, false);
        RestoreDigestInput in;
        in.rowsets = c.chain;
        in.schema = c.schema;
        in.keys_type = DUP_KEYS;
        in.version = c.chain.back()->end_version() + 1;
        RestoreDigestDecomposed d;
        EXPECT_TRUE(decompose_restore_digest(in, kPrefixTabletId, &d).is<ErrorCode::INVALID_ARGUMENT>());
    }
}

TEST_F(RestoreDigestPrefixTest, FileIsCheckedAgainstItsRoot) {
    auto c = make_dup_chain(71, 6, true);
    auto dec = decompose(c.schema, c.chain, DUP_KEYS, false, nullptr);
    const std::string content = dec.serialize();
    const std::string root = dec.file_root();
    EXPECT_EQ(64U, root.size());
    RestoreDigestDecomposed out;
    EXPECT_TRUE(RestoreDigestDecomposed::parse(content, root, kPrefixTabletId, &out).ok());
    // wrong root, wrong tablet
    EXPECT_TRUE(RestoreDigestDecomposed::parse(content, std::string(64, '0'), kPrefixTabletId, &out)
                        .is<ErrorCode::RESTORE_MANIFEST_MISMATCH>());
    EXPECT_TRUE(RestoreDigestDecomposed::parse(content, root, kPrefixTabletId + 1, &out)
                        .is<ErrorCode::RESTORE_MANIFEST_MISMATCH>());
    // a flipped byte, a truncated file, trailing bytes: the root catches them, and a file whose root
    // was recomputed over bad content is rejected by the parser
    auto sha = [](const std::string& data) {
        SHA256Digest d;
        d.reset(data.data(), data.size());
        return std::string(d.digest());
    };
    for (size_t pos : {size_t(0), size_t(5), content.size() / 2, content.size() - 1}) {
        std::string bad = content;
        bad[pos] = static_cast<char>(bad[pos] ^ 0x5A);
        EXPECT_TRUE(RestoreDigestDecomposed::parse(bad, root, kPrefixTabletId, &out)
                            .is<ErrorCode::RESTORE_MANIFEST_MISMATCH>());
        Status st = RestoreDigestDecomposed::parse(bad, sha(bad), kPrefixTabletId, &out);
        // a flip inside a bucket value is a valid file with other numbers; anything structural is not
        if (!st.ok()) {
            EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
        }
    }
    for (std::string bad : {content.substr(0, content.size() - 3), content + "x", std::string(),
                            content.substr(0, 10)}) {
        EXPECT_TRUE(RestoreDigestDecomposed::parse(bad, sha(bad), kPrefixTabletId, &out)
                            .is<ErrorCode::RESTORE_MANIFEST_MISMATCH>());
    }
    // sparse: a rowset with few rows takes few bytes
    EXPECT_LT(content.size(), 6 * (RestoreDigest::kNumBuckets * 26 + 64) + 4096);
}

// A digest of a different shape (complex columns, a wide schema) decomposes the same way.
TEST_F(RestoreDigestPrefixTest, WideSchemaComposes) {
    auto schema = make_schema(DUP_KEYS, wide_cols());
    std::vector<RowsetSharedPtr> chain;
    int next = 0;
    for (int64_t v = 2; v <= 5; ++v) {
        std::vector<Row> rows;
        for (int i = 0; i < 40; ++i) {
            rows.push_back(wide_row(next++ % 90));
        }
        chain.push_back(write_rowset(schema, rows, v, 17, v % 2 == 0));
    }
    auto dec = decompose(schema, chain, DUP_KEYS, false, nullptr, 2);
    expect_compose_equals_direct(schema, chain, DUP_KEYS, false, nullptr, dec, "wide");
}

// ===== 13. decomposed digest of real tablets ==================================================

TEST_F(RestoreDigestTabletTest, DecomposedDigestOfMowAndDuplicateTablets) {
    _next_id = 910000; // not the ids of another test, see the segment cache
    // unique merge on write: the scenario of the partial update test, with versions 2..4
    auto mow = create_tablet(7201, true);
    ASSERT_NE(nullptr, mow);
    auto schema = mow->tablet_schema();
    auto rs2 = write(mow, schema, 2, {{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}, {6, 60}}}, false, true);
    auto rs3 = write(mow, schema, 3, {{{3, 31}, {5, 51}}, {{3, 32}}}, true, true);
    auto& bitmap = mow->tablet_meta()->delete_bitmap();
    bitmap.add({rs2->rowset_id(), 0, 3}, 2);
    bitmap.add({rs2->rowset_id(), 0, 3}, 4);
    bitmap.add({rs3->rowset_id(), 0, 3}, 0);
    auto rs4 = write(mow, schema, 4, {{{1, 0, 1}, {6, 61}}}, false, true);
    bitmap.add({rs2->rowset_id(), 0, 4}, 0);
    bitmap.add({rs2->rowset_id(), 0, 4}, 5);
    (void)rs4;

    RestoreDigestDecomposed dec;
    ASSERT_TRUE(compute_tablet_restore_digest_decomposed(*_engine, 7201, 4, 2, &dec).ok());
    EXPECT_EQ(7201, dec.tablet_id);
    EXPECT_EQ(4, dec.base_version);
    EXPECT_TRUE(dec.mow);
    for (int64_t v : {int64_t(1), int64_t(2), int64_t(3), int64_t(4)}) {
        RestoreDigest composed;
        ASSERT_TRUE(dec.compose(v, &composed).ok()) << v;
        EXPECT_TRUE(digests_equal(tablet_digest(7201, v), composed)) << "version " << v;
    }
    // the base version of the backup may be below the head of the tablet
    RestoreDigestDecomposed at3;
    ASSERT_TRUE(compute_tablet_restore_digest_decomposed(*_engine, 7201, 3, 1, &at3).ok());
    EXPECT_EQ(3, at3.base_version);
    RestoreDigest c3;
    RestoreDigest c3_of_head;
    ASSERT_TRUE(at3.compose(3, &c3).ok());
    ASSERT_TRUE(dec.compose(3, &c3_of_head).ok());
    EXPECT_TRUE(digests_equal(c3_of_head, c3));
    EXPECT_FALSE(at3.is_boundary(4));

    // duplicate with a DELETE condition
    auto dup = create_tablet(7202, false);
    ASSERT_NE(nullptr, dup);
    auto dschema = dup->tablet_schema();
    std::vector<TabRow> a;
    std::vector<TabRow> b;
    for (int k = 0; k < 100; ++k) {
        a.push_back({k, k % 10});
    }
    for (int k = 50; k < 150; ++k) {
        b.push_back({k, (k * 7) % 10});
    }
    write(dup, dschema, 2, {a}, false, true);
    write(dup, dschema, 3, {b}, false, true);
    write_delete(dup, 4, {cond("v", "=", "3")});
    write(dup, dschema, 5, {{{500, 3}, {501, 4}}}, false, true);
    write_delete(dup, 6, {cond("k", "<", "20")});
    RestoreDigestDecomposed ddec;
    ASSERT_TRUE(compute_tablet_restore_digest_decomposed(*_engine, 7202, 6, 3, &ddec).ok());
    EXPECT_FALSE(ddec.mow);
    ASSERT_EQ(2U, ddec.marks.size());
    EXPECT_EQ(4, ddec.marks[0].mark_version);
    EXPECT_EQ(6, ddec.marks[1].mark_version);
    for (int64_t v : {int64_t(1), int64_t(2), int64_t(3), int64_t(4), int64_t(5), int64_t(6)}) {
        RestoreDigest composed;
        ASSERT_TRUE(ddec.compose(v, &composed).ok()) << v;
        EXPECT_TRUE(digests_equal(tablet_digest(7202, v), composed)) << "version " << v;
    }

    // not supported: merge on read, with the reason
    auto mor = create_tablet(7203, /*mow=*/false, /*unique=*/true);
    ASSERT_NE(nullptr, mor);
    write(mor, mor->tablet_schema(), 2, {{{1, 10}}}, false, true);
    RestoreDigestDecomposed none;
    Status st = compute_tablet_restore_digest_decomposed(*_engine, 7203, 2, 1, &none);
    EXPECT_TRUE(st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << st;
}

TEST_F(RestoreDigestTabletTest, LogicalDigestComesFromTheDecomposedDigest) {
    _next_id = 920000; // not the ids of another test, see the segment cache
    auto tablet = create_tablet(7211, false);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    write(tablet, schema, 2, {{{1, 10}, {2, 20}, {3, 30}}}, false, true);
    write(tablet, schema, 3, {{{4, 40}, {5, 50}}}, false, true);
    RestoreDigest direct = tablet_digest(7211, 3);

    // a miss: one pass, the digest is composed from the parts and cached
    RestoreDigestCache cache(/*capacity=*/16);
    LogicalDigestResult first;
    RestoreDigestDecomposed dec;
    Status dec_status = Status::InternalError("not set");
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7211, 3, 2, &cache, &first, &dec, &dec_status).ok());
    EXPECT_TRUE(dec_status.ok()) << dec_status;
    EXPECT_FALSE(first.from_cache);
    EXPECT_EQ(direct.root, first.root);
    EXPECT_EQ(direct.schema_sig, first.schema_sig);
    EXPECT_EQ(direct.rows, first.rows);
    EXPECT_EQ(1U, cache.size());
    EXPECT_EQ(first.root, dec.root);
    EXPECT_EQ(3, dec.base_version);

    // a hit with the same root: the decomposed digest is produced all the same
    LogicalDigestResult second;
    RestoreDigestDecomposed dec2;
    dec_status = Status::InternalError("not set");
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7211, 3, 1, &cache, &second, &dec2, &dec_status).ok());
    EXPECT_TRUE(dec_status.ok()) << dec_status;
    EXPECT_TRUE(second.from_cache);
    EXPECT_EQ(dec.serialize(), dec2.serialize());

    // without asking for it: nothing changes
    LogicalDigestResult plain;
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7211, 3, 1, &cache, &plain).ok());
    EXPECT_EQ(direct.root, plain.root);

    // the cache holds another root for the key: the decomposed digest is dropped, the cached value wins
    RestoreDigestCache poisoned(/*capacity=*/16);
    RestoreDigestCache::Key key;
    key.tablet_id = 7211;
    key.version = 3;
    key.algo_version = RestoreDigest::kAlgoVersion;
    key.schema_sig = direct.schema_sig;
    LogicalDigestResult bogus;
    bogus.schema_sig = direct.schema_sig;
    bogus.root = std::string(64, 'f');
    bogus.rows = 5;
    poisoned.insert(key, bogus);
    LogicalDigestResult got;
    RestoreDigestDecomposed dec3;
    dec_status = Status::OK();
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7211, 3, 1, &poisoned, &got, &dec3, &dec_status).ok());
    EXPECT_FALSE(dec_status.ok());
    EXPECT_TRUE(dec3.rowsets.empty());
    EXPECT_EQ(bogus.root, got.root);

    // merge on read: the digest is computed as before, the decomposed digest says why it is absent
    auto mor = create_tablet(7212, /*mow=*/false, /*unique=*/true);
    ASSERT_NE(nullptr, mor);
    write(mor, mor->tablet_schema(), 2, {{{1, 10}, {2, 20}}}, false, true);
    write(mor, mor->tablet_schema(), 3, {{{2, 21}}}, false, true);
    LogicalDigestResult mor_result;
    RestoreDigestDecomposed mor_dec;
    dec_status = Status::OK();
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7212, 3, 1, nullptr, &mor_result, &mor_dec, &dec_status).ok());
    EXPECT_TRUE(dec_status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << dec_status;
    EXPECT_TRUE(mor_dec.rowsets.empty());
    EXPECT_EQ(tablet_digest(7212, 3).root, mor_result.root);
    EXPECT_EQ(2U, mor_result.rows);
}

TEST_F(RestoreDigestTabletTest, TaskProducesTheDecomposedDigest) {
    _next_id = 930000; // not the ids of another test, see the segment cache
    auto tablet = create_tablet(7221, false);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    write(tablet, schema, 2, {{{1, 10}, {2, 20}}}, false, true);
    write(tablet, schema, 3, {{{3, 30}}}, false, true);
    RestoreDigestCache::instance()->clear();
    PrefixDigestOutput prefix;
    TLogicalDigest d = compute_logical_digest_for_task(*_engine, 7221, 3, 0, &prefix);
    EXPECT_EQ("OK", d.status_code);
    EXPECT_TRUE(prefix.produced) << prefix.reason;
    EXPECT_EQ(d.root, prefix.digest.root);
    EXPECT_EQ(7221, prefix.digest.tablet_id);
    EXPECT_GE(prefix.digest.rowsets.size(), 2U);

    // merge on read: the logical digest is fine, there is no decomposed digest
    auto mor = create_tablet(7222, /*mow=*/false, /*unique=*/true);
    ASSERT_NE(nullptr, mor);
    write(mor, mor->tablet_schema(), 2, {{{1, 10}}}, false, true);
    PrefixDigestOutput none;
    TLogicalDigest md = compute_logical_digest_for_task(*_engine, 7222, 2, 0, &none);
    EXPECT_EQ("OK", md.status_code);
    EXPECT_FALSE(none.produced);
    EXPECT_FALSE(none.reason.empty());
}

} // namespace doris
