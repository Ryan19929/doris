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
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>
#include <unistd.h>
#include <xxh3.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/data_dir.h"
#include "storage/merger.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/rowid_conversion.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "util/defer_op.h"

namespace doris {
using namespace ErrorCode;

namespace restore_digest_ut {

struct Cell {
    bool null = false;
    std::string b; // raw bytes for fixed width types, content for strings
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
                                 const std::vector<uint32_t>& cluster_key_uids = {}) {
        auto schema = std::make_shared<TabletSchema>();
        TabletSchemaPB pb;
        pb.set_keys_type(keys_type);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_NONE);
        pb.set_next_column_unique_id(static_cast<int32_t>(cols.size()) + 1);
        if (sequence_idx >= 0) {
            pb.set_sequence_col_idx(sequence_idx);
        }
        for (uint32_t uid : cluster_key_uids) {
            pb.add_cluster_key_uids(uid);
        }
        int uid = 1;
        for (const auto& spec : cols) {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(uid++);
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
                                 int64_t version, size_t rows_per_segment, bool overlapping) {
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
        auto ctx = make_writer_context(schema, Version(version, version), overlapping);
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

    // models
    {
        auto cols = base;
        cols[1].aggregation = "SUM";
        expect_unsupported(make_schema(AGG_KEYS, cols), AGG_KEYS, false, "aggregate");
    }
    auto uniq_cols = base;
    uniq_cols.push_back({DELETE_SIGN, "TINYINT", false, false, 1});
    expect_unsupported(make_schema(UNIQUE_KEYS, uniq_cols), UNIQUE_KEYS, false, "merge on read");
    expect_unsupported(make_schema(UNIQUE_KEYS, base), UNIQUE_KEYS, true, "no delete sign");
    expect_unsupported(make_schema(UNIQUE_KEYS, uniq_cols, -1, {1}), UNIQUE_KEYS, true,
                       "cluster key");

    // types
    for (const char* type :
         {"DATE", "DATETIME", "DECIMAL", "JSONB", "HLL", "BITMAP", "QUANTILE_STATE"}) {
        auto cols = base;
        cols.push_back({"x", type, false, true, 16});
        expect_unsupported(make_schema(DUP_KEYS, cols), DUP_KEYS, false, type);
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

} // namespace doris
