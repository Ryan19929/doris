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

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <xxh3.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstring>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <thread>

#include "common/config.h"
#include "common/consts.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/value/vdatetime_value.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "storage/delete/delete_handler.h"
#include "storage/iterator/block_reader.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/schema.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_reader.h"
#include "storage/utils.h"
#include "util/sha.h"

namespace doris {

static_assert(std::endian::native == std::endian::little,
              "restore digest encoding assumes a little endian host");
static_assert(sizeof(VecDateTimeValue) == 8, "DATE/DATETIME v1 are 8 bytes in memory");

namespace restore_digest_detail {

enum class ColRole : uint8_t { DIGEST, DELETE_SIGN, EXCLUDED };
enum class ColKind : uint8_t {
    FIXED,
    STRING,
    CHAR,
    DATE_V1,
    DATETIME_V1,
    ARRAY,
    MAP,
    STRUCT,
};

struct ColCodec {
    ColKind kind = ColKind::FIXED;
    uint8_t width = 0; // for FIXED
    // ARRAY: the element, MAP: key and value, STRUCT: the fields
    std::vector<ColCodec> children;
};

// Whether the column type is supported, and how it is encoded. `path` names the column for the
// error message (nested elements are reported as a.b).
Status codec_of(const TabletColumn& col, const std::string& path, ColCodec* codec) {
    auto fixed = [&](uint8_t w) {
        codec->kind = ColKind::FIXED;
        codec->width = w;
        return Status::OK();
    };
    auto nested = [&](size_t expect_min, size_t expect_max) -> Status {
        const auto& subs = col.get_sub_columns();
        if (subs.size() < expect_min || subs.size() > expect_max) {
            return Status::NotSupported("restore digest: column {} has {} sub columns", path,
                                        subs.size());
        }
        codec->children.resize(subs.size());
        for (size_t i = 0; i < subs.size(); ++i) {
            RETURN_IF_ERROR(codec_of(*subs[i], fmt::format("{}.{}", path, i), &codec->children[i]));
        }
        return Status::OK();
    };
    switch (col.type()) {
    case FieldType::OLAP_FIELD_TYPE_BOOL:
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return fixed(1);
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        return fixed(2);
    case FieldType::OLAP_FIELD_TYPE_INT:
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
    case FieldType::OLAP_FIELD_TYPE_IPV4:
        return fixed(4);
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        return fixed(8);
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL: // DECIMALV2, int128 in memory
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        return fixed(16);
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
        return fixed(32);
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
    case FieldType::OLAP_FIELD_TYPE_STRING:
    case FieldType::OLAP_FIELD_TYPE_JSONB:
        codec->kind = ColKind::STRING;
        return Status::OK();
    case FieldType::OLAP_FIELD_TYPE_CHAR:
        codec->kind = ColKind::CHAR;
        return Status::OK();
    case FieldType::OLAP_FIELD_TYPE_DATE:
        codec->kind = ColKind::DATE_V1;
        return Status::OK();
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
        codec->kind = ColKind::DATETIME_V1;
        return Status::OK();
    case FieldType::OLAP_FIELD_TYPE_ARRAY:
        codec->kind = ColKind::ARRAY;
        return nested(1, 1);
    case FieldType::OLAP_FIELD_TYPE_MAP:
        codec->kind = ColKind::MAP;
        return nested(2, 2);
    case FieldType::OLAP_FIELD_TYPE_STRUCT:
        codec->kind = ColKind::STRUCT;
        return nested(1, SIZE_MAX);
    default:
        return Status::NotSupported("restore digest: column {} has unsupported type {}", path,
                                    TabletColumn::get_string_by_field_type(col.type()));
    }
}

// Part of the schema signature: the type, precision and scale, and for nested types the same
// for every sub column. Scalars keep the exact text of the first cut.
void append_type_sig(std::string& sig, const TabletColumn& col) {
    sig += fmt::format("{}:{}:{}", static_cast<int>(col.type()), col.precision(), col.frac());
    const auto& subs = col.get_sub_columns();
    if (!subs.empty()) {
        sig += '<';
        for (size_t i = 0; i < subs.size(); ++i) {
            if (i > 0) {
                sig += ',';
            }
            append_type_sig(sig, *subs[i]);
        }
        sig += '>';
    }
}

// Classifies a schema column; returns NotSupported for columns the algorithm can not digest.
Status classify_column(const TabletColumn& col, KeysType keys_type, ColRole* role,
                       ColCodec* codec) {
    const std::string& name = col.name();
    if (name == DELETE_SIGN && keys_type == UNIQUE_KEYS) {
        *role = ColRole::DELETE_SIGN;
        return Status::OK();
    }
    if (name == VERSION_COL || name == COMMIT_TSO_COL || name == SKIP_BITMAP_COL ||
        name == BeConsts::ROW_STORE_COL) {
        *role = ColRole::EXCLUDED;
        return Status::OK();
    }
    if (name == BINLOG_TSO_COL || name == BINLOG_LSN_COL || name == BINLOG_OP_COL ||
        name == ROW_LSN_COL) {
        return Status::NotSupported("restore digest: row binlog column {}", name);
    }
    if (name != SEQUENCE_COL && name != DELETE_SIGN && name.starts_with("__DORIS_")) {
        return Status::NotSupported("restore digest: unknown hidden column {}", name);
    }
    RETURN_IF_ERROR(codec_of(col, name, codec));
    *role = ColRole::DIGEST;
    return Status::OK();
}

struct ColumnPlan {
    std::vector<uint32_t> digest_ordinals; // ordinals in the schema, schema order
    std::vector<ColCodec> codecs;          // parallel to digest_ordinals
    int32_t delete_sign_ordinal = -1;
    std::string schema_sig;
};

Status build_plan(const TabletSchema& schema, KeysType keys_type, bool enable_mow,
                  ColumnPlan* plan) {
    if (keys_type == DUP_KEYS) {
        // supported
    } else if (keys_type == UNIQUE_KEYS) {
        // MoW and MoR are both supported
    } else {
        return Status::NotSupported("restore digest: keys type {} is not supported",
                                    static_cast<int>(keys_type));
    }
    if (!schema.cluster_key_uids().empty()) {
        return Status::NotSupported("restore digest: cluster key is not supported");
    }
    if (schema.has_seq_map()) {
        // The merge of a sequence mapping table (BlockReader::_replace_key_next_block) replaces
        // value column groups by their own sequence column and does not drop delete sign rows, so
        // neither the checksum reader nor the MoW direct read gives a defined visible row set.
        return Status::NotSupported("restore digest: sequence mapping is not supported");
    }
    if (schema.binlog_tso_col_idx() != -1 || schema.binlog_lsn_col_idx() != -1 ||
        schema.binlog_op_col_idx() != -1) {
        return Status::NotSupported("restore digest: row binlog is not supported");
    }

    SHA256Digest sig;
    std::string sig_data = fmt::format("restore-digest-schema|algo={}|keys_type={}|mow={}",
                                       RestoreDigest::kAlgoVersion, static_cast<int>(keys_type),
                                       enable_mow ? 1 : 0);
    const auto& columns = schema.columns();
    for (uint32_t i = 0; i < columns.size(); ++i) {
        const TabletColumn& col = *columns[i];
        ColRole role = ColRole::DIGEST;
        ColCodec codec;
        RETURN_IF_ERROR(classify_column(col, keys_type, &role, &codec));
        if (role == ColRole::DELETE_SIGN) {
            plan->delete_sign_ordinal = static_cast<int32_t>(i);
        } else if (role == ColRole::DIGEST) {
            plan->digest_ordinals.push_back(i);
            plan->codecs.push_back(std::move(codec));
            sig_data += '|';
            append_type_sig(sig_data, col);
            sig_data += fmt::format(":{}", col.is_key() ? 1 : 0);
        }
    }
    if (keys_type == UNIQUE_KEYS && plan->delete_sign_ordinal < 0) {
        return Status::NotSupported("restore digest: unique table without delete sign column");
    }
    if (plan->digest_ordinals.empty()) {
        return Status::NotSupported("restore digest: no column takes part in the digest");
    }
    sig.reset(sig_data.data(), sig_data.size());
    plan->schema_sig = std::string(sig.digest());
    return Status::OK();
}

// A read-only view of one column of a block.
struct ColView {
    ColKind kind = ColKind::FIXED;
    uint8_t width = 0;
    const uint8_t* null_map = nullptr;
    const char* data = nullptr;
    const ColumnString* str = nullptr;
    const IColumn* col = nullptr;      // the column without its top level Nullable wrapper
    const ColCodec* codec = nullptr;
};

Status make_view(const ColumnPtr& column, const ColCodec& codec, size_t rows, ColView* view) {
    const IColumn* c = column.get();
    view->kind = codec.kind;
    view->width = codec.width;
    view->null_map = nullptr;
    view->codec = &codec;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(c)) {
        view->null_map = nullable->get_null_map_data().data();
        c = &nullable->get_nested_column();
    }
    view->col = c;
    if (c->size() != rows) {
        return Status::InternalError("restore digest: column {} has {} elements for {} rows",
                                     c->get_name(), c->size(), rows);
    }
    if (codec.kind == ColKind::FIXED) {
        // get_raw_data() reports the element count in `size`, not bytes
        StringRef raw = c->get_raw_data();
        if (raw.size != rows) {
            return Status::InternalError(
                    "restore digest: column {} has {} elements for {} rows, expect width {}",
                    c->get_name(), raw.size, rows, codec.width);
        }
        view->data = raw.data;
    } else if (codec.kind == ColKind::STRING || codec.kind == ColKind::CHAR) {
        view->str = check_and_get_column<ColumnString>(c);
        if (view->str == nullptr) {
            return Status::InternalError("restore digest: expect a string column but got {}",
                                         c->get_name());
        }
    } else if (codec.kind == ColKind::DATE_V1 || codec.kind == ColKind::DATETIME_V1) {
        StringRef raw = c->get_raw_data();
        if (raw.size != rows) {
            return Status::InternalError("restore digest: date column {} has {} raw elements",
                                         c->get_name(), raw.size);
        }
        view->data = raw.data;
    }
    return Status::OK();
}

inline void append_u32(std::string& buf, uint32_t v) {
    buf.append(reinterpret_cast<const char*>(&v), sizeof(v));
}
inline void append_u64(std::string& buf, uint64_t v) {
    buf.append(reinterpret_cast<const char*>(&v), sizeof(v));
}

constexpr XXH64_hash_t kHashSeed = RestoreDigest::kAlgoVersion;

bool encode_slot(const IColumn& col, const ColCodec& codec, size_t row, std::string& buf);

inline void encode_date_v1(const char* raw8, bool with_time, std::string& buf) {
    VecDateTimeValue v;
    std::memcpy(&v, raw8, sizeof(v));
    const uint16_t year = v.year();
    buf.append(reinterpret_cast<const char*>(&year), sizeof(year));
    buf.push_back(static_cast<char>(v.month()));
    buf.push_back(static_cast<char>(v.day()));
    if (with_time) {
        buf.push_back(static_cast<char>(v.hour()));
        buf.push_back(static_cast<char>(v.minute()));
        buf.push_back(static_cast<char>(v.second()));
    }
}

// Appends the encoding of a non-NULL value (no flag byte). Returns false if the column object is
// not what the schema promises.
bool encode_value(const IColumn& col, const ColCodec& codec, size_t row, std::string& buf) {
    switch (codec.kind) {
    case ColKind::FIXED: {
        StringRef raw = col.get_raw_data();
        if (raw.size <= row) {
            return false;
        }
        buf.append(raw.data + row * codec.width, codec.width);
        return true;
    }
    case ColKind::STRING:
    case ColKind::CHAR: {
        StringRef s = col.get_data_at(row);
        size_t len = s.size;
        if (codec.kind == ColKind::CHAR) {
            while (len > 0 && s.data[len - 1] == '\0') {
                --len;
            }
        }
        append_u32(buf, static_cast<uint32_t>(len));
        buf.append(s.data, len);
        return true;
    }
    case ColKind::DATE_V1:
    case ColKind::DATETIME_V1: {
        StringRef raw = col.get_raw_data();
        if (raw.size <= row) {
            return false;
        }
        encode_date_v1(raw.data + row * sizeof(VecDateTimeValue), codec.kind == ColKind::DATETIME_V1,
                       buf);
        return true;
    }
    case ColKind::ARRAY: {
        const auto* arr = check_and_get_column<ColumnArray>(&col);
        if (arr == nullptr || codec.children.size() != 1) {
            return false;
        }
        const size_t begin = arr->offset_at(row);
        const size_t n = arr->size_at(row);
        append_u64(buf, n);
        for (size_t i = 0; i < n; ++i) {
            if (!encode_slot(arr->get_data(), codec.children[0], begin + i, buf)) {
                return false;
            }
        }
        return true;
    }
    case ColKind::MAP: {
        const auto* map = check_and_get_column<ColumnMap>(&col);
        if (map == nullptr || codec.children.size() != 2) {
            return false;
        }
        const size_t begin = map->offset_at(row);
        const size_t n = map->size_at(row);
        append_u64(buf, n);
        for (size_t i = 0; i < n; ++i) {
            if (!encode_slot(map->get_keys(), codec.children[0], begin + i, buf) ||
                !encode_slot(map->get_values(), codec.children[1], begin + i, buf)) {
                return false;
            }
        }
        return true;
    }
    case ColKind::STRUCT: {
        const auto* st = check_and_get_column<ColumnStruct>(&col);
        if (st == nullptr || st->tuple_size() != codec.children.size()) {
            return false;
        }
        for (size_t i = 0; i < codec.children.size(); ++i) {
            if (!encode_slot(st->get_column(i), codec.children[i], row, buf)) {
                return false;
            }
        }
        return true;
    }
    }
    return false;
}

// A nested element: a flag byte, then the value if it is not NULL.
bool encode_slot(const IColumn& col, const ColCodec& codec, size_t row, std::string& buf) {
    const IColumn* c = &col;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(c)) {
        if (nullable->is_null_at(row)) {
            buf.push_back('\x00');
            return true;
        }
        c = &nullable->get_nested_column();
    }
    buf.push_back('\x01');
    return encode_value(*c, codec, row, buf);
}

// Encodes one row, appending to `buf`. The encoding is injective for a fixed schema: every field
// is a flag byte followed by either a fixed number of bytes or a length / count prefixed value.
// Returns false if a column does not have the shape the schema promises.
inline bool encode_row(const std::vector<ColView>& views, size_t row, std::string& buf) {
    for (const ColView& v : views) {
        if (v.null_map != nullptr && v.null_map[row] != 0) {
            buf.push_back('\x00');
            continue;
        }
        buf.push_back('\x01');
        if (v.kind == ColKind::FIXED) {
            buf.append(v.data + row * v.width, v.width);
        } else if (v.kind == ColKind::STRING || v.kind == ColKind::CHAR) {
            StringRef s = v.str->get_data_at(row);
            size_t len = s.size;
            if (v.kind == ColKind::CHAR) {
                while (len > 0 && s.data[len - 1] == '\0') {
                    --len;
                }
            }
            append_u32(buf, static_cast<uint32_t>(len));
            buf.append(s.data, len);
        } else if (v.kind == ColKind::DATE_V1 || v.kind == ColKind::DATETIME_V1) {
            encode_date_v1(v.data + row * sizeof(VecDateTimeValue), v.kind == ColKind::DATETIME_V1,
                           buf);
        } else if (!encode_value(*v.col, *v.codec, row, buf)) {
            return false;
        }
    }
    return true;
}

// Hashes the visible rows of blocks into a partial digest. One instance per worker.
class BlockHasher {
public:
    explicit BlockHasher(const ColumnPlan& plan) : _plan(plan), _views(plan.digest_ordinals.size()) {
        _row_buf.reserve(256);
    }

    // The block has the digest columns first, then (when the schema has one) the delete sign.
    Status add_block(const Block& block, RestoreDigest* acc) {
        const size_t n = block.rows();
        if (n == 0) {
            return Status::OK();
        }
        const size_t num_digest = _plan.digest_ordinals.size();
        std::vector<ColumnPtr> keep_alive;
        keep_alive.reserve(num_digest + 1);
        for (size_t j = 0; j < num_digest; ++j) {
            keep_alive.push_back(
                    block.get_by_position(j).column->convert_to_full_column_if_const());
            RETURN_IF_ERROR(make_view(keep_alive.back(), _plan.codecs[j], n, &_views[j]));
        }
        ColView delete_sign_view;
        ColCodec delete_sign_codec;
        delete_sign_codec.kind = ColKind::FIXED;
        delete_sign_codec.width = 1;
        const bool has_delete_sign = _plan.delete_sign_ordinal >= 0;
        if (has_delete_sign) {
            keep_alive.push_back(
                    block.get_by_position(num_digest).column->convert_to_full_column_if_const());
            RETURN_IF_ERROR(make_view(keep_alive.back(), delete_sign_codec, n, &delete_sign_view));
        }
        acc->rows_scanned += n;
        for (size_t row = 0; row < n; ++row) {
            if (has_delete_sign &&
                ((delete_sign_view.null_map != nullptr && delete_sign_view.null_map[row] != 0) ||
                 delete_sign_view.data[row] != 0)) {
                continue;
            }
            _row_buf.clear();
            if (!encode_row(_views, row, _row_buf)) {
                return Status::InternalError(
                        "restore digest: a column does not have the shape its schema type needs");
            }
            XXH128_hash_t h = XXH3_128bits_withSeed(_row_buf.data(), _row_buf.size(), kHashSeed);
            auto& bucket = acc->buckets[h.high64 >> 56];
            bucket.sum += (static_cast<unsigned __int128>(h.high64) << 64) | h.low64;
            bucket.count++;
            acc->rows++;
            acc->encoded_bytes += _row_buf.size();
        }
        return Status::OK();
    }

private:
    const ColumnPlan& _plan;
    std::vector<ColView> _views;
    std::string _row_buf;
};

// Bucket sums are plain sums modulo 2^128, so partial digests just add up.
void merge_partial(const RestoreDigest& part, RestoreDigest* total) {
    for (size_t i = 0; i < RestoreDigest::kNumBuckets; ++i) {
        total->buckets[i].sum += part.buckets[i].sum;
        total->buckets[i].count += part.buckets[i].count;
    }
    total->rows += part.rows;
    total->rows_scanned += part.rows_scanned;
    total->encoded_bytes += part.encoded_bytes;
}

// The read schema: all digest columns, then the delete sign column.
ReadSchemaSPtr make_read_schema(const TabletSchema& schema, const ColumnPlan& plan) {
    std::vector<ColumnId> read_ordinals(plan.digest_ordinals.begin(), plan.digest_ordinals.end());
    if (plan.delete_sign_ordinal >= 0) {
        read_ordinals.push_back(static_cast<ColumnId>(plan.delete_sign_ordinal));
    }
    return std::make_shared<ReadSchema>(project_columns_by_ordinal(schema.columns(), read_ordinals));
}

struct DigestScanTask {
    size_t rowset_idx = 0;
    int64_t seg_begin = 0;
    int64_t seg_end = 0; // [seg_begin, seg_end); the whole rowset when both are 0
};

// Reads the visible rows of a part of one rowset (no merge) and hashes them.
Status run_direct_task(const RestoreDigestInput& input, const ColumnPlan& plan,
                       const std::vector<RowsetMetaSharedPtr>& delete_metas, const DigestScanTask& task,
                       RestoreDigest* acc) {
    const RowsetSharedPtr& rowset = input.rowsets[task.rowset_idx];
    // Every task owns its read schema and delete handler: appending the columns of delete
    // conditions mutates the schema, and the handler's predicates are not shared across readers
    // in the query path either.
    auto read_schema = make_read_schema(*input.schema, plan);
    DeleteHandler delete_handler;
    if (!delete_metas.empty()) {
        std::vector<TabletColumn> dropped_columns;
        RETURN_IF_ERROR(delete_handler.init(delete_metas, input.version, read_schema,
                                            &dropped_columns));
        read_schema->append_dropped_columns(std::move(dropped_columns));
    }
    RETURN_IF_ERROR(read_schema->init_from_tablet_schema(*input.schema,
                                                         /*merge_by_sequence_mapping=*/false,
                                                         /*map_row_binlog_columns=*/false));

    RowsetReaderSharedPtr reader;
    RETURN_IF_ERROR(rowset->create_reader(&reader));

    OlapReaderStatistics stats;
    RowsetReaderContext ctx;
    ctx.reader_type = ReaderType::READER_CHECKSUM;
    ctx.version = Version(0, input.version);
    ctx.need_ordered_result = false;
    ctx.read_schema = read_schema;
    ctx.stats = &stats;
    ctx.use_page_cache = false;
    ctx.batch_size = input.batch_size;
    ctx.enable_unique_key_merge_on_write = input.enable_mow;
    ctx.delete_bitmap = input.delete_bitmap;
    ctx.delete_handler = delete_handler.empty() ? nullptr : &delete_handler;
    RowSetSplits splits(reader);
    splits.segment_offsets = {task.seg_begin, task.seg_end};
    RETURN_IF_ERROR(reader->init(&ctx, splits));

    BlockHasher hasher(plan);
    while (true) {
        Block block = read_schema->create_read_block();
        Status st = reader->next_batch(&block);
        RETURN_IF_ERROR(hasher.add_block(block, acc));
        if (st.is<ErrorCode::END_OF_FILE>()) {
            return Status::OK();
        }
        RETURN_IF_ERROR(st);
    }
}

// Unique MoR: the rows of all rowsets are merged by key with the reader the checksum task uses
// (highest version wins, sequence column first, delete sign rows dropped after the merge, delete
// conditions applied), then hashed. Single threaded: the merge needs all rowsets at once.
Status run_mor(const RestoreDigestInput& input, const ColumnPlan& plan, RestoreDigest* acc) {
    if (input.tablet == nullptr) {
        return Status::InvalidArgument("restore digest: unique MoR needs the tablet");
    }
    if (input.rowsets.empty()) {
        return Status::OK();
    }
    TabletReader::ReaderParams params;
    params.tablet = input.tablet;
    params.reader_type = ReaderType::READER_CHECKSUM;
    params.version =
            Version(input.rowsets.front()->start_version(), input.rowsets.back()->end_version());
    TabletReadSource read_source;
    for (const auto& rowset : input.rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_IF_ERROR(rowset->create_reader(&rs_reader));
        read_source.rs_splits.emplace_back(std::move(rs_reader));
    }
    read_source.fill_delete_predicates();
    params.set_read_source(std::move(read_source), /*skip_delete_bitmap=*/true);
    params.tablet_schema = input.schema;
    params.read_schema = make_read_schema(*input.schema, plan);

    BlockReader reader;
    reader.set_batch_size(input.batch_size);
    RETURN_IF_ERROR(reader.init(params));
    Block block = params.read_schema->create_read_block();
    BlockHasher hasher(plan);
    bool eof = false;
    while (!eof) {
        RETURN_IF_ERROR(reader.next_block_with_aggregation(&block, &eof));
        RETURN_IF_ERROR(hasher.add_block(block, acc));
        block.clear_column_data();
    }
    return Status::OK();
}

// Runs `fn` for every task on up to `threads` workers. Each worker accumulates into its own
// partial digest; the partials are added to `total`.
template <typename Fn>
Status run_tasks(const std::vector<DigestScanTask>& tasks, int threads, Fn&& fn, RestoreDigest* total) {
    if (tasks.empty()) {
        return Status::OK();
    }
    const size_t workers = std::min<size_t>(static_cast<size_t>(std::max(1, threads)), tasks.size());
    if (workers == 1) {
        RestoreDigest local;
        for (const DigestScanTask& task : tasks) {
            RETURN_IF_ERROR(fn(task, &local));
        }
        merge_partial(local, total);
        return Status::OK();
    }
    std::atomic<size_t> next {0};
    std::atomic<bool> failed {false};
    std::mutex mu;
    Status first_error = Status::OK();
    std::vector<std::unique_ptr<RestoreDigest>> partials(workers);
    std::shared_ptr<MemTrackerLimiter> tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "RestoreDigestWorkers");
    std::vector<std::thread> pool;
    pool.reserve(workers);
    for (size_t w = 0; w < workers; ++w) {
        partials[w] = std::make_unique<RestoreDigest>();
        pool.emplace_back([&, w] {
            SCOPED_ATTACH_TASK(tracker);
            while (!failed.load(std::memory_order_relaxed)) {
                size_t i = next.fetch_add(1);
                if (i >= tasks.size()) {
                    break;
                }
                Status st = fn(tasks[i], partials[w].get());
                if (!st.ok()) {
                    std::lock_guard lock(mu);
                    if (first_error.ok()) {
                        first_error = st;
                    }
                    failed.store(true);
                    break;
                }
            }
        });
    }
    for (auto& t : pool) {
        t.join();
    }
    RETURN_IF_ERROR(first_error);
    for (const auto& part : partials) {
        merge_partial(*part, total);
    }
    return Status::OK();
}


} // namespace restore_digest_detail
Status check_restore_digest_supported(const TabletSchema& schema, KeysType keys_type,
                                      bool enable_mow) {
    using namespace restore_digest_detail;
    ColumnPlan plan;
    return build_plan(schema, keys_type, enable_mow, &plan);
}

void RestoreDigest::finalize() {
    SHA256Digest sha;
    std::string header =
            fmt::format("DORIS-RESTORE-DIGEST|algo={}|buckets={}|", kAlgoVersion, kNumBuckets);
    sha.reset(header.data(), header.size());
    for (const Bucket& b : buckets) {
        uint64_t lo = static_cast<uint64_t>(b.sum);
        uint64_t hi = static_cast<uint64_t>(b.sum >> 64);
        sha.update(&lo, sizeof(lo));
        sha.update(&hi, sizeof(hi));
        sha.update(&b.count, sizeof(b.count));
    }
    root = std::string(sha.digest());
}

std::string RestoreDigest::to_json() const {
    std::string out;
    out.reserve(kNumBuckets * 64 + 512);
    out += fmt::format(
            "{{\"algo_version\":{},\"rows\":{},\"rows_scanned\":{},\"root\":\"{}\","
            "\"schema_sig\":\"{}\",\"rowset_count\":{},\"segment_count\":{},\"bytes_read\":{},"
            "\"encoded_bytes\":{},\"elapsed_ms\":{},\"threads\":{},\"delete_predicates\":{},"
            "\"buckets\":[",
            kAlgoVersion, rows, rows_scanned, root, schema_sig, rowset_count, segment_count,
            bytes_read, encoded_bytes, elapsed_ms, threads, delete_predicates);
    for (size_t i = 0; i < buckets.size(); ++i) {
        const Bucket& b = buckets[i];
        if (i > 0) {
            out += ',';
        }
        out += fmt::format("{{\"sum\":\"{:016x}{:016x}\",\"count\":{}}}",
                           static_cast<uint64_t>(b.sum >> 64), static_cast<uint64_t>(b.sum),
                           b.count);
    }
    out += "]}";
    return out;
}

Status compute_restore_digest(const RestoreDigestInput& input, RestoreDigest* digest) {
    using namespace restore_digest_detail;
    if (input.schema == nullptr || digest == nullptr) {
        return Status::InvalidArgument("restore digest: null schema or output");
    }
    auto start = std::chrono::steady_clock::now();
    ColumnPlan plan;
    RETURN_IF_ERROR(build_plan(*input.schema, input.keys_type, input.enable_mow, &plan));
    if (input.enable_mow && input.delete_bitmap == nullptr) {
        return Status::InvalidArgument("restore digest: MoW needs a delete bitmap snapshot");
    }
    const bool is_mor = input.keys_type == UNIQUE_KEYS && !input.enable_mow;

    RestoreDigest total;
    total.schema_sig = plan.schema_sig;

    std::vector<RowsetMetaSharedPtr> delete_metas;
    for (const RowsetSharedPtr& rowset : input.rowsets) {
        total.rowset_count++;
        total.segment_count += static_cast<uint32_t>(rowset->num_segments());
        total.bytes_read += rowset->data_disk_size();
        if (rowset->rowset_meta()->has_delete_predicate() &&
            rowset->version().first <= input.version) {
            delete_metas.push_back(rowset->rowset_meta());
        }
    }
    total.delete_predicates = static_cast<uint32_t>(delete_metas.size());

    if (is_mor) {
        total.threads = 1;
        RETURN_IF_ERROR(run_mor(input, plan, &total));
    } else {
        const int threads = std::max(1, input.threads);
        std::vector<DigestScanTask> tasks;
        for (size_t i = 0; i < input.rowsets.size(); ++i) {
            const RowsetSharedPtr& rowset = input.rowsets[i];
            if (rowset->num_rows() == 0) {
                continue;
            }
            const int64_t n = static_cast<int64_t>(rowset->num_segments());
            // Cut a rowset into runs of segments. Every task walks the delete bitmap of all the
            // segments of its rowset once, so the runs are kept long: about 4 tasks per worker.
            const int64_t chunk =
                    threads == 1 ? n : std::max<int64_t>(1, (n + threads * 4 - 1) / (threads * 4));
            for (int64_t b = 0; b < n; b += chunk) {
                DigestScanTask task;
                task.rowset_idx = i;
                const int64_t e = std::min(n, b + chunk);
                if (!(b == 0 && e == n)) {
                    task.seg_begin = b;
                    task.seg_end = e;
                }
                tasks.push_back(task);
            }
        }
        total.threads = static_cast<uint32_t>(
                std::min<size_t>(static_cast<size_t>(threads), std::max<size_t>(1, tasks.size())));
        RETURN_IF_ERROR(run_tasks(
                tasks, threads,
                [&](const DigestScanTask& task, RestoreDigest* acc) {
                    return run_direct_task(input, plan, delete_metas, task, acc);
                },
                &total));
    }

    total.finalize();
    total.elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::steady_clock::now() - start)
                               .count();
    *digest = std::move(total);
    return Status::OK();
}

namespace {

// Captures the rowsets (and the delete bitmap snapshot for MoW) and the schema of a tablet at
// (0, version].
Status prepare_tablet_input(StorageEngine& engine, int64_t tablet_id, int64_t version, int threads,
                            RestoreDigestInput* input) {
    TabletSharedPtr tablet = engine.tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        return Status::NotFound("restore digest: could not find tablet {}", tablet_id);
    }
    if (tablet->is_row_binlog_tablet()) {
        return Status::NotSupported("restore digest: row binlog tablet is not supported");
    }
    if (tablet->tablet_meta()->cooldown_meta_id().initialized()) {
        return Status::NotSupported("restore digest: tablet with cooldown data is not supported");
    }

    input->version = version;
    input->keys_type = tablet->keys_type();
    input->enable_mow = tablet->enable_unique_key_merge_on_write();
    input->threads = threads;
    input->tablet = tablet;
    {
        // Rowsets and the delete bitmap snapshot must be taken in the same critical section.
        std::shared_lock rdlock(tablet->get_header_lock());
        auto ret = tablet->capture_consistent_rowsets_unlocked(Version(0, version),
                                                               CaptureRowsetOps {});
        if (!ret) {
            return std::move(ret.error());
        }
        input->rowsets = std::move(ret->rowsets);
        if (input->enable_mow) {
            input->delete_bitmap = std::make_shared<DeleteBitmap>(
                    tablet->tablet_meta()->delete_bitmap().snapshot(version));
        }
    }
    std::vector<RowsetMetaSharedPtr> metas;
    metas.reserve(input->rowsets.size());
    for (const auto& rs : input->rowsets) {
        if (!rs->is_local()) {
            return Status::NotSupported("restore digest: remote rowset {} is not supported",
                                        rs->rowset_id().to_string());
        }
        metas.push_back(rs->rowset_meta());
    }
    // The newest schema among the rowsets and the tablet itself: after a light schema change the
    // rowsets may all carry an older schema, and the segments just read the default for the
    // columns they do not have.
    input->schema = metas.empty() ? tablet->tablet_schema()
                                  : BaseTablet::tablet_schema_with_merged_max_schema_version(metas);
    if (tablet->tablet_schema()->schema_version() > input->schema->schema_version()) {
        input->schema = tablet->tablet_schema();
    }
    return Status::OK();
}

} // namespace

Status compute_tablet_restore_digest(StorageEngine& engine, int64_t tablet_id, int64_t version,
                                     RestoreDigest* digest, int threads) {
    RestoreDigestInput input;
    RETURN_IF_ERROR(prepare_tablet_input(engine, tablet_id, version, threads, &input));
    return compute_restore_digest(input, digest);
}

Status compute_restore_digest_schema_sig(const TabletSchema& schema, KeysType keys_type,
                                         bool enable_mow, std::string* schema_sig) {
    using namespace restore_digest_detail;
    ColumnPlan plan;
    RETURN_IF_ERROR(build_plan(schema, keys_type, enable_mow, &plan));
    *schema_sig = std::move(plan.schema_sig);
    return Status::OK();
}

size_t RestoreDigestCache::KeyHash::operator()(const Key& k) const {
    size_t h = std::hash<std::string>()(k.schema_sig);
    auto mix = [&h](uint64_t v) {
        h ^= std::hash<uint64_t>()(v) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
    };
    mix(static_cast<uint64_t>(k.tablet_id));
    mix(static_cast<uint64_t>(k.version));
    mix(k.algo_version);
    return h;
}

RestoreDigestCache* RestoreDigestCache::instance() {
    static RestoreDigestCache cache;
    return &cache;
}

int64_t RestoreDigestCache::capacity() const {
    return _capacity > 0 ? _capacity : config::restore_digest_cache_capacity;
}

bool RestoreDigestCache::lookup(const Key& key, LogicalDigestResult* out) {
    std::lock_guard lock(_mtx);
    auto it = _map.find(key);
    if (it == _map.end()) {
        ++_misses;
        return false;
    }
    _lru.splice(_lru.begin(), _lru, it->second);
    *out = it->second->second;
    out->from_cache = true;
    ++_hits;
    return true;
}

void RestoreDigestCache::insert(const Key& key, const LogicalDigestResult& value) {
    const int64_t cap = capacity();
    std::lock_guard lock(_mtx);
    auto it = _map.find(key);
    if (it != _map.end()) {
        it->second->second = value;
        _lru.splice(_lru.begin(), _lru, it->second);
    } else if (cap > 0) {
        _lru.emplace_front(key, value);
        _map[key] = _lru.begin();
    }
    while (!_lru.empty() && static_cast<int64_t>(_lru.size()) > std::max<int64_t>(cap, 0)) {
        _map.erase(_lru.back().first);
        _lru.pop_back();
    }
}

size_t RestoreDigestCache::size() const {
    std::lock_guard lock(_mtx);
    return _lru.size();
}

void RestoreDigestCache::clear() {
    std::lock_guard lock(_mtx);
    _lru.clear();
    _map.clear();
}

Status get_tablet_logical_digest(StorageEngine& engine, int64_t tablet_id, int64_t version,
                                 int threads, RestoreDigestCache* cache,
                                 LogicalDigestResult* result) {
    if (threads <= 0) {
        threads = std::max(1, config::restore_digest_threads);
    }
    RestoreDigestInput input;
    RETURN_IF_ERROR(prepare_tablet_input(engine, tablet_id, version, threads, &input));
    RestoreDigestCache::Key key;
    key.tablet_id = tablet_id;
    key.version = version;
    key.algo_version = RestoreDigest::kAlgoVersion;
    // NotSupported schemas are reported here, before anything is scanned or cached.
    RETURN_IF_ERROR(compute_restore_digest_schema_sig(*input.schema, input.keys_type,
                                                      input.enable_mow, &key.schema_sig));
    if (cache != nullptr && cache->lookup(key, result)) {
        return Status::OK();
    }
    RestoreDigest digest;
    RETURN_IF_ERROR(compute_restore_digest(input, &digest));
    result->algo_version = RestoreDigest::kAlgoVersion;
    result->schema_sig = digest.schema_sig;
    result->root = digest.root;
    result->rows = digest.rows;
    result->from_cache = false;
    if (cache != nullptr) {
        cache->insert(key, *result);
    }
    return Status::OK();
}

} // namespace doris
