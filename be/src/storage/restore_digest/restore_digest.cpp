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

#include <bit>
#include <chrono>
#include <cstring>
#include <memory>
#include <shared_mutex>
#include <string_view>

#include "common/consts.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/schema.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/utils.h"
#include "util/sha.h"

namespace doris {

static_assert(std::endian::native == std::endian::little,
              "restore digest encoding assumes a little endian host");

namespace restore_digest_detail {

enum class ColRole : uint8_t { DIGEST, DELETE_SIGN, EXCLUDED };
enum class ColKind : uint8_t { FIXED, STRING, CHAR };

struct ColCodec {
    ColKind kind = ColKind::FIXED;
    uint8_t width = 0; // for FIXED
};

// Whether the field type is supported, and how it is encoded.
bool codec_of(FieldType type, ColCodec* codec) {
    auto fixed = [&](uint8_t w) {
        codec->kind = ColKind::FIXED;
        codec->width = w;
        return true;
    };
    switch (type) {
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
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        return fixed(16);
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
        return fixed(32);
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
    case FieldType::OLAP_FIELD_TYPE_STRING:
        codec->kind = ColKind::STRING;
        return true;
    case FieldType::OLAP_FIELD_TYPE_CHAR:
        codec->kind = ColKind::CHAR;
        return true;
    default:
        return false;
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
    if (!codec_of(col.type(), codec)) {
        return Status::NotSupported("restore digest: column {} has unsupported type {}", name,
                                    TabletColumn::get_string_by_field_type(col.type()));
    }
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
        if (!enable_mow) {
            return Status::NotSupported(
                    "restore digest: unique key merge-on-read is not supported");
        }
    } else {
        return Status::NotSupported("restore digest: keys type {} is not supported",
                                    static_cast<int>(keys_type));
    }
    if (!schema.cluster_key_uids().empty()) {
        return Status::NotSupported("restore digest: cluster key is not supported");
    }
    if (schema.has_seq_map()) {
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
            plan->codecs.push_back(codec);
            sig_data += fmt::format("|{}:{}:{}:{}", static_cast<int>(col.type()), col.precision(),
                                    col.frac(), col.is_key() ? 1 : 0);
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
};

Status make_view(const ColumnPtr& column, const ColCodec& codec, size_t rows, ColView* view) {
    const IColumn* c = column.get();
    view->kind = codec.kind;
    view->width = codec.width;
    view->null_map = nullptr;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(c)) {
        view->null_map = nullable->get_null_map_data().data();
        c = &nullable->get_nested_column();
    }
    if (codec.kind == ColKind::FIXED) {
        // get_raw_data() reports the element count in `size`, not bytes
        StringRef raw = c->get_raw_data();
        if (raw.size != rows || c->size() != rows) {
            return Status::InternalError(
                    "restore digest: column {} has {} elements for {} rows, expect width {}",
                    c->get_name(), raw.size, rows, codec.width);
        }
        view->data = raw.data;
    } else {
        view->str = check_and_get_column<ColumnString>(c);
        if (view->str == nullptr) {
            return Status::InternalError("restore digest: expect a string column but got {}",
                                         c->get_name());
        }
    }
    return Status::OK();
}

inline void append_u32(std::string& buf, uint32_t v) {
    buf.append(reinterpret_cast<const char*>(&v), sizeof(v));
}

constexpr XXH64_hash_t kHashSeed = RestoreDigest::kAlgoVersion;

// Encodes one row, appending to `buf`. The encoding is injective for a fixed schema: every field
// is a flag byte followed by either a fixed number of bytes or a length-prefixed byte string.
inline void encode_row(const std::vector<ColView>& views, size_t row, std::string& buf) {
    for (const ColView& v : views) {
        if (v.null_map != nullptr && v.null_map[row] != 0) {
            buf.push_back('\x00');
            continue;
        }
        buf.push_back('\x01');
        if (v.kind == ColKind::FIXED) {
            buf.append(v.data + row * v.width, v.width);
        } else {
            StringRef s = v.str->get_data_at(row);
            size_t len = s.size;
            if (v.kind == ColKind::CHAR) {
                while (len > 0 && s.data[len - 1] == '\0') {
                    --len;
                }
            }
            append_u32(buf, static_cast<uint32_t>(len));
            buf.append(s.data, len);
        }
    }
}

} // namespace restore_digest_detail

using namespace restore_digest_detail;

Status check_restore_digest_supported(const TabletSchema& schema, KeysType keys_type,
                                      bool enable_mow) {
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
            "\"encoded_bytes\":{},\"elapsed_ms\":{},\"buckets\":[",
            kAlgoVersion, rows, rows_scanned, root, schema_sig, rowset_count, segment_count,
            bytes_read, encoded_bytes, elapsed_ms);
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
    if (input.schema == nullptr || digest == nullptr) {
        return Status::InvalidArgument("restore digest: null schema or output");
    }
    auto start = std::chrono::steady_clock::now();
    ColumnPlan plan;
    RETURN_IF_ERROR(build_plan(*input.schema, input.keys_type, input.enable_mow, &plan));
    if (input.enable_mow && input.delete_bitmap == nullptr) {
        return Status::InvalidArgument("restore digest: MoW needs a delete bitmap snapshot");
    }

    *digest = RestoreDigest {};
    digest->schema_sig = plan.schema_sig;

    // The read schema: all digest columns, then the delete sign column for MoW.
    std::vector<ColumnId> read_ordinals(plan.digest_ordinals.begin(), plan.digest_ordinals.end());
    const size_t num_digest = plan.digest_ordinals.size();
    if (plan.delete_sign_ordinal >= 0) {
        read_ordinals.push_back(static_cast<ColumnId>(plan.delete_sign_ordinal));
    }
    auto read_schema = std::make_shared<ReadSchema>(
            project_columns_by_ordinal(input.schema->columns(), read_ordinals));
    RETURN_IF_ERROR(read_schema->init_from_tablet_schema(*input.schema,
                                                         /*merge_by_sequence_mapping=*/false,
                                                         /*map_row_binlog_columns=*/false));

    ColCodec delete_sign_codec;
    delete_sign_codec.kind = ColKind::FIXED;
    delete_sign_codec.width = 1;

    std::vector<ColView> views(num_digest);
    ColView delete_sign_view;
    std::string row_buf;
    row_buf.reserve(256);

    for (const RowsetSharedPtr& rowset : input.rowsets) {
        digest->rowset_count++;
        digest->segment_count += static_cast<uint32_t>(rowset->num_segments());
        digest->bytes_read += rowset->data_disk_size();
        if (rowset->num_rows() == 0) {
            continue;
        }
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
        RETURN_IF_ERROR(reader->init(&ctx));

        while (true) {
            Block block = read_schema->create_read_block();
            Status st = reader->next_batch(&block);
            const size_t n = block.rows();
            if (n > 0) {
                std::vector<ColumnPtr> keep_alive;
                keep_alive.reserve(num_digest + 1);
                for (size_t j = 0; j < num_digest; ++j) {
                    keep_alive.push_back(
                            block.get_by_position(j).column->convert_to_full_column_if_const());
                    RETURN_IF_ERROR(make_view(keep_alive.back(), plan.codecs[j], n, &views[j]));
                }
                if (plan.delete_sign_ordinal >= 0) {
                    keep_alive.push_back(block.get_by_position(num_digest)
                                                 .column->convert_to_full_column_if_const());
                    RETURN_IF_ERROR(
                            make_view(keep_alive.back(), delete_sign_codec, n, &delete_sign_view));
                }
                digest->rows_scanned += n;
                for (size_t row = 0; row < n; ++row) {
                    if (plan.delete_sign_ordinal >= 0 && ((delete_sign_view.null_map != nullptr &&
                                                           delete_sign_view.null_map[row] != 0) ||
                                                          delete_sign_view.data[row] != 0)) {
                        continue;
                    }
                    row_buf.clear();
                    encode_row(views, row, row_buf);
                    XXH128_hash_t h =
                            XXH3_128bits_withSeed(row_buf.data(), row_buf.size(), kHashSeed);
                    auto& bucket = digest->buckets[h.high64 >> 56];
                    bucket.sum += (static_cast<unsigned __int128>(h.high64) << 64) | h.low64;
                    bucket.count++;
                    digest->rows++;
                    digest->encoded_bytes += row_buf.size();
                }
            }
            if (st.is<ErrorCode::END_OF_FILE>()) {
                break;
            }
            RETURN_IF_ERROR(st);
        }
    }

    digest->finalize();
    digest->elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - start)
                                 .count();
    return Status::OK();
}

Status compute_tablet_restore_digest(StorageEngine& engine, int64_t tablet_id, int64_t version,
                                     RestoreDigest* digest) {
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

    RestoreDigestInput input;
    input.version = version;
    input.keys_type = tablet->keys_type();
    input.enable_mow = tablet->enable_unique_key_merge_on_write();
    {
        // Rowsets and the delete bitmap snapshot must be taken in the same critical section.
        std::shared_lock rdlock(tablet->get_header_lock());
        auto ret = tablet->capture_consistent_rowsets_unlocked(Version(0, version),
                                                               CaptureRowsetOps {});
        if (!ret) {
            return std::move(ret.error());
        }
        input.rowsets = std::move(ret->rowsets);
        if (input.enable_mow) {
            input.delete_bitmap = std::make_shared<DeleteBitmap>(
                    tablet->tablet_meta()->delete_bitmap().snapshot(version));
        }
    }
    std::vector<RowsetMetaSharedPtr> metas;
    metas.reserve(input.rowsets.size());
    for (const auto& rs : input.rowsets) {
        if (!rs->is_local()) {
            return Status::NotSupported("restore digest: remote rowset {} is not supported",
                                        rs->rowset_id().to_string());
        }
        if (rs->rowset_meta()->has_delete_predicate()) {
            return Status::NotSupported(
                    "restore digest: rowset {} carries a delete predicate, not supported",
                    rs->version().to_string());
        }
        metas.push_back(rs->rowset_meta());
    }
    input.schema = metas.empty() ? tablet->tablet_schema()
                                 : BaseTablet::tablet_schema_with_merged_max_schema_version(metas);
    return compute_restore_digest(input, digest);
}

} // namespace doris
