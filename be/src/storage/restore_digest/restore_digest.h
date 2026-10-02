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

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/tablet/tablet_fwd.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

class StorageEngine;

// Order-independent logical digest of a tablet at a fixed version (0, V].
//
// algo_version = 1:
//   * Every visible row is encoded into a canonical byte string. For each digest column, in
//     schema order, one null-flag byte is written (0x00 = NULL, 0x01 = not NULL), followed by the
//     value if it is not NULL:
//       BOOLEAN/TINYINT/SMALLINT/INT/BIGINT/LARGEINT: native width, little endian
//       FLOAT/DOUBLE: raw IEEE-754 bits (no normalization of -0 / NaN payload)
//       DECIMAL32/64/128I/256: the unscaled integer at native width, little endian; precision and
//                              scale come from the schema signature, not from the row
//       DATEV2/DATETIMEV2: raw u32/u64 bit pattern; scale comes from the schema signature
//       IPV4/IPV6: u32/u128, little endian
//       CHAR: trailing '\0' padding stripped, then u32 length + bytes
//       VARCHAR/STRING: u32 length + bytes
//   * The row bytes are hashed with XXH3-128 (seed = algo_version). The high 8 bits of the high
//     64-bit half pick one of 256 buckets; a bucket keeps the sum (mod 2^128) of the row hashes
//     and its row count.
//   * root = SHA-256 over a fixed header and all 256 (sum, count) pairs.
//   * schema_sig = SHA-256 over the keys type and the (type, precision, scale) of every digest
//     column. Two digests are comparable only if algo_version and schema_sig are equal.
//
// Hidden columns: the delete sign is only used to filter rows (MoW), the sequence column takes
// part in the digest, version / commit TSO / skip bitmap / row store columns are excluded.
struct RestoreDigest {
    static constexpr uint32_t kAlgoVersion = 1;
    static constexpr size_t kNumBuckets = 256;

    struct Bucket {
        unsigned __int128 sum = 0;
        uint64_t count = 0;
    };

    std::array<Bucket, kNumBuckets> buckets {};
    uint64_t rows = 0;
    // rows read from the segments, including rows removed by the delete sign
    uint64_t rows_scanned = 0;
    // on-disk size (data part) of the scanned rowsets
    uint64_t bytes_read = 0;
    // size of the canonical row encoding that was hashed
    uint64_t encoded_bytes = 0;
    uint32_t rowset_count = 0;
    uint32_t segment_count = 0;
    int64_t elapsed_ms = 0;
    std::string schema_sig; // hex
    std::string root;       // hex

    // Computes `root` from the buckets.
    void finalize();
    std::string to_json() const;
};

struct RestoreDigestInput {
    // The rowsets which make up (0, version]; the caller keeps them alive.
    std::vector<RowsetSharedPtr> rowsets;
    // The schema used for reading, normally the merged max-schema-version schema of the rowsets.
    TabletSchemaSPtr schema;
    KeysType keys_type = DUP_KEYS;
    bool enable_mow = false;
    // Non-null for MoW: an immutable snapshot of the delete bitmap at `version`.
    DeleteBitmapPtr delete_bitmap;
    int64_t version = 0;
    int batch_size = 4096;
};

// Checks that the model and every column are supported by algo_version 1. Returns NotSupported
// with the reason otherwise. Never computes anything.
Status check_restore_digest_supported(const TabletSchema& schema, KeysType keys_type,
                                      bool enable_mow);

// Computes the digest from a given set of rowsets (the layer which unit tests drive directly).
Status compute_restore_digest(const RestoreDigestInput& input, RestoreDigest* digest);

// Computes the digest of tablet `tablet_id` at (0, version]. The rowsets and (for MoW) the delete
// bitmap snapshot are taken under the header lock. Read only.
Status compute_tablet_restore_digest(StorageEngine& engine, int64_t tablet_id, int64_t version,
                                     RestoreDigest* digest);

} // namespace doris
