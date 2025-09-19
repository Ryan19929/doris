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

#include <unicode/umachine.h>

namespace doris::segment_v2::inverted_index {

/**
 * Rune 表示一个 Unicode 字符及其在 UTF-8 字符串中的位置信息
 * 用于统一拼音分词系统中的字符表示，避免重复UTF-8解码
 */
struct Rune {
    int32_t byte_start {0}; // 字符在UTF-8字符串中的起始字节位置
    int32_t byte_end {0};   // 字符在UTF-8字符串中的结束字节位置
    UChar32 cp {0};         // Unicode码点

    // 默认构造函数
    Rune() = default;

    // 带参数构造函数
    Rune(int32_t start, int32_t end, UChar32 codepoint)
            : byte_start(start), byte_end(end), cp(codepoint) {}

    // 获取字符的字节长度
    int32_t byte_length() const { return byte_end - byte_start; }

    // 比较操作符，用于排序和查找
    bool operator<(const Rune& other) const { return cp < other.cp; }

    bool operator==(const Rune& other) const { return cp == other.cp; }

    bool operator!=(const Rune& other) const { return cp != other.cp; }
};

} // namespace doris::segment_v2::inverted_index
