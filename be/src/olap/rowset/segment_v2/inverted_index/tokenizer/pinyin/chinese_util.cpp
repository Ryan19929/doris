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

#include "chinese_util.h"

#include <unicode/unistr.h>

namespace doris::segment_v2::inverted_index {

// 与 Java 版本一致：对输入字符串按 UTF-16 逐单元检查是否在 [\u4E00, \u9FA5]
// 返回列表长度等于 Java 的 str.length()（UTF-16 code units 数）
// - 命中：返回该字符的 UTF-8 表示
// - 否则：返回空字符串（表示 Java 的 null）
std::vector<std::string> ChineseUtil::segmentChinese(const std::string& utf8_text) {
    if (utf8_text.empty()) return {};

    icu::UnicodeString u = icu::UnicodeString::fromUTF8(utf8_text);
    std::vector<std::string> out;
    out.reserve(static_cast<size_t>(u.length()));

    for (int i = 0; i < u.length(); ++i) {
        UChar cu = u.charAt(i);
        if (cu >= CJK_UNIFIED_IDEOGRAPHS_START && cu <= CJK_UNIFIED_IDEOGRAPHS_END) {
            icu::UnicodeString one(cu);
            std::string s;
            one.toUTF8String(s);
            out.emplace_back(std::move(s));
        } else {
            out.emplace_back(""); // 表示 Java 的 null
        }
    }
    return out;
}

} // namespace doris::segment_v2::inverted_index
