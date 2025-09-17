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

#include <string>
#include <vector>

#include "unicode/uchar.h"

namespace doris::segment_v2::inverted_index {

// ChineseUtil 的 C++ 版本，对齐 Java ChineseUtil：
// - 常量 CJK_UNIFIED_IDEOGRAPHS_START/END
// - segmentChinese: 输入 UTF-8 文本，返回与 Java 等价的列表：
//   - 若该“Java 字符”(UTF-16 code unit)是中文 [\u4E00, \u9FA5]，则返回该字符的 UTF-8 文本
//   - 否则位置返回一个空字符串，等价 Java 的 null
class ChineseUtil {
public:
    static constexpr UChar32 CJK_UNIFIED_IDEOGRAPHS_START = 0x4E00;
    static constexpr UChar32 CJK_UNIFIED_IDEOGRAPHS_END = 0x9FA5;

    // 对齐 Java: public static List<String> segmentChinese(String str)
    // 说明：
    // - 修改为按 Unicode 码点处理，确保与其他组件索引对齐
    // - 非中文处返回空字符串，表示 Java 的 null
    static std::vector<std::string> segmentChinese(const std::string& utf8_text);

    // 直接接受 Unicode 码点向量的版本，更高效
    // @param codepoints Unicode 码点向量
    // @return 按码点索引的中文字符列表，长度等于 codepoints.size()
    static std::vector<std::string> segmentChinese(const std::vector<UChar32>& codepoints);
};

} // namespace doris::segment_v2::inverted_index
