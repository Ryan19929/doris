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

#include "pinyin.h"

#include "pinyin_format.h"
#include "pinyin_util.h"

namespace doris::segment_v2::inverted_index {

std::vector<std::string> Pinyin::pinyin(const std::string& str) {
    // 对应Java：return PinyinUtil.INSTANCE.convert(str, PinyinFormat.TONELESS_PINYIN_FORMAT);
    return PinyinUtil::instance().convert(str, PinyinFormat::TONELESS_PINYIN_FORMAT);
}

std::vector<std::string> Pinyin::firstChar(const std::string& str) {
    // 对应Java：return PinyinUtil.INSTANCE.convert(str, PinyinFormat.ABBR_PINYIN_FORMAT);
    return PinyinUtil::instance().convert(str, PinyinFormat::ABBR_PINYIN_FORMAT);
}

std::vector<std::string> Pinyin::unicodePinyin(const std::string& str) {
    // 对应Java：return PinyinUtil.INSTANCE.convert(str, PinyinFormat.UNICODE_PINYIN_FORMAT);
    return PinyinUtil::instance().convert(str, PinyinFormat::UNICODE_PINYIN_FORMAT);
}

std::vector<std::string> Pinyin::tonePinyin(const std::string& str) {
    // 对应Java：return PinyinUtil.INSTANCE.convert(str, PinyinFormat.DEFAULT_PINYIN_FORMAT);
    return PinyinUtil::instance().convert(str, PinyinFormat::DEFAULT_PINYIN_FORMAT);
}

std::string Pinyin::list2String(const std::vector<std::string>& list,
                                const std::string& separator) {
    // 对应Java的StringBuilder逻辑
    if (list.empty()) {
        return "";
    }

    std::string result;
    bool first = true;

    for (const std::string& str : list) {
        std::string current_str = str;
        if (current_str.empty()) {
            current_str = "NULL"; // 对应Java中的null处理
        }

        if (first) {
            result += current_str;
            first = false;
        } else {
            result += separator;
            result += current_str;
        }
    }

    return result;
}

std::string Pinyin::list2String(const std::vector<std::string>& list) {
    // 对应Java：return list2String(list, " ");
    return list2String(list, " ");
}

void Pinyin::insertPinyin(const std::string& word, const std::vector<std::string>& pinyins) {
    // 对应Java：PinyinUtil.INSTANCE.insertPinyin(word, pinyins);
    PinyinUtil::instance().insertPinyin(word, pinyins);
}

std::string Pinyin::list2StringSkipNull(const std::vector<std::string>& list) {
    // 对应Java：return list2StringSkipNull(list, " ");
    return list2StringSkipNull(list, " ");
}

std::string Pinyin::list2StringSkipNull(const std::vector<std::string>& list,
                                        const std::string& separator) {
    // 对应Java的跳过null逻辑
    if (list.empty()) {
        return "";
    }

    std::string result;
    bool first = true;

    for (const std::string& str : list) {
        if (str.empty()) {
            continue; // 跳过空字符串（对应Java中的null）
        }

        if (first) {
            result += str;
            first = false;
        } else {
            result += separator;
            result += str;
        }
    }

    return result;
}

} // namespace doris::segment_v2::inverted_index
