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

#include "pinyin_formatter.h"

#include <algorithm>
#include <cctype>
#include <regex>

#include "unicode/uchar.h"
#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

// 对应Java中的声调Unicode标记映射表
namespace {
constexpr const char* ALL_UNMARKED_VOWEL_STR = "aeiouv";
constexpr const char* ALL_MARKED_VOWEL_STR = "āáăàaēéĕèeīíĭìiōóŏòoūúŭùuǖǘǚǜü";
} // namespace

std::string PinyinFormatter::formatPinyin(const std::string& pinyin_str,
                                          const PinyinFormat& format) {
    if (pinyin_str.empty()) {
        return pinyin_str;
    }

    std::string result = pinyin_str;

    // 创建可修改的副本（在函数顶级作用域）
    PinyinFormat working_format = format;

    // 处理缩写格式（对应Java：ToneType.WITH_ABBR）
    if (format.getToneType() == ToneType::WITH_ABBR) {
        return abbr(result);
    } else {
        // 处理声调标记与ü字符的兼容性
        // 对应Java：if ((ToneType.WITH_TONE_MARK == format.getToneType()) && ...)
        if (working_format.getToneType() == ToneType::WITH_TONE_MARK &&
            (working_format.getYuCharType() == YuCharType::WITH_V ||
             working_format.getYuCharType() == YuCharType::WITH_U_AND_COLON)) {
            // ToneType.WITH_TONE_MARK force YuCharType.WITH_U_UNICODE
            working_format.setYuCharType(YuCharType::WITH_U_UNICODE);
        }

        // 处理声调类型
        switch (working_format.getToneType()) {
        case ToneType::WITHOUT_TONE:
            // 对应Java：pinyinStr.replaceAll("[1-5]", "")
            result = std::regex_replace(result, std::regex("[1-5]"), "");
            break;
        case ToneType::WITH_TONE_MARK:
            // 先将u:转换为v，然后转换声调标记
            result = std::regex_replace(result, std::regex("u:"), "v");
            result = convertToneNumber2ToneMark(result);
            break;
        case ToneType::WITH_TONE_NUMBER:
        default:
            // 保持原样
            break;
        }

        // 处理ü字符类型（如果不是声调标记格式）
        if (working_format.getToneType() != ToneType::WITH_TONE_MARK) {
            switch (working_format.getYuCharType()) {
            case YuCharType::WITH_V:
                // 对应Java：pinyinStr.replaceAll("u:", "v")
                result = std::regex_replace(result, std::regex("u:"), "v");
                break;
            case YuCharType::WITH_U_UNICODE:
                // 对应Java：pinyinStr.replaceAll("u:", "ü")
                result = std::regex_replace(result, std::regex("u:"), "ü");
                break;
            case YuCharType::WITH_U_AND_COLON:
            default:
                // 保持原样 u:
                break;
            }
        }
    }

    // 处理大小写
    switch (working_format.getCaseType()) {
    case CaseType::UPPERCASE:
        std::transform(result.begin(), result.end(), result.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        break;
    case CaseType::CAPITALIZE:
        result = capitalize(result);
        break;
    case CaseType::LOWERCASE:
    default:
        // 保持原样（已经是小写）
        break;
    }

    return result;
}

std::string PinyinFormatter::abbr(const std::string& str) {
    if (str.empty()) {
        return str;
    }

    // 正确处理UTF-8字符：取第一个完整的Unicode字符
    const char* str_ptr = str.c_str();
    int str_len = static_cast<int>(str.length());
    int byte_pos = 0;
    UChar32 first_char;

    // 使用ICU解码第一个字符
    U8_NEXT(str_ptr, byte_pos, str_len, first_char);

    if (first_char < 0) {
        // 无效的UTF-8序列
        return str.substr(0, 1);
    }

    // 将第一个字符重新编码为UTF-8
    std::string result;
    char utf8_buffer[4];
    int32_t utf8_len = 0;
    U8_APPEND_UNSAFE(utf8_buffer, utf8_len, first_char);
    result.assign(utf8_buffer, utf8_len);

    return result;
}

std::string PinyinFormatter::capitalize(const std::string& str) {
    if (str.empty()) {
        return str;
    }
    // 对应Java：Character.toTitleCase(str.charAt(0)) + str.substring(1)
    std::string result = str;
    result[0] = static_cast<char>(std::toupper(static_cast<unsigned char>(result[0])));
    return result;
}

std::string PinyinFormatter::convertToneNumber2ToneMark(const std::string& pinyin_str) {
    std::string lower_case_pinyin = pinyin_str;
    std::transform(lower_case_pinyin.begin(), lower_case_pinyin.end(), lower_case_pinyin.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    // 检查格式：必须是字母后跟可选的数字1-5
    // 对应Java：lowerCasePinyinStr.matches("[a-z]*[1-5]?")
    if (!std::regex_match(lower_case_pinyin, std::regex("[a-z]*[1-5]?"))) {
        return lower_case_pinyin;
    }

    const char DEFAULT_CHAR_VALUE = '$';
    const int DEFAULT_INDEX_VALUE = -1;

    char unmarked_vowel = DEFAULT_CHAR_VALUE;
    int index_of_unmarked_vowel = DEFAULT_INDEX_VALUE;

    // 检查是否有声调数字
    if (std::regex_match(lower_case_pinyin, std::regex("[a-z]*[1-5]"))) {
        // 获取声调数字
        int tune_number = lower_case_pinyin.back() - '0';

        // 查找声调标记应该放置的位置
        // 对应Java的算法：
        // 1. 首先查找 'a' 或 'e'
        // 2. 如果没有找到，查找 "ou"
        // 3. 否则查找最后一个元音字母

        size_t index_of_a = lower_case_pinyin.find('a');
        size_t index_of_e = lower_case_pinyin.find('e');
        size_t ou_index = lower_case_pinyin.find("ou");

        if (index_of_a != std::string::npos) {
            index_of_unmarked_vowel = static_cast<int>(index_of_a);
            unmarked_vowel = 'a';
        } else if (index_of_e != std::string::npos) {
            index_of_unmarked_vowel = static_cast<int>(index_of_e);
            unmarked_vowel = 'e';
        } else if (ou_index != std::string::npos) {
            index_of_unmarked_vowel = static_cast<int>(ou_index);
            unmarked_vowel = 'o';
        } else {
            // 查找最后一个元音字母
            for (int i = static_cast<int>(lower_case_pinyin.length()) - 1; i >= 0; i--) {
                char c = lower_case_pinyin[i];
                if (std::string(ALL_UNMARKED_VOWEL_STR).find(c) != std::string::npos) {
                    index_of_unmarked_vowel = i;
                    unmarked_vowel = c;
                    break;
                }
            }
        }

        // 如果找到了目标元音字母，进行声调标记转换
        if (unmarked_vowel != DEFAULT_CHAR_VALUE &&
            index_of_unmarked_vowel != DEFAULT_INDEX_VALUE) {
            std::string unmarked_vowel_str = ALL_UNMARKED_VOWEL_STR;
            int row_index = static_cast<int>(unmarked_vowel_str.find(unmarked_vowel));
            if (row_index != static_cast<int>(std::string::npos)) {
                int column_index = tune_number - 1;
                int vowel_location = row_index * 5 + column_index;

                // 防止越界
                std::string marked_vowel_str = ALL_MARKED_VOWEL_STR;
                if (vowel_location < static_cast<int>(marked_vowel_str.length())) {
                    // 构建结果字符串
                    std::string result;

                    // 前缀部分（将v替换为ü）
                    std::string prefix = lower_case_pinyin.substr(0, index_of_unmarked_vowel);
                    result += std::regex_replace(prefix, std::regex("v"), "ü");

                    // 声调标记字符
                    // 注意：这里需要处理UTF-8编码的Unicode字符
                    // 简化处理：直接按字符位置取子字符串
                    result += marked_vowel_str.substr(vowel_location, 1);

                    // 后缀部分（将v替换为ü，去掉最后的数字）
                    if (index_of_unmarked_vowel + 1 <
                        static_cast<int>(lower_case_pinyin.length()) - 1) {
                        std::string suffix = lower_case_pinyin.substr(
                                index_of_unmarked_vowel + 1,
                                lower_case_pinyin.length() - 1 - index_of_unmarked_vowel - 1);
                        result += std::regex_replace(suffix, std::regex("v"), "ü");
                    }

                    return result;
                }
            }
        }
    } else {
        // 没有声调数字，只替换v为ü
        return std::regex_replace(lower_case_pinyin, std::regex("v"), "ü");
    }

    // 出错时返回原字符串
    return lower_case_pinyin;
}

} // namespace doris::segment_v2::inverted_index
