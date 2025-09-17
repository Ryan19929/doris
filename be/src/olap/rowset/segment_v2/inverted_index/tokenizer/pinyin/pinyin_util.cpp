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

#include "pinyin_util.h"

#include <unicode/utf8.h>

#include <fstream>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "pinyin_formatter.h"
#include "smart_get_word.h"

namespace doris::segment_v2::inverted_index {

namespace {
// CJK 统一汉字基本块范围
constexpr uint32_t CJK_START = 0x4E00; // CJK Unified Ideographs start
constexpr uint32_t CJK_END = 0x9FA5;   // CJK Unified Ideographs end
// 词典文件路径：动态从配置中获取，参考其他分词器的做法
inline std::string get_pinyin_dict_path() {
    return config::inverted_index_dict_path + "/pinyin/pinyin.txt";
}
inline std::string get_polyphone_dict_path() {
    return config::inverted_index_dict_path + "/pinyin/polyphone.txt";
}
} // namespace

PinyinUtil& PinyinUtil::instance() {
    static PinyinUtil inst;
    return inst;
}

PinyinUtil::PinyinUtil() : max_polyphone_len_(2) {
    load_pinyin_mapping();
    load_polyphone_mapping();
}

void PinyinUtil::load_pinyin_mapping() {
    // pinyin.txt: 每行形如：<汉字>=<pinyin1,pinyin2,...>
    // Java 实现按行序填充一个数组，index = ch-0x4E00，这里等价处理：
    _pinyin_dict.clear();
    _pinyin_dict.resize(static_cast<size_t>(CJK_END - CJK_START + 1));

    std::string pinyin_path = get_pinyin_dict_path();
    std::ifstream in(pinyin_path);
    if (!in.is_open()) {
        return; // 保持空表
    }
    std::string line;
    size_t idx = 0;
    while (std::getline(in, line)) {
        // 允许注释行以#开头；允许空行
        if (line.empty() || line[0] == '#') {
            ++idx;
            continue;
        }
        // 格式 key=value；我们仅取 value，保留逗号分隔
        size_t pos = line.find('=');
        std::string value;
        if (pos == std::string::npos) {
            value = "";
        } else {
            value = line.substr(pos + 1);
        }
        if (idx < _pinyin_dict.size()) {
            _pinyin_dict[idx] = value;
        }
        ++idx;
    }
}

std::string PinyinUtil::to_pinyin(uint32_t cp) const {
    if (cp < CJK_START || cp > CJK_END) return "";
    size_t idx = static_cast<size_t>(cp - CJK_START);
    if (idx >= _pinyin_dict.size()) return "";
    const std::string& raw = _pinyin_dict[idx];
    if (raw.empty()) return "";
    // 取第一个逗号前的拼音
    size_t comma = raw.find(',');
    if (comma == std::string::npos) return raw;
    return raw.substr(0, comma);
}

void PinyinUtil::load_polyphone_mapping() {
    // polyphone.txt: 每行形如：<词汇>=<拼音1> <拼音2> ...
    // 例如：长江=chang jiang
    polyphone_dict_ = std::make_unique<PolyphoneForest>();

    std::string polyphone_path = get_polyphone_dict_path();
    std::ifstream in(polyphone_path);
    if (!in.is_open()) {
        return; // 保持空字典
    }

    std::string line;
    while (std::getline(in, line)) {
        // 允许注释行以#开头；允许空行
        if (line.empty() || line[0] == '#') {
            continue;
        }

        // 格式：key=value，其中 value 是空格分隔的拼音
        size_t pos = line.find('=');
        if (pos == std::string::npos || pos + 1 >= line.length()) {
            continue;
        }

        std::string word = line.substr(0, pos);
        std::string pinyin_str = line.substr(pos + 1);

        // 解析拼音列表（空格分隔）
        std::vector<std::string> pinyins;
        std::istringstream iss(pinyin_str);
        std::string pinyin;
        while (iss >> pinyin) {
            pinyins.push_back(pinyin);
        }

        if (!word.empty() && !pinyins.empty()) {
            polyphone_dict_->add(word, pinyins);
            // 更新最大长度
            if (static_cast<int>(word.length()) > max_polyphone_len_) {
                max_polyphone_len_ = static_cast<int>(word.length());
            }
        }
    }
}

std::vector<std::string> PinyinUtil::convert(const std::string& text) const {
    std::vector<std::string> result;
    if (text.empty()) return result;

    // 首先将文本转换为字符数组，以便按字符索引处理
    const char* text_ptr = text.c_str();
    int text_len = static_cast<int>(text.length());
    std::vector<UChar32> chars;
    std::vector<int> char_byte_starts; // 记录每个字符在原始UTF-8中的字节起始位置

    // 解码UTF-8为字符数组
    int byte_pos = 0;
    while (byte_pos < text_len) {
        char_byte_starts.push_back(byte_pos);
        UChar32 cp;
        U8_NEXT(text_ptr, byte_pos, text_len, cp);
        if (cp < 0) cp = 0xFFFD; // 替换无效字符
        chars.push_back(cp);
    }

    // 初始化结果数组，长度等于字符数
    result.resize(chars.size());

    // 使用 SmartGetWord 进行多音字匹配
    PolyphoneGetWord word_matcher(polyphone_dict_.get(), text);

    // 标记已处理的字符位置
    std::vector<bool> processed(chars.size(), false);

    // 对应 Java：while ((temp = word.getFrontWords()) != null)
    std::string matched_word;
    while ((matched_word = word_matcher.getFrontWords()) != word_matcher.getNullResult() &&
           !matched_word.empty()) {
        int match_start_byte = word_matcher.offe;
        int match_end_byte = match_start_byte + static_cast<int>(matched_word.length());

        VLOG(4) << "PinyinUtil::convert - 多音字匹配: '" << matched_word
                << "' (字节: " << match_start_byte << "-" << match_end_byte << ")";

        // 找到匹配词对应的字符范围
        int char_start = -1, char_end = -1;

        // 找到包含 match_start_byte 的字符索引
        for (size_t i = 0; i < char_byte_starts.size(); ++i) {
            int char_byte_start = char_byte_starts[i];
            int char_byte_end = (i + 1 < char_byte_starts.size()) ? char_byte_starts[i + 1]
                                                                  : static_cast<int>(text.length());

            // 如果匹配开始位置在当前字符范围内
            if (match_start_byte >= char_byte_start && match_start_byte < char_byte_end) {
                char_start = static_cast<int>(i);
                break;
            }
        }

        // 找到包含 match_end_byte 的字符索引（或第一个超过的字符）
        for (size_t i = 0; i < char_byte_starts.size(); ++i) {
            int char_byte_start = char_byte_starts[i];

            // 如果当前字符的起始位置 >= 匹配结束位置，这就是结束字符索引
            if (char_byte_start >= match_end_byte) {
                char_end = static_cast<int>(i);
                break;
            }
        }
        if (char_end == -1) char_end = static_cast<int>(chars.size());

        // 将多音字的拼音分配给对应的字符位置
        const auto& pinyins = word_matcher.getParam();
        int word_char_count = char_end - char_start;

        for (int i = 0; i < word_char_count && i < static_cast<int>(pinyins.size()); ++i) {
            if (char_start + i < static_cast<int>(result.size())) {
                result[char_start + i] = pinyins[i];
                processed[char_start + i] = true;
            }
        }
    }

    // 处理未被多音字匹配的字符，使用单字拼音
    for (size_t i = 0; i < chars.size(); ++i) {
        if (!processed[i]) {
            // 对应 Java：toPinyin(str.charAt(i), format)
            std::string pinyin = to_pinyin(static_cast<uint32_t>(chars[i]));
            result[i] = pinyin; // 非中文字符会返回空字符串
        }
    }

    return result;
}

std::vector<std::string> PinyinUtil::convert(const std::string& text,
                                             const PinyinFormat& format) const {
    if (text.empty()) {
        return {};
    }

    // 首先获取原始拼音（带数字声调）
    std::vector<std::string> raw_result = convert_with_raw_pinyin(text);

    // 应用格式化
    std::vector<std::string> result;
    result.reserve(raw_result.size());

    for (const std::string& pinyin : raw_result) {
        if (pinyin.empty()) {
            // 非中文字符处理
            if (format.isOnlyPinyin()) {
                // 如果只输出拼音，跳过非中文字符
                continue;
            } else {
                result.push_back("");
            }
        } else {
            // 格式化拼音
            std::string formatted = PinyinFormatter::formatPinyin(pinyin, format);
            result.push_back(formatted);
        }
    }

    return result;
}

void PinyinUtil::insertPinyin(const std::string& word, const std::vector<std::string>& pinyins) {
    if (word.empty() || pinyins.empty()) {
        return;
    }

    // 对应 Java：polyphoneDict.add(word, pinyins.toArray(new String[0]))
    if (polyphone_dict_) {
        polyphone_dict_->add(word, pinyins);
    }
}

std::vector<std::string> PinyinUtil::convert_with_raw_pinyin(const std::string& text) const {
    std::vector<std::string> result;
    if (text.empty()) return result;

    VLOG(3) << "PinyinUtil::convert_with_raw_pinyin - 开始转换: '" << text << "'";

    // 首先将文本转换为字符数组，以便按字符索引处理
    const char* text_ptr = text.c_str();
    int text_len = static_cast<int>(text.length());
    std::vector<UChar32> chars;
    std::vector<int> char_byte_starts; // 记录每个字符在原始UTF-8中的字节起始位置

    // 解码UTF-8为字符数组
    int byte_pos = 0;
    while (byte_pos < text_len) {
        UChar32 cp;
        U8_NEXT(text_ptr, byte_pos, text_len, cp);
        char_byte_starts.push_back(byte_pos - U8_LENGTH(cp));
        chars.push_back(cp);
    }

    // 初始化结果数组，长度等于字符数
    result.resize(chars.size());

    // 使用 SmartGetWord 进行多音字匹配
    PolyphoneGetWord word_matcher(polyphone_dict_.get(), text);

    // 标记已处理的字符位置
    std::vector<bool> processed(chars.size(), false);

    // 对应 Java：while ((temp = word.getFrontWords()) != null)
    std::string matched_word;
    while ((matched_word = word_matcher.getFrontWords()) != word_matcher.getNullResult() &&
           !matched_word.empty()) {
        int match_start_byte = word_matcher.offe;
        int match_end_byte = match_start_byte + static_cast<int>(matched_word.length());

        VLOG(4) << "PinyinUtil::convert - 多音字匹配: '" << matched_word
                << "' (字节: " << match_start_byte << "-" << match_end_byte << ")";

        // 找到匹配词对应的字符范围
        int char_start = -1, char_end = -1;

        // 找到包含 match_start_byte 的字符索引
        for (size_t i = 0; i < char_byte_starts.size(); ++i) {
            int char_byte_start = char_byte_starts[i];
            int char_byte_end = (i + 1 < char_byte_starts.size()) ? char_byte_starts[i + 1]
                                                                  : static_cast<int>(text.length());

            // 如果匹配开始位置在当前字符范围内
            if (match_start_byte >= char_byte_start && match_start_byte < char_byte_end) {
                char_start = static_cast<int>(i);
                break;
            }
        }

        // 找到包含 match_end_byte 的字符索引（或第一个超过的字符）
        for (size_t i = 0; i < char_byte_starts.size(); ++i) {
            int char_byte_start = char_byte_starts[i];

            // 如果当前字符的起始位置 >= 匹配结束位置，这就是结束字符索引
            if (char_byte_start >= match_end_byte) {
                char_end = static_cast<int>(i);
                break;
            }
        }
        if (char_end == -1) char_end = static_cast<int>(chars.size());

        // 将多音字的拼音分配给对应的字符位置（使用原始拼音，保留声调数字）
        const auto& pinyins = word_matcher.getParam();
        int word_char_count = char_end - char_start;

        VLOG(4) << "PinyinUtil::convert - 字符范围: [" << char_start << ", " << char_end
                << "), 词长: " << word_char_count << ", 拼音数: " << pinyins.size();

        for (int i = 0; i < word_char_count && i < static_cast<int>(pinyins.size()); ++i) {
            int char_idx = char_start + i;
            if (char_idx >= 0 && char_idx < static_cast<int>(result.size())) {
                VLOG(5) << "PinyinUtil::convert - 分配拼音: 字符[" << char_idx << "] = '"
                        << pinyins[i] << "'";
                result[char_idx] = pinyins[i]; // 保留原始拼音格式
                processed[char_idx] = true;
            }
        }
    }

    // 处理未被多音字匹配的字符，使用单字拼音（原始格式）
    VLOG(4) << "PinyinUtil::convert - 处理未匹配的单字符";
    for (size_t i = 0; i < chars.size(); ++i) {
        if (!processed[i]) {
            std::string pinyin = to_raw_pinyin(static_cast<uint32_t>(chars[i]));
            VLOG(5) << "PinyinUtil::convert - 单字符: 字符[" << i << "] U+" << std::hex << chars[i]
                    << std::dec << " -> '" << pinyin << "'";
            result[i] = pinyin; // 非中文字符会返回空字符串
        }
    }

    return result;
}

std::string PinyinUtil::to_raw_pinyin(uint32_t cp) const {
    if (cp < CJK_START || cp > CJK_END) return "";
    size_t idx = static_cast<size_t>(cp - CJK_START);
    if (idx >= _pinyin_dict.size()) return "";
    const std::string& raw = _pinyin_dict[idx];
    if (raw.empty()) return "";
    // 直接返回第一个拼音（包含声调数字），不去除逗号后的部分
    size_t comma = raw.find(',');
    if (comma == std::string::npos) return raw;
    return raw.substr(0, comma);
}

// 直接接受 Unicode 码点向量的转换方法，更高效
std::vector<std::string> PinyinUtil::convert(const std::vector<UChar32>& codepoints,
                                             const PinyinFormat& format) const {
    if (codepoints.empty()) {
        return {};
    }

    // 初始化结果数组，长度等于码点数
    std::vector<std::string> result(codepoints.size());

    // 如果没有多音字词典，直接使用单字拼音
    if (!polyphone_dict_) {
        for (size_t i = 0; i < codepoints.size(); ++i) {
            std::string pinyin = to_pinyin(static_cast<uint32_t>(codepoints[i]));
            if (pinyin.empty()) {
                if (!format.isOnlyPinyin()) {
                    result[i] = "";
                }
            } else {
                result[i] = PinyinFormatter::formatPinyin(pinyin, format);
            }
        }
        return result;
    }

    // 构造UTF-8字符串用于多音字匹配（临时方案，后续可优化为直接在码点上工作）
    std::string text;
    for (UChar32 cp : codepoints) {
        // 直接使用 ICU 的 UTF-8 编码函数，避免 UnicodeString
        char utf8_buffer[4];
        int32_t utf8_len = 0;
        U8_APPEND_UNSAFE(utf8_buffer, utf8_len, cp);
        text.append(utf8_buffer, utf8_len);
    }

    // 使用现有的多音字处理逻辑
    std::vector<std::string> raw_result = convert_with_raw_pinyin(text);

    // 应用格式化
    result.clear();
    result.reserve(raw_result.size());

    for (const std::string& pinyin : raw_result) {
        if (pinyin.empty()) {
            if (!format.isOnlyPinyin()) {
                result.push_back("");
            }
        } else {
            std::string formatted = PinyinFormatter::formatPinyin(pinyin, format);
            if (!formatted.empty() || !format.isOnlyPinyin()) {
                result.push_back(formatted);
            }
        }
    }

    return result;
}

} // namespace doris::segment_v2::inverted_index
