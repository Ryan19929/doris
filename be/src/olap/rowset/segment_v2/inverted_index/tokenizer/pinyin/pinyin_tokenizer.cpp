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

#include "pinyin_tokenizer.h"

#include <algorithm>
#include <cctype>

// 引入CLucene核心
#include <unicode/unistr.h>
#include <unicode/utf8.h>

#include "CLucene/analysis/AnalysisHeader.h"
#include "common/exception.h"
// 核心拼音分词逻辑实现
#include "chinese_util.h"
#include "pinyin_alphabet_tokenizer.h"
#include "pinyin_format.h"
#include "pinyin_util.h"

namespace doris::segment_v2::inverted_index {

PinyinTokenizer::PinyinTokenizer() = default;

PinyinTokenizer::PinyinTokenizer(std::shared_ptr<doris::segment_v2::PinyinConfig> config) {
    config_ = std::move(config);
    if (!config_) {
        config_ = std::make_shared<doris::segment_v2::PinyinConfig>();
    }
    if (!(config_->keepFirstLetter || config_->keepSeparateFirstLetter || config_->keepFullPinyin ||
          config_->keepJoinedFullPinyin || config_->keepSeparateChinese)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "pinyin config error, can't disable separate_first_letter, first_letter "
                        "and full_pinyin at the same time.");
    }
    candidate_.clear();
    terms_filter_.clear();
    first_letters_.clear();
    full_pinyin_letters_.clear();
}

void PinyinTokenizer::reset() {
    DorisTokenizer::reset();
    position_ = 0;
    candidate_offset_ = 0;
    done_ = false;
    processed_candidate_ = false;
    processed_first_letter_ = false;
    processed_full_pinyin_letter_ = false;
    processed_original_ = false;
    processed_sort_candidate_ = false;
    first_letters_.clear();
    full_pinyin_letters_.clear();
    terms_filter_.clear();
    candidate_.clear();
    source_.clear();
    last_increment_position_ = 0;
    last_offset_ = 0;

    // 读取 UTF-8 原文到本地缓冲
    _char_buffer = nullptr;
    _char_length = _in->read((const void**)&_char_buffer, 0, static_cast<int32_t>(_in->size()));

    // 将 UTF-8 字节一次性复制为原文快照（仅当需要 keepOriginal/keepJoinedFullPinyin 时真正使用）
    source_.clear();
    if (_char_buffer && _char_length > 0) {
        source_.assign(_char_buffer, _char_buffer + _char_length);
    }

    // 立即完成 UTF-8 → Unicode 码点的解码，并记录字节偏移
    decode_to_runes();
}

// 读取输入，并一次性解析，生成候选项。
// 与 Java 版本一致：首次调用时读取全部文本，做拼音转换、候选生成与排序；
// 后续每次 next() 从 candidate_ 中取一个，写入 Token。
void PinyinTokenizer::processInput() {
    if (!processed_candidate_) {
        processed_candidate_ = true;
        // Java 等价步骤（说明性占位）：
        // processedCandidate = true;
        // 读取 input -> termAtt.buffer() -> source（本实现已在 reset() 中读取到 source_ 并完成解码）
        // 对应 Java：List<String> pinyinList = Pinyin.pinyin(source);
        // Java 的 Pinyin.pinyin() 实际调用 PinyinUtil.INSTANCE.convert(str, PinyinFormat.TONELESS_PINYIN_FORMAT)
        auto pinyin_list =
                PinyinUtil::instance().convert(source_, PinyinFormat::TONELESS_PINYIN_FORMAT);

        // 对应 Java：List<String> chineseList = ChineseUtil.segmentChinese(source);
        auto chinese_list = ChineseUtil::segmentChinese(source_);

        // 对应 Java：if (pinyinList.size() == 0 || chineseList.size() == 0) return false;
        if (pinyin_list.empty() || chinese_list.empty()) {
            return;
        }

        // 若没有任何可用的 Unicode 码点，提前返回
        if (runes_.empty()) {
            return;
        }
        // 生成候选（解码已在 reset 完成）
        // 为贴近 Java 的 incrementToken，这里直接展开候选生成逻辑
        position_ = 0;
        int ascii_buff_start = -1;      // ASCII 段的字节起始（用于 start/end offset）
        int ascii_buff_char_start = -1; // ASCII 段的“字符索引”起始（对齐 Java 的 i）
        std::string ascii_buff;
        int char_index = 0; // 遍历 runes_ 时的"字符索引"，等价于 Java 中的 i

        // Java 等价：遍历 source 的字符下标 i
        // String pinyin = pinyinList.get(i); String chinese = chineseList.get(i);
        for (const auto& r : runes_) {
            // 从 pinyin_list 和 chinese_list 获取当前字符的拼音和中文
            std::string pinyin = (char_index < static_cast<int>(pinyin_list.size()))
                                         ? pinyin_list[char_index]
                                         : "";
            std::string chinese = (char_index < static_cast<int>(chinese_list.size()))
                                          ? chinese_list[char_index]
                                          : "";
            // 对齐 Java：c < 128 进入 ASCII 分支；其中仅字母/数字会参与 keepNoneChinese 逻辑
            bool is_ascii_context = r.cp >= 0 && r.cp < 128;
            bool is_alnum = (r.cp >= 'a' && r.cp <= 'z') || (r.cp >= 'A' && r.cp <= 'Z') ||
                            (r.cp >= '0' && r.cp <= '9');

            if (is_ascii_context) {
                // 进入 ASCII 上下文时，若缓冲为空，记录段起始（字节与字符索引）
                if (ascii_buff_start < 0) ascii_buff_start = r.byte_start;
                if (ascii_buff_char_start < 0)
                    ascii_buff_char_start = char_index; // 记录 Java 的 buffStartPosition
                // 仅对字母/数字参与 keepNoneChinese 逻辑；标点等不进入 ascii_buff
                if (is_alnum && config_->keepNoneChinese) {
                    if (config_->keepNoneChineseTogether) {
                        ascii_buff.push_back(static_cast<char>(r.cp));
                    } else {
                        // 对齐 Java：逐字符输出时，TermItem.position 使用 buffStartPosition+1
                        position_++;
                        addCandidate(std::string(1, static_cast<char>(r.cp)), r.byte_start,
                                     r.byte_end, ascii_buff_char_start + 1);
                    }
                }
                // 对齐 Java：仅当为字母/数字时，参与 firstLetters 与 joinedFullPinyin
                if (is_alnum && config_->keepNoneChineseInFirstLetter) {
                    first_letters_.push_back(static_cast<char>(r.cp));
                }
                if (is_alnum && config_->keepNoneChineseInJoinedFullPinyin) {
                    full_pinyin_letters_.push_back(static_cast<char>(r.cp));
                }
            } else {
                if (!ascii_buff.empty()) {
                    parseBuff(ascii_buff, ascii_buff_start);
                    ascii_buff_char_start = -1; // 结束一个 ASCII 段
                }

                // 中文字符处理：使用从 pinyin_list 获取的拼音（已经过多音字处理）
                // 对应 Java：String pinyin = pinyinList.get(i); String chinese = chineseList.get(i);
                bool incr_position = false;
                if (!pinyin.empty()) {
                    first_letters_.push_back(pinyin[0]);
                    if (config_->keepSeparateFirstLetter && pinyin.length() > 1) {
                        position_++;
                        incr_position = true;
                        addCandidate(std::string(1, pinyin[0]), r.byte_start, r.byte_end,
                                     position_);
                    }
                    if (config_->keepFullPinyin) {
                        if (!incr_position) position_++;
                        addCandidate(pinyin, r.byte_start, r.byte_end, position_);
                    }
                    if (config_->keepSeparateChinese) {
                        addCandidate(chinese, r.byte_start, r.byte_end, position_);
                    }
                    if (config_->keepJoinedFullPinyin) {
                        full_pinyin_letters_ += pinyin;
                    }
                }
                last_offset_ = r.byte_end - 1;
            }
            last_offset_ = r.byte_end - 1;
            char_index++; // 前进一个"字符索引"，无论ASCII还是中文分支都要递增
        }
        if (!ascii_buff.empty()) {
            parseBuff(ascii_buff, ascii_buff_start);
            ascii_buff_char_start = -1;
        }
    }
    if (config_->keepOriginal && !processed_original_) {
        processed_original_ = true;
        addCandidate(source_, 0, static_cast<int>(source_.length()), 1);
    }
    if (config_->keepJoinedFullPinyin && !processed_full_pinyin_letter_ &&
        !full_pinyin_letters_.empty()) {
        processed_full_pinyin_letter_ = true;
        addCandidate(full_pinyin_letters_, 0, static_cast<int>(source_.length()), 1);
        full_pinyin_letters_.clear();
    }
    if (config_->keepFirstLetter && !first_letters_.empty() && !processed_first_letter_) {
        processed_first_letter_ = true;
        std::string fl = first_letters_;
        if (config_->limitFirstLetterLength > 0 &&
            static_cast<int>(fl.length()) > config_->limitFirstLetterLength) {
            fl = fl.substr(0, config_->limitFirstLetterLength);
        }
        if (config_->lowercase) {
            std::transform(fl.begin(), fl.end(), fl.begin(),
                           [](unsigned char x) { return static_cast<char>(std::tolower(x)); });
        }
        if (!(config_->keepSeparateFirstLetter && fl.length() <= 1)) {
            addCandidate(fl, 0, static_cast<int>(fl.length()), 1);
        }
    }

    if (!processed_sort_candidate_) {
        processed_sort_candidate_ = true;
        std::sort(candidate_.begin(), candidate_.end());
    }
}

// 根据候选项输出token。若无更多候选则返回nullptr。
Token* PinyinTokenizer::next(Token* token) {
    if (!done_) {
        processInput();
    }

    if (candidate_offset_ < static_cast<int>(candidate_.size())) {
        const TermItem& item = candidate_[candidate_offset_++];

        // 设置 term 文本（注意：setNoCopy 的第三个参数是 term 长度，不是原文 offset）
        const std::string& text = item.term;
        size_t size = std::min(text.size(), static_cast<size_t>(LUCENE_MAX_WORD_LEN));
        token->setNoCopy(text.data(), 0, static_cast<int32_t>(size));

        // 始终写入真实 offset（UTF-8 字节）
        token->setStartOffset(item.start_offset);
        token->setEndOffset(item.end_offset);

        // 对齐 Java：PositionIncrement 基于传入 setTerm 的参数 position（即 item.position）
        // 计算：offset = item.position - last_increment_position_
        int offset = item.position - last_increment_position_;
        if (offset < 0) offset = 0;
        token->setPositionIncrement(offset);
        last_increment_position_ = item.position;
        return token;
    }

    done_ = true;
    return nullptr;
}

// 添加候选项（Java 等价版本）：入参是 TermItem
void PinyinTokenizer::addCandidate(const TermItem& item_in) {
    std::string term = item_in.term;
    if (config_->lowercase) {
        std::transform(term.begin(), term.end(), term.begin(),
                       [](unsigned char x) { return static_cast<char>(std::tolower(x)); });
    }
    if (config_->trimWhitespace) {
        // 左右trim
        auto not_space = [](int ch) { return !std::isspace(ch); };
        term.erase(term.begin(), std::find_if(term.begin(), term.end(), not_space));
        term.erase(std::find_if(term.rbegin(), term.rend(), not_space).base(), term.end());
    }
    if (term.empty()) return;

    // 去重key：term + position，或（去重所有位置）仅term
    std::string key = config_->removeDuplicateTerm ? term : term + std::to_string(item_in.position);
    if (terms_filter_.find(key) != terms_filter_.end()) {
        return;
    }
    terms_filter_.insert(std::move(key));

    candidate_.emplace_back(term, item_in.start_offset, item_in.end_offset, item_in.position);
}

void PinyinTokenizer::setTerm(std::string term, int start_offset, int end_offset, int position) {
    // 为对齐 Java 的 setTerm 行为：这里不直接写 Token，而是转为候选
    addCandidate(term, start_offset, end_offset, position);
}

bool PinyinTokenizer::hasMoreTokens() const {
    return candidate_offset_ < static_cast<int>(candidate_.size());
}

void PinyinTokenizer::decode_to_runes() {
    runes_.clear();
    if (!_char_buffer || _char_length <= 0) return;
    runes_.reserve(static_cast<size_t>(_char_length));
    int32_t i = 0;
    while (i < _char_length) {
        UChar32 c = U_UNASSIGNED;
        int32_t prev = i;
        U8_NEXT(_char_buffer, i, _char_length, c);
        if (c < 0) {
            // 对齐 Java Reader 行为：将非法序列映射为 U+FFFD 替代字符，而非直接丢弃
            Rune r;
            r.cp = 0xFFFD;
            r.byte_start = prev;
            r.byte_end = i; // U8_NEXT 已前进 1 字节
            runes_.push_back(r);
            continue;
        }
        Rune r;
        r.cp = c;
        r.byte_start = prev;
        r.byte_end = i;
        runes_.push_back(r);
    }
}

void PinyinTokenizer::parseBuff(std::string& ascii_buff, int& ascii_buff_start) {
    if (ascii_buff.empty()) return;
    if (!config_->keepNoneChinese) {
        ascii_buff.clear();
        ascii_buff_start = -1;
        return;
    }
    int32_t seg_start = ascii_buff_start;
    int32_t seg_end = seg_start + static_cast<int32_t>(ascii_buff.size());
    if (config_->noneChinesePinyinTokenize) {
        // 对齐 Java：调用 PinyinAlphabetTokenizer.walk
        std::vector<std::string> result = PinyinAlphabetTokenizer::walk(ascii_buff);
        int32_t start = seg_start;
        for (const std::string& t : result) {
            int32_t end = config_->fixedPinyinOffset ? start + 1
                                                     : start + static_cast<int32_t>(t.length());
            position_++;
            addCandidate(t, start, end, position_);
            start = end;
        }
    } else if (config_->keepFirstLetter || config_->keepSeparateFirstLetter ||
               config_->keepFullPinyin || !config_->keepNoneChineseInJoinedFullPinyin) {
        position_++;
        addCandidate(ascii_buff, seg_start, seg_end, position_);
    }
    ascii_buff.clear();
    ascii_buff_start = -1;
}

} // namespace doris::segment_v2::inverted_index
