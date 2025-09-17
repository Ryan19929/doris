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
#include "common/logging.h"
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
    VLOG(3) << "PinyinTokenizer::reset() - 开始重置分词器";

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
    source_codepoints_.clear();
    last_increment_position_ = 0;
    last_offset_ = 0;

    // 读取 UTF-8 原文到本地缓冲
    _char_buffer = nullptr;
    _char_length = _in->read((const void**)&_char_buffer, 0, static_cast<int32_t>(_in->size()));

    VLOG(3) << "PinyinTokenizer::reset() - 读取输入数据: " << _char_length << " 字节";
    if (_char_length > 0 && _char_buffer) {
        std::string sample(_char_buffer, std::min(_char_length, 50));
        VLOG(3) << "PinyinTokenizer::reset() - 输入内容示例: \"" << sample
                << ((_char_length > 50) ? "...\"" : "\"");
    }

    // 立即完成 UTF-8 → Unicode 码点的解码，并记录字节偏移
    decode_to_runes();

    VLOG(3) << "PinyinTokenizer::reset() - 解码完成，Unicode码点数量: " << source_codepoints_.size()
            << ", Runes数量: " << runes_.size();
}

// 读取输入，并一次性解析，生成候选项。
// 与 Java 版本一致：首次调用时读取全部文本，做拼音转换、候选生成与排序；
// 后续每次 next() 从 candidate_ 中取一个，写入 Token。
void PinyinTokenizer::processInput() {
    if (!processed_candidate_) {
        VLOG(3) << "PinyinTokenizer::processInput() - 开始处理输入，生成候选项";
        processed_candidate_ = true;

        // Java 等价步骤（说明性占位）：
        // processedCandidate = true;
        // 读取 input -> termAtt.buffer() -> source（本实现已在 reset() 中读取到 source_ 并完成解码）
        // 使用 Unicode 码点向量版本，避免重复UTF-8解码，确保索引完全对齐
        // 对应 Java：List<String> pinyinList = Pinyin.pinyin(source);
        VLOG(3) << "PinyinTokenizer::processInput() - 开始拼音转换，输入码点数量: "
                << source_codepoints_.size();
        auto pinyin_list = PinyinUtil::instance().convert(source_codepoints_,
                                                          PinyinFormat::TONELESS_PINYIN_FORMAT);
        VLOG(3) << "PinyinTokenizer::processInput() - 拼音转换完成，拼音列表大小: "
                << pinyin_list.size();

        // 对应 Java：List<String> chineseList = ChineseUtil.segmentChinese(source);
        VLOG(3) << "PinyinTokenizer::processInput() - 开始中文分割";
        auto chinese_list = ChineseUtil::segmentChinese(source_codepoints_);
        VLOG(3) << "PinyinTokenizer::processInput() - 中文分割完成，中文列表大小: "
                << chinese_list.size();

        // 验证索引对齐
        bool index_aligned = (source_codepoints_.size() == pinyin_list.size() &&
                              pinyin_list.size() == chinese_list.size());
        VLOG(3) << "PinyinTokenizer::processInput() - 索引对齐检查: "
                << (index_aligned ? "通过" : "失败") << " (码点:" << source_codepoints_.size()
                << ", 拼音:" << pinyin_list.size() << ", 中文:" << chinese_list.size() << ")";

        // 对应 Java：if (pinyinList.size() == 0 || chineseList.size() == 0) return false;
        if (pinyin_list.empty() || chinese_list.empty()) {
            VLOG(3) << "PinyinTokenizer::processInput() - 拼音或中文列表为空，跳过处理";
            return;
        }

        // 若没有任何可用的 Unicode 码点，提前返回
        if (runes_.empty()) {
            VLOG(3) << "PinyinTokenizer::processInput() - Runes为空，跳过处理";
            return;
        }

        // 生成候选（解码已在 reset 完成）
        // 为贴近 Java 的 incrementToken，这里直接展开候选生成逻辑
        VLOG(3) << "PinyinTokenizer::processInput() - 开始生成候选项，处理字符数量: "
                << runes_.size();
        position_ = 0;
        int ascii_buff_start = -1;      // ASCII 段的字节起始（用于 start/end offset）
        int ascii_buff_char_start = -1; // ASCII 段的"字符索引"起始（对齐 Java 的 i）
        std::string ascii_buff;
        int char_index = 0; // 遍历 runes_ 时的"字符索引"，等价于 Java 中的 i

        int chinese_count = 0;
        int ascii_count = 0;

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
                ascii_count++;
                VLOG(4) << "PinyinTokenizer::processInput() - 处理ASCII字符: '"
                        << static_cast<char>(r.cp) << "' (0x" << std::hex << r.cp << std::dec
                        << "), 索引: " << char_index;

                // 进入 ASCII 上下文时，若缓冲为空，记录段起始（字节与字符索引）
                if (ascii_buff_start < 0) ascii_buff_start = r.byte_start;
                if (ascii_buff_char_start < 0)
                    ascii_buff_char_start = char_index; // 记录 Java 的 buffStartPosition
                // 仅对字母/数字参与 keepNoneChinese 逻辑；标点等不进入 ascii_buff
                if (is_alnum && config_->keepNoneChinese) {
                    if (config_->keepNoneChineseTogether) {
                        ascii_buff.push_back(static_cast<char>(r.cp));
                        VLOG(4) << "PinyinTokenizer::processInput() - ASCII字符加入缓冲: '"
                                << ascii_buff << "'";
                    } else {
                        // 对齐 Java：逐字符输出时，TermItem.position 使用 buffStartPosition+1
                        position_++;
                        std::string single_char(1, static_cast<char>(r.cp));
                        addCandidate(single_char, r.byte_start, r.byte_end,
                                     ascii_buff_char_start + 1);
                        VLOG(4) << "PinyinTokenizer::processInput() - 添加单个ASCII候选项: '"
                                << single_char << "', position: " << (ascii_buff_char_start + 1);
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
                // 处理非ASCII字符（主要是中文）
                chinese_count++;
                if (!ascii_buff.empty()) {
                    VLOG(4) << "PinyinTokenizer::processInput() - 处理缓冲的ASCII段: '"
                            << ascii_buff << "'";
                    parseBuff(ascii_buff, ascii_buff_start);
                    ascii_buff_char_start = -1; // 结束一个 ASCII 段
                }

                // 中文字符处理：使用从 pinyin_list 获取的拼音（已经过多音字处理）
                // 对应 Java：String pinyin = pinyinList.get(i); String chinese = chineseList.get(i);
                VLOG(4) << "PinyinTokenizer::processInput() - 处理中文字符: '" << chinese
                        << "', 拼音: '" << pinyin << "', 索引: " << char_index << ", 码点: 0x"
                        << std::hex << r.cp << std::dec;

                bool incr_position = false;
                if (!pinyin.empty()) {
                    first_letters_.push_back(pinyin[0]);
                    if (config_->keepSeparateFirstLetter && pinyin.length() > 1) {
                        position_++;
                        incr_position = true;
                        std::string first_letter(1, pinyin[0]);
                        addCandidate(first_letter, r.byte_start, r.byte_end, position_);
                        VLOG(4) << "PinyinTokenizer::processInput() - 添加首字母候选项: '"
                                << first_letter << "', position: " << position_;
                    }
                    if (config_->keepFullPinyin) {
                        if (!incr_position) position_++;
                        addCandidate(pinyin, r.byte_start, r.byte_end, position_);
                        VLOG(4) << "PinyinTokenizer::processInput() - 添加完整拼音候选项: '"
                                << pinyin << "', position: " << position_;
                    }
                    if (config_->keepSeparateChinese) {
                        addCandidate(chinese, r.byte_start, r.byte_end, position_);
                        VLOG(4) << "PinyinTokenizer::processInput() - 添加中文字符候选项: '"
                                << chinese << "', position: " << position_;
                    }
                    if (config_->keepJoinedFullPinyin) {
                        full_pinyin_letters_ += pinyin;
                        VLOG(4) << "PinyinTokenizer::processInput() - 连接拼音累积: '"
                                << full_pinyin_letters_ << "'";
                    }
                }
                last_offset_ = r.byte_end - 1;
            }
            last_offset_ = r.byte_end - 1;
            char_index++; // 前进一个"字符索引"，无论ASCII还是中文分支都要递增
        }

        // 处理剩余的ASCII缓冲
        if (!ascii_buff.empty()) {
            VLOG(4) << "PinyinTokenizer::processInput() - 处理剩余ASCII缓冲: '" << ascii_buff
                    << "'";
            parseBuff(ascii_buff, ascii_buff_start);
            ascii_buff_char_start = -1;
        }

        VLOG(3) << "PinyinTokenizer::processInput() - 字符处理完成，中文字符: " << chinese_count
                << ", ASCII字符: " << ascii_count << ", 首字母累积: '" << first_letters_ << "'"
                << ", 连接拼音累积: '" << full_pinyin_letters_ << "'";
    }

    // 处理全局候选项
    if (config_->keepOriginal && !processed_original_) {
        processed_original_ = true;
        std::string source_utf8 = codepointsToUtf8(source_codepoints_);
        addCandidate(source_utf8, 0, static_cast<int>(source_utf8.length()), 1);
        VLOG(3) << "PinyinTokenizer::processInput() - 添加原文候选项: '" << source_utf8 << "'";
    }
    if (config_->keepJoinedFullPinyin && !processed_full_pinyin_letter_ &&
        !full_pinyin_letters_.empty()) {
        processed_full_pinyin_letter_ = true;
        std::string source_utf8 = codepointsToUtf8(source_codepoints_);
        addCandidate(full_pinyin_letters_, 0, static_cast<int>(source_utf8.length()), 1);
        VLOG(3) << "PinyinTokenizer::processInput() - 添加连接拼音候选项: '" << full_pinyin_letters_
                << "'";
        full_pinyin_letters_.clear();
    }
    if (config_->keepFirstLetter && !first_letters_.empty() && !processed_first_letter_) {
        processed_first_letter_ = true;
        std::string fl = first_letters_;
        VLOG(3) << "PinyinTokenizer::processInput() - 处理首字母候选项，原始首字母: '" << fl << "'";

        if (config_->limitFirstLetterLength > 0 &&
            static_cast<int>(fl.length()) > config_->limitFirstLetterLength) {
            fl = fl.substr(0, config_->limitFirstLetterLength);
            VLOG(3) << "PinyinTokenizer::processInput() - 首字母长度限制，截取后: '" << fl << "'";
        }
        if (config_->lowercase) {
            std::transform(fl.begin(), fl.end(), fl.begin(),
                           [](unsigned char x) { return static_cast<char>(std::tolower(x)); });
            VLOG(3) << "PinyinTokenizer::processInput() - 首字母转小写后: '" << fl << "'";
        }
        if (!(config_->keepSeparateFirstLetter && fl.length() <= 1)) {
            addCandidate(fl, 0, static_cast<int>(fl.length()), 1);
            VLOG(3) << "PinyinTokenizer::processInput() - 添加首字母候选项: '" << fl << "'";
        } else {
            VLOG(3) << "PinyinTokenizer::processInput() - 跳过首字母候选项（单字符且启用分离模式）";
        }
    }

    if (!processed_sort_candidate_) {
        processed_sort_candidate_ = true;
        VLOG(3) << "PinyinTokenizer::processInput() - 对候选项进行排序，候选项数量: "
                << candidate_.size();
        std::sort(candidate_.begin(), candidate_.end());

        // 如果启用详细日志，输出前几个候选项
        if (VLOG_IS_ON(4) && !candidate_.empty()) {
            int show_count = std::min(static_cast<int>(candidate_.size()), 10);
            VLOG(4) << "PinyinTokenizer::processInput() - 前" << show_count << "个候选项:";
            for (int i = 0; i < show_count; ++i) {
                const auto& item = candidate_[i];
                VLOG(4) << "  [" << i << "] '" << item.term << "' (offset: " << item.start_offset
                        << "-" << item.end_offset << ", position: " << item.position << ")";
            }
        }
    }
}

// 根据候选项输出token。若无更多候选则返回nullptr。
Token* PinyinTokenizer::next(Token* token) {
    if (!done_) {
        VLOG(3) << "PinyinTokenizer::next() - 首次调用，开始处理输入";
        processInput();
        done_ = true;
        VLOG(3) << "PinyinTokenizer::next() - 输入处理完成，总候选项数量: " << candidate_.size();
    }

    if (candidate_offset_ < static_cast<int>(candidate_.size())) {
        const TermItem& item = candidate_[candidate_offset_];

        VLOG(4) << "PinyinTokenizer::next() - 返回候选项[" << candidate_offset_ << "/"
                << candidate_.size() << "]: '" << item.term << "' (offset: " << item.start_offset
                << "-" << item.end_offset << ", position: " << item.position << ")";

        candidate_offset_++;

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

        VLOG(4) << "PinyinTokenizer::next() - Token设置完成: text='" << text
                << "', startOffset=" << item.start_offset << ", endOffset=" << item.end_offset
                << ", positionIncrement=" << offset;

        return token;
    }

    VLOG(3) << "PinyinTokenizer::next() - 所有候选项已输出完毕，返回nullptr";
    done_ = true;
    return nullptr;
}

// 添加候选项（Java 等价版本）：入参是 TermItem
void PinyinTokenizer::addCandidate(const TermItem& item_in) {
    std::string term = item_in.term;
    VLOG(5) << "PinyinTokenizer::addCandidate() - 尝试添加候选项: '" << term
            << "' (offset: " << item_in.start_offset << "-" << item_in.end_offset
            << ", position: " << item_in.position << ")";
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
    if (term.empty()) {
        VLOG(5) << "PinyinTokenizer::addCandidate() - 候选项为空，跳过";
        return;
    }

    // 去重key：term + position，或（去重所有位置）仅term
    std::string key = config_->removeDuplicateTerm ? term : term + std::to_string(item_in.position);
    if (terms_filter_.find(key) != terms_filter_.end()) {
        VLOG(5) << "PinyinTokenizer::addCandidate() - 候选项重复，跳过: '" << term << "'";
        return;
    }
    terms_filter_.insert(std::move(key));

    candidate_.emplace_back(term, item_in.start_offset, item_in.end_offset, item_in.position);
    VLOG(5) << "PinyinTokenizer::addCandidate() - 候选项添加成功: '" << term
            << "', 当前候选项总数: " << candidate_.size();
}

// 便捷重载：将参数封装为 TermItem 并复用上面的实现
void PinyinTokenizer::addCandidate(const std::string& term, int start_offset, int end_offset,
                                   int position) {
    TermItem item(term, start_offset, end_offset, position);
    addCandidate(item);
}

void PinyinTokenizer::setTerm(std::string term, int start_offset, int end_offset, int position) {
    // 为对齐 Java 的 setTerm 行为：这里不直接写 Token，而是转为候选
    addCandidate(term, start_offset, end_offset, position);
}

bool PinyinTokenizer::hasMoreTokens() const {
    return candidate_offset_ < static_cast<int>(candidate_.size());
}

void PinyinTokenizer::decode_to_runes() {
    VLOG(3) << "PinyinTokenizer::decode_to_runes() - 开始UTF-8解码";

    runes_.clear();
    source_codepoints_.clear();

    if (!_char_buffer || _char_length <= 0) {
        VLOG(3) << "PinyinTokenizer::decode_to_runes() - 输入为空，跳过解码";
        return;
    }

    runes_.reserve(static_cast<size_t>(_char_length));
    source_codepoints_.reserve(static_cast<size_t>(_char_length));

    int32_t i = 0;
    int invalid_char_count = 0;

    while (i < _char_length) {
        UChar32 c = U_UNASSIGNED;
        int32_t prev = i;
        U8_NEXT(_char_buffer, i, _char_length, c);
        if (c < 0) {
            // 对齐 Java Reader 行为：将非法序列映射为 U+FFFD 替代字符，而非直接丢弃
            c = 0xFFFD;
            invalid_char_count++;
            VLOG(4) << "PinyinTokenizer::decode_to_runes() - 发现非法UTF-8序列，字节位置: " << prev
                    << ", 替换为U+FFFD";
        }

        // 同时填充 runes_ 和 source_codepoints_
        Rune r;
        r.cp = c;
        r.byte_start = prev;
        r.byte_end = i;
        runes_.push_back(r);
        source_codepoints_.push_back(c);

        if (VLOG_IS_ON(5)) {
            // 详细日志：输出每个字符
            if (c >= 32 && c < 127) {
                VLOG(5) << "PinyinTokenizer::decode_to_runes() - 码点[" << (runes_.size() - 1)
                        << "]: '" << static_cast<char>(c) << "' (U+" << std::hex << c << std::dec
                        << ", 字节: " << prev << "-" << i << ")";
            } else {
                VLOG(5) << "PinyinTokenizer::decode_to_runes() - 码点[" << (runes_.size() - 1)
                        << "]: U+" << std::hex << c << std::dec << " (字节: " << prev << "-" << i
                        << ")";
            }
        }
    }

    VLOG(3) << "PinyinTokenizer::decode_to_runes() - UTF-8解码完成"
            << ", 总字节数: " << _char_length << ", 解码出码点数: " << source_codepoints_.size()
            << ", 非法字符数: " << invalid_char_count;
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

std::string PinyinTokenizer::codepointsToUtf8(const std::vector<UChar32>& codepoints) const {
    std::string result;
    for (UChar32 cp : codepoints) {
        // 直接使用 ICU 的 UTF-8 编码函数，避免 UnicodeString
        char utf8_buffer[4];
        int32_t utf8_len = 0;
        U8_APPEND_UNSAFE(utf8_buffer, utf8_len, cp);
        result.append(utf8_buffer, utf8_len);
    }
    return result;
}

} // namespace doris::segment_v2::inverted_index
