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
#include "CLucene/analysis/AnalysisHeader.h"
// 最小实现：不引入任何算法或外部字典。

// 未来接入：拼音转换、中文识别等依赖（占位符头，暂不实现）
// #include "pinyin_util.h"
// #include "chinese_util.h"

namespace doris::segment_v2::inverted_index {

PinyinTokenizer::PinyinTokenizer() = default;
PinyinTokenizer::PinyinTokenizer(std::shared_ptr<doris::segment_v2::PinyinConfig> config)
        : PinyinTokenizer(DEFAULT_BUFFER_SIZE) {
    config_ = config;
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

explicit PinyinTokenizer(int buffer_size) {
    read_buffer_.reserve(buffer_size);
}
void PinyinTokenizer::reset(lucene::util::Reader* reader) {
    this->input = reader;
    initializeState();
}

// 读取输入，并一次性解析，生成候选项。
// 与Java版本一致：首次调用时读取全部文本，做拼音转换、候选生成与排序；
// 后续每次next()从candidate_中取一个，写入Token。
void PinyinTokenizer::processInput() {
    if (processed_candidate_ || done_) return;
    processed_candidate_ = true;

    // 读取全部输入到缓冲（最小实现，不做任何拼音转换算法）
    std::string buffer;
    buffer.reserve(DEFAULT_BUFFER_SIZE);

    // CLucene的Reader是面向TCHAR的，但我们在Doris内部通常使用UTF-8。
    // 这里先按字节流方式读取，作为占位实现。
    // 注意：生产实现需保证与拼音库相同的编码处理。
    {
        // 采用Token::termBuffer流程较复杂，这里直接分批读入到std::string
        const int kChunk = 4096;
        std::vector<char> chunk(kChunk);
        while (true) {
            // Reader::read接口在不同CLucene实现中签名不同，这里占位假设为：int read(char*, int, int)
            // 若签名不符，后续接入时替换为正确的读取逻辑。
            int n = this->input ? this->input->read((TCHAR*)chunk.data(), 0, kChunk) : -1;
            if (n <= 0) break;
            buffer.append(chunk.data(), n);
        }
    }
    source_ = buffer;

    // 最小实现：不做拼音映射，仅按配置生成原文、首字母序列（对 ASCII），
    // 以及一个固定的占位 token（例如 "py"），以打通 custom_analyzer 管线。

    std::string tmp_buff;        // 暂存非中文（ASCII）连续串
    int buff_start_position = 0; // 暂存开始位置
    int buff_size = 0;           // 暂存长度
    position_ = 0;               // 位置计数器

    auto flush_ascii_buff = [&]() {
        // 将ASCII缓冲区按配置转为候选项
        if (!config_->keepNoneChinese) {
            tmp_buff.clear();
            buff_size = 0;
            return;
        }

        if (config_->noneChinesePinyinTokenize) {
            // Java中这里会调用 PinyinAlphabetTokenizer.walk 对英文进行细粒度切分。
            // 这里先按字符逐个切分作为占位。
            int start = (last_offset_ - buff_size + 1);
            for (size_t i = 0; i < tmp_buff.size(); ++i) {
                int end;
                std::string t(1, tmp_buff[i]);
                if (config_->fixedPinyinOffset) {
                    end = start + 1;
                } else {
                    end = start + static_cast<int>(t.length());
                }
                position_++;
                addCandidate(t, start, end, position_);
                start = end;
            }
        } else if (config_->keepFirstLetter || config_->keepSeparateFirstLetter ||
                   config_->keepFullPinyin || !config_->keepNoneChineseInJoinedFullPinyin) {
            position_++;
            addCandidate(tmp_buff, last_offset_ - buff_size, last_offset_, position_);
        }

        tmp_buff.clear();
        buff_size = 0;
    };

    for (size_t i = 0; i < source_.size(); ++i) {
        unsigned char c = static_cast<unsigned char>(source_[i]);
        if (c < 128) {
            // ASCII：
            if (tmp_buff.empty()) {
                buff_start_position = static_cast<int>(i);
            }
            // 大小写字母和数字
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
                if (config_->keepNoneChinese) {
                    if (config_->keepNoneChineseTogether) {
                        tmp_buff.push_back(static_cast<char>(c));
                        buff_size++;
                    } else {
                        position_++;
                        addCandidate(std::string(1, static_cast<char>(c)), static_cast<int>(i),
                                     static_cast<int>(i + 1), buff_start_position + 1);
                    }
                }
                if (config_->keepNoneChineseInFirstLetter) {
                    first_letters_.push_back(static_cast<char>(c));
                }
                if (config_->keepNoneChineseInJoinedFullPinyin) {
                    full_pinyin_letters_.push_back(static_cast<char>(c));
                }
            }
        } else {
            // 非ASCII：最小实现，不做真实拼音；仅输出固定占位 token
            if (!tmp_buff.empty()) {
                flush_ascii_buff();
            }

            bool incr_position = false;

            std::string pinyin = "py";
            std::string chinese = source_.substr(i, 1);

            if (!pinyin.empty()) {
                // 首字母
                first_letters_.push_back(pinyin[0]);
                if (config_->keepSeparateFirstLetter && pinyin.length() > 1) {
                    position_++;
                    incr_position = true;
                    addCandidate(std::string(1, pinyin[0]), static_cast<int>(i),
                                 static_cast<int>(i + 1), position_);
                }
                // 全拼
                if (config_->keepFullPinyin) {
                    if (!incr_position) {
                        position_++;
                    }
                    addCandidate(pinyin, static_cast<int>(i), static_cast<int>(i + 1), position_);
                }
                // 保留中文原文
                if (config_->keepSeparateChinese) {
                    addCandidate(chinese, static_cast<int>(i), static_cast<int>(i + 1), position_);
                }
                // 连接全拼
                if (config_->keepJoinedFullPinyin) {
                    full_pinyin_letters_ += pinyin;
                }
            }
        }

        last_offset_ = static_cast<int>(i);
    }

    if (!tmp_buff.empty()) {
        flush_ascii_buff();
    }

    // 保留原文
    if (config_->keepOriginal && !processed_original_) {
        processed_original_ = true;
        addCandidate(source_, 0, static_cast<int>(source_.length()), 1);
    }

    // 连接全拼
    if (config_->keepJoinedFullPinyin && !processed_full_pinyin_letter_ &&
        !full_pinyin_letters_.empty()) {
        processed_full_pinyin_letter_ = true;
        addCandidate(full_pinyin_letters_, 0, static_cast<int>(source_.length()), 1);
        full_pinyin_letters_.clear();
    }

    // 首字母序列
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
    if (!processed_candidate_ && !done_) {
        processInput();
    }

    if (candidate_offset_ < static_cast<int>(candidate_.size())) {
        const TermItem& item = candidate_[candidate_offset_++];

        // setNoCopy会同时设置offset；positionIncrement需单独设置
        const std::string& text = item.term;
        size_t size = std::min(text.size(), static_cast<size_t>(LUCENE_MAX_WORD_LEN));
        token->setNoCopy(text.data(), 0, static_cast<int32_t>(size));

        if (!config_->ignorePinyinOffset) {
            token->setStartOffset(item.start_offset);
            token->setEndOffset(item.end_offset);
        }

        int offset = item.position - last_increment_position_;
        if (offset < 0) offset = 0;
        token->setPositionIncrement(offset);
        last_increment_position_ = item.position;
        return token;
    }

    done_ = true;
    return nullptr;
}

// 添加候选项，包含：大小写、trim、去重逻辑
void PinyinTokenizer::addCandidate(const std::string& term_in, int start_offset, int end_offset,
                                   int position) {
    std::string term = term_in;
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
    std::string key = config_->removeDuplicateTerm ? term : term + std::to_string(position);
    if (terms_filter_.find(key) != terms_filter_.end()) {
        return;
    }
    terms_filter_.insert(std::move(key));

    candidate_.emplace_back(term, start_offset, end_offset, position);
}

bool PinyinTokenizer::hasMoreTokens() const {
    return candidate_offset_ < static_cast<int>(candidate_.size());
}

void PinyinTokenizer::generateCandidates() {
    // 已在processInput中实现整体生成流程；保留此函数以贴合头文件结构
}

} // namespace doris::segment_v2::inverted_index
