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

// 未来接入：拼音转换、中文识别等依赖（占位符头，暂不实现）
// #include "pinyin_util.h"
// #include "chinese_util.h"

namespace doris::segment_v2 {

PinyinTokenizer::PinyinTokenizer() {
    this->lowercase = false;
    this->ownReader = false;
    // 默认配置占位。真实环境由Analyzer或Factory注入
    config_ = std::make_shared<PinyinConfig>();
    initializeState();
}

PinyinTokenizer::PinyinTokenizer(std::shared_ptr<PinyinConfig> config, bool ownReader) {
    this->lowercase = config ? config->lowercase : false;
    this->ownReader = ownReader;
    config_ = std::move(config);
    if (!config_) config_ = std::make_shared<PinyinConfig>();
    initializeState();
}

// 初始化内部状态（对应Java reset()中清理逻辑）
void PinyinTokenizer::initializeState() {
    done_ = false;
    processed_candidate_ = false;
    processed_sort_candidate_ = false;
    processed_first_letter_ = false;
    processed_full_pinyin_letter_ = false;
    processed_original_ = false;
    position_ = 0;
    last_offset_ = 0;
    candidate_offset_ = 0;
    last_increment_position_ = 0;
    first_letters_.clear();
    full_pinyin_letters_.clear();
    terms_filter_.clear();
    candidate_.clear();
    source_.clear();
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

    // 读取全部输入到缓冲（参考 BasicTokenizer/IKTokenizer 的读取方式）
    // 这里简化处理：使用CLucene Reader的read到临时缓冲再拼接
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

    // 以下为算法流程说明与占位逻辑：
    // 1) 对source_进行拼音转换，得到每个字符对应的pinyin，以及是否是中文字符。
    //    Java版本使用 org.nlpcn.commons.lang.pinyin.Pinyin.pinyin(source)
    //    和 ChineseUtil.segmentChinese(source) 来得到两个等长列表。
    //    在C++中，我们后续会接入对应的拼音与中文检测工具，这里先用占位实现：

    // 占位：简单逐字扫描；ASCII字母数字直接保留；非ASCII当作“中文”处理。
    // 注意：真实实现中需精确的Unicode解码与拼音映射。

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
            // 非ASCII：占位认为是中文字符
            if (!tmp_buff.empty()) {
                flush_ascii_buff();
            }

            bool incr_position = false;

            // 占位拼音：此处应调用拼音库获取拼音 pinyin，并获取原始中文字符 chinese
            // 这里用"zh"作为演示拼音，source_[i]作为中文
            std::string pinyin = "zh";
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

} // namespace doris::segment_v2
