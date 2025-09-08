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

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/tokenizer/tokenizer.h"
#include "pinyin_config.h"
#include "term_item.h"

namespace doris::segment_v2::inverted_index {

class PinyinTokenizer : public DorisTokenizer {
public:
    PinyinTokenizer();
    PinyinTokenizer(std::shared_ptr<doris::segment_v2::PinyinConfig> config);
    ~PinyinTokenizer() override = default;

    // 生成下一个分词结果，返回写入后的token指针；若无更多token则返回nullptr
    Token* next(Token* token) override;
    // Doris 风格：无参 reset，从 _in 中读取
    void reset() override;

private:
    // Core tokenizer state variables (ported from Java)
    bool done_;
    bool processed_candidate_;
    bool processed_sort_candidate_;
    bool processed_first_letter_;
    bool processed_full_pinyin_letter_;
    bool processed_original_;

    // Position tracking (ported from Java)
    int position_;
    int last_offset_;
    int candidate_offset_; // Indicate candidates process offset
    int last_increment_position_;

    // Configuration
    std::shared_ptr<doris::segment_v2::PinyinConfig> config_;

    // Term processing data structures (ported from Java)
    std::vector<TermItem> candidate_;
    std::unordered_set<std::string> terms_filter_;
    std::string first_letters_;
    std::string full_pinyin_letters_;
    std::string source_;

    // Doris 风格：reset 时从 _in 读取到这两个指针/长度，processInput 中使用
    const char* _char_buffer {nullptr};
    int32_t _char_length {0};

    // 将 UTF-8 解码后的码点及其字节偏移缓存下来，避免在候选生成时重复计算
    struct Rune {
        int32_t byte_start {0};
        int32_t byte_end {0};
        UChar32 cp {0};
    };
    std::vector<Rune> runes_;

    // 是否仍有未输出的候选项
    bool hasMoreTokens() const;
    // 添加一个候选项（与 Java 签名一致）：包含去重与大小写/空白处理
    void addCandidate(const TermItem& item);

    // 可选：对齐 Java 的 setTerm 语义，这里作为便捷方式转为候选（非必须）
    void setTerm(std::string term, int start_offset, int end_offset, int position);
    // 便捷重载：保持现有调用点，内部组装 TermItem 并复用上面的实现
    void addCandidate(const std::string& term, int start_offset, int end_offset, int position);

    // 解码：UTF-8 字节串 -> runes_（码点+字节起止）
    void decode_to_runes();
    // 候选生成逻辑将直接内联到 next()，以贴近 Java incrementToken 流程
    void processInput();
    // 解析 ASCII 缓冲（对齐 Java 的 parseBuff）：根据配置将缓冲区转为候选
    void parseBuff(std::string& ascii_buff, int& ascii_buff_start);
};

} // namespace doris::segment_v2::inverted_index