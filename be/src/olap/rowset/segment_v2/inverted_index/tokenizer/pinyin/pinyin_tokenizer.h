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
    static constexpr int DEFAULT_BUFFER_SIZE = 1024;

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

    // Helper methods
    // 初始化内部状态（对应Java的reset内部清理逻辑）
    void initializeState();
    // 读取input中的全部文本并进行一次性处理（对应Java的incrementToken首段读取+解析）
    void processInput();
    // 是否仍有未输出的候选项
    bool hasMoreTokens() const;
    // 生成候选项（包含拼音首字母、全拼、原文等，按照配置决定）
    void generateCandidates();
    // 添加一个候选项，包含去重与大小写/空白处理（对应Java的addCandidate与setTerm前处理）
    void addCandidate(const std::string& term, int start_offset, int end_offset, int position);
};

} // namespace doris::segment_v2::inverted_index