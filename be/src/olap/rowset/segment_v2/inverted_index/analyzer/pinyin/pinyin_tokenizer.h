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

#include "CLucene.h"
#include "CLucene/analysis/AnalysisHeader.h"
#include "pinyin_config.h"
#include "term_item.h"

using namespace lucene::analysis;

namespace doris::segment_v2 {

class PinyinTokenizer : public Tokenizer {
public:
    PinyinTokenizer();
    PinyinTokenizer(std::shared_ptr<PinyinConfig> config, bool ownReader = false);
    ~PinyinTokenizer() override = default;

    // 生成下一个分词结果，返回写入后的token指针；若无更多token则返回nullptr
    Token* next(Token* token) override;
    // 重置读取器，准备重新分词
    void reset(lucene::util::Reader* reader) override;

private:
    static constexpr int DEFAULT_BUFFER_SIZE = 256;

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
    std::shared_ptr<PinyinConfig> config_;

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

} // namespace doris::segment_v2