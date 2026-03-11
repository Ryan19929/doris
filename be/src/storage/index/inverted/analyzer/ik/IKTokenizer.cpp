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

#include "storage/index/inverted/analyzer/ik/IKTokenizer.h"

#include "storage/index/inverted/analyzer/ik/core/CharacterUtil.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

void IKTokenizer::initialize(std::shared_ptr<segment_v2::Configuration> config, bool lower) {
    _config = std::move(config);
    _lowercase = lower;
    _ik_segmenter = std::make_unique<segment_v2::IKSegmenter>(_config);
}

Token* IKTokenizer::next(Token* token) {
    segment_v2::Lexeme lexeme;
    if (!_ik_segmenter->next(lexeme)) {
        return nullptr;
    }
    _current_text = lexeme.getText();
    segment_v2::CharacterUtil::regularizeString(_current_text, _lowercase);
    size_t size = std::min(_current_text.size(), static_cast<size_t>(LUCENE_MAX_WORD_LEN));
    set(token, std::string_view(_current_text.data(), size));
    return token;
}

void IKTokenizer::reset() {
    DorisTokenizer::reset();
    _ik_segmenter->reset(_in);
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index
