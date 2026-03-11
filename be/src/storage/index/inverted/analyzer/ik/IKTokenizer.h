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

#include "storage/index/inverted/analyzer/ik/cfg/Configuration.h"
#include "storage/index/inverted/analyzer/ik/core/IKSegmenter.h"
#include "storage/index/inverted/tokenizer/tokenizer.h"

namespace doris::segment_v2::inverted_index {

class IKTokenizer : public DorisTokenizer {
public:
    IKTokenizer() = default;
    ~IKTokenizer() override = default;

    void initialize(std::shared_ptr<segment_v2::Configuration> config, bool lower);

    Token* next(Token* token) override;
    void reset() override;

private:
    std::shared_ptr<segment_v2::Configuration> _config;
    std::unique_ptr<segment_v2::IKSegmenter> _ik_segmenter;
    bool _lowercase = true;
    std::string _current_text;
};

} // namespace doris::segment_v2::inverted_index
