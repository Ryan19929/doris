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

#include "common/config.h"
#include "storage/index/inverted/analyzer/ik/IKTokenizer.h"
#include "storage/index/inverted/analyzer/ik/dic/Dictionary.h"
#include "storage/index/inverted/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

class IKTokenizerFactory : public TokenizerFactory {
public:
    IKTokenizerFactory() = default;
    ~IKTokenizerFactory() override = default;

    void initialize(const Settings& settings) override {
        bool use_smart = settings.get_bool("mode_smart", true);
        bool lowercase = settings.get_bool("lowercase", true);

        _config = std::make_shared<segment_v2::Configuration>(use_smart, lowercase);
        _config->setDictPath(config::inverted_index_dict_path + "/ik");
        segment_v2::Dictionary::initial(*_config);

        _lowercase = lowercase;
    }

    TokenizerPtr create() override {
        auto tokenizer = std::make_shared<IKTokenizer>();
        tokenizer->initialize(_config, _lowercase);
        return tokenizer;
    }

private:
    std::shared_ptr<segment_v2::Configuration> _config;
    bool _lowercase = true;
};

} // namespace doris::segment_v2::inverted_index
