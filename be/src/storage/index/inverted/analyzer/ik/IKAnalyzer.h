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

#include "storage/index/inverted/analyzer/ik/IKTokenizer.h"
#include "storage/index/inverted/analyzer/ik/dic/Dictionary.h"
#include "storage/index/inverted/token_stream.h"

namespace doris::segment_v2::inverted_index {

class IKAnalyzer : public Analyzer {
public:
    IKAnalyzer() {
        _lowercase = true;
        _ownReader = false;
    }

    ~IKAnalyzer() override = default;

    bool isSDocOpt() override { return true; }

    void initDict(const std::string& dictPath) override {
        _dict_path = dictPath;
        auto config = std::make_shared<segment_v2::Configuration>(_use_smart, _lowercase);
        config->setDictPath(dictPath);
        segment_v2::Dictionary::initial(*config);
    }

    void setMode(bool isSmart) { _use_smart = isSmart; }

    TokenStream* tokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) override {
        throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED,
                        "IKAnalyzer::tokenStream(Reader*) not supported");
    }

    TokenStream* reusableTokenStream(const TCHAR* fieldName,
                                     lucene::util::Reader* reader) override {
        throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED,
                        "IKAnalyzer::reusableTokenStream(Reader*) not supported");
    }

    TokenStream* tokenStream(const TCHAR* fieldName, const ReaderPtr& reader) override {
        auto components = create_components();
        components->set_reader(reader);
        components->get_token_stream()->reset();
        return new TokenStreamWrapper(components->get_token_stream());
    }

    TokenStream* reusableTokenStream(const TCHAR* fieldName, const ReaderPtr& reader) override {
        if (!_reuse_token_stream) {
            _reuse_token_stream = create_components();
        }
        _reuse_token_stream->set_reader(reader);
        return _reuse_token_stream->get_token_stream().get();
    }

private:
    TokenStreamComponentsPtr create_components() {
        auto config = std::make_shared<segment_v2::Configuration>(_use_smart, _lowercase);
        config->setDictPath(_dict_path);
        auto tk = std::make_shared<IKTokenizer>();
        tk->initialize(config, _lowercase);
        TokenStreamPtr ts = tk;
        return std::make_shared<TokenStreamComponents>(tk, ts);
    }

    bool _use_smart = true;
    std::string _dict_path;
    TokenStreamComponentsPtr _reuse_token_stream;
};

} // namespace doris::segment_v2::inverted_index
