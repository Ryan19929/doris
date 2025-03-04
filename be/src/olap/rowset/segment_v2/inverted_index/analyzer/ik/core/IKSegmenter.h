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

#include <iostream>
#include <memory>
#include <vector>

#include "AnalyzeContext.h"
#include "CJKSegmenter.h"
#include "CN_QuantifierSegmenter.h"
#include "IKArbitrator.h"
#include "ISegmenter.h"
#include "LetterSegmenter.h"
namespace doris::segment_v2 {

class IKSegmenter {
public:
    IKSegmenter();
    void setContext(lucene::util::Reader* input, std::shared_ptr<Configuration> config);
    bool next(Lexeme& lexeme);
    void reset(lucene::util::Reader* newInput);
    int getLastUselessCharNum();

private:
    std::vector<std::unique_ptr<ISegmenter>> loadSegmenters();
    IKMemoryPool<Cell> pool_;
    lucene::util::Reader* input_;
    std::unique_ptr<AnalyzeContext> context_;
    std::vector<std::unique_ptr<ISegmenter>> segmenters_;
    IKArbitrator arbitrator_;
    std::shared_ptr<Configuration> config_;
};
} // namespace doris::segment_v2
