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
