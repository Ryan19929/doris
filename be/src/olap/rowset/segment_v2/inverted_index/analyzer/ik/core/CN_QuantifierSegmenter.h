#pragma once

#include <memory>
#include <vector>

#include "AnalyzeContext.h"
#include "../util/IKContainer.h"
#include "ISegmenter.h"
namespace doris::segment_v2 {

class CN_QuantifierSegmenter : public ISegmenter {
public:
    static constexpr AnalyzeContext::SegmenterType SEGMENTER_TYPE =
            AnalyzeContext::SegmenterType::CN_QUANTIFIER;
    static const std::string SEGMENTER_NAME;
    static const std::u32string CHINESE_NUMBERS;

    CN_QuantifierSegmenter();
    ~CN_QuantifierSegmenter() override = default;

    void analyze(AnalyzeContext& context) override;
    void reset() override;

private:
    void processCNumber(AnalyzeContext& context);
    void processCount(AnalyzeContext& context);
    bool needCountScan(AnalyzeContext& context);
    void outputNumLexeme(AnalyzeContext& context);

    int number_start_;
    int number_end_;
    IKVector<Hit> count_hits_;
};
} // namespace doris::segment_v2
