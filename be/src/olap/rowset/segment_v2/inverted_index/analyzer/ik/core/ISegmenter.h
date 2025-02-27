#pragma once

#include "AnalyzeContext.h"
#include "CLucene/_ApiHeader.h"

namespace doris::segment_v2 {

class CLUCENE_EXPORT ISegmenter {
public:
    virtual ~ISegmenter() {}

    // Read the next possible token from the analyzer
    // param context Segmentation algorithm context
    virtual void analyze(AnalyzeContext& context) = 0;

    // Reset the sub-analyzer state
    virtual void reset() = 0;
};

} // namespace doris::segment_v2
