#pragma once

#include <list>
#include <memory>
#include <string>

#include "AnalyzeContext.h"
#include "../dic/Dictionary.h"
#include "../util/IKContainer.h"
#include "CharacterUtil.h"
#include "ISegmenter.h"

namespace doris::segment_v2 {

class CJKSegmenter : public ISegmenter {
private:
    static constexpr AnalyzeContext::SegmenterType SEGMENTER_TYPE =
            AnalyzeContext::SegmenterType::CJK_SEGMENTER;
    IKList<Hit> tmp_hits_;

public:
    CJKSegmenter();

    void analyze(AnalyzeContext& context) override;
    void reset() override;
};

} // namespace doris::segment_v2
