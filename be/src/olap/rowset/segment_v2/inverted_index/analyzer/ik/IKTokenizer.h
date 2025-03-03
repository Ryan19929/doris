#pragma once

#include <memory>
#include <string_view>

#include "CLucene.h"
#include "CLucene/analysis/AnalysisHeader.h"
#include "cfg/Configuration.h"
#include "core/IKSegmenter.h"

using namespace lucene::analysis;

namespace doris::segment_v2 {

class IKSegmentSingleton {
public:
    static IKSegmenter& getInstance() {
        static IKSegmenter instance;
        return instance;
    }

private:
    IKSegmentSingleton() = default;
};

class IKTokenizer : public Tokenizer {
public:
    IKTokenizer();
    IKTokenizer(bool lowercase, bool ownReader, bool isSmart);
    ~IKTokenizer() override = default;

    void initialize(const std::string& dictPath);
    Token* next(Token* token) override;
    void reset(lucene::util::Reader* reader) override;

private:
    int32_t buffer_index_ {0};
    int32_t data_length_ {0};
    std::string buffer_;
    std::vector<std::string> tokens_text_;
    std::shared_ptr<Configuration> config_;
};

} // namespace doris::segment_v2
