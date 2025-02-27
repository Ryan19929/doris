#pragma once

#include <memory>
#include <set>
#include <stack>

#include "AnalyzeContext.h"
#include "../util/IKContainer.h"
#include "LexemePath.h"
#include "QuickSortSet.h"

namespace doris::segment_v2 {

class IKArbitrator {
public:
    IKArbitrator(IKMemoryPool<Cell>& pool) : pool_(pool) {}
    // Ambiguity handling
    void process(AnalyzeContext& context, bool use_smart);

private:
    IKMemoryPool<Cell>& pool_;
    // Ambiguity identification
    LexemePath* judge(Cell* lexeme_cell, size_t full_text_length);

    // Forward traversal, add lexeme, construct a non-ambiguous token combination
    void forwardPath(Cell* lexeme_cell, LexemePath* path_option, IKStack<Cell*>& conflictStack);
    void forwardPath(Cell* lexeme_cell, LexemePath* path_option);
    // Roll back the token chain until it can accept the specified token
    void backPath(const Lexeme& lexeme, LexemePath* path_option);
};

} // namespace doris::segment_v2
