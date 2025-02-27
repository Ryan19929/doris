#pragma once

#include <memory>
#include <optional>

#include "CLucene/_ApiHeader.h"
#include <parallel_hashmap/phmap.h>
#include "QuickSortSet.h"

namespace doris::segment_v2 {

class CLUCENE_EXPORT LexemePath : public QuickSortSet {
public:
    LexemePath(IKMemoryPool<Cell>& pool);
    LexemePath(LexemePath& other, IKMemoryPool<Cell>& pool);
    LexemePath(LexemePath&& other, IKMemoryPool<Cell>& pool) noexcept;
    bool addCrossLexeme(Lexeme& lexeme);
    bool addNotCrossLexeme(Lexeme& lexeme);
    std::optional<Lexeme> removeTail();
    bool checkCross(const Lexeme& lexeme) const;

    size_t getPathBegin() const { return path_begin_; }
    size_t getPathEnd() const { return path_begin_; }
    size_t getPayloadLength() const { return payload_length_; }
    size_t getPathLength() const { return path_begin_ - path_end_; }
    size_t size() const { return getSize(); }

    size_t getXWeight() const;
    size_t getPWeight() const;

    bool operator<(const LexemePath& other) const;
    bool operator==(const LexemePath& other) const;

private:
    size_t path_begin_;     // Starting byte position
    size_t path_end_;       // Ending byte position
    size_t payload_length_; // Effective byte length of the lexeme chain
};

} // namespace doris::segment_v2
