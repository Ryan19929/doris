#include "IKTokenizer.h"

namespace doris::segment_v2 {

IKTokenizer::IKTokenizer() {
    this->lowercase = false;
    this->ownReader = false;

    config_ = std::make_shared<Configuration>(true, false);
}

IKTokenizer::IKTokenizer(bool lower_case, bool own_reader, bool is_smart) : IKTokenizer() {
    this->lowercase = lower_case;
    this->ownReader = own_reader;
    config_->setEnableLowercase(lower_case);
    config_->setUseSmart(is_smart);
}

// IKTokenizer::IKTokenizer(Reader* reader, std::shared_ptr<Configuration> config)
//         : Tokenizer(reader), config_(config) {
//     reset(reader);
//     Tokenizer::lowercase = false;
//     Tokenizer::ownReader = false;
// }

// IKTokenizer::IKTokenizer(Reader* reader, std::shared_ptr<Configuration> config, bool isSmart,
//                          bool in_lowercase, bool in_ownReader)
//         : Tokenizer(reader), config_(config) {
//     config_->setUseSmart(isSmart);
//     config_->setEnableLowercase(in_lowercase);
//     reset(reader);
//     Tokenizer::lowercase = in_lowercase;
//     Tokenizer::ownReader = in_ownReader;
// }

void IKTokenizer::initialize(const std::string& dictPath) {
    config_->setDictPath(dictPath);
    Dictionary::initial(*config_);
}

Token* IKTokenizer::next(Token* token) {
    if (buffer_index_ >= data_length_) {
        return nullptr;
    }

    std::string& token_text = tokens_text_[buffer_index_++];
    size_t size = std::min(token_text.size(), static_cast<size_t>(LUCENE_MAX_WORD_LEN));
    if (Tokenizer::lowercase) {
        if (!token_text.empty() && static_cast<uint8_t>(token_text[0]) < 0x80) {
            std::transform(token_text.begin(), token_text.end(), token_text.begin(),
                           [](char c) { return to_lower(c); });
        }
    }
    token->setNoCopy(token_text.data(), 0, size);
    return token;
}

void IKTokenizer::reset(lucene::util::Reader* reader) {
    this->input = reader;
    this->buffer_index_ = 0;
    this->data_length_ = 0;
    this->tokens_text_.clear();

    buffer_.reserve(input->size());

    IKSegmentSingleton::getInstance().setContext(reader, config_);

    Lexeme lexeme;
    while (IKSegmentSingleton::getInstance().next(lexeme)) {
        tokens_text_.emplace_back(lexeme.getText());
    }

    data_length_ = tokens_text_.size();
}

} // namespace doris::segment_v2
