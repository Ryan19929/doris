#include "Dictionary.h"

#include <fstream>

#include "CLucene/_ApiHeader.h"

namespace doris::segment_v2 {

Dictionary::Dictionary(const Configuration& cfg, bool use_ext_dict)
        : main_dict_(std::make_unique<DictSegment>(0)),
          quantifier_dict_(std::make_unique<DictSegment>(0)),
          stop_words_(std::make_unique<DictSegment>(0)),
          config_(std::make_unique<Configuration>(cfg)),
          load_ext_dict_(use_ext_dict) {}

void Dictionary::loadDictFile(DictSegment* dict, const std::string& file_path, bool critical,
                              const std::string& dict_name) {
    try {
        std::ifstream in(file_path);
        if (!in.good()) {
            if (critical) {
                _CLTHROWA(CL_ERR_IO, (dict_name + " dictionary file not found: " + file_path).c_str());
            }
            return;
        }

        std::string line;
        while (in.good() && !in.eof()) {
            try {
                std::getline(in, line);
                if (line.empty() || line[0] == '#') {
                    continue;
                }
                try {
                    dict->fillSegment(line.c_str());
                } catch (const std::bad_alloc& e) {
                    // Handle memory allocation failure
                    LOG(WARNING) << "Memory allocation failed when filling segment with line from " 
                                << dict_name << ": " << e.what();
                    continue;
                } catch (...) {
                    // Handle other exceptions
                    LOG(WARNING) << "Exception when filling segment with line from " << dict_name;
                    continue;
                }
            } catch (const std::ios_base::failure& e) {
                // Handle line reading failure
                LOG(WARNING) << "Failed to read line from " << dict_name 
                            << " dictionary file: " << e.what();
                continue;
            } catch (...) {
                // Handle other exceptions
                LOG(WARNING) << "Exception when reading line from " << dict_name << " dictionary file";
                continue;
            }
        }
    } catch (const std::ios_base::failure& e) {
        // Handle file IO exceptions
        if (critical) {
            _CLTHROWA(CL_ERR_IO, (dict_name + " dictionary file IO error: " + e.what()).c_str());
        }
        LOG(WARNING) << "IO error when reading " << dict_name 
                    << " dictionary file: " << e.what();
    } catch (...) {
        // Handle other exceptions
        if (critical) {
            _CLTHROWA(CL_ERR_Runtime, (dict_name + " dictionary loading error").c_str());
        }
        LOG(WARNING) << "Error when loading " << dict_name << " dictionary file";
    }
}

void Dictionary::loadMainDict() {
    try {
        loadDictFile(main_dict_.get(), config_->getDictPath() + "/" + config_->getMainDictFile(), true,
                    "Main Dict");

        // Load extension dictionaries
        if (load_ext_dict_) {
            for (const auto& extDict : config_->getExtDictFiles()) {
                try {
                    loadDictFile(main_dict_.get(), config_->getDictPath() + "/" + extDict, false,
                                "Extra Dict");
                } catch (...) {
                    LOG(WARNING) << "Error loading extension dictionary " << extDict;
                    // Continue loading other extension dictionaries
                }
            }
        }
    } catch (...) {
        LOG(ERROR) << "Failed to load main dictionary";
        // Main dictionary loading failure is a serious error, but we log and continue
    }
}

void Dictionary::loadStopWordDict() {
    try {
        loadDictFile(stop_words_.get(), config_->getDictPath() + "/" + config_->getStopWordDictFile(),
                    false, "Stopword");
        // Load extension stop words dictionary
        if (load_ext_dict_) {
            for (const auto& extDict : config_->getExtStopWordDictFiles()) {
                try {
                    loadDictFile(stop_words_.get(), config_->getDictPath() + "/" + extDict, false,
                                "Extra Stopword");
                } catch (...) {
                    LOG(WARNING) << "Error loading extension stop word dictionary " << extDict;
                    // Continue loading other extension stop word dictionaries
                }
            }
        }
    } catch (...) {
        LOG(WARNING) << "Failed to load stop word dictionary";
        // Stop word dictionary loading failure is not critical, log and continue
    }
}

void Dictionary::loadQuantifierDict() {
    try {
        loadDictFile(quantifier_dict_.get(),
                    config_->getDictPath() + "/" + config_->getQuantifierDictFile(), true,
                    "Quantifier");
    } catch (...) {
        LOG(ERROR) << "Failed to load quantifier dictionary";
        // Quantifier dictionary loading failure is serious, but we log and continue
    }
}

void Dictionary::reload() {
    if (singleton_) {
        try {
            singleton_->loadMainDict();
            singleton_->loadStopWordDict();
            singleton_->loadQuantifierDict();
            LOG(INFO) << "Dictionary reloaded successfully";
        } catch (...) {
            LOG(ERROR) << "Failed to reload dictionary";
        }
    }
}

Hit Dictionary::matchInMainDict(const CharacterUtil::TypedRuneArray& typed_runes,
                                size_t unicode_offset, size_t length) {
    Hit result = main_dict_->match(typed_runes, unicode_offset, length);

    if (!result.isUnmatch()) {
        result.setByteBegin(typed_runes[unicode_offset].offset);
        result.setCharBegin(unicode_offset);
        result.setByteEnd(typed_runes[unicode_offset + length - 1].getNextBytePosition());
        result.setCharEnd(unicode_offset + length);
    }
    return result;
}

Hit Dictionary::matchInQuantifierDict(const CharacterUtil::TypedRuneArray& typed_runes,
                                      size_t unicode_offset, size_t length) {
    Hit result = quantifier_dict_->match(typed_runes, unicode_offset, length);

    if (!result.isUnmatch()) {
        result.setByteBegin(typed_runes[unicode_offset].offset);
        result.setCharBegin(unicode_offset);
        result.setByteEnd(typed_runes[unicode_offset + length - 1].getNextBytePosition());
        result.setCharEnd(unicode_offset + length);
    }
    return result;
}

void Dictionary::matchWithHit(const CharacterUtil::TypedRuneArray& typed_runes, int current_index,
                              Hit& hit) {
    if (auto matchedSegment = hit.getMatchedDictSegment()) {
        matchedSegment->match(typed_runes, current_index, 1, hit);
        return;
    }
    hit.setUnmatch();
}

bool Dictionary::isStopWord(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
                            size_t length) {
    if (typed_runes.empty() || unicode_offset >= typed_runes.size()) {
        return false;
    }

    Hit result = stop_words_->match(typed_runes, unicode_offset, length);

    return result.isMatch();
}

} // namespace doris::segment_v2
