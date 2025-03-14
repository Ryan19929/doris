#pragma once

#include <CLucene.h>

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "CLucene/LuceneThreads.h"
#include "CLucene/analysis/AnalysisHeader.h"
#include "common/logging.h"
#include "../cfg/Configuration.h"
#include "../core/CharacterUtil.h"
#include "DictSegment.h"
#include "Hit.h"

namespace doris::segment_v2 {
class Dictionary {
private:
    static Dictionary* singleton_;
    static std::once_flag init_flag_;
    // Dictionary segment mappings
    std::unique_ptr<DictSegment> main_dict_;
    std::unique_ptr<DictSegment> quantifier_dict_;
    std::unique_ptr<DictSegment> stop_words_;
    std::unique_ptr<Configuration> config_;
    bool load_ext_dict_;
    class Cleanup {
    public:
        ~Cleanup() {
            if (Dictionary::singleton_) {
                delete Dictionary::singleton_;
                Dictionary::singleton_ = nullptr;
            }
        }
    };
    static Cleanup cleanup_;

    // Dictionary paths
    static const std::string PATH_DIC_MAIN;
    static const std::string PATH_DIC_QUANTIFIER;
    static const std::string PATH_DIC_STOP;

    explicit Dictionary(const Configuration& cfg, bool use_ext_dict = false);

    void loadMainDict();
    void loadStopWordDict();
    void loadQuantifierDict();

    void loadDictFile(DictSegment* dict, const std::string& file_path, bool critical,
                      const std::string& dict_name);

    Dictionary(const Dictionary&) = delete;
    Dictionary& operator=(const Dictionary&) = delete;

public:
    static void destroy() {
        if (singleton_) {
            delete singleton_;
            singleton_ = nullptr;
        }
    }
    ~Dictionary() {}

    static void initial(const Configuration& cfg, bool useExtDict = false) {
        getSingleton(cfg, useExtDict);
    }

    static Dictionary* getSingleton() {
        if (!singleton_) {
            _CLTHROWA(CL_ERR_IllegalState, "Dictionary not initialized");
        }
        return singleton_;
    }

    static Dictionary* getSingleton(const Configuration& cfg, bool useExtDict = false) {
        std::call_once(init_flag_, [&]() {
            try {
                singleton_ = new Dictionary(cfg, useExtDict);
                try {
                    singleton_->loadMainDict();
                    singleton_->loadQuantifierDict();
                    singleton_->loadStopWordDict();
                } catch (...) {
                    // If dictionary loading fails, log the error but don't terminate initialization
                    LOG(ERROR) << "Error during dictionary initialization";
                }
            } catch (const std::bad_alloc&) {
                // Memory allocation failure is a critical error
                LOG(ERROR) << "Failed to allocate memory for Dictionary";
                // In this case, we cannot continue and must throw an exception
                _CLTHROWA(CL_ERR_OutOfMemory, "Failed to allocate memory for Dictionary");
            } catch (...) {
                // Other exceptions
                LOG(ERROR) << "Exception during Dictionary initialization";
                _CLTHROWA(CL_ERR_Runtime, "Exception during Dictionary initialization");
            }
        });
        return singleton_;
    }

    Configuration* getConfiguration() const { return config_.get(); }

    static void reload();

    Hit matchInMainDict(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
                        size_t length);
    Hit matchInQuantifierDict(const CharacterUtil::TypedRuneArray& typed_runes,
                              size_t unicode_offset, size_t length);
    void matchWithHit(const CharacterUtil::TypedRuneArray& typed_runes, int current_index,
                      Hit& hit);
    bool isStopWord(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
                    size_t length);

    void printStats() const;
};

inline Dictionary* Dictionary::singleton_ = nullptr;
inline std::once_flag Dictionary::init_flag_;

inline const std::string Dictionary::PATH_DIC_MAIN = "main.dic";
inline const std::string Dictionary::PATH_DIC_QUANTIFIER = "quantifier.dic";
inline const std::string Dictionary::PATH_DIC_STOP = "stopword.dic";
inline Dictionary::Cleanup Dictionary::cleanup_;
} // namespace doris::segment_v2
