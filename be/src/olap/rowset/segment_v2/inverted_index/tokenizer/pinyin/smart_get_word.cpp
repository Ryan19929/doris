// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "smart_get_word.h"

#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

template <typename T>
const std::string SmartGetWord<T>::EMPTYSTRING = "";

template <typename T>
SmartGetWord<T>::SmartGetWord(SmartForest<T>* forest, const std::string& content)
        : forest_(forest), branch_(forest) {
    chars_ = utf8_to_unicode(content);
    i_ = root_;
}

template <typename T>
SmartGetWord<T>::SmartGetWord(SmartForest<T>* forest, const std::vector<UChar32>& chars)
        : forest_(forest), chars_(chars), branch_(forest) {
    i_ = root_;
}

template <typename T>
std::string SmartGetWord<T>::getFrontWords() {
    std::string temp;
    do {
        temp = frontWords();
        temp = checkNumberOrEnglish(temp);
    } while (temp == EMPTYSTRING);
    return temp;
}

template <typename T>
T SmartGetWord<T>::getParam() const {
    return param_;
}

template <typename T>
void SmartGetWord<T>::reset(const std::string& content) {
    offe = 0;
    status_ = 0;
    root_ = 0;
    i_ = root_;
    isBack_ = false;
    tempOffe_ = 0;
    chars_ = utf8_to_unicode(content);
    branch_ = forest_;
}

template <typename T>
std::string SmartGetWord<T>::frontWords() {
    // 对应 Java 中的 frontWords() 方法
    // 实现前向最大匹配算法
    for (; i_ < static_cast<int>(chars_.size()) + 1; i_++) {
        if (i_ == static_cast<int>(chars_.size())) {
            branch_ = nullptr;
        } else {
            branch_ = branch_->getBranch(chars_[i_]);
        }

        if (branch_ == nullptr) {
            branch_ = forest_;
            if (isBack_) {
                offe = root_;
                str_ = unicode_to_utf8(chars_, root_, tempOffe_);
                if (str_.length() == 0) {
                    root_ += 1;
                    i_ = root_;
                } else {
                    i_ = root_ + tempOffe_;
                    root_ = i_;
                }
                isBack_ = false;
                return str_;
            }
            i_ = root_;
            root_ += 1;
        } else {
            switch (branch_->getStatus()) {
            case SmartForest<T>::WORD_CONTINUE: // status = 2
                isBack_ = true;
                tempOffe_ = i_ - root_ + 1;
                param_ = branch_->getParam();
                break;

            case SmartForest<T>::WORD_END: // status = 3
                offe = root_;
                str_ = unicode_to_utf8(chars_, root_, i_ - root_ + 1);
                {
                    std::string temp = str_;
                    param_ = branch_->getParam();
                    branch_ = forest_;
                    isBack_ = false;
                    if (temp.length() > 0) {
                        i_ += 1;
                        root_ = i_;
                    } else {
                        i_ = root_ + 1;
                    }
                    return str_;
                }
                break;
            }
        }
    }

    tempOffe_ += static_cast<int>(chars_.size());
    return ""; // 对应 Java 中的 null
}

template <typename T>
std::string SmartGetWord<T>::checkNumberOrEnglish(const std::string& temp) {
    // 验证一个词语的左右边界，不是英文和数字
    if (temp.empty() || temp == EMPTYSTRING) {
        return temp;
    }

    // 先验证最左边
    std::vector<UChar32> temp_chars = utf8_to_unicode(temp);
    if (temp_chars.empty()) {
        return temp;
    }

    UChar32 l = temp_chars[0];

    if (l < 127 && offe > 0) {
        if (checkSame(l, chars_[offe - 1])) {
            return EMPTYSTRING;
        }
    }

    UChar32 r = l;
    if (temp_chars.size() > 1) {
        r = temp_chars[temp_chars.size() - 1];
    }

    if (r < 127 && (offe + static_cast<int>(temp_chars.size())) < static_cast<int>(chars_.size())) {
        if (checkSame(r, chars_[offe + static_cast<int>(temp_chars.size())])) {
            return EMPTYSTRING;
        }
    }

    return temp;
}

template <typename T>
bool SmartGetWord<T>::checkSame(UChar32 l, UChar32 c) {
    // 验证两个字符是否都是数字或者都是英文
    if (isE(l) && isE(c)) {
        return true;
    }

    if (isNum(l) && isNum(c)) {
        return true;
    }

    return false;
}

template <typename T>
bool SmartGetWord<T>::isE(UChar32 c) const {
    // 判断是否为英文字母
    return (c >= 'A' && c <= 'z');
}

template <typename T>
bool SmartGetWord<T>::isNum(UChar32 c) const {
    // 判断是否为数字
    return (c >= '0' && c <= '9');
}

template <typename T>
std::vector<UChar32> SmartGetWord<T>::utf8_to_unicode(const std::string& utf8_str) {
    // 使用 ICU 将 UTF-8 字符串解码为 UChar32 数组
    std::vector<UChar32> result;
    const char* text_ptr = utf8_str.c_str();
    int32_t text_len = static_cast<int32_t>(utf8_str.length());
    int32_t i = 0;

    while (i < text_len) {
        UChar32 cp;
        U8_NEXT(text_ptr, i, text_len, cp);
        if (cp == U_SENTINEL) {
            // 无效的 UTF-8 序列，使用替换字符
            cp = 0xFFFD; // Unicode 替换字符
        }
        result.push_back(cp);
    }

    return result;
}

template <typename T>
std::string SmartGetWord<T>::unicode_to_utf8(const std::vector<UChar32>& unicode_chars, int start,
                                             int length) {
    // 将 UChar32 数组的子串编码为 UTF-8 字符串
    if (start < 0 || start >= static_cast<int>(unicode_chars.size()) || length <= 0) {
        return "";
    }

    int end = std::min(start + length, static_cast<int>(unicode_chars.size()));
    std::string result;

    for (int i = start; i < end; i++) {
        UChar32 cp = unicode_chars[i];
        char utf8_buffer[4];
        int32_t utf8_length = 0;

        U8_APPEND_UNSAFE(utf8_buffer, utf8_length, cp);
        result.append(utf8_buffer, utf8_length);
    }

    return result;
}

// 显式实例化模板
template class SmartGetWord<std::vector<std::string>>;

} // namespace doris::segment_v2::inverted_index