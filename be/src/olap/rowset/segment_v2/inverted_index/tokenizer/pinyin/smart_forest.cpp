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

#include "smart_forest.h"

#include <algorithm>
#include <iostream>

#include "smart_get_word.h"
#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

// ===== UTF-8 <-> UChar32 转换工具 =====

template <typename T>
std::vector<UChar32> SmartForest<T>::utf8_to_unicode(const std::string& utf8_str) {
    std::vector<UChar32> result;
    const char* text_ptr = utf8_str.c_str();
    int32_t text_len = static_cast<int32_t>(utf8_str.length());
    int32_t i = 0;

    while (i < text_len) {
        UChar32 cp;
        U8_NEXT(text_ptr, i, text_len, cp);
        if (cp == U_SENTINEL) {
            cp = 0xFFFD; // Unicode 替换字符
        }
        result.push_back(cp);
    }
    return result;
}

template <typename T>
std::string SmartForest<T>::unicode_to_utf8(const std::vector<UChar32>& unicode_chars) {
    return unicode_to_utf8(unicode_chars, 0, static_cast<int>(unicode_chars.size()));
}

template <typename T>
std::string SmartForest<T>::unicode_to_utf8(const std::vector<UChar32>& unicode_chars, int start,
                                            int length) {
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

// ===== 核心算法实现 =====

template <typename T>
SmartForest<T>* SmartForest<T>::add(std::unique_ptr<SmartForest<T>> branch_node) {
    // 对应Java的 public synchronized SmartForest<T> add(SmartForest<T> branch)

    if (branches.empty()) {
        branches.resize(0); // 对应 branches = new SmartForest[0]
    }

    int bs = getIndex(branch_node->getC());

    if (bs >= 0) {
        // 找到现有位置
        // 先保存需要的信息，因为可能会移动branch_node
        uint8_t branch_status = branch_node->getStatus();
        T branch_param = branch_node->getParam();

        if (bs < static_cast<int>(branches.size()) && !branches[bs]) {
            branches[bs] = std::move(branch_node);
        }

        branch = branches[bs].get();

        // 状态合并逻辑 - 完全对应Java的switch语句
        switch (branch_status) {
        case static_cast<uint8_t>(-1): // case -1:
            branch->setStatus(1);
            break;
        case 1: // case 1:
            if (branch->getStatus() == 3) {
                branch->setStatus(2);
            }
            break;
        case 3: // case 3:
            if (branch->getStatus() != 3) {
                branch->setStatus(2);
            }
            branch->setParam(std::move(branch_param));
            break;
        }

        return branch;
    }

    if (bs < 0) {
        // 需要插入新元素
        UChar32 c = branch_node->getC(); // 保存字符，因为移动后无法访问
        SmartForest<T>* result_ptr = nullptr;

        // 自动扩展逻辑：如果接近最大值，切换为直接数组定位
        if (!branches.empty() && branches.size() >= MAX_SIZE * rate_) {
            // 对应Java的自动扩展为hash表
            std::vector<std::unique_ptr<SmartForest<T>>> tempBranches(MAX_SIZE);

            // 复制现有元素 - 对应Java的 tempBranches[b.getC()] = b
            for (auto& b : branches) {
                if (b) {
                    tempBranches[b->getC()] = std::move(b);
                }
            }

            // 插入新元素并保存指针
            result_ptr = branch_node.get();
            tempBranches[c] = std::move(branch_node);

            // 替换数组
            branches = std::move(tempBranches);
        } else {
            // 二分插入 - 对应Java的数组插入逻辑
            int insert = -(bs + 1);

            // 创建新数组
            std::vector<std::unique_ptr<SmartForest<T>>> newBranches(branches.size() + 1);

            // 复制前半部分 - 对应 System.arraycopy(this.branches, 0, newBranches, 0, insert)
            for (int i = 0; i < insert; i++) {
                newBranches[i] = std::move(branches[i]);
            }

            // 插入新元素并保存指针
            result_ptr = branch_node.get();
            newBranches[insert] = std::move(branch_node);

            // 复制后半部分 - 对应 System.arraycopy(branches, insert, newBranches, insert + 1, ...)
            for (int i = insert; i < static_cast<int>(branches.size()); i++) {
                newBranches[i + 1] = std::move(branches[i]);
            }

            // 替换数组
            branches = std::move(newBranches);
        }

        return result_ptr;
    }

    return nullptr; // 不应该到达这里
}

template <typename T>
int SmartForest<T>::getIndex(UChar32 c) {
    // 对应Java的 public int getIndex(char c)

    if (branches.empty()) {
        return -1; // 对应 if (branches == null) return -1
    }

    if (branches.size() == MAX_SIZE) {
        // 直接数组定位 - 对应 if (branches.length == MAX_SIZE) return c
        return static_cast<int>(c);
    }

    // 二分查找 - 对应 Arrays.binarySearch(this.branches, new SmartForest<T>(c))
    auto temp_node = std::make_unique<SmartForest<T>>(c);

    // 找到第一个非空元素的位置进行二分查找
    auto it = std::lower_bound(branches.begin(), branches.end(), temp_node, CharComparator());

    if (it != branches.end() && (*it) && (*it)->getC() == c) {
        return static_cast<int>(it - branches.begin());
    } else {
        // 返回插入位置的负数减1，对应Java的 binarySearch 行为
        return -static_cast<int>(it - branches.begin()) - 1;
    }
}

template <typename T>
bool SmartForest<T>::contains(UChar32 c) {
    // 对应Java的 public boolean contains(char c)
    if (branches.empty()) {
        return false;
    }
    return getIndex(c) >= 0; // 简化版：直接使用getIndex
}

template <typename T>
int SmartForest<T>::compareTo(UChar32 c) const {
    // 对应Java的 public int compareTo(char c)
    if (c_ > c) return 1;
    if (c_ < c) return -1;
    return 0;
}

template <typename T>
bool SmartForest<T>::equals(UChar32 c) const {
    // 对应Java的 public boolean equals(char c)
    return c_ == c;
}

// ===== 便利方法实现 =====

template <typename T>
void SmartForest<T>::add(const std::string& keyWord, T t) {
    // 对应Java的 public void add(String keyWord, T t)

    SmartForest<T>* tempBranch = this;
    auto chars = utf8_to_unicode(keyWord);

    for (size_t i = 0; i < chars.size(); i++) {
        if (i == chars.size() - 1) {
            // 最后一个字符 - 对应 keyWord.length() == i + 1
            auto new_node = std::make_unique<SmartForest<T>>(chars[i], 3, std::move(t));
            tempBranch->add(std::move(new_node));
        } else {
            // 中间字符
            auto new_node = std::make_unique<SmartForest<T>>(chars[i], 1, T {});
            tempBranch->add(std::move(new_node));
        }

        // 移动到下一个节点 - 对应 tempBranch = tempBranch.branches[tempBranch.getIndex(...)]
        int index = tempBranch->getIndex(chars[i]);
        if (index >= 0 && index < static_cast<int>(tempBranch->branches.size())) {
            tempBranch = tempBranch->branches[index].get();
        }
    }
}

template <typename T>
SmartForest<T>* SmartForest<T>::getBranch(UChar32 c) {
    // 对应Java的 public SmartForest<T> getBranch(char c)
    int index = getIndex(c);
    if (index < 0) {
        return nullptr;
    } else {
        return (index < static_cast<int>(branches.size())) ? branches[index].get() : nullptr;
    }
}

template <typename T>
SmartForest<T>* SmartForest<T>::getBranch(const std::string& keyWord) {
    // 对应Java的 public SmartForest<T> getBranch(String keyWord)

    SmartForest<T>* tempBranch = this;
    auto chars = utf8_to_unicode(keyWord);

    for (UChar32 c : chars) {
        int index = tempBranch->getIndex(c);
        if (index < 0) {
            return nullptr;
        }

        if (index >= static_cast<int>(tempBranch->branches.size()) ||
            !tempBranch->branches[index]) {
            return nullptr;
        }

        tempBranch = tempBranch->branches[index].get();
    }

    return tempBranch;
}

template <typename T>
SmartForest<T>* SmartForest<T>::getBranch(const std::vector<UChar32>& chars) {
    // 对应Java的 public SmartForest<T> getBranch(char[] chars)

    SmartForest<T>* tempBranch = this;

    for (UChar32 c : chars) {
        int index = tempBranch->getIndex(c);
        if (index < 0) {
            return nullptr;
        }

        if (index >= static_cast<int>(tempBranch->branches.size()) ||
            !tempBranch->branches[index]) {
            return nullptr;
        }

        tempBranch = tempBranch->branches[index].get();
    }

    return tempBranch;
}

template <typename T>
std::unique_ptr<SmartGetWord<T>> SmartForest<T>::getWord(const std::string& str) {
    // 对应Java的 public SmartGetWord<T> getWord(String str)
    return std::make_unique<SmartGetWord<T>>(this, str);
}

template <typename T>
std::unique_ptr<SmartGetWord<T>> SmartForest<T>::getWord(const std::vector<UChar32>& chars) {
    // 对应Java的 public SmartGetWord<T> getWord(char[] chars)
    return std::make_unique<SmartGetWord<T>>(this, chars);
}

template <typename T>
void SmartForest<T>::remove(const std::string& word) {
    // 对应Java的 public void remove(String word)
    SmartForest<T>* node = getBranch(word);
    if (node) {
        node->status_ = 1;   // 对应 .status = 1
        node->param_ = T {}; // 对应 .param = null
    }
}

template <typename T>
void SmartForest<T>::clear() {
    // 对应Java的 public void clear()
    branches.clear();
    branches.resize(MAX_SIZE); // 对应 branches = new SmartForest[MAX_SIZE]
}

template <typename T>
std::map<std::string, T> SmartForest<T>::toMap() {
    // 对应Java的 public Map<String, T> toMap()

    std::map<std::string, T> result;

    if (branches.empty()) {
        return result;
    }

    putMap(result, "", branches);
    return result;
}

template <typename T>
void SmartForest<T>::putMap(std::map<std::string, T>& result, const std::string& pre,
                            const std::vector<std::unique_ptr<SmartForest<T>>>& branches_vec) {
    // 对应Java的 private void putMap(HashMap<String, T> result, String pre, SmartForest<T>[] branches)

    for (const auto& sf : branches_vec) {
        if (!sf) {
            continue;
        }

        std::string key = pre + unicode_to_utf8({sf->c_});

        if (sf->getStatus() == 3) {
            // 对应 if (branches[i].getStatus() == 3)
            result[key] = sf->getParam();
        } else if (sf->getStatus() == 2) {
            // 对应 else if (branches[i].getStatus() == 2)
            result[key] = sf->getParam();
            putMap(result, key, sf->branches);
        } else {
            // 对应 else
            putMap(result, key, sf->branches);
        }
    }
}

// ===== 调试功能 =====

template <typename T>
void SmartForest<T>::print(int depth) const {
    for (int i = 0; i < depth; i++) {
        std::cout << "  ";
    }

    if (c_ == 0) {
        std::cout << "ROOT";
    } else {
        std::cout << "U+" << std::hex << c_ << std::dec;
    }

    std::cout << " (status=" << static_cast<int>(status_) << ")";

    if (status_ == WORD_CONTINUE || status_ == WORD_END) {
        std::cout << " [HAS_PARAM]";
    }

    std::cout << std::endl;

    for (const auto& child : branches) {
        if (child) {
            child->print(depth + 1);
        }
    }
}

// 显式实例化模板
template class SmartForest<std::vector<std::string>>;

} // namespace doris::segment_v2::inverted_index