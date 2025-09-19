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

#include <iostream>

#include "common/logging.h"
#include "smart_get_word.h"
#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

std::vector<Rune> SmartForest::utf8_to_runes(const std::string& utf8_str) {
    std::vector<Rune> runes;
    runes.reserve(utf8_str.length()); // 预分配空间

    int32_t byte_pos = 0;
    const char* str_ptr = utf8_str.c_str();
    int32_t str_length = static_cast<int32_t>(utf8_str.length());

    while (byte_pos < str_length) {
        UChar32 cp;
        int32_t byte_start = byte_pos;

        // 使用ICU的UTF-8解码
        U8_NEXT(str_ptr, byte_pos, str_length, cp);

        if (cp >= 0) { // 有效的Unicode码点
            runes.emplace_back(byte_start, byte_pos, cp);
        }
    }

    return runes;
}

std::string SmartForest::runes_to_utf8(const std::vector<Rune>& runes) {
    return runes_to_utf8(runes, 0, runes.size());
}

std::string SmartForest::runes_to_utf8(const std::vector<Rune>& runes, size_t start, size_t end) {
    std::string result;
    for (size_t i = start; i < end && i < runes.size(); ++i) {
        UChar32 cp = runes[i].cp;
        char utf8_buffer[4];
        int32_t utf8_len = 0;
        U8_APPEND_UNSAFE(utf8_buffer, utf8_len, cp);
        result.append(utf8_buffer, utf8_len);
    }
    return result;
}

SmartForest* SmartForest::add(std::unique_ptr<SmartForest> branch_node) {
    // 对应Java的 public synchronized SmartForest<T> add(SmartForest<T> branch)
    if (!branch_node) {
        return nullptr;
    }

    UChar32 c = branch_node->getC();
    SmartForest* result_ptr = nullptr;

    // 检查是否已存在该字符的分支
    auto it = branches.find(c);
    if (it != branches.end()) {
        // 节点已存在，需要合并状态
        SmartForest* existing = it->second.get();
        uint8_t new_status = branch_node->getStatus();
        uint8_t existing_status = existing->getStatus();

        // 状态合并逻辑 - 完全对应Java版本的switch语句
        switch (new_status) {
        case CONTINUE:                              // case 1: 新节点是CONTINUE
            if (existing_status == WORD_END) {      // 现有节点是WORD_END
                existing->setStatus(WORD_CONTINUE); // 改为WORD_CONTINUE
            }
            break;
        case WORD_END:                              // case 3: 新节点是WORD_END
            if (existing_status != WORD_END) {      // 现有节点不是WORD_END
                existing->setStatus(WORD_CONTINUE); // 改为WORD_CONTINUE
            }
            // Java版本：总是设置参数
            existing->setParam(branch_node->getParam());
            break;
        default:
            // 其他状态保持现有逻辑
            break;
        }

        result_ptr = existing;
    } else {
        // 节点不存在，直接插入
        result_ptr = branch_node.get();
        branches[c] = std::move(branch_node);
    }

    return result_ptr;
}

int SmartForest::getIndex(UChar32 c) {
    // 使用哈希表，不再需要索引概念
    // 简单返回字符是否存在
    return branches.find(c) != branches.end() ? static_cast<int>(c) : -1;
}

bool SmartForest::contains(UChar32 c) {
    return branches.find(c) != branches.end();
}

int SmartForest::compareTo(UChar32 c) const {
    if (c_ < c) return -1;
    if (c_ > c) return 1;
    return 0;
}

bool SmartForest::equals(UChar32 c) const {
    return c_ == c;
}

void SmartForest::add(const std::string& keyWord, const ParamType& param) {
    // 转换为Rune向量后调用Rune版本
    std::vector<Rune> runes = utf8_to_runes(keyWord);
    add(runes, param);
}

void SmartForest::add(const std::vector<Rune>& runes, const ParamType& param) {
    // 对应Java的 public void add(String keyWord, T t)
    // 移除调试输出以提高性能

    SmartForest* tempBranch = this;

    for (size_t i = 0; i < runes.size(); i++) {
        UChar32 cp = runes[i].cp;
        // 处理字符

        if (i == runes.size() - 1) {
            // 最后一个字符，标记为词语结束
            auto new_node = std::make_unique<SmartForest>(cp, WORD_END, param);
            tempBranch->add(std::move(new_node));
        } else {
            // 中间字符，标记为继续
            auto new_node = std::make_unique<SmartForest>(cp, CONTINUE, ParamType {});
            tempBranch->add(std::move(new_node));
        }

        // 关键修复：完全对应Java版本逻辑
        // Java: tempBranch = tempBranch.branches[tempBranch.getIndex(keyWord.charAt(i))];
        tempBranch = tempBranch->getBranch(cp);

        // 安全检查：如果getBranch返回nullptr，说明添加失败
        if (!tempBranch) {
            break;
        }
    }
    // 添加完成
}

SmartForest* SmartForest::getBranch(UChar32 c) {
    // 对应Java的 public SmartForest<T> getBranch(char c)
    auto it = branches.find(c);
    return it != branches.end() ? it->second.get() : nullptr;
}

SmartForest* SmartForest::getBranch(const std::string& keyWord) {
    // 对应Java的 public SmartForest<T> getBranch(String keyWord)
    std::vector<Rune> runes = utf8_to_runes(keyWord);
    return getBranch(runes);
}

SmartForest* SmartForest::getBranch(const std::vector<Rune>& runes) {
    // 对应Java的 public SmartForest<T> getBranch(char[] chars)
    SmartForest* tempBranch = this;

    for (const auto& rune : runes) {
        if (!tempBranch) {
            return nullptr;
        }
        tempBranch = tempBranch->getBranch(rune.cp);
    }

    return tempBranch;
}

std::unique_ptr<SmartGetWord> SmartForest::getWord(const std::string& str) {
    // 对应Java的 public SmartGetWord<T> getWord(String str)
    return std::make_unique<SmartGetWord>(this, str);
}

std::unique_ptr<SmartGetWord> SmartForest::getWord(const std::vector<Rune>& runes) {
    // 对应Java的 public SmartGetWord<T> getWord(char[] chars)
    return std::make_unique<SmartGetWord>(this, runes);
}

void SmartForest::remove(const std::string& word) {
    // 简化实现：将节点标记为CONTINUE状态，清空参数
    SmartForest* node = getBranch(word);
    if (node) {
        node->setStatus(CONTINUE);
        node->setParam(ParamType {});
    }
}

void SmartForest::clear() {
    // 清空所有子节点
    branches.clear();
    branch = nullptr;
}

std::map<std::string, SmartForest::ParamType> SmartForest::toMap() {
    std::map<std::string, ParamType> result;
    putMap(result, "", branches);
    return result;
}

void SmartForest::putMap(
        std::map<std::string, ParamType>& result, const std::string& pre,
        const std::unordered_map<UChar32, std::unique_ptr<SmartForest>>& branches_map) {
    // 对应Java的 private void putMap(HashMap<String, T> result, String pre, SmartForest<T>[] branches)
    for (const auto& [c, branch] : branches_map) {
        if (!branch) continue;

        std::string current_word = pre;
        char utf8_buffer[4];
        int32_t utf8_len = 0;
        U8_APPEND_UNSAFE(utf8_buffer, utf8_len, branch->getC());
        current_word.append(utf8_buffer, utf8_len);

        if (branch->getStatus() == WORD_END || branch->getStatus() == WORD_CONTINUE) {
            result[current_word] = branch->getParam();
        }

        if (!branch->getBranches().empty()) {
            putMap(result, current_word, branch->getBranches());
        }
    }
}

void SmartForest::print(int depth) const {
    // 调试输出，打印树结构
    std::string indent(depth * 2, ' ');
    char utf8_buffer[4];
    int32_t utf8_len = 0;
    U8_APPEND_UNSAFE(utf8_buffer, utf8_len, c_);
    std::string char_str(utf8_buffer, utf8_len);

    std::cout << indent << "Node: '" << char_str << "' status=" << static_cast<int>(status_)
              << " param_size=" << param_.size() << std::endl;

    for (const auto& [c, branch] : branches) {
        if (branch) {
            branch->print(depth + 1);
        }
    }
}

} // namespace doris::segment_v2::inverted_index
