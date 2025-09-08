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

#pragma once

#include <string>
#include <vector>

namespace doris::segment_v2::inverted_index {

// C++ 版本的 PinyinAlphabetTokenizer，尽可能与 Java 版本保持一致：
// - 提供静态方法 walk(text) 返回分词结果
// - 内部实现 splitByNoletter、positiveMaxMatch、reverseMaxMatch
// - 依赖一个单例字典 PinyinAlphabetDict 来判定拼音片段是否命中
class PinyinAlphabetTokenizer {
public:
    // 与 Java: public static List<String> walk(String text) 等价
    static std::vector<std::string> walk(const std::string& text);

private:
    static std::vector<std::string> segPinyinStr(const std::string& content);
    static std::vector<std::string> splitByNoletter(const std::string& pinyin_str);
    static std::vector<std::string> positiveMaxMatch(const std::string& pinyin_text,
                                                     int max_length);
    static std::vector<std::string> reverseMaxMatch(const std::string& pinyin_text, int max_length);
};

// 单例字典，与 Java 的 PinyinAlphabetDict 对齐。
// 加载路径：be/dict/pinyin/pinyin_alphabet.dict
class PinyinAlphabetDict {
public:
    static PinyinAlphabetDict& instance();
    bool match(const std::string& token) const;

private:
    PinyinAlphabetDict();
    void load();

    // 简单的哈希集合；所有词条应为小写
    std::vector<std::string> _alphabet;
};

} // namespace doris::segment_v2::inverted_index
