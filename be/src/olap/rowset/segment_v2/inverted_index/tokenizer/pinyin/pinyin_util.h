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

#include <memory>
#include <string>
#include <vector>

#include "pinyin_format.h"
#include "smart_forest.h"
#include "unicode/uchar.h"

namespace doris::segment_v2::inverted_index {

// PinyinUtil：支持单字拼音（pinyin.txt）+ 多音字短语（polyphone.txt）
// 与 Java 行为对齐：
// - 单字拼音：U+4E00..U+9FA5 范围内时，从表中取出拼音串，若有多个以逗号分隔，取第一个。
// - 多音字处理：使用 SmartForest Trie 树进行最长匹配，如 "长江" -> ["chang", "jiang"]
// - 其他字符返回空字符串。
class PinyinUtil {
public:
    static PinyinUtil& instance();

    // 输入 Unicode 码点（UTF-32 / UChar32 语义），返回第一拼音，若无则返回空串
    // 仅对 BMP 范围内的中文 [0x4E00, 0x9FA5] 有效，其它返回空串
    std::string to_pinyin(uint32_t cp) const;

    // 多音字转换：输入文本，返回按字符索引的拼音列表（无声调格式）
    // 对应 Java 的 PinyinUtil.INSTANCE.convert(str, PinyinFormat.TONELESS_PINYIN_FORMAT)
    // 注意：返回的列表长度等于输入字符串的字符数，每个位置对应一个字符的拼音
    // 例如："长江a" -> ["chang", "jiang", ""] (非中文字符返回空字符串)
    std::vector<std::string> convert(const std::string& text) const;

    // 多音字转换：输入文本和格式，返回按字符索引的格式化拼音列表
    // 对应 Java 的 PinyinUtil.INSTANCE.convert(str, format)
    // 支持各种格式：带/无声调、Unicode标记、首字母缩写等
    // 例如：convert("长江", PinyinFormat::DEFAULT_PINYIN_FORMAT) -> ["chang2", "jiang1"]
    std::vector<std::string> convert(const std::string& text, const PinyinFormat& format) const;

    // 直接接受 Unicode 码点向量的转换方法，更高效，避免重复UTF-8解码
    // @param codepoints Unicode 码点向量
    // @param format 输出格式
    // @return 按码点索引的格式化拼音列表，长度等于 codepoints.size()
    std::vector<std::string> convert(const std::vector<UChar32>& codepoints,
                                     const PinyinFormat& format) const;

    // 动态增加拼音到多音字词典
    // 对应 Java 的 PinyinUtil.INSTANCE.insertPinyin(word, pinyins)
    // @param word 词汇，如 "大长今"
    // @param pinyins 拼音数组，如 ["da4", "chang2", "jing1"]
    void insertPinyin(const std::string& word, const std::vector<std::string>& pinyins);

    // 获取多音字字典（用于测试）
    PolyphoneForest* getPolyphoneDict() { return polyphone_dict_.get(); }

private:
    PinyinUtil();
    void load_pinyin_mapping();
    void load_polyphone_mapping();

    // 内部方法：获取原始拼音（带声调数字）
    std::vector<std::string> convert_with_raw_pinyin(const std::string& text) const;

    // 内部方法：单字符原始拼音转换（带声调数字）
    std::string to_raw_pinyin(uint32_t cp) const;

    std::vector<std::string> _pinyin_dict;            // index: ch-0x4E00，单字拼音字典
    std::unique_ptr<PolyphoneForest> polyphone_dict_; // 多音字 Trie 字典
    int max_polyphone_len_;                           // 多音字词汇的最大长度
};

} // namespace doris::segment_v2::inverted_index
