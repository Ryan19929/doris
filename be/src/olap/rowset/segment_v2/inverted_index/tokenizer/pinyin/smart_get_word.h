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

#include "rune.h"
#include "unicode/uchar.h"
#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

// 前向声明
class SmartForest;

/**
 * C++ 移植版本的 SmartGetWord（重构为使用 vector<Rune>）
 * 基于Java原代码进行移植，专门为拼音分词优化
 * 
 * 核心功能：
 * 1. 最长匹配优先的分词算法
 * 2. 支持数字和英文的特殊处理
 * 3. 循环获取匹配的词汇和对应参数
 * 4. 使用vector<string>作为参数类型（多音字列表）
 * 5. 使用vector<Rune>作为输入，包含字符位置信息
 */
class SmartGetWord {
public:
    // 参数类型（多音字列表）
    using ParamType = std::vector<std::string>;

    // 特殊返回值常量
    static const std::string EMPTYSTRING; // 表示空字符串，继续搜索
    static const std::string NULL_RESULT; // 表示Java中的null，结束搜索

public:
    // 对应Java的公有成员变量
    int offe = 0; // 当前匹配词汇的起始偏移位置（字符位置，不是字节位置）

public:
    /**
     * 构造函数：基于字符串输入
     * @param forest SmartForest字典树
     * @param content 输入文本
     */
    SmartGetWord(SmartForest* forest, const std::string& content);

    /**
     * 构造函数：基于Rune向量输入
     * @param forest SmartForest字典树
     * @param runes Rune向量（包含字符和位置信息）
     */
    SmartGetWord(SmartForest* forest, const std::vector<Rune>& runes);

    /**
     * 获取下一个匹配的词汇 - 对应Java的 public String getFrontWords()
     * @return 匹配的词汇，如果没有更多匹配则返回NULL_RESULT
     */
    std::string getFrontWords();

    /**
     * 获取当前匹配词汇的参数 - 对应Java的 public T getParam()
     * @return 当前词汇对应的参数
     */
    const ParamType& getParam() const;

    /**
     * 重置搜索状态（基于字符串）
     * @param content 新的输入文本
     */
    void reset(const std::string& content);

    /**
     * 重置搜索状态（基于Rune向量）
     * @param runes 新的输入Rune向量
     */
    void reset(const std::vector<Rune>& runes);

    /**
     * 获取当前字符在原始文本中的字节偏移量
     */
    int getByteOffset() const;

    /**
     * 获取当前匹配词汇在原始文本中的字节起始位置
     */
    int getMatchStartByte() const;

    /**
     * 获取当前匹配词汇在原始文本中的字节结束位置
     */
    int getMatchEndByte() const;

    // 静态方法获取常量值
    static const std::string& getNullResult() { return NULL_RESULT; }

    static const std::string& getEmptyString() { return EMPTYSTRING; }

private:
    // 核心成员变量
    SmartForest* forest_;     // SmartForest字典树
    std::vector<Rune> runes_; // 输入的Rune向量
    SmartForest* branch_;     // 当前匹配分支

    // 搜索状态（对应Java中的成员变量）
    uint8_t status_ = 0;   // byte status = 0
    size_t root_ = 0;      // int root = 0
    size_t i_ = 0;         // int i = this.root
    bool is_back_ = false; // boolean isBack = false
    size_t temp_offe_ = 0; // int tempOffe
    ParamType param_;      // T param

    /**
     * 内部匹配逻辑 - 对应Java的 private String frontWords()
     */
    std::string frontWords();

    /**
     * 对应Java的 private String allWords()
     */
    std::string allWords();

    /**
     * 检查并处理数字和英文 - 对应Java的 private String checkNumberOrEnglish(String temp)
     */
    std::string checkNumberOrEnglish(const std::string& temp);

    /**
     * 检查字符是否相同（忽略大小写）
     */
    bool checkSame(UChar32 l, UChar32 c);

    /**
     * 检查是否为英文字母
     */
    bool isE(UChar32 c) const;

    /**
     * 检查是否为数字
     */
    bool isNum(UChar32 c) const;

    /**
     * 将UTF-8字符串转换为Rune向量
     */
    static std::vector<Rune> utf8_to_runes(const std::string& utf8_str);

    /**
     * 将Rune向量的一部分转换为UTF-8字符串
     */
    static std::string runes_to_utf8(const std::vector<Rune>& runes, size_t start, size_t end);
};

// 类型别名，兼容原有代码
using PolyphoneGetWord = SmartGetWord;

} // namespace doris::segment_v2::inverted_index
