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

#include "smart_forest.h"
#include "unicode/uchar.h"
#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

/**
 * C++ 移植版本的 SmartGetWord<T>
 * 对应 Java 中的 org.nlpcn.commons.lang.tire.SmartGetWord
 * 
 * 用于在 SmartForest Trie 树中进行最长前向匹配查找
 * 主要用于多音字词典中找到最长匹配的词汇
 * 
 * 成员变量和算法与Java版本完全一致
 */
template <typename T>
class SmartGetWord {
public:
    // Java中的public成员变量
    int offe = 0; // 当前匹配词汇的起始偏移位置

public:
    // 构造函数
    SmartGetWord(SmartForest<T>* forest, const std::string& content);
    SmartGetWord(SmartForest<T>* forest, const std::vector<UChar32>& chars);

    // 主要接口方法（对应Java中使用的方法）
    std::string getFrontWords();            // 获取前向匹配的词汇（主要使用）
    T getParam() const;                     // 获取当前匹配词汇的参数
    void reset(const std::string& content); // 重置搜索状态

    // 公共常量，方便外部访问
    static const std::string EMPTYSTRING;
    static const std::string NULL_RESULT; // 表示Java中的null
    
    // 静态方法获取常量值，解决跨编译单元链接问题
    static const std::string& getNullResult();

private:
    // 对应Java的私有成员变量
    uint8_t status_ = 0;         // byte status
    int root_ = 0;               // int root
    int i_ = 0;                  // int i = this.root
    bool isBack_ = false;        // boolean isBack
    SmartForest<T>* forest_;     // SmartForest<T> forest
    std::vector<UChar32> chars_; // char[] chars (使用UChar32代替char)
    std::string str_;            // String str
    int tempOffe_ = 0;           // int tempOffe
    T param_;                    // T param
    SmartForest<T>* branch_;     // SmartForest<T> branch

    // 私有方法（对应Java的私有方法）
    std::string frontWords();                                  // 前向匹配算法核心实现
    std::string checkNumberOrEnglish(const std::string& temp); // 验证词语左右边界
    bool checkSame(UChar32 l, UChar32 c);                      // 验证字符是否同类型
    bool isE(UChar32 c) const;                                 // 判断是否为英文字母
    bool isNum(UChar32 c) const;                               // 判断是否为数字

    // UTF-8 字符串解码为 UChar32 数组
    static std::vector<UChar32> utf8_to_unicode(const std::string& utf8_str);
    // UChar32 数组编码为 UTF-8 字符串
    static std::string unicode_to_utf8(const std::vector<UChar32>& unicode_chars, int start,
                                       int length);
};

// 为多音字字典特化的类型别名
using PolyphoneGetWord = SmartGetWord<std::vector<std::string>>;

} // namespace doris::segment_v2::inverted_index
