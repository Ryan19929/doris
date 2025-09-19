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

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "rune.h"
#include "unicode/uchar.h"
#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

// 前向声明
class SmartGetWord;

/**
 * C++ 移植版本的 SmartForest（重构为使用 vector<Rune>）
 * 基于Java原代码进行移植，专门为拼音分词优化
 * 
 * 核心特性：
 * 1. 动态数组存储子节点，自动从二分查找切换到直接hash索引
 * 2. 支持状态合并：1-继续，2-词语可继续，3-确定结束
 * 3. 使用vector<string>作为参数类型（多音字列表）
 * 4. 使用vector<Rune>作为输入，包含字符位置信息
 * 
 * 主要用于多音字字典的 Trie 树结构，支持快速前缀匹配
 */
class SmartForest {
public:
    // 参数类型（多音字列表）
    using ParamType = std::vector<std::string>;

    // 对应Java常量
    static constexpr int MAX_SIZE = 65536; // 0x10000，支持BMP平面所有字符

    // 节点状态：对应Java中的status值
    enum Status : uint8_t {
        CONTINUE = 1,      // 继续匹配（Java中的1）
        WORD_CONTINUE = 2, // 词语可继续（Java中的2）
        WORD_END = 3       // 确定是词语结束（Java中的3）
    };

public:
    // 对应Java的公有成员变量
    std::unordered_map<UChar32, std::unique_ptr<SmartForest>> branches; // 按需分配的分支映射
    SmartForest* branch = nullptr;                                      // 临时查找结果

public:
    // 构造函数们 - 对应Java的多个构造函数

    // 默认根节点构造函数: public SmartForest()
    SmartForest() : rate_(0.9), c_(0), status_(CONTINUE) {
        // 使用哈希表，无需预分配内存
    }

    // 根节点构造函数（带rate参数）: public SmartForest(double rate)
    explicit SmartForest(double rate) : rate_(rate), c_(0), status_(CONTINUE) {
        // 使用哈希表，无需预分配内存
    }

    // 字符构造函数: public SmartForest(char c)
    explicit SmartForest(UChar32 c) : rate_(0.9), c_(c), status_(CONTINUE) {
        // 使用哈希表，无需预分配内存
    }

    // 状态构造函数: public SmartForest(char c, byte status)
    SmartForest(UChar32 c, uint8_t status) : rate_(0.9), c_(c), status_(status) {
        // 使用哈希表，无需预分配内存
    }

    // 全参数构造函数: public SmartForest(char c, int status, T param)
    SmartForest(UChar32 c, uint8_t status, ParamType param)
            : rate_(0.9), c_(c), status_(status), param_(std::move(param)) {
        // 使用哈希表，无需预分配内存
    }

    /**
     * 增加子节点 - 对应Java的 public synchronized SmartForest<T> add(SmartForest<T> branch)
     * @param branch_node 要添加的子节点
     * @return 返回添加后的节点指针
     */
    SmartForest* add(std::unique_ptr<SmartForest> branch_node);

    /**
     * 向字典树中添加一个词汇及其参数（基于UTF-8字符串）
     * @param keyWord 要添加的词汇
     * @param param 词汇对应的参数
     */
    void add(const std::string& keyWord, const ParamType& param);

    /**
     * 向字典树中添加一个词汇及其参数（基于Rune向量）
     * @param runes Rune向量（包含字符和位置信息）
     * @param param 词汇对应的参数
     */
    void add(const std::vector<Rune>& runes, const ParamType& param);

    /**
     * 获取子节点别名 - 对应Java的 public SmartForest<T> get(char c)
     */
    SmartForest* get(UChar32 c) { return getBranch(c); }

    /**
     * 获取子节点 - 对应Java的 public SmartForest<T> getBranch(char c)
     */
    SmartForest* getBranch(UChar32 c);

    /**
     * 根据词汇获取节点 - 对应Java的 public SmartForest<T> getBranch(String keyWord)
     */
    SmartForest* getBranch(const std::string& keyWord);

    /**
     * 根据Rune向量获取节点
     */
    SmartForest* getBranch(const std::vector<Rune>& runes);

    /**
     * 获取分词器（基于UTF-8字符串）
     */
    std::unique_ptr<SmartGetWord> getWord(const std::string& str);

    /**
     * 获取分词器（基于Rune向量）
     */
    std::unique_ptr<SmartGetWord> getWord(const std::vector<Rune>& runes);

    /**
     * 获取所有分支
     */
    const std::unordered_map<UChar32, std::unique_ptr<SmartForest>>& getBranches() const {
        return branches;
    }

    /**
     * 移除一个词汇
     */
    void remove(const std::string& word);

    /**
     * 清空所有子节点
     */
    void clear();

    /**
     * 转换为Map格式 - 对应Java的 public HashMap<String, T> toMap()
     */
    std::map<std::string, ParamType> toMap();

    // Getter和Setter方法
    UChar32 getC() const { return c_; }
    void setC(UChar32 c) { c_ = c; }

    uint8_t getStatus() const { return status_; }
    void setStatus(uint8_t status) { status_ = status; }

    const ParamType& getParam() const { return param_; }
    void setParam(const ParamType& param) { param_ = param; }

    double getRate() const { return rate_; }
    void setRate(double rate) { rate_ = rate; }

    // 比较器 - 对应Java的 Comparable<SmartForest<T>>
    bool operator<(const SmartForest& other) const { return c_ < other.c_; }

    // 自定义比较器，用于二分查找
    struct Compare {
        bool operator()(const std::unique_ptr<SmartForest>& a,
                        const std::unique_ptr<SmartForest>& b) const {
            return a->c_ < b->c_;
        }
    };

    struct CompareChar {
        bool operator()(const std::unique_ptr<SmartForest>& a, UChar32 c) const {
            return a->c_ < c;
        }

        bool operator()(UChar32 c, const std::unique_ptr<SmartForest>& a) const {
            return c < a->c_;
        }
    };

    // 调试输出
    void print(int depth = 0) const;

private:
    // 对应Java的私有成员变量
    double rate_;     // private double rate = 0.9;  内存和速度的平衡参数
    UChar32 c_;       // private char c;              当前字符
    uint8_t status_;  // private byte status = 1;     状态
    ParamType param_; // 词典后的参数（多音字列表）

    // 辅助方法
    /**
     * 递归构建Map
     */
    void putMap(std::map<std::string, ParamType>& result, const std::string& pre,
                const std::unordered_map<UChar32, std::unique_ptr<SmartForest>>& branches_map);

    /**
     * 获取字符索引
     */
    int getIndex(UChar32 c);

    /**
     * 检查是否包含某个字符
     */
    bool contains(UChar32 c);

    /**
     * 比较当前节点字符与给定字符
     */
    int compareTo(UChar32 c) const;

    /**
     * 检查字符是否相等
     */
    bool equals(UChar32 c) const;

    /**
     * 将UTF-8字符串转换为Rune向量
     */
    static std::vector<Rune> utf8_to_runes(const std::string& utf8_str);

    /**
     * 将Rune向量转换为UTF-8字符串
     */
    static std::string runes_to_utf8(const std::vector<Rune>& runes);

    /**
     * 将Rune向量的一部分转换为UTF-8字符串
     */
    static std::string runes_to_utf8(const std::vector<Rune>& runes, size_t start, size_t end);
};

// 类型别名，兼容原有代码
using PolyphoneForest = SmartForest;

} // namespace doris::segment_v2::inverted_index
