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

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "unicode/uchar.h"
#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

// 前向声明
template <typename T>
class SmartGetWord;

/**
 * C++ 移植版本的 SmartForest<T>
 * 完全对应 Java 中的 org.nlpcn.commons.lang.tire.domain.SmartForest
 * 
 * 核心特性：
 * 1. 动态数组存储子节点，自动从二分查找切换到直接hash索引
 * 2. 支持状态合并：1-继续，2-词语可继续，3-确定结束
 * 3. 优化的内存和速度平衡，rate参数控制扩展阈值
 * 
 * 主要用于多音字字典的 Trie 树结构，支持快速前缀匹配
 */
template <typename T>
class SmartForest {
public:
    // 对应Java常量
    static constexpr int MAX_SIZE = 65536; // 0x10000，支持BMP平面所有字符

    // 节点状态：对应Java中的status值
    enum Status : uint8_t {
        CONTINUE = 1,      // 继续匹配（Java中的1）
        WORD_CONTINUE = 2, // 是词语但还可以继续（Java中的2）
        WORD_END = 3       // 确定是词语结束（Java中的3）
    };

public:
    // 对应Java的公有成员变量
    std::vector<std::unique_ptr<SmartForest<T>>> branches; // SmartForest<T>[] branches = null
    SmartForest<T>* branch = nullptr; // SmartForest<T> branch = null (临时查找结果)

public:
    // 构造函数们 - 对应Java的多个构造函数

    // 默认根节点构造函数: public SmartForest()
    SmartForest() : rate_(0.9), c_(0), status_(CONTINUE), param_() {}

    // 带rate的构造函数: public SmartForest(double rate)
    explicit SmartForest(double rate) : rate_(rate), c_(0), status_(CONTINUE), param_() {
        branches.resize(MAX_SIZE); // 对应 branches = new SmartForest[MAX_SIZE]
    }

    // 临时分支构造函数: private SmartForest(char c)
    explicit SmartForest(UChar32 c) : rate_(0.9), c_(c), status_(CONTINUE), param_() {}

    // 完整构造函数: public SmartForest(char c, int status, T param)
    SmartForest(UChar32 c, int status, T param)
            : rate_(0.9), c_(c), status_(static_cast<uint8_t>(status)), param_(std::move(param)) {}

    // 析构函数
    ~SmartForest() = default;

    // 核心方法 - 对应Java的核心功能

    /**
     * 增加子节点 - 对应Java的 public synchronized SmartForest<T> add(SmartForest<T> branch)
     * 这是Java中最复杂的方法，包含状态合并逻辑和动态扩展
     */
    SmartForest<T>* add(std::unique_ptr<SmartForest<T>> branch_node);

    /**
     * 获取索引 - 对应Java的 public int getIndex(char c)
     * 核心算法：二分查找 + 自动hash切换
     */
    int getIndex(UChar32 c);

    /**
     * 二分查找是否包含 - 对应Java的 public boolean contains(char c)
     */
    bool contains(UChar32 c);

    /**
     * 比较方法 - 对应Java的 public int compareTo(char c)
     */
    int compareTo(UChar32 c) const;

    /**
     * 相等比较 - 对应Java的 public boolean equals(char c)
     */
    bool equals(UChar32 c) const;

    // Getter/Setter方法 - 对应Java的访问器
    uint8_t getStatus() const { return status_; } // public byte getStatus()
    void setStatus(int status) {
        status_ = static_cast<uint8_t>(status);
    } // public void setStatus(int status)
    UChar32 getC() const { return c_; }          // public char getC()
    const T& getParam() const { return param_; } // public T getParam()
    T& getParam() { return param_; }
    void setParam(T param) { param_ = std::move(param); } // public void setParam(T param)

    // 便利方法们 - 对应Java的便利接口

    /**
     * 添加词汇到Trie树 - 对应Java的 public void add(String keyWord, T t)
     */
    void add(const std::string& keyWord, T t);

    /**
     * 添加分支的别名 - 对应Java的 public void addBranch(String keyWord, T t)
     */
    void addBranch(const std::string& keyWord, T t) { add(keyWord, std::move(t)); }

    /**
     * 获取子节点别名 - 对应Java的 public SmartForest<T> get(char c)
     */
    SmartForest<T>* get(UChar32 c) { return getBranch(c); }

    /**
     * 获取子节点 - 对应Java的 public SmartForest<T> getBranch(char c)
     */
    SmartForest<T>* getBranch(UChar32 c);

    /**
     * 根据词汇获取节点 - 对应Java的 public SmartForest<T> getBranch(String keyWord)
     */
    SmartForest<T>* getBranch(const std::string& keyWord);

    /**
     * 根据字符数组获取节点 - 对应Java的 public SmartForest<T> getBranch(char[] chars)
     */
    SmartForest<T>* getBranch(const std::vector<UChar32>& chars);

    /**
     * 获取分词器 - 对应Java的 public SmartGetWord<T> getWord(String str)
     */
    std::unique_ptr<SmartGetWord<T>> getWord(const std::string& str);

    /**
     * 获取分词器（字符数组版本） - 对应Java的 public SmartGetWord<T> getWord(char[] chars)
     */
    std::unique_ptr<SmartGetWord<T>> getWord(const std::vector<UChar32>& chars);

    /**
     * 获取所有分支 - 对应Java的 public SmartForest<T>[] getBranches()
     */
    const std::vector<std::unique_ptr<SmartForest<T>>>& getBranches() const { return branches; }

    /**
     * 删除词汇 - 对应Java的 public void remove(String word)
     */
    void remove(const std::string& word);

    /**
     * 清空树 - 对应Java的 public void clear()
     */
    void clear();

    /**
     * 转换为Map - 对应Java的 public Map<String, T> toMap()
     */
    std::map<std::string, T> toMap();

    // 调试：打印树结构（额外功能，Java中没有）
    void print(int depth = 0) const;

    // 比较器 - 对应Java的 Comparable<SmartForest<T>>
    bool operator<(const SmartForest<T>& other) const { return c_ < other.c_; }

    // 用于二分查找的比较
    struct CharComparator {
        bool operator()(const std::unique_ptr<SmartForest<T>>& a,
                        const std::unique_ptr<SmartForest<T>>& b) const {
            if (!a) return true;
            if (!b) return false;
            return a->getC() < b->getC();
        }

        bool operator()(const std::unique_ptr<SmartForest<T>>& a, UChar32 c) const {
            if (!a) return true;
            return a->getC() < c;
        }

        bool operator()(UChar32 c, const std::unique_ptr<SmartForest<T>>& a) const {
            if (!a) return false;
            return c < a->getC();
        }
    };

private:
    // 对应Java的私有成员变量
    double rate_;    // private double rate = 0.9;  内存和速度的平衡参数
    UChar32 c_;      // private char c;              当前字符
    uint8_t status_; // private byte status = 1;     状态
    T param_;        // private T param = null;      词典后的参数

    // 辅助方法
    /**
     * 递归构建Map - 对应Java的 private void putMap(HashMap<String, T> result, String pre, SmartForest<T>[] branches)
     */
    void putMap(std::map<std::string, T>& result, const std::string& pre,
                const std::vector<std::unique_ptr<SmartForest<T>>>& branches_vec);

    // UTF-8 <-> UChar32 转换工具
    static std::vector<UChar32> utf8_to_unicode(const std::string& utf8_str);
    static std::string unicode_to_utf8(const std::vector<UChar32>& unicode_chars);
    static std::string unicode_to_utf8(const std::vector<UChar32>& unicode_chars, int start,
                                       int length);
};

// 为多音字字典特化的类型别名
using PolyphoneForest = SmartForest<std::vector<std::string>>;

} // namespace doris::segment_v2::inverted_index