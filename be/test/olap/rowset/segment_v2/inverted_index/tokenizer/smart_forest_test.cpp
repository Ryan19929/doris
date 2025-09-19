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

#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/smart_forest.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <vector>

#include "common/config.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/smart_get_word.h"

namespace doris::segment_v2::inverted_index {

/**
 * C++ 版本的前缀树单元测试
 * 基于Java的SmartGetWordTest.java移植
 */
class SmartForestTest : public ::testing::Test {
protected:
    std::string original_dict_path_; // 保存原始字典路径以便恢复
    void SetUp() override {
        std::cout << "🔧 初始化 SmartForest 测试..." << std::endl;
        // 保存原始配置值以便在TearDown中恢复
        original_dict_path_ = config::inverted_index_dict_path;
        std::cout << "📍 原始 inverted_index_dict_path: " << original_dict_path_ << std::endl;

        const char* doris_home = std::getenv("DORIS_HOME");
        config::inverted_index_dict_path = std::string(doris_home) + "/../../dict";
        std::cout << "✅ 设置 inverted_index_dict_path (相对路径): "
                  << config::inverted_index_dict_path << std::endl;
    }

    void TearDown() override {
        std::cout << "🧹 清理 SmartForest 测试..." << std::endl;
        config::inverted_index_dict_path = original_dict_path_;
    }
};

/**
 * 测试 SmartGetWord 的基本前向匹配功能
 * 对应 Java SmartGetWordTest.test()
 */
TEST_F(SmartForestTest, TestSmartGetWordBasic) {
    std::cout << "🧪 测试 SmartGetWord 基本功能..." << std::endl;

    // 创建词典 - 对应Java代码
    auto forest = std::make_unique<SmartForest>();

    // 添加词汇 - 对应Java的forest.add()
    forest->add("中国", {"zhong1", "guo2"});
    forest->add("android", {"android"});
    forest->add("java", {"java"});
    forest->add("中国人", {"zhong1", "guo2", "ren2"});

    std::string content = " Android-java-中国人";

    // 移除词汇 - 对应Java的forest.remove()
    forest->remove("中国人");

    std::cout << "📝 测试内容: '" << content << "'" << std::endl;

    // 转换为小写并获取分词器 - 对应Java的content.toLowerCase()
    std::transform(content.begin(), content.end(), content.begin(), ::tolower);
    auto word_getter = forest->getWord(content);

    std::vector<std::string> expected_words = {"android", "java", "中国"};
    std::vector<std::string> actual_words;

    // 获取前向匹配的词汇 - 对应Java的while循环
    std::string temp;
    while ((temp = word_getter->getFrontWords()) != word_getter->getNullResult()) {
        auto param = word_getter->getParam();
        std::cout << "🔍 找到词汇: '" << temp << "' 参数: [";
        for (size_t i = 0; i < param.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << param[i];
        }
        std::cout << "]" << std::endl;
        actual_words.push_back(temp);
    }

    // 验证结果
    ASSERT_EQ(expected_words.size(), actual_words.size())
            << "词汇数量不匹配，期望: " << expected_words.size()
            << ", 实际: " << actual_words.size();

    for (size_t i = 0; i < expected_words.size(); i++) {
        EXPECT_EQ(expected_words[i], actual_words[i])
                << "第" << i << "个词汇不匹配，期望: '" << expected_words[i] << "', 实际: '"
                << actual_words[i] << "'";
    }

    std::cout << "✅ SmartGetWord 基本功能测试通过" << std::endl;
}

} // namespace doris::segment_v2::inverted_index
