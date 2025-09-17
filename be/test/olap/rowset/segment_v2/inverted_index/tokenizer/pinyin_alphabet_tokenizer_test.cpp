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

#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_alphabet_tokenizer.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "common/config.h"

namespace doris::segment_v2::inverted_index {

class PinyinAlphabetTokenizerTest : public ::testing::Test {
protected:
    std::string original_dict_path_; // 保存原始字典路径以便恢复
    void SetUp() override {
        // 直接设置config::inverted_index_dict_path为正确的路径
        // 这是最直接和可靠的方法，不依赖环境变量
        std::cout << "🔧 设置字典配置路径..." << std::endl;

        // 保存原始配置值以便在TearDown中恢复
        original_dict_path_ = config::inverted_index_dict_path;
        std::cout << "📍 原始 inverted_index_dict_path: " << original_dict_path_ << std::endl;

        // 使用相对路径设置字典目录
        // 测试二进制在 ut_build_ASAN/test/ 下，字典在 be/dict/ 下
        // 相对路径：../../dict
        const char* doris_home = std::getenv("DORIS_HOME");
        config::inverted_index_dict_path = std::string(doris_home) + "/../../dict";
        std::cout << "✅ 设置 inverted_index_dict_path (相对路径): "
                  << config::inverted_index_dict_path << std::endl;

        // 验证字典文件是否存在
        std::string expected_dict_path =
                config::inverted_index_dict_path + "/pinyin/pinyin_alphabet.dict";
        std::ifstream dict_test(expected_dict_path);
        if (dict_test.is_open()) {
            std::cout << "✅ 字典文件存在: " << expected_dict_path << std::endl;
            dict_test.close();
        } else {
            std::cout << "❌ 字典文件不存在: " << expected_dict_path << std::endl;
        }

        // 确保字典加载
        std::cout << "📚 初始化 PinyinAlphabetDict..." << std::endl;
        PinyinAlphabetDict::instance();
        std::cout << "✅ PinyinAlphabetDict 初始化完成" << std::endl;
    }

    void TearDown() override {
        // 恢复原始配置
        config::inverted_index_dict_path = original_dict_path_;
        std::cout << "🔄 恢复原始 inverted_index_dict_path: " << original_dict_path_ << std::endl;
    }

    // 辅助方法：将vector转换为字符串进行比较（类似Java的Arrays.asList().toString()）
    std::string vectorToString(const std::vector<std::string>& vec) {
        if (vec.empty()) {
            return "[]";
        }

        std::string result = "[";
        for (size_t i = 0; i < vec.size(); ++i) {
            if (i > 0) {
                result += ", ";
            }
            result += vec[i];
        }
        result += "]";
        return result;
    }

    // 辅助方法：期望结果和实际结果比较
    void assertTokensEqual(const std::vector<std::string>& expected,
                           const std::vector<std::string>& actual, const std::string& input) {
        // 添加详细日志输出
        std::cout << "\n=== 测试详情 ===" << std::endl;
        std::cout << "Input: '" << input << "'" << std::endl;
        std::cout << "Expected: " << vectorToString(expected) << " (size=" << expected.size() << ")"
                  << std::endl;
        std::cout << "Actual:   " << vectorToString(actual) << " (size=" << actual.size() << ")"
                  << std::endl;

        if (expected.size() != actual.size()) {
            std::cout << "❌ SIZE MISMATCH!" << std::endl;
        } else {
            std::cout << "✅ Size matches" << std::endl;
        }

        // 逐个比较token
        for (size_t i = 0; i < std::max(expected.size(), actual.size()); ++i) {
            std::string exp = (i < expected.size()) ? expected[i] : "<MISSING>";
            std::string act = (i < actual.size()) ? actual[i] : "<MISSING>";

            if (i < expected.size() && i < actual.size() && expected[i] == actual[i]) {
                std::cout << "  [" << i << "] ✅ '" << exp << "'" << std::endl;
            } else {
                std::cout << "  [" << i << "] ❌ Expected: '" << exp << "', Actual: '" << act << "'"
                          << std::endl;
            }
        }
        std::cout << "==================" << std::endl;

        EXPECT_EQ(expected.size(), actual.size())
                << "Token count mismatch for input: '" << input << "'\n"
                << "Expected: " << vectorToString(expected) << "\n"
                << "Actual: " << vectorToString(actual);

        for (size_t i = 0; i < std::min(expected.size(), actual.size()); ++i) {
            EXPECT_EQ(expected[i], actual[i])
                    << "Token mismatch at position " << i << " for input: '" << input << "'\n"
                    << "Expected: " << vectorToString(expected) << "\n"
                    << "Actual: " << vectorToString(actual);
        }
    }
};

// 测试用例1：简单单个拼音
TEST_F(PinyinAlphabetTokenizerTest, TestSinglePinyin) {
    std::cout << "\n🧪 TestSinglePinyin 开始..." << std::endl;
    // Java测试：Assert.assertEquals(Arrays.asList("xian").toString(), PinyinAlphabetTokenizer.walk("xian").toString());
    std::string input = "xian";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"xian"};

    assertTokensEqual(expected, result, input);
}

// 测试用例2：连续拼音分词
TEST_F(PinyinAlphabetTokenizerTest, TestContinuousPinyin) {
    std::cout << "\n🧪 TestContinuousPinyin 开始..." << std::endl;
    // Java测试：Assert.assertEquals(Arrays.asList("wo", "shi", "liang").toString(),
    //                PinyinAlphabetTokenizer.walk("woshiliang").toString());
    std::string input = "woshiliang";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"wo", "shi", "liang"};

    assertTokensEqual(expected, result, input);
}

// 测试用例3：长拼音串分词
TEST_F(PinyinAlphabetTokenizerTest, TestLongPinyinString) {
    std::cout << "\n🧪 TestLongPinyinString 开始..." << std::endl;
    // Java测试：Assert.assertEquals(Arrays.asList("zhong", "hua", "ren", "min", "gong", "he", "guo").toString(),
    //                PinyinAlphabetTokenizer.walk("zhonghuarenmingongheguo").toString());
    std::string input = "zhonghuarenmingongheguo";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"zhong", "hua", "ren", "min", "gong", "he", "guo"};

    assertTokensEqual(expected, result, input);
}

// 测试用例4：包含数字的混合字符串
TEST_F(PinyinAlphabetTokenizerTest, TestMixedWithNumbers) {
    std::cout << "\n🧪 TestMixedWithNumbers 开始..." << std::endl;
    // Java测试：Assert.assertEquals(
    //                Arrays.asList("5", "zhong", "hua", "ren", "89", "min", "gong", "he", "guo", "234").toString(),
    //                PinyinAlphabetTokenizer.walk("5zhonghuaren89mingongheguo234").toString());
    std::string input = "5zhonghuaren89mingongheguo234";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"5",   "zhong", "hua", "ren", "89",
                                         "min", "gong",  "he",  "guo", "234"};

    assertTokensEqual(expected, result, input);
}

// 测试用例5：空字符串
TEST_F(PinyinAlphabetTokenizerTest, TestEmptyString) {
    std::string input = "";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {};

    assertTokensEqual(expected, result, input);
}

// 测试用例6：只有数字
TEST_F(PinyinAlphabetTokenizerTest, TestOnlyNumbers) {
    std::string input = "12345";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"12345"};

    assertTokensEqual(expected, result, input);
}

// 测试用例7：大小写处理
TEST_F(PinyinAlphabetTokenizerTest, TestCaseHandling) {
    std::cout << "\n🧪 TestCaseHandling 开始..." << std::endl;
    std::string input = "WoShiLiang";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"wo", "shi", "liang"};

    assertTokensEqual(expected, result, input);
}

// 测试用例8：包含特殊字符
TEST_F(PinyinAlphabetTokenizerTest, TestWithSpecialCharacters) {
    std::cout << "\n🧪 TestWithSpecialCharacters 开始..." << std::endl;
    std::string input = "wo-shi_liang.txt";
    auto result = PinyinAlphabetTokenizer::walk(input);
    // 期望按非字母分割
    std::vector<std::string> expected = {"wo", "-", "shi", "_", "liang", ".", "t", "x", "t"};

    assertTokensEqual(expected, result, input);
}

// 测试用例9：单字符
TEST_F(PinyinAlphabetTokenizerTest, TestSingleCharacter) {
    std::string input = "a";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"a"};

    assertTokensEqual(expected, result, input);
}

// 测试用例10：复杂混合案例
TEST_F(PinyinAlphabetTokenizerTest, TestComplexMixed) {
    std::string input = "hello123world-ni456hao";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"he", "l", "lo", "123", "wo",  "r",
                                         "l",  "d", "-",  "ni",  "456", "hao"};
    // 输出实际结果用于调试
    assertTokensEqual(expected, result, input);
}

} // namespace doris::segment_v2::inverted_index
