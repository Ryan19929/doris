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

#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_util.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_format.h"

namespace doris::segment_v2::inverted_index {

/**
 * C++ 版本的拼音工具单元测试
 * 基于Java的PinyinTest.java移植
 */
class PinyinUtilTest : public ::testing::Test {
protected:
    std::string original_dict_path_; // 保存原始字典路径以便恢复

    // 测试用的长字符串，对应Java版本的str
    std::string test_str =
            "正品行货 正品行货 "
            "码完代码，他起身关上电脑，用滚烫的开水为自己泡制一碗腾着热气的老坛酸菜面。中国的程序员"
            "更偏爱拉上窗帘，在黑暗中享受这独特的美食。这是现代工业给一天辛苦劳作的人最好的馈赠。南"
            "方一带生长的程序员虽然在京城多年，但仍口味清淡，他们往往不加料包，由脸颊自然淌下的热泪"
            "补充恰当的盐分。他们相信，用这种方式，能够抹平思考着现在是不是过去想要的未来而带来的大"
            "部分忧伤…小李的父亲在年轻的时候也是从爷爷手里接收了祖传的代码，不过令人惊讶的是，到了"
            "小李这一代，很多东西都遗失了，但是程序员苦逼的味道保存的是如此的完整。 "
            "就在24小时之前，最新的需求从PM处传来，为了得到这份自然的馈赠，码农们开机、写码、调试、"
            "重构，四季轮回的等待换来这难得的丰收时刻。码农知道，需求的保鲜期只有短短的两天，码农们"
            "要以最快的速度对代码进行精致的加工，任何一个需求都可能在24小时之后失去原本的活力，变成"
            "一文不值的垃圾创意。";

    void SetUp() override {
        std::cout << "🔧 初始化 PinyinUtil 测试..." << std::endl;

        // 设置详细日志级别来调试
        FLAGS_v = 5; // 设置VLOG级别为5，显示详细日志

        // 保存原始配置值以便在TearDown中恢复
        original_dict_path_ = config::inverted_index_dict_path;
        std::cout << "📍 原始 inverted_index_dict_path: " << original_dict_path_ << std::endl;

        // 动态配置字典路径 - 直接指向be/dict目录
        config::inverted_index_dict_path = "/root/doris/be/dict";

        std::cout << "✅ 设置 inverted_index_dict_path: " << config::inverted_index_dict_path
                  << std::endl;
    }

    void TearDown() override {
        std::cout << "🧹 清理 PinyinUtil 测试..." << std::endl;
        config::inverted_index_dict_path = original_dict_path_;
    }

    // 辅助函数：正确计算UTF-8字符数
    size_t getUtf8CharCount(const std::string& text) {
        size_t char_count = 0;
        int32_t i = 0;
        const char* str = text.c_str();
        int32_t length = static_cast<int32_t>(text.length());
        while (i < length) {
            UChar32 cp;
            U8_NEXT(str, i, length, cp); // i 会自动更新到下一个字符
            char_count++;
        }
        return char_count;
    }

    // 辅助方法：打印拼音列表，用于调试
    void printPinyinList(const std::vector<std::string>& pinyins, const std::string& label) {
        std::cout << "📋 " << label << " (size=" << pinyins.size() << "): [";
        for (size_t i = 0; i < pinyins.size() && i < 20; ++i) { // 最多显示前20个
            if (i > 0) std::cout << ", ";
            std::cout << "\"" << pinyins[i] << "\"";
        }
        if (pinyins.size() > 20) {
            std::cout << ", ... (" << (pinyins.size() - 20) << " more)";
        }
        std::cout << "]" << std::endl;
    }

    // 辅助方法：将拼音列表转换为字符串（跳过空项）
    std::string list2StringSkipNull(const std::vector<std::string>& list) {
        std::string result;
        for (const auto& item : list) {
            if (!item.empty()) {
                if (!result.empty()) result += " ";
                result += item;
            }
        }
        return result;
    }

    // 辅助方法：将拼音列表转换为字符串（包含空项）
    std::string list2String(const std::vector<std::string>& list) {
        std::string result;
        for (size_t i = 0; i < list.size(); ++i) {
            if (i > 0) result += " ";
            result += "\"" + list[i] + "\"";
        }
        return result;
    }
};

/**
 * 测试Unicode拼音转换功能
 * 对应 Java 的 testStr2Pinyin() - 使用 Pinyin.unicodePinyin(str)
 */
TEST_F(PinyinUtilTest, TestStr2Pinyin) {
    std::cout << "🧪 测试 Unicode 拼音转换功能..." << std::endl;

    auto& pinyin_util = PinyinUtil::instance();

    // 使用默认格式（对应Java的unicodePinyin，包含Unicode标记）
    std::vector<std::string> parse_result =
            pinyin_util.convert(test_str, PinyinFormat::DEFAULT_PINYIN_FORMAT);

    printPinyinList(parse_result, "Unicode拼音结果");

    // 验证结果长度应该等于输入字符串的字符数
    size_t expected_length = getUtf8CharCount(test_str);

    EXPECT_EQ(parse_result.size(), expected_length)
            << "拼音结果长度应等于输入字符数，期望: " << expected_length
            << ", 实际: " << parse_result.size();

    std::cout << "✅ Unicode 拼音转换测试通过" << std::endl;
}

/**
 * 测试拼音字符串转换功能（带声调）
 * 对应 Java 的 testPinyinStr() - 使用 Pinyin.pinyin(str) 
 */
TEST_F(PinyinUtilTest, TestPinyinStr) {
    std::cout << "🧪 测试拼音字符串转换功能（带声调）..." << std::endl;

    auto& pinyin_util = PinyinUtil::instance();

    // 使用带声调格式（对应Java的pinyin方法）
    std::vector<std::string> result =
            pinyin_util.convert(test_str, PinyinFormat::DEFAULT_PINYIN_FORMAT);

    printPinyinList(result, "带声调拼音结果");

    // 验证结果长度
    size_t expected_length = getUtf8CharCount(test_str);

    EXPECT_EQ(result.size(), expected_length)
            << "拼音结果长度应等于输入字符数，期望: " << expected_length
            << ", 实际: " << result.size();

    std::cout << "✅ 带声调拼音转换测试通过" << std::endl;
}

/**
 * 测试无声调拼音转换功能
 * 对应 Java 的 testPinyinWithoutTone()
 */
TEST_F(PinyinUtilTest, TestPinyinWithoutTone) {
    std::cout << "🧪 测试无声调拼音转换功能..." << std::endl;

    auto& pinyin_util = PinyinUtil::instance();

    // 使用无声调格式（对应Java的Pinyin.pinyin不带声调版本）
    std::vector<std::string> result =
            pinyin_util.convert(test_str, PinyinFormat::TONELESS_PINYIN_FORMAT);

    printPinyinList(result, "无声调拼音结果");

    // 验证结果长度
    size_t expected_length = getUtf8CharCount(test_str);

    EXPECT_EQ(result.size(), expected_length)
            << "拼音结果长度应等于输入字符数，期望: " << expected_length
            << ", 实际: " << result.size();

    std::cout << "✅ 无声调拼音转换测试通过" << std::endl;
}

/**
 * 测试首字母提取功能
 * 对应 Java 的 testStr2FirstCharArr() - 使用 Pinyin.firstChar(str)
 */
TEST_F(PinyinUtilTest, TestStr2FirstCharArr) {
    std::cout << "🧪 测试首字母提取功能..." << std::endl;

    auto& pinyin_util = PinyinUtil::instance();

    // 使用首字母格式
    std::vector<std::string> result =
            pinyin_util.convert(test_str, PinyinFormat::ABBR_PINYIN_FORMAT);

    printPinyinList(result, "首字母结果");

    // 验证结果长度
    size_t expected_length = getUtf8CharCount(test_str);

    EXPECT_EQ(result.size(), expected_length)
            << "首字母结果长度应等于输入字符数，期望: " << expected_length
            << ", 实际: " << result.size();

    std::cout << "✅ 首字母提取测试通过" << std::endl;
}

/**
 * 测试动态添加拼音功能
 * 对应 Java 的 testInsertPinyin()
 */
TEST_F(PinyinUtilTest, TestInsertPinyin) {
    std::cout << "🧪 测试动态添加拼音功能..." << std::endl;

    auto& pinyin_util = PinyinUtil::instance();

    // 测试字符串包含"行货" - 使用更长的字符串以确保包含"行货"
    std::string test_phrase = test_str; // 使用完整的测试字符串

    // 第一次转换（使用默认拼音）
    std::vector<std::string> result1 =
            pinyin_util.convert(test_phrase, PinyinFormat::DEFAULT_PINYIN_FORMAT);
    printPinyinList(result1, "第一次转换结果");

    // 动态添加"行货"的拼音
    pinyin_util.insertPinyin("行货", {"hang2", "huo4"});
    std::cout << "✨ 动态添加 '行货' -> ['hang2', 'huo4']" << std::endl;

    // 第二次转换（应该使用新添加的拼音）
    std::vector<std::string> result2 =
            pinyin_util.convert(test_phrase, PinyinFormat::DEFAULT_PINYIN_FORMAT);
    printPinyinList(result2, "第二次转换结果");

    // 验证结果不同（至少在"行货"位置应该不同）
    EXPECT_EQ(result1.size(), result2.size()) << "两次转换结果长度应该相同";

    // 找到"行"字的位置并验证变化
    bool found_difference = false;
    for (size_t i = 0; i < std::min(result1.size(), result2.size()); ++i) {
        if (result1[i] != result2[i]) {
            found_difference = true;
            std::cout << "🔍 位置 " << i << " 发现变化: '" << result1[i] << "' -> '" << result2[i]
                      << "'" << std::endl;
        }
    }

    EXPECT_TRUE(found_difference) << "动态添加拼音后，转换结果应该有变化";

    std::cout << "✅ 动态添加拼音测试通过" << std::endl;
}

/**
 * 测试列表转字符串功能
 * 对应 Java 的 testList2String()
 */
TEST_F(PinyinUtilTest, TestList2String) {
    std::cout << "🧪 测试列表转字符串功能..." << std::endl;

    auto& pinyin_util = PinyinUtil::instance();

    // 使用一个较短的字符串进行测试
    std::string short_str = "中国程序员";
    std::vector<std::string> pinyin_list =
            pinyin_util.convert(short_str, PinyinFormat::TONELESS_PINYIN_FORMAT);

    printPinyinList(pinyin_list, "拼音列表");

    // 测试包含空项的转换
    std::string result_with_null = list2String(pinyin_list);
    std::cout << "📝 包含空项的字符串: " << result_with_null << std::endl;

    // 测试跳过空项的转换
    std::string result_skip_null = list2StringSkipNull(pinyin_list);
    std::cout << "📝 跳过空项的字符串: " << result_skip_null << std::endl;

    // 验证结果不为空
    EXPECT_FALSE(result_with_null.empty()) << "包含空项的结果不应该为空";
    EXPECT_FALSE(result_skip_null.empty()) << "跳过空项的结果不应该为空";

    std::cout << "✅ 列表转字符串测试通过" << std::endl;
}

/**
 * 测试基本单字符拼音转换
 */
TEST_F(PinyinUtilTest, TestSingleCharPinyin) {
    std::cout << "🧪 测试基本单字符拼音转换..." << std::endl;

    auto& pinyin_util = PinyinUtil::instance();

    // 测试一些基本汉字
    struct TestCase {
        std::string character;
        std::string expected_pinyin_start; // 期望拼音的开头
    };

    std::vector<TestCase> test_cases = {{"中", "zhong"}, {"国", "guo"},   {"你", "ni"},
                                        {"好", "hao"},   {"程", "cheng"}, {"序", "xu"},
                                        {"员", "yuan"}};

    for (const auto& test_case : test_cases) {
        std::vector<std::string> result =
                pinyin_util.convert(test_case.character, PinyinFormat::TONELESS_PINYIN_FORMAT);

        EXPECT_EQ(result.size(), 1) << "单字符 '" << test_case.character << "' 应该返回1个拼音";

        if (!result.empty()) {
            EXPECT_TRUE(result[0].find(test_case.expected_pinyin_start) == 0)
                    << "字符 '" << test_case.character << "' 的拼音 '" << result[0] << "' 应该以 '"
                    << test_case.expected_pinyin_start << "' 开头";

            std::cout << "✓ '" << test_case.character << "' -> '" << result[0] << "'" << std::endl;
        }
    }

    std::cout << "✅ 单字符拼音转换测试通过" << std::endl;
}

} // namespace doris::segment_v2::inverted_index
