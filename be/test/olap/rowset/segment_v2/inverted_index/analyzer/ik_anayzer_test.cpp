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

#include <gtest/gtest.h>

#include <fstream>
#include <memory>
#include <sstream>
#include <filesystem>

#include "olap/rowset/segment_v2/inverted_index/analyzer/ik/IKAnalyzer.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/ik/IKAnalyzer.h"
#include "CLucene/analysis/LanguageBasedAnalyzer.h"
using namespace lucene::analysis;

namespace doris::segment_v2 {

class IKTokenizerTest : public ::testing::Test {
protected:
    void tokenize(const std::string& s, std::vector<std::string>& datas, bool isSmart) {
        try {
            IKAnalyzer analyzer;
            analyzer.initDict("./be/dict/ik");
            analyzer.setMode(isSmart);
            analyzer.set_lowercase(true);

            lucene::util::SStringReader<char> reader;
            reader.init(s.data(), s.size(), false);

            std::unique_ptr<IKTokenizer> tokenizer;
            tokenizer.reset((IKTokenizer*)analyzer.tokenStream(L"", &reader));

            Token t;
            while (tokenizer->next(&t)) {
                std::string term(t.termBuffer<char>(), t.termLength<char>());
                datas.emplace_back(term);
            }
        } catch (CLuceneError& e) {
            std::cout << e.what() << std::endl;
            throw;
        }
    }
};

TEST_F(IKTokenizerTest, TestIKTokenizer) {
    std::vector<std::string> datas;

    // smart mode
    std::string Text1 = "中华人民共和国国歌";
    tokenize(Text1, datas, false);
    ASSERT_EQ(datas.size(), 10);
    datas.clear();

    // max_word mode
    tokenize(Text1, datas, true);
    ASSERT_EQ(datas.size(), 2);
    datas.clear();

    std::string Text2 = "人民可以得到更多实惠";
    tokenize(Text2, datas, false);
    ASSERT_EQ(datas.size(), 5);
    datas.clear();

    // max_word mode
    tokenize(Text2, datas, true);
    ASSERT_EQ(datas.size(), 5);
    datas.clear();

    std::string Text3 = "中国人民银行";
    tokenize(Text3, datas, false);
    ASSERT_EQ(datas.size(), 8);
    datas.clear();

    // max_word mode
    tokenize(Text3, datas, true);
    ASSERT_EQ(datas.size(), 1);
    datas.clear();
}

TEST_F(IKTokenizerTest, TestIKRareTokenizer) {
    std::vector<std::string> datas;

    std::string Text = "菩𪜮龟龙麟凤凤";
    tokenize(Text, datas, true);
    ASSERT_EQ(datas.size(), 4);
    std::vector<std::string> result = {"菩", "𪜮", "龟龙麟凤", "凤"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result[i]);
    }
}

TEST_F(IKTokenizerTest, TestIKSmartModeTokenizer) {
    std::vector<std::string> datas;

    std::string Text1 = "我来到北京清华大学";
    tokenize(Text1, datas, true);
    ASSERT_EQ(datas.size(), 4);
    std::vector<std::string> result1 = {"我", "来到", "北京", "清华大学"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result1[i]);
    }
    datas.clear();

    std::string Text2 = "中国的科技发展在世界上处于领先";
    tokenize(Text2, datas, true);
    ASSERT_EQ(datas.size(), 7);
    std::vector<std::string> result2 = {"中国", "的", "科技", "发展", "在世界上", "处于", "领先"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result2[i]);
    }
    datas.clear();

}

TEST_F(IKTokenizerTest, TestIKMaxWordModeTokenizer) {
    std::vector<std::string> datas;

    std::string Text1 = "我来到北京清华大学";
    tokenize(Text1, datas, false);
    ASSERT_EQ(datas.size(), 6);
    std::vector<std::string> result1 = {"我", "来到", "北京", "清华大学", "清华", "大学"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result1[i]);
    }
    datas.clear();

    std::string Text2 = "中国的科技发展在世界上处于领先";
    tokenize(Text2, datas, false);
    ASSERT_EQ(datas.size(), 11);
    std::vector<std::string> result2 = {"中国", "的", "科技", "发展", "在世界上", "在世", "世界上", "世界", "上", "处于", "领先"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result2[i]);
    }
    datas.clear();
}

TEST_F(IKTokenizerTest, TestEmptyInput) {
    std::vector<std::string> datas;
    std::string emptyText = "";
    tokenize(emptyText, datas, true);
    ASSERT_EQ(datas.size(), 0);
}

TEST_F(IKTokenizerTest, TestSingleByteInput) {
    std::vector<std::string> datas;
    std::string singleByteText = "b";
    tokenize(singleByteText, datas, true);
    ASSERT_EQ(datas.size(), 1);
    ASSERT_EQ(datas[0], "b");
}

TEST_F(IKTokenizerTest, TestMultiByteCharBoundary) {
    std::vector<std::string> datas;
    // 使用一个包含多字节字符的文本，确保字符边界处理正确
    std::string multiByteText = "你好世界";
    tokenize(multiByteText, datas, true);
    ASSERT_EQ(datas.size(), 2);
    ASSERT_EQ(datas[0], "你好");
    ASSERT_EQ(datas[1], "世界");
}

TEST_F(IKTokenizerTest, TestLargeInput) {
    std::vector<std::string> datas;
    // 创建一个大于BUFF_SIZE(4096)的输入，测试缓冲区重新填充
    std::string largeText;
    for (int i = 0; i < 1000; i++) {
        largeText += "中国的科技发展在世界上处于领先";
    }
    tokenize(largeText, datas, true);
    // 验证分词结果数量应该是7000（每个短语分成7个词，重复1000次）
    ASSERT_EQ(datas.size(), 7000);
}

TEST_F(IKTokenizerTest, TestBufferExhaustCritical) {
    std::vector<std::string> datas;
    // 创建一个接近BUFF_EXHAUST_CRITICAL(100)的输入
    std::string criticalText;
    for (int i = 0; i < 95; i++) {
        criticalText += "的";
    }
    tokenize(criticalText, datas, true);
    ASSERT_EQ(datas.size(), 95);
}

TEST_F(IKTokenizerTest, TestMixedLanguageInput) {
    std::vector<std::string> datas;
    // 测试混合语言输入（中英文、数字、符号等）
    std::string mixedText = "Doris是一个现代化的MPP分析型数据库，可以处理PB级别的数据，支持SQL92和SQL99。";
    tokenize(mixedText, datas, true);
    // 验证分词结果包含英文单词和中文词语
    std::vector<std::string> expectedTokens = {"doris", "是", "一个", "现代化", "的", "mpp", "分析", "型", "数据库", "可以", "处理", "pb", "级", "别的", "数据", "支持", "sql92", "和", "sql99"};
    ASSERT_EQ(datas.size(), expectedTokens.size());
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], expectedTokens[i]);
    }
}

TEST_F(IKTokenizerTest, TestSpecialCharacters) {
    std::vector<std::string> datas;
    // 测试特殊字符和罕见字符的处理
    std::string specialText = "😊🚀👍测试特殊符号：@#¥%……&*（）";
    tokenize(specialText, datas, true);
    // 验证特殊字符的处理结果
    ASSERT_GT(datas.size(), 0);
}
}
