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

#include <memory>
#include <sstream>

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


TEST_F(IKTokenizerTest, TestLargeInput) {
    std::vector<std::string> datas;
    std::string largeText;
    for (int i = 0; i < 1000; i++) {
        largeText += "中国的科技发展在世界上处于领先";
    }
    tokenize(largeText, datas, true);
    ASSERT_EQ(datas.size(), 7000);
}

TEST_F(IKTokenizerTest, TestBufferExhaustCritical) {
    std::vector<std::string> datas;
    std::string criticalText;
    for (int i = 0; i < 95; i++) {
        criticalText += "的";
    }
    tokenize(criticalText, datas, true);
    ASSERT_EQ(datas.size(), 95);
}

TEST_F(IKTokenizerTest, TestMixedLanguageInput) {
    std::vector<std::string> datas;
    std::string mixedText = "Doris是一个现代化的MPP分析型数据库，可以处理PB级别的数据，支持SQL92和SQL99。";
    tokenize(mixedText, datas, true);
    std::vector<std::string> expectedTokens = {"doris", "是", "一个", "现代化", "的", "mpp", "分析", "型", "数据库", "可以", "处理", "pb", "级", "别的", "数据", "支持", "sql92", "和", "sql99"};
    ASSERT_EQ(datas.size(), expectedTokens.size());
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], expectedTokens[i]);
    }
}

TEST_F(IKTokenizerTest, TestSpecialCharacters) {
    std::vector<std::string> datas;
    std::string specialText = "😊🚀👍测试特殊符号：@#¥%……&*（）";
    tokenize(specialText, datas, true);
    ASSERT_EQ(datas.size(), 2);
}

TEST_F(IKTokenizerTest, TestBufferBoundaryWithSpace) {
    std::vector<std::string> datas;
    
    std::string exactText;
    int charCount = 4096 / 3;
    for (int i = 0; i < charCount; i++) {
        exactText += "中";
    }
    exactText += " "; 
    
    tokenize(exactText, datas, true);
    ASSERT_EQ(datas.size(), charCount); 
    datas.clear();
    
    std::string overText;
    charCount = 4096 / 3 + 1; 
    for (int i = 0; i < charCount; i++) {
        overText += "中";
    }
    overText += " "; 
    
    tokenize(overText, datas, true);
    ASSERT_EQ(datas.size(), charCount);
    datas.clear();
    
    std::string multiSpaceText;
    charCount = 4096 / 3 - 3; 
    for (int i = 0; i < charCount; i++) {
        multiSpaceText += "中";
    }
    multiSpaceText += "   ";
    
    tokenize(multiSpaceText, datas, true);
    ASSERT_EQ(datas.size(), charCount); 
    datas.clear();
    
    std::string spaceAroundBoundaryText;
    charCount = 4096 / 3 - 2; 
    for (int i = 0; i < charCount / 2; i++) {
        spaceAroundBoundaryText += "中";
    }
    spaceAroundBoundaryText += " "; 
    for (int i = 0; i < charCount / 2; i++) {
        spaceAroundBoundaryText += "中";
    }
    spaceAroundBoundaryText += "  "; 
    
    tokenize(spaceAroundBoundaryText, datas, true);
    ASSERT_EQ(datas.size(), charCount-1);
}

TEST_F(IKTokenizerTest, TestChineseCharacterAtBufferBoundary) {
    std::vector<std::string> datas;
    
    std::string boundaryText;
    // case1: a complete chinese character cut at the first byte
    int completeChars = 4096 / 3;
    for (int i = 0; i < completeChars; i++) {
        boundaryText += "中";
    }

    boundaryText += "国";
    
    
    tokenize(boundaryText, datas, true);
    ASSERT_EQ(datas.size(), completeChars+1);
    ASSERT_EQ(datas[datas.size() - 1], "国");
    datas.clear();
    boundaryText.clear();
    // case2: a complete chinese character cut at the second byte
    boundaryText += "  ";

    for (int i = 0; i < completeChars; i++) {
        boundaryText += "中";
    }

    boundaryText += "国";
    
    tokenize(boundaryText, datas, true);
    ASSERT_EQ(datas.size(), completeChars);
    ASSERT_EQ(datas[datas.size() - 1], "中国");

    datas.clear();
    boundaryText.clear();
}

TEST_F(IKTokenizerTest, TestLongTextCompareWithJava) {
    std::vector<std::string> datas;
    
    std::string longText = 
        "随着人工智能技术的快速发展，深度学习、机器学习和神经网络等技术已经在各个领域得到了广泛应用。"
        "从语音识别、图像处理到自然语言处理，人工智能正在改变我们的生活方式和工作方式。"
        "在医疗领域，AI辅助诊断系统可以帮助医生更准确地识别疾病；在金融领域，智能算法可以预测市场趋势和风险；"
        "在教育领域，个性化学习平台可以根据学生的学习情况提供定制化的教学内容。"
        "然而，随着AI技术的普及，也带来了一系列的伦理和隐私问题。如何确保AI系统的公平性和透明度，"
        "如何保护用户数据的安全，如何防止AI被滥用，这些都是我们需要思考的问题。"
        "此外，AI的发展也可能对就业市场产生影响，一些传统工作可能会被自动化系统取代，"
        "但同时也会创造出新的工作岗位和机会。因此，我们需要积极适应这一变化，"
        "提升自己的技能和知识，以便在AI时代保持竞争力。"
        "总的来说，人工智能是一把双刃剑，它既带来了巨大的机遇，也带来了挑战。"
        "我们需要理性看待AI的发展，既要充分利用它的优势，也要警惕可能的风险，"
        "共同推动AI技术向着更加健康、可持续的方向发展。";
    
    // repeate 4 times
    int i = 0;
    while (i < 4) {
        longText += longText;
        i++;
    }    
    // smart mode
    tokenize(longText, datas, true); 
    
    ASSERT_EQ(datas.size(), 3312);

    // compare first 20 tokens with java
    std::vector<std::string> javaFirst20Results = {
        "随着", "人工智能技术", "的", "快速", 
        "发展", "深度", "学习", "机器", "学习", "和", "神经网络", "等", 
        "技术", "已经在", "各个领域", "得", "到了", "广泛应用", "从", "语音"
    };
    for (size_t i = 0; i < 20; i++) {
        ASSERT_EQ(datas[i], javaFirst20Results[i]);
    }

    // compare last 20 tokens with java
    std::vector<std::string> javaLast20Results = {
        "发展", "方向", "的", "持续", "可",
        "健康", "更加", "向着", "技术", "ai", "推动", 
        "共同", "风险", "的", "可能", "警惕", "也要", "优势", "的", "它"
    };
    for (size_t i = 0; i < 20; i++) {
        ASSERT_EQ(datas[datas.size() - i - 1], javaLast20Results[i]);
    }

    // max_word mode
    datas.clear();
    javaFirst20Results = {
        "随着", "人工智能技术", "人工智能", "人工", "智能", "技术", "的", 
        "快速", "发展", "深度", "学习", "机器", "学习", "和", "神经网络", 
        "神经", "网络", "等", "技术", "已经在"
    };
    javaLast20Results = {
        "发展", "方向", "的", "持续", "可",
        "健康", "更加", "向着", "技术", "ai", "推动", 
        "共同", "风险", "的", "可能", "警惕", "也要", "优势", "的", "用它"
    };
    
    tokenize(longText, datas, false);
    ASSERT_EQ(datas.size(), 4336);

    // compare first 20 tokens with java
    for (size_t i = 0; i < 20; i++) {
        ASSERT_EQ(datas[i], javaFirst20Results[i]);
    }

    // compare last 20 tokens with java
    for (size_t i = 0; i < 20; i++) {
        ASSERT_EQ(datas[datas.size() - i - 1], javaLast20Results[i]);
    }
}

}