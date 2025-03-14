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
#include <fstream>

#include "olap/rowset/segment_v2/inverted_index/analyzer/ik/IKAnalyzer.h"
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
    
    // Helper method to create a temporary dictionary file for testing
    std::string createTempDictFile(const std::string& content) {
        std::string temp_file = "/tmp/temp_dict_" + std::to_string(rand()) + ".dic";
        std::ofstream out(temp_file);
        out << content;
        out.close();
        return temp_file;
    }

    // Flag to indicate if the current test is an exception test
    bool is_exception_test = true;
};

// Test for Dictionary exception handling
TEST_F(IKTokenizerTest, TestDictionaryExceptionHandling) {
    // Test case 1: Test loadDictFile with non-existent file
    Configuration cfg;
    cfg.setDictPath("/non_existent_path");
    
    // Initialize dictionary with non-existent path
    // This should not throw exception due to our error handling in getSingleton
    ASSERT_NO_THROW({
        Dictionary::initial(cfg, false);
    });
    
    // getSingleton() should not throw exception because singleton exists
    Dictionary* dict = nullptr;
    ASSERT_NO_THROW({
        dict = Dictionary::getSingleton();
    });
    ASSERT_NE(dict, nullptr);
    
    // But operations on the dictionary should fail or return default values
    // because the dictionary files weren't loaded properly
    CharacterUtil::TypedRuneArray typed_runes;
    CharacterUtil::decodeStringToRunes("测试", 6, typed_runes, false);
    Hit result = dict->matchInMainDict(typed_runes, 0, 0);
    ASSERT_TRUE(result.isUnmatch());
    
    // Test case 2: Test with invalid file content by reloading
    std::string temp_file = createTempDictFile("# This is a comment\nvalid_word\ninvalid\xFF\xFF\xFF");
    
    Configuration cfg2;
    cfg2.setDictPath("/tmp");
    
    // Override main dictionary file
    cfg2.setMainDictFile(temp_file.substr(temp_file.find_last_of('/') + 1));
    
    // Instead of destroying and reinitializing, update the configuration and reload
    dict->getConfiguration()->setDictPath("/tmp");
    dict->getConfiguration()->setMainDictFile(temp_file.substr(temp_file.find_last_of('/') + 1));
    
    // This should not throw exception due to our error handling in reload
    ASSERT_NO_THROW({
        Dictionary::reload();
    });
    
    // Test case 3: Test with out-of-bounds index
    // Create a valid TypedRuneArray with Chinese characters
    CharacterUtil::TypedRuneArray typed_runes2;
    CharacterUtil::decodeStringToRunes("测试分词", 12, typed_runes2, false);
    
    // This should not throw exception due to our error handling
    ASSERT_NO_THROW({
        Hit result = dict->matchInMainDict(typed_runes2, 100, 1);
        ASSERT_TRUE(result.isUnmatch());
    });
    
    // Test case 4: Test matchInQuantifierDict with valid input
    ASSERT_NO_THROW({
        Hit result = dict->matchInQuantifierDict(typed_runes2, 0, 0);
        ASSERT_TRUE(result.isUnmatch());
    });
    
    // Test case 5: Test isStopWord with valid input
    ASSERT_NO_THROW({
        bool result = dict->isStopWord(typed_runes2, 0, 0);
        ASSERT_FALSE(result);
    });
    
    // Test case 6: Test reload with valid configuration
    // Update configuration to a valid path
    dict->getConfiguration()->setDictPath("./be/dict/ik");
    dict->getConfiguration()->setMainDictFile("main.dic");
    dict->getConfiguration()->setQuantifierDictFile("quantifier.dic");
    dict->getConfiguration()->setStopWordDictFile("stopword.dic");
    
    // Reload with valid configuration
    ASSERT_NO_THROW({
        Dictionary::reload();
    });
    
    // Now dictionary should be properly loaded
    result = dict->matchInMainDict(typed_runes, 0, 0);
    
    // Clean up temporary file
    std::remove(temp_file.c_str());
    
    // Reset exception test flag
    is_exception_test = false;
}

// Combine all other dictionary tests into one test case
TEST_F(IKTokenizerTest, TestDictionaryOtherFunctions) {
    // Initialize dictionary with valid path
    Configuration cfg;
    cfg.setDictPath("./be/dict/ik");
    
    Dictionary::initial(cfg, true);
    Dictionary* dict = Dictionary::getSingleton();
    ASSERT_NE(dict, nullptr);
    
    // Test reload functionality
    ASSERT_NO_THROW({
        Dictionary::reload();
    });
    
    // Create a temporary directory for testing
    std::string temp_dir = "/tmp/ik_test_" + std::to_string(rand());
    system(("mkdir -p " + temp_dir).c_str());
    
    // Update configuration to use temporary directory
    dict->getConfiguration()->setDictPath(temp_dir);
    
    // Reload with empty directory
    ASSERT_NO_THROW({
        Dictionary::reload();
    });
    
    // Create main dictionary but missing quantifier dictionary
    std::string main_dict_path = temp_dir + "/main.dic";
    std::ofstream main_dict(main_dict_path);
    main_dict << "测试\n词语\n分词器\n";
    main_dict.close();
    
    // Reload with partial dictionary files
    ASSERT_NO_THROW({
        Dictionary::reload();
    });
    
    // Clean up
    system(("rm -rf " + temp_dir).c_str());
    
    // Reset exception test flag
    is_exception_test = false;
}

TEST_F(IKTokenizerTest, TestIKTokenizer) {
    std::vector<std::string> datas;

    // Test with max_word mode
    std::string Text1 = "中华人民共和国国歌";
    tokenize(Text1, datas, false);
    ASSERT_EQ(datas.size(), 10);
    datas.clear();

    // Test with smart mode
    tokenize(Text1, datas, true);
    ASSERT_EQ(datas.size(), 2);
    datas.clear();

    std::string Text2 = "人民可以得到更多实惠";
    tokenize(Text2, datas, false);
    ASSERT_EQ(datas.size(), 5);
    datas.clear();

    // Test with smart mode
    tokenize(Text2, datas, true);
    ASSERT_EQ(datas.size(), 5);
    datas.clear();

    std::string Text3 = "中国人民银行";
    tokenize(Text3, datas, false);
    ASSERT_EQ(datas.size(), 8);
    datas.clear();

    // Test with smart mode
    tokenize(Text3, datas, true);
    ASSERT_EQ(datas.size(), 1);
    datas.clear();
}

TEST_F(IKTokenizerTest, TestIKRareTokenizer) {
    std::vector<std::string> datas;

    // Test with rare characters
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

    // Test smart mode tokenization
    std::string Text1 = "我来到北京清华大学";
    tokenize(Text1, datas, true);
    ASSERT_EQ(datas.size(), 4);
    std::vector<std::string> result1 = {"我", "来到", "北京", "清华大学"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result1[i]);
    }
    datas.clear();

    // Test another example with smart mode
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

    // Test max word mode tokenization
    std::string Text1 = "我来到北京清华大学";
    tokenize(Text1, datas, false);
    ASSERT_EQ(datas.size(), 6);
    std::vector<std::string> result1 = {"我", "来到", "北京", "清华大学", "清华", "大学"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result1[i]);
    }
    datas.clear();

    // Test another example with max word mode
    std::string Text2 = "中国的科技发展在世界上处于领先";
    tokenize(Text2, datas, false);
    ASSERT_EQ(datas.size(), 11);
    std::vector<std::string> result2 = {"中国",   "的",   "科技", "发展", "在世界上", "在世",
                                        "世界上", "世界", "上",   "处于", "领先"};
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], result2[i]);
    }
    datas.clear();
}

TEST_F(IKTokenizerTest, TestEmptyInput) {
    std::vector<std::string> datas;
    // Test with empty input
    std::string emptyText = "";
    tokenize(emptyText, datas, true);
    ASSERT_EQ(datas.size(), 0);
}

TEST_F(IKTokenizerTest, TestSingleByteInput) {
    std::vector<std::string> datas;
    // Test with single byte input
    std::string singleByteText = "b";
    tokenize(singleByteText, datas, true);
    ASSERT_EQ(datas.size(), 1);
    ASSERT_EQ(datas[0], "b");
}

TEST_F(IKTokenizerTest, TestLargeInput) {
    std::vector<std::string> datas;
    // Test with large input
    std::string largeText;
    for (int i = 0; i < 1000; i++) {
        largeText += "中国的科技发展在世界上处于领先";
    }
    tokenize(largeText, datas, true);
    ASSERT_EQ(datas.size(), 7000);
}

TEST_F(IKTokenizerTest, TestBufferExhaustCritical) {
    std::vector<std::string> datas;
    // Test with buffer exhaustion critical case
    std::string criticalText;
    for (int i = 0; i < 95; i++) {
        criticalText += "的";
    }
    tokenize(criticalText, datas, true);
    ASSERT_EQ(datas.size(), 95);
}

TEST_F(IKTokenizerTest, TestMixedLanguageInput) {
    std::vector<std::string> datas;
    // Test with mixed language input
    std::string mixedText =
            "Doris是一个现代化的MPP分析型数据库，可以处理PB级别的数据，支持SQL92和SQL99。";
    tokenize(mixedText, datas, true);

    std::vector<std::string> expectedTokens = {
            "doris", "是", "一个", "现代化", "的",   "mpp",  "分析",  "型", "数据库", "可以",
            "处理",  "pb", "级",   "别的",   "数据", "支持", "sql92", "和", "sql99"};
    ASSERT_EQ(datas.size(), expectedTokens.size());
    for (size_t i = 0; i < datas.size(); i++) {
        ASSERT_EQ(datas[i], expectedTokens[i]);
    }
}

TEST_F(IKTokenizerTest, TestSpecialCharacters) {
    std::vector<std::string> datas;
    // Test with special characters
    std::string specialText = "😊🚀👍测试特殊符号：@#¥%……&*（）";
    tokenize(specialText, datas, true);
    ASSERT_EQ(datas.size(), 2);
}

TEST_F(IKTokenizerTest, TestBufferBoundaryWithSpace) {
    std::vector<std::string> datas;

    // Test with exact buffer boundary
    std::string exactText;
    int charCount = 4096 / 3;
    for (int i = 0; i < charCount; i++) {
        exactText += "中";
    }
    exactText += " ";

    tokenize(exactText, datas, true);
    ASSERT_EQ(datas.size(), charCount);
    datas.clear();

    // Test with buffer boundary overflow
    std::string overText;
    charCount = 4096 / 3 + 1;
    for (int i = 0; i < charCount; i++) {
        overText += "中";
    }
    overText += " ";

    tokenize(overText, datas, true);
    ASSERT_EQ(datas.size(), charCount);
    datas.clear();

    // Test with multiple spaces at buffer boundary
    std::string multiSpaceText;
    charCount = 4096 / 3 - 3;
    for (int i = 0; i < charCount; i++) {
        multiSpaceText += "中";
    }
    multiSpaceText += "   ";

    tokenize(multiSpaceText, datas, true);
    ASSERT_EQ(datas.size(), charCount);
    datas.clear();

    // Test with spaces around buffer boundary
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
    ASSERT_EQ(datas.size(), charCount - 1);
}

TEST_F(IKTokenizerTest, TestChineseCharacterAtBufferBoundary) {
    std::vector<std::string> datas;

    std::string boundaryText;
    // Test with a complete Chinese character cut at the first byte
    int completeChars = 4096 / 3;
    for (int i = 0; i < completeChars; i++) {
        boundaryText += "中";
    }

    boundaryText += "国";

    tokenize(boundaryText, datas, true);
    ASSERT_EQ(datas.size(), completeChars + 1);
    ASSERT_EQ(datas[datas.size() - 1], "国");
    datas.clear();
    boundaryText.clear();
    // Test with a complete Chinese character cut at the second byte
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

    // Test with long text and compare results with Java implementation
    std::string longText =
            "随着人工智能技术的快速发展，深度学习、机器学习和神经网络等技术已经在各个领域得到了广泛"
            "应用。"
            "从语音识别、图像处理到自然语言处理，人工智能正在改变我们的生活方式和工作方式。"
            "在医疗领域，AI辅助诊断系统可以帮助医生更准确地识别疾病；在金融领域，智能算法可以预测市"
            "场趋势和风险；"
            "在教育领域，个性化学习平台可以根据学生的学习情况提供定制化的教学内容。"
            "然而，随着AI技术的普及，也带来了一系列的伦理和隐私问题。如何确保AI系统的公平性和透明度"
            "，"
            "如何保护用户数据的安全，如何防止AI被滥用，这些都是我们需要思考的问题。"
            "此外，AI的发展也可能对就业市场产生影响，一些传统工作可能会被自动化系统取代，"
            "但同时也会创造出新的工作岗位和机会。因此，我们需要积极适应这一变化，"
            "提升自己的技能和知识，以便在AI时代保持竞争力。"
            "总的来说，人工智能是一把双刃剑，它既带来了巨大的机遇，也带来了挑战。"
            "我们需要理性看待AI的发展，既要充分利用它的优势，也要警惕可能的风险，"
            "共同推动AI技术向着更加健康、可持续的方向发展。";

    // Repeat 4 times to create a long text
    int i = 0;
    while (i < 4) {
        longText += longText;
        i++;
    }
    // Test with smart mode
    tokenize(longText, datas, true);

    ASSERT_EQ(datas.size(), 3312);

    // Compare first 20 tokens with Java implementation
    std::vector<std::string> javaFirst20Results = {
            "随着",     "人工智能技术", "的",   "快速",     "发展", "深度", "学习",
            "机器",     "学习",         "和",   "神经网络", "等",   "技术", "已经在",
            "各个领域", "得",           "到了", "广泛应用", "从",   "语音"};
    for (size_t i = 0; i < 20; i++) {
        ASSERT_EQ(datas[i], javaFirst20Results[i]);
    }

    // Compare last 20 tokens with Java implementation
    std::vector<std::string> javaLast20Results = {
            "发展", "方向", "的",   "持续", "可",   "健康", "更加", "向着", "技术", "ai",
            "推动", "共同", "风险", "的",   "可能", "警惕", "也要", "优势", "的",   "它"};
    for (size_t i = 0; i < 20; i++) {
        ASSERT_EQ(datas[datas.size() - i - 1], javaLast20Results[i]);
    }

    // Test with max_word mode
    datas.clear();
    javaFirst20Results = {"随着",     "人工智能技术", "人工智能", "人工", "智能", "技术",  "的",
                          "快速",     "发展",         "深度",     "学习", "机器", "学习",  "和",
                          "神经网络", "神经",         "网络",     "等",   "技术", "已经在"};
    javaLast20Results = {"发展", "方向", "的",   "持续", "可",   "健康", "更加",
                         "向着", "技术", "ai",   "推动", "共同", "风险", "的",
                         "可能", "警惕", "也要", "优势", "的",   "用它"};

    tokenize(longText, datas, false);
    ASSERT_EQ(datas.size(), 4336);

    // Compare first 20 tokens with Java implementation
    for (size_t i = 0; i < 20; i++) {
        ASSERT_EQ(datas[i], javaFirst20Results[i]);
    }

    // Compare last 20 tokens with Java implementation
    for (size_t i = 0; i < 20; i++) {
        ASSERT_EQ(datas[datas.size() - i - 1], javaLast20Results[i]);
    }
}

} // namespace doris::segment_v2
