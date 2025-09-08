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

namespace doris::segment_v2::inverted_index {

/**
 * C++ 移植版本的 Pinyin
 * 对应 Java 中的 org.nlpcn.commons.lang.pinyin.Pinyin
 * 提供拼音转换的便利接口，所有方法都是静态方法
 */
class Pinyin {
public:
    /**
     * 拼音返回（无声调）
     * 对应 Java：public static List<String> pinyin(String str)
     * 
     * @param str 输入文本
     * @return 拼音列表，如 ["chang", "jiang", "cheng", "zhang"]
     */
    static std::vector<std::string> pinyin(const std::string& str);

    /**
     * 取得每个字的首字符
     * 对应 Java：public static List<String> firstChar(String str)
     * 
     * @param str 输入文本
     * @return 首字母列表，如 ["c", "j", "c", "z"]
     */
    static std::vector<std::string> firstChar(const std::string& str);

    /**
     * 取得每个字的带音标拼音
     * 对应 Java：public static List<String> unicodePinyin(String str)
     * 
     * @param str 输入文本
     * @return Unicode声调标记拼音列表，如 ["cháng", "jiāng", "chéng", "zhăng"]
     */
    static std::vector<std::string> unicodePinyin(const std::string& str);

    /**
     * 带数字声调的拼音
     * 对应 Java：public static List<String> tonePinyin(String str)
     * 
     * @param str 输入文本
     * @return 带数字声调拼音列表，如 ["chang2", "jiang1", "cheng2", "zhang3"]
     */
    static std::vector<std::string> tonePinyin(const std::string& str);

    /**
     * 列表转换为字符串
     * 对应 Java：public static String list2String(List<String> list, String separator)
     * 
     * @param list 字符串列表
     * @param separator 分隔符
     * @return 连接后的字符串
     */
    static std::string list2String(const std::vector<std::string>& list,
                                   const std::string& separator);

    /**
     * 列表转换为字符串（默认空格分隔）
     * 对应 Java：public static String list2String(List<String> list)
     * 
     * @param list 字符串列表
     * @return 用空格连接的字符串
     */
    static std::string list2String(const std::vector<std::string>& list);

    /**
     * 动态增加到拼音词典中
     * 对应 Java：public static void insertPinyin(String word, String[] pinyins)
     * 
     * @param word 词汇，如 "大长今"
     * @param pinyins 拼音数组，如 ["da4", "chang2", "jing1"]
     */
    static void insertPinyin(const std::string& word, const std::vector<std::string>& pinyins);

    /**
     * 列表转换为字符串（跳过null/空字符串，默认空格分隔）
     * 对应 Java：public static String list2StringSkipNull(List<String> list)
     * 
     * @param list 字符串列表
     * @return 用空格连接的非空字符串
     */
    static std::string list2StringSkipNull(const std::vector<std::string>& list);

    /**
     * 列表转换为字符串（跳过null/空字符串）
     * 对应 Java：public static String list2StringSkipNull(List<String> list, String separator)
     * 
     * @param list 字符串列表
     * @param separator 分隔符
     * @return 连接后的非空字符串
     */
    static std::string list2StringSkipNull(const std::vector<std::string>& list,
                                           const std::string& separator);
};

} // namespace doris::segment_v2::inverted_index
