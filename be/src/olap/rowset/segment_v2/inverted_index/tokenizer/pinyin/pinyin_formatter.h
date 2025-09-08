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

#include "pinyin_format.h"

namespace doris::segment_v2::inverted_index {

/**
 * C++ 移植版本的 PinyinFormatter
 * 对应 Java 中的 org.nlpcn.commons.lang.pinyin.PinyinFormatter
 * 拼音格式化处理器，负责将原始拼音字符串按指定格式进行转换
 */
class PinyinFormatter {
public:
    /**
     * 格式化拼音字符串
     * 对应 Java：public static String formatPinyin(String pinyinStr, PinyinFormat format)
     * 
     * @param pinyin_str 原始拼音字符串（如 "da3", "lv4" 等）
     * @param format 目标格式配置
     * @return 格式化后的拼音字符串
     */
    static std::string formatPinyin(const std::string& pinyin_str, const PinyinFormat& format);

    /**
     * 获取拼音首字母缩写
     * 对应 Java：public static String abbr(String str)
     * 
     * @param str 输入字符串
     * @return 首字母（如果字符串为空则返回原字符串）
     */
    static std::string abbr(const std::string& str);

    /**
     * 首字母大写
     * 对应 Java：public static String capitalize(String str)
     * 
     * @param str 输入字符串
     * @return 首字母大写的字符串
     */
    static std::string capitalize(const std::string& str);

private:
    /**
     * 将声调数字转换为Unicode声调标记
     * 对应 Java：private static String convertToneNumber2ToneMark(final String pinyinStr)
     * 
     * 算法规则：
     * 1. 首先查找 "a" 或 "e"，如果存在则在其上加声调标记
     * 2. 如果没有 "a" 或 "e"，查找 "ou"，在 "o" 上加声调标记
     * 3. 否则在最后一个元音字母上加声调标记
     * 
     * @param pinyin_str 带数字声调的拼音字符串（如 "da3"）
     * @return 带Unicode声调标记的拼音字符串（如 "dǎ"）
     */
    static std::string convertToneNumber2ToneMark(const std::string& pinyin_str);
};

} // namespace doris::segment_v2::inverted_index
