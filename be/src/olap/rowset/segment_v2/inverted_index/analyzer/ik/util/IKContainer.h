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

#include <deque>
#include <memory>
#include <queue>
#include <stack>
#include <type_traits>

#include "IKAllocator.h"

namespace doris::segment_v2 {

// Define a vector type that uses the custom IKAllocator for memory management
template <typename T>
using IKVector = std::vector<T, IKAllocator<T>>;

// Define a deque type that uses the custom IKAllocator for memory management
template <typename T>
using IKDeque = std::deque<T, IKAllocator<T>>;

// Define a stack type that uses the custom IKAllocator for memory management
template <typename T>
using IKStack = std::stack<T, IKDeque<T>>;

// Define a map type that uses the custom IKAllocator for memory management
template <typename K, typename V, typename Compare = std::less<K>>
using IKMap = std::map<K, V, Compare, IKAllocator<std::pair<const K, V>>>;

// Define an unordered map type that uses the custom IKAllocator for memory management
template <typename K, typename V, typename Hash = std::hash<K>>
using IKUnorderedMap =
        std::unordered_map<K, V, Hash, std::equal_to<K>, IKAllocator<std::pair<const K, V>>>;

// Define a set type that uses the custom IKAllocator for memory management
template <typename T, typename Compare = std::less<T>>
using IKSet = std::set<T, Compare, IKAllocator<T>>;

// Define a list type that uses the custom IKAllocator for memory management
template <typename T>
using IKList = std::list<T, IKAllocator<T>>;

template <typename T>
using IKQue = std::queue<T, IKDeque<T>>;

} // namespace doris::segment_v2
