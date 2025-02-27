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
