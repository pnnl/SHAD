//===------------------------------------------------------------*- C++ -*-===//
//
//                                     SHAD
//
//      The Scalable High-performance Algorithms and Data Structure Library
//
//===----------------------------------------------------------------------===//
//
// Copyright 2018 Battelle Memorial Institute
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy
// of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
//
//===----------------------------------------------------------------------===//

#ifndef INCLUDE_SHAD_DATA_STRUCTURES_COMPARE_AND_HASH_UTILS_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_COMPARE_AND_HASH_UTILS_H_

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <type_traits>
#include <vector>

#include "shad/core/type_traits.h"

namespace shad {

/// @brief Comparison functor.
///
/// This functor compares two object of type KeyTy.
///
/// Typical Usage:
/// @code
/// struct A {
///   int a, b;
/// };
///
/// void fn(A value1, A value2) {
///   if (!MemCmp()(&value1, &value2)) {
///     // they are equal so do something
///   } else {
///     // do sothing different
///   }
/// }
/// @endcode
///
/// @tparam KeyTy The type of the two keys to compare.
///
/// @param[in] first The first object to be compared
/// @param[in] second The second object to be compared
/// @return false if first == second, true otherwise.
template <typename KeyTy>
class MemCmp {
 public:
  bool operator()(const KeyTy *first, const KeyTy *second) const {
    return std::memcmp(first, second, sizeof(KeyTy));
  }
};

/// @brief Comparison functor specialized for std::vector.
///
/// This functor compares the content of two std::vector.
///
/// Typical Usage:
/// @code
/// struct A {
///   int a, b;
/// };
///
/// void fn(std::vector<A> &value1, std::vector<A> & value2) {
///   if (!MemCmp()(&value1, &value2)) {
///     // they are equal so do something
///   } else {
///     // do sothing different
///   }
/// }
/// @endcode
///
/// @tparam KeyTy The type of the two keys to compare.
///
/// @param[in] first The first std::vector to be compared
/// @param[in] second The second std::vector to be compared
/// @return false if first == second, true otherwise.
template <typename KeyTy>
class MemCmp<std::vector<KeyTy>> {
 public:
  bool operator()(const std::vector<KeyTy> *first,
                  const std::vector<KeyTy> *sec) const {
    return !std::equal(first->begin(), first->end(), sec->begin());
  }
};

/// @brief Jenkins one-at-a-time hash function.
///
/// The original algorithm was proposed by Bob Jenkins in 1997 in a Dr. Dobbs
/// article.  It is a non-cryptographic hash-function for multi-byte keys.
/// Our implementation produces a 64-bit hashing two bytes at time.
///
/// Typical Usage:
/// @code
/// ValueType value;
/// uint8_t ultimateSeed = 42;
/// auto hash = shad::HashFunction(value, ultimateSeed);
/// @endcode
///
/// @tparam KeyTy The type of the key to hash.
///
/// @param[in] key The key to be hashed.
/// @param[in] seed A random seed for the hashing process.
/// @return A 8-bytes long hash value.
template <typename KeyTy>
uint64_t HashFunction(const KeyTy &key, uint8_t seed) {
  constexpr size_t keyWords = sizeof(KeyTy) / sizeof(uint8_t);

  uint8_t const *const key_uint8 = reinterpret_cast<uint8_t const *const>(&key);

  uint64_t hash = 0;
  for (size_t i = 0; i < keyWords; ++i) {
    hash += key_uint8[i] + seed;  // NOLINT
    hash += (hash << 10);
    hash ^= (hash >> 6);
  }
  hash += (hash << 3);
  hash ^= (hash >> 11);
  hash += (hash << 15);
  return hash;
}

/// @brief Jenkins one-at-a-time hash function for std::vector.
///
/// The original algorithm was proposed by Bob Jenkins in 1997 in a Dr. Dobbs
/// article.  It is a non-cryptographic hash-function for multi-byte keys.  Our
/// implementation produces a 64-bit hashing two bytes at time.  This
/// specialization use the content of the std::vector to produce the hash value.
///
/// Typical Usage:
/// @code
/// ValueType value;
/// uint8_t ultimateSeed = 42;
/// auto hash = shad::HashFunction(value, ultimateSeed);
/// @endcode
///
/// @tparam KeyTy The type of the key to hash.
///
/// @param[in] key The std::vector storing the byte sequence to be hashed.
/// @param[in] seed A random seed for the hashing process.
/// @return A 8-bytes long hash value.
template <typename KeyTy>
uint64_t HashFunction(const std::vector<KeyTy> &key, uint8_t seed) {
  uint16_t const *const key_uint16 =
      reinterpret_cast<uint16_t const *const>(key.data());

  uint64_t hash = 0;
  size_t keyWords = (sizeof(KeyTy) * key.size()) / sizeof(uint16_t);

  for (size_t i = 0; i < keyWords; ++i) {
    hash += key_uint16[i] + seed;
    hash += (hash << 10);
    hash ^= (hash >> 6);
  }
  hash += (hash << 3);
  hash ^= (hash >> 11);
  hash += (hash << 15);
  return hash;
}

template <typename Key, bool=is_std_hashable<Key>::value>
struct hash {
  size_t operator()(const Key &k) const noexcept { return hasher(k); }
  std::hash<Key> hasher;
};

template <typename Key>
struct hash<Key, false> {
  size_t operator()(const Key &k) const noexcept {
    return shad::HashFunction(k, 0u);
  }
};

}  // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_COMPARE_AND_HASH_UTILS_H_
