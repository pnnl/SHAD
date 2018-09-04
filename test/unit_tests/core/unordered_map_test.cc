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

#include "gtest/gtest.h"

#include "shad/core/unordered_map.h"

static const uint64_t kKeysPerEntry = 3;
static const uint64_t kValuesPerEntry = 5;

struct Key {
  uint64_t key[kKeysPerEntry];
  friend std::ostream &operator<<(std::ostream &os, const Key &rhs) {
    return os << rhs.key[0];
  }
};

struct Value {
  uint64_t value[kValuesPerEntry];
  friend std::ostream &operator<<(std::ostream &os, const Value &rhs) {
    return os << rhs.value[0];
  }
};

static void FillKey(Key *keys, uint64_t key_seed) {
  for (uint64_t i = 0; i < kKeysPerEntry; ++i) {
    keys->key[i] = (key_seed + i);
  }
}

static void FillValue(Value *values, uint64_t value_seed) {
  for (uint64_t i = 0; i < kValuesPerEntry; ++i) {
    values->value[i] = (value_seed + i);
  }
}

using T = shad::unordered_map<Key, Value>;

static void CheckKeyValue(typename T::iterator entry, uint64_t key_seed,
                          uint64_t value_seed) {
  auto &obs_keys((*entry).first);
  auto &obs_values((*entry).second);
  Key exp_keys;
  Value exp_values;
  FillKey(&exp_keys, key_seed);
  FillValue(&exp_values, value_seed);
  for (uint64_t i = 0; i < kKeysPerEntry; ++i)
    ASSERT_EQ(obs_keys.key[i], exp_keys.key[i]);
  for (uint64_t i = 0; i < kValuesPerEntry; ++i)
    ASSERT_EQ(obs_values.value[i], obs_values.value[i]);
}

static std::pair<typename T::iterator, bool> DoInsert(
    T *h0, const uint64_t key_seed, const uint64_t value_seed) {
  Key keys;
  Value values;
  FillKey(&keys, key_seed);
  FillValue(&values, value_seed);
  return (h0->insert(std::make_pair(keys, values)));
}

static const uint64_t kToInsert = 4096;

TEST(unordered_map, InsertReturnTest) {
  T c(kToInsert);
  uint64_t i;

  // successful inserts
  for (i = 1; i <= kToInsert; i++) {
    auto res = DoInsert(&c, i, i + 11);
    ASSERT_TRUE(res.second);
    CheckKeyValue(res.first, i, i + 11);
  }

  // failing inserts
  for (i = 1; i <= kToInsert; i++) {
    auto res = DoInsert(&c, i, i + 11);
    ASSERT_FALSE(res.second);
    CheckKeyValue(res.first, i, i + 11);
  }
}
