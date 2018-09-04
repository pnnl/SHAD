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

#include "shad/core/unordered_set.h"

static const uint64_t kElementsPerEntry = 3;

struct Entry {
  uint64_t element[kElementsPerEntry];
  friend std::ostream &operator<<(std::ostream &os, const Entry &rhs) {
    return os << rhs.element[0];
  }
};

using T = shad::unordered_set<Entry>;

static void FillEntry(Entry *entry, uint64_t key_seed) {
  uint64_t i;
  for (i = 0; i < kElementsPerEntry; ++i) {
    entry->element[i] = (key_seed + i);
  }
}

static void CheckElement(typename T::iterator entry, const uint64_t key_seed) {
  uint64_t i;
  for (i = 0; i < kElementsPerEntry; ++i) {
    ASSERT_EQ((*entry).element[i], (key_seed + i));
  }
}

static std::pair<typename T::iterator, bool> DoInsert(T *setPtr,
                                                      const uint64_t key_seed) {
  Entry entry;
  FillEntry(&entry, key_seed);
  return setPtr->insert(entry);
}

constexpr int kToInsert = 1024;

TEST(unordered_set, InsertReturnTest) {
  std::pair<typename shad::unordered_set<Entry>::iterator, bool> res;

  // successful inserts
  shad::unordered_set<Entry> s(kToInsert);
  for (uint64_t i = 0; i < kToInsert; ++i) {
    res = DoInsert(&s, i);
    ASSERT_TRUE(res.second);
    CheckElement(res.first, i);
  }

  // failing inserts
  for (uint64_t i = 0; i < kToInsert; ++i) {
    res = DoInsert(&s, i);
    ASSERT_FALSE(res.second);
    CheckElement(res.first, i);
  }
}
