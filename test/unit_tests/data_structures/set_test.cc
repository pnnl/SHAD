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

#include <vector>

#include "gtest/gtest.h"

#include "shad/data_structures/set.h"
#include "shad/runtime/runtime.h"

class SetTest : public ::testing::Test {
 public:
  SetTest() {}
  void SetUp() {}
  void TearDown() {}
  static const uint64_t kToInsert = 4096;
  static const uint64_t kNumBuckets = kToInsert / 16;
  static const uint64_t kElementsPerEntry = 3;
  static const uint64_t kMagicValue = 9999;

  struct Entry {
    uint64_t element[kElementsPerEntry];
    friend std::ostream &operator<<(std::ostream &os, const Entry &rhs) {
      return os << rhs.element[0];
    }
  };

  static void FillEntry(Entry *entry, uint64_t key_seed) {
    uint64_t i;
    for (i = 0; i < kElementsPerEntry; ++i) {
      entry->element[i] = (key_seed + i);
    }
  }

  static void CheckElement(const Entry *entry, const uint64_t key_seed) {
    uint64_t i;
    for (i = 0; i < kElementsPerEntry; ++i) {
      ASSERT_EQ(entry->element[i], (key_seed + i));
    }
  }

  // Returns the seed used for this key
  static uint64_t GetSeed(const Entry *entry) { return entry->element[0]; }

  static void DoInsert(shad::Set<Entry>::ObjectID oid,
                       const uint64_t key_seed) {
    auto setPtr = shad::Set<Entry>::GetPtr(oid);
    Entry entry;
    FillEntry(&entry, key_seed);
    setPtr->Insert(entry);
  }

  static void DoAsyncInsert(shad::rt::Handle &handle,
                            shad::Set<Entry>::ObjectID oid,
                            const uint64_t key_seed) {
    auto setPtr = shad::Set<Entry>::GetPtr(oid);
    Entry entry;
    FillEntry(&entry, key_seed);
    setPtr->AsyncInsert(handle, entry);
  }

  static bool DoFind(shad::Set<Entry>::ObjectID oid, const uint64_t key_seed) {
    auto setPtr = shad::Set<Entry>::GetPtr(oid);
    Entry entry;
    FillEntry(&entry, key_seed);
    return setPtr->Find(entry);
  }

  static void DoAsyncFind(shad::rt::Handle &handle,
                          shad::Set<Entry>::ObjectID oid,
                          const uint64_t key_seed, bool *found) {
    auto setPtr = shad::Set<Entry>::GetPtr(oid);
    Entry entry;
    FillEntry(&entry, key_seed);
    setPtr->AsyncFind(handle, entry, found);
  }

  static void InsertTestParallelFunc(
      shad::rt::Handle & /*unused*/,
      const std::tuple<shad::Set<Entry>::ObjectID, size_t> &t,
      const size_t iter) {
    const uint64_t start_it = std::get<1>(t);
    DoInsert(std::get<0>(t), start_it + iter);
  }

  static void FindTestParallelFunc(
      const std::tuple<shad::Set<Entry>::ObjectID, size_t> &t,
      const size_t iter) {
    const size_t start_it = std::get<1>(t);
    ASSERT_TRUE(DoFind(std::get<0>(t), start_it + iter));
  }
};

TEST_F(SetTest, InsertFindTest) {
  auto setPtr = shad::Set<Entry>::Create(kToInsert);
  auto oid = setPtr->GetGlobalID();
  uint64_t i;
  for (i = 1; i <= kToInsert; i++) {
    DoInsert(oid, i);
  }
  size_t toinsert = kToInsert;
  ASSERT_EQ(setPtr->Size(), toinsert);
  for (i = 1; i <= kToInsert; i++) {
    ASSERT_TRUE(DoFind(oid, i));
  }
  ASSERT_FALSE(DoFind(oid, 1234567890));
  shad::Set<Entry>::Destroy(oid);
}

TEST_F(SetTest, AsyncInsertFindTest) {
  auto setPtr = shad::Set<Entry>::Create(kToInsert);
  auto oid = setPtr->GetGlobalID();
  uint64_t i;
  shad::rt::Handle handle;
  for (i = 1; i <= kToInsert; i++) {
    DoAsyncInsert(handle, oid, i);
  }
  shad::rt::waitForCompletion(handle);
  size_t toinsert = kToInsert;
  ASSERT_EQ(setPtr->Size(), toinsert);
  for (i = 1; i <= kToInsert; i++) {
    ASSERT_TRUE(DoFind(oid, i));
  }
  ASSERT_FALSE(DoFind(oid, 1234567890));
  shad::Set<Entry>::Destroy(oid);
}

TEST_F(SetTest, AsyncInsertAsyncFindTest) {
  auto setPtr = shad::Set<Entry>::Create(kToInsert);
  auto oid = setPtr->GetGlobalID();
  uint64_t i;
  shad::rt::Handle handle;
  for (i = 1; i <= kToInsert; i++) {
    DoAsyncInsert(handle, oid, i);
  }
  shad::rt::waitForCompletion(handle);
  bool *found = new bool[kToInsert];
  for (i = 1; i < kToInsert; i++) {
    DoAsyncFind(handle, oid, i, &found[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (i = 1; i < kToInsert; i++) {
    ASSERT_TRUE(found[i]);
  }
  delete[] found;
  shad::Set<Entry>::Destroy(oid);
}

TEST_F(SetTest, InsertFindParallel) {
  auto setPtr = shad::Set<Entry>::Create(kToInsert);
  auto oid = setPtr->GetGlobalID();
  size_t it_chunk = 1;
  shad::rt::Handle handle;
  for (size_t i = 0; i < kToInsert;) {
    auto args = std::make_tuple(oid, i);
    shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                             InsertTestParallelFunc, args,
                             kToInsert / it_chunk);
    i += (kToInsert / it_chunk);
  }
  shad::rt::waitForCompletion(handle);
  size_t toinsert = kToInsert;
  ASSERT_EQ(setPtr->Size(), toinsert);
  for (size_t i = 0; i < kToInsert;) {
    auto args = std::make_tuple(oid, i);
    shad::rt::forEachAt(shad::rt::thisLocality(), FindTestParallelFunc, args,
                        kToInsert / it_chunk);
    i += (kToInsert / it_chunk);
  }
  shad::Set<Entry>::Destroy(oid);
}

TEST_F(SetTest, Erase) {
  auto setPtr = shad::Set<Entry>::Create(kToInsert);
  auto oid = setPtr->GetGlobalID();
  size_t it_chunk = 1;
  shad::rt::Handle handle;
  for (size_t i = 0; i < kToInsert;) {
    auto args = std::make_tuple(oid, i);
    shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                             InsertTestParallelFunc, args,
                             kToInsert / it_chunk);
    i += (kToInsert / it_chunk);
  }
  shad::rt::waitForCompletion(handle);
  size_t currSize = setPtr->Size();
  size_t i;
  for (i = 0; i < kToInsert; i++) {
    if ((i % 3) != 0u) {
      Entry k;
      FillEntry(&k, i);
      setPtr->Erase(k);
      currSize--;
    }
  }
  ASSERT_EQ(setPtr->Size(), currSize);
  for (i = 0; i < kToInsert; i++) {
    Entry k;
    FillEntry(&k, i);
    bool found = setPtr->Find(k);
    if ((i % 3) != 0u) {
      ASSERT_FALSE(found);
    } else {
      ASSERT_TRUE(found);
    }
  }
  shad::Set<Entry>::Destroy(oid);
}

TEST_F(SetTest, AsyncErase) {
  auto setPtr = shad::Set<Entry>::Create(kToInsert);
  auto oid = setPtr->GetGlobalID();
  size_t it_chunk = 1;
  shad::rt::Handle handle;
  for (size_t i = 0; i < kToInsert; i += (kToInsert / it_chunk)) {
    auto args = std::make_tuple(oid, i);
    shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                             InsertTestParallelFunc, args,
                             kToInsert / it_chunk);
  }
  shad::rt::waitForCompletion(handle);
  size_t currSize = setPtr->Size();
  size_t i;
  for (i = 0; i < kToInsert; i++) {
    if ((i % 3) != 0u) {
      Entry k;
      FillEntry(&k, i);
      setPtr->AsyncErase(handle, k);
      currSize--;
    }
  }
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(setPtr->Size(), currSize);
  for (i = 0; i < kToInsert; i++) {
    Entry k;
    FillEntry(&k, i);
    bool res = setPtr->Find(k);
    if ((i % 3) != 0u) {
      ASSERT_FALSE(res);
    } else {
      ASSERT_TRUE(res);
    }
  }
  shad::Set<Entry>::Destroy(oid);
}

TEST_F(SetTest, ForEachElement) {
  auto setPtr = shad::Set<Entry>::Create(kToInsert);
  auto oid = setPtr->GetGlobalID();
  auto args = std::make_tuple(oid, 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  auto ForEachElementLambda0args = [](const Entry &entry) {
    CheckElement(&entry, GetSeed(&entry));
  };
  auto ForEachElementLambda1arg = [](const Entry &entry, uint64_t &magicValue) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckElement(&entry, GetSeed(&entry));
  };
  auto ForEachElementLambda = [](const Entry &entry, uint64_t &magicValue,
                                 uint64_t &value2) {
    ASSERT_TRUE(magicValue == kMagicValue);
    ASSERT_TRUE(value2 == kMagicValue * 2);
    CheckElement(&entry, GetSeed(&entry));
  };
  uint64_t magicValue = kMagicValue;
  uint64_t value2 = kMagicValue * 2;
  setPtr->ForEachElement(ForEachElementLambda0args);
  setPtr->ForEachElement(ForEachElementLambda1arg, magicValue);
  setPtr->ForEachElement(ForEachElementLambda, magicValue, value2);
  shad::Set<Entry>::Destroy(oid);
}

TEST_F(SetTest, AsyncForEachElement) {
  auto setPtr = shad::Set<Entry>::Create(kToInsert);
  auto oid = setPtr->GetGlobalID();
  auto args = std::make_tuple(oid, 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  auto AsyncForEachElementLambda0args = [](shad::rt::Handle &,
                                           const Entry &entry) {
    CheckElement(&entry, GetSeed(&entry));
  };
  auto AsyncForEachElementLambda1arg =
      [](shad::rt::Handle &, const Entry &entry, uint64_t &magicValue) {
        ASSERT_TRUE(magicValue == kMagicValue);
        CheckElement(&entry, GetSeed(&entry));
      };
  auto AsyncForEachElementLambda = [](shad::rt::Handle &, const Entry &entry,
                                      uint64_t &magicValue, uint64_t &value2) {
    ASSERT_TRUE(magicValue == kMagicValue);
    ASSERT_TRUE(value2 == kMagicValue * 2);
    CheckElement(&entry, GetSeed(&entry));
  };
  uint64_t magicValue = kMagicValue;
  uint64_t value2 = kMagicValue * 2;
  setPtr->AsyncForEachElement(handle, AsyncForEachElementLambda0args);
  setPtr->AsyncForEachElement(handle, AsyncForEachElementLambda1arg,
                              magicValue);
  setPtr->AsyncForEachElement(handle, AsyncForEachElementLambda, magicValue,
                              value2);
  shad::rt::waitForCompletion(handle);
  shad::Set<Entry>::Destroy(oid);
}
