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

#include "shad/data_structures/local_set.h"
#include "shad/runtime/runtime.h"

class LocalSetTest : public ::testing::Test {
 public:
  LocalSetTest() {}
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

  static void CheckElement(typename shad::LocalSet<Entry>::iterator entry,
                           const uint64_t key_seed) {
    uint64_t i;
    for (i = 0; i < kElementsPerEntry; ++i) {
      ASSERT_EQ((*entry).element[i], (key_seed + i));
    }
  }

  // Returns the seed used for this key
  static uint64_t GetSeed(const Entry *entry) { return entry->element[0]; }

  static std::pair<typename shad::LocalSet<Entry>::iterator, bool>
  DoInsert(shad::LocalSet<Entry> *setPtr, const uint64_t key_seed) {
    Entry entry;
    FillEntry(&entry, key_seed);
    return setPtr->Insert(entry);
  }

  static void DoAsyncInsert(shad::rt::Handle &handle,
                            shad::LocalSet<Entry> *setPtr,
                            const uint64_t key_seed) {
    Entry entry;
    FillEntry(&entry, key_seed);
    setPtr->AsyncInsert(handle, entry);
  }

  static bool DoFind(shad::LocalSet<Entry> *setPtr, const uint64_t key_seed) {
    Entry entry;
    FillEntry(&entry, key_seed);
    return setPtr->Find(entry);
  }

  static void DoAsyncFind(shad::rt::Handle &handle,
                          shad::LocalSet<Entry> *setPtr,
                          const uint64_t key_seed, bool *found) {
    Entry entry;
    FillEntry(&entry, key_seed);
    setPtr->AsyncFind(handle, entry, found);
  }

  static void InsertTestParallelFunc(
      shad::rt::Handle & /*unused*/,
      const std::tuple<shad::LocalSet<Entry> *, size_t> &t, const size_t iter) {
    auto *setPtr = std::get<0>(t);
    const uint64_t start_it = std::get<1>(t);
    DoInsert(setPtr, start_it + iter);
  }

  static void FindTestParallelFunc(
      const std::tuple<shad::LocalSet<Entry> *, size_t> &t, const size_t iter) {
    auto setPtr = std::get<0>(t);
    const size_t start_it = std::get<1>(t);
    ASSERT_TRUE(DoFind(setPtr, start_it + iter));
  }
};

TEST_F(LocalSetTest, InsertFindTest) {
  shad::LocalSet<Entry> set(kNumBuckets);
  uint64_t i;
  for (i = 1; i <= kToInsert; i++) {
    DoInsert(&set, i);
  }
  size_t toinsert = kToInsert;
  ASSERT_EQ(set.Size(), toinsert);
  for (i = 1; i <= kToInsert; i++) {
    ASSERT_TRUE(DoFind(&set, i));
  }
  ASSERT_FALSE(DoFind(&set, 1234567890));
}

TEST_F(LocalSetTest, InsertReturnTest) {
  shad::LocalSet<Entry> set(kNumBuckets);
  std::pair<typename shad::LocalSet<Entry>::iterator, bool> res;

  // successful inserts
  for (uint64_t i = 1; i <= kToInsert; i++) {
    res = DoInsert(&set, i);
    ASSERT_TRUE(res.second);
    CheckElement(res.first, i);
  }

  // failing inserts
  for (uint64_t i = 1; i <= kToInsert; i++) {
	res = DoInsert(&set, i);
    ASSERT_FALSE(res.second);
    CheckElement(res.first, i);
  }
}

TEST_F(LocalSetTest, AsyncInsertFindTest) {
  shad::LocalSet<Entry> set(kNumBuckets);
  uint64_t i;
  shad::rt::Handle handle;
  for (i = 1; i <= kToInsert; i++) {
    DoAsyncInsert(handle, &set, i);
  }
  shad::rt::waitForCompletion(handle);
  size_t toinsert = kToInsert;
  ASSERT_EQ(set.Size(), toinsert);
  for (i = 1; i <= kToInsert; i++) {
    ASSERT_TRUE(DoFind(&set, i));
  }
  ASSERT_FALSE(DoFind(&set, 1234567890));
}

TEST_F(LocalSetTest, AsyncInsertAsyncFindTest) {
  shad::LocalSet<Entry> set(kNumBuckets);
  uint64_t i;
  shad::rt::Handle handle;
  for (i = 1; i <= kToInsert; i++) {
    DoAsyncInsert(handle, &set, i);
  }
  shad::rt::waitForCompletion(handle);
  bool *found = new bool[kToInsert];
  for (i = 1; i < kToInsert; i++) {
    DoAsyncFind(handle, &set, i, &found[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (i = 1; i < kToInsert; i++) {
    ASSERT_TRUE(found[i]);
  }
  delete[] found;
}

TEST_F(LocalSetTest, InsertFindParallel) {
  shad::LocalSet<Entry> set(kNumBuckets);
  size_t it_chunk = 1;
  shad::rt::Handle handle;
  for (size_t i = 0; i < kToInsert;) {
    auto args = std::make_tuple(&set, i);
    shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                             InsertTestParallelFunc, args,
                             kToInsert / it_chunk);
    i += (kToInsert / it_chunk);
  }
  shad::rt::waitForCompletion(handle);
  size_t toinsert = kToInsert;
  ASSERT_EQ(set.Size(), toinsert);
  for (size_t i = 0; i < kToInsert;) {
    auto args = std::make_tuple(&set, i);
    shad::rt::forEachAt(shad::rt::thisLocality(), FindTestParallelFunc, args,
                        kToInsert / it_chunk);
    i += (kToInsert / it_chunk);
  }
}

TEST_F(LocalSetTest, Erase) {
  shad::LocalSet<Entry> set(kNumBuckets);
  size_t it_chunk = 1;
  shad::rt::Handle handle;
  for (size_t i = 0; i < kToInsert;) {
    auto args = std::make_tuple(&set, i);
    shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                             InsertTestParallelFunc, args,
                             kToInsert / it_chunk);
    i += (kToInsert / it_chunk);
  }
  shad::rt::waitForCompletion(handle);
  size_t currSize = set.Size();
  size_t i;
  for (i = 0; i < kToInsert; i++) {
    if ((i % 3) != 0u) {
      Entry k;
      FillEntry(&k, i);
      set.Erase(k);
      currSize--;
    }
  }
  ASSERT_EQ(set.Size(), currSize);
  for (i = 0; i < kToInsert; i++) {
    Entry k;
    FillEntry(&k, i);
    bool found = set.Find(k);
    if ((i % 3) != 0u) {
      ASSERT_FALSE(found);
    } else {
      ASSERT_TRUE(found);
    }
  }
}

TEST_F(LocalSetTest, AsyncErase) {
  shad::LocalSet<Entry> set(kNumBuckets);
  size_t it_chunk = 1;
  shad::rt::Handle handle;
  for (size_t i = 0; i < kToInsert;) {
    auto args = std::make_tuple(&set, i);
    shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                             InsertTestParallelFunc, args,
                             kToInsert / it_chunk);
    i += (kToInsert / it_chunk);
  }
  shad::rt::waitForCompletion(handle);
  size_t currSize = set.Size();
  size_t i;
  for (i = 0; i < kToInsert; i++) {
    if ((i % 3) != 0u) {
      Entry k;
      FillEntry(&k, i);
      set.AsyncErase(handle, k);
      currSize--;
    }
  }
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(set.Size(), currSize);
  for (i = 0; i < kToInsert; i++) {
    Entry k;
    FillEntry(&k, i);
    bool res = set.Find(k);
    if ((i % 3) != 0u) {
      ASSERT_FALSE(res);
    } else {
      ASSERT_TRUE(res);
    }
  }
}

TEST_F(LocalSetTest, ForEachElement) {
  shad::LocalSet<Entry> set(kNumBuckets);
  auto args = std::make_tuple(&set, 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  uint64_t cnt = 0;
  auto ForEachElementLambda = [](const Entry &entry, uint64_t &magicValue,
                                 uint64_t *&cntPtr) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckElement(&entry, GetSeed(&entry));
    __sync_fetch_and_add(cntPtr, 1);
  };
  auto ForEachElementLambda0args = [](const Entry &entry) {
    CheckElement(&entry, GetSeed(&entry));
  };
  auto ForEachElementLambda1arg = [](const Entry &entry, uint64_t *&cntPtr) {
    CheckElement(&entry, GetSeed(&entry));
    __sync_fetch_and_add(cntPtr, 1);
  };
  uint64_t *cntPtr = &cnt;
  uint64_t magicValue = kMagicValue;
  set.ForEachElement(ForEachElementLambda0args);
  set.ForEachElement(ForEachElementLambda1arg, cntPtr);
  set.ForEachElement(ForEachElementLambda, magicValue, cntPtr);
  auto toinsert = kToInsert * 2;
  ASSERT_EQ(cnt, toinsert);
}

TEST_F(LocalSetTest, AsyncForEachElement) {
  shad::LocalSet<Entry> set(kNumBuckets);
  auto args = std::make_tuple(&set, 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  uint64_t cnt = 0;
  auto AsyncForEachElementLambda = [](shad::rt::Handle &, const Entry &entry,
                                      uint64_t &magicValue, uint64_t *&cntPtr) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckElement(&entry, GetSeed(&entry));
    __sync_fetch_and_add(cntPtr, 1);
  };
  auto AsyncForEachElementLambda0args = [](shad::rt::Handle &,
                                           const Entry &entry) {
    CheckElement(&entry, GetSeed(&entry));
  };
  auto AsyncForEachElementLambda1arg =
      [](shad::rt::Handle &, const Entry &entry, uint64_t *&cntPtr) {
        CheckElement(&entry, GetSeed(&entry));
        __sync_fetch_and_add(cntPtr, 1);
      };
  uint64_t *cntPtr = &cnt;
  uint64_t magicValue = kMagicValue;
  set.AsyncForEachElement(handle, AsyncForEachElementLambda0args);
  set.AsyncForEachElement(handle, AsyncForEachElementLambda1arg, cntPtr);
  set.AsyncForEachElement(handle, AsyncForEachElementLambda, magicValue,
                          cntPtr);
  shad::rt::waitForCompletion(handle);
  auto toinsert = kToInsert * 2;
  ASSERT_EQ(cnt, toinsert);
}
