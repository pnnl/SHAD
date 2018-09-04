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

#include "shad/data_structures/local_hashmap.h"
#include "shad/runtime/runtime.h"

class LocalHashmapTest : public ::testing::Test {
 public:
  LocalHashmapTest() : hmap(kNumBuckets) {}
  void SetUp() {}
  void TearDown() {}
  static const uint64_t kToInsert = 4096;
  static const uint64_t kNumBuckets = kToInsert / 16;
  static const uint64_t kKeysPerEntry = 3;
  static const uint64_t kValuesPerEntry = 5;
  static const uint64_t kMagicValue = 9999;

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

  typedef shad::LocalHashmap<Key, Value> HashmapType;
  HashmapType hmap;
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

  static void CheckValue(const Value *values, const uint64_t value_seed) {
    for (uint64_t i = 0; i < kValuesPerEntry; ++i) {
      ASSERT_EQ(values->value[i], (value_seed + i));
    }
  }

  static void CheckKey(const Key *keys, const uint64_t key_seed) {
    for (uint64_t i = 0; i < kKeysPerEntry; ++i) {
      ASSERT_EQ(keys->key[i], (key_seed + i));
    }
  }

  static void CheckKeyValue(typename HashmapType::iterator entry,
                            uint64_t key_seed, uint64_t value_seed) {
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

  // Returns the seed used for this key
  static uint64_t GetSeed(const Key *keys) { return keys->key[0]; }

  // Returns the seed used for this value
  static uint64_t GetSeed(const Value *values) { return values->value[0]; }

  static std::pair<typename HashmapType::iterator, bool> DoInsert(
      HashmapType *h0, const uint64_t key_seed, const uint64_t value_seed) {
    Key keys;
    Value values;
    FillKey(&keys, key_seed);
    FillValue(&values, value_seed);
    return (h0->Insert(keys, values));
  }

  static void DoAsyncInsert(shad::rt::Handle &handle, HashmapType *h0,
                            const uint64_t key_seed,
                            const uint64_t value_seed) {
    Key keys;
    Value values;
    FillKey(&keys, key_seed);
    FillValue(&values, value_seed);
    h0->AsyncInsert(handle, keys, values);
  }

  static bool DoLookup(HashmapType *h0, const uint64_t key_seed,
                       Value **values) {
    Key keys;
    FillKey(&keys, key_seed);
    *values = h0->Lookup(keys);
    return *values != nullptr;
  }

  static void DoAsyncLookup(shad::rt::Handle &handle, HashmapType *h0,
                            const uint64_t key_seed, Value **values) {
    Key keys;
    FillKey(&keys, key_seed);
    h0->AsyncLookup(handle, keys, values);
    // h0->Lookup(keys, values);
  }

  static void DoAsyncLookup2(shad::rt::Handle &handle, HashmapType *h0,
                             const uint64_t key_seed,
                             HashmapType::LookupResult *values) {
    Key keys;
    FillKey(&keys, key_seed);
    h0->AsyncLookup(handle, keys, values);
  }

  static void InsertTestParallelFunc(shad::rt::Handle & /*unused*/,
                                     const std::tuple<HashmapType *, size_t> &t,
                                     const size_t iter) {
    HashmapType *hm = std::get<0>(t);
    const uint64_t start_it = std::get<1>(t);
    DoInsert(hm, start_it + iter, start_it + iter);
  }

  static void
  LookupTestParallelFunc(  // HashmapType &hm, const size_t start_it,
      const std::tuple<HashmapType *, size_t> &t, const size_t iter) {
    HashmapType *hm = std::get<0>(t);
    const uint64_t start_it = std::get<1>(t);
    Value *values;
    ASSERT_TRUE(DoLookup(hm, start_it + iter, &values));
    CheckValue(values, start_it + iter);
  }
};

TEST_F(LocalHashmapTest, InsertLookupTest) {
  HashmapType hmap(kNumBuckets);
  uint64_t i;
  for (i = 1; i <= kToInsert; i++) {
    DoInsert(&hmap, i, i + 11);
  }
  size_t toinsert = kToInsert;
  ASSERT_EQ(hmap.Size(), toinsert);

  // Lookup
  Value *values;
  for (i = 1; i <= kToInsert; i++) {
    ASSERT_TRUE(DoLookup(&hmap, i, &values));
    CheckValue(values, i + 11);
  }
  ASSERT_FALSE(DoLookup(&hmap, 1234567890, &values));
}

TEST_F(LocalHashmapTest, InsertReturnTest) {
  HashmapType set(kNumBuckets);
  uint64_t i;

  // successful inserts
  for (i = 1; i <= kToInsert; i++) {
    auto res = DoInsert(&hmap, i, i + 11);
    ASSERT_TRUE(res.second);
    CheckKeyValue(res.first, i, i + 11);
  }

  // overwriting inserts
  for (i = 1; i <= kToInsert; i++) {
    auto res = DoInsert(&hmap, i, i + 11);
    ASSERT_TRUE(res.second);
    CheckKeyValue(res.first, i, i + 11);
  }
}

TEST_F(LocalHashmapTest, AsyncInsertLookupTest) {
  HashmapType hmap(kNumBuckets);
  uint64_t i;
  shad::rt::Handle handle;
  for (i = 1; i <= kToInsert; i++) {
    DoAsyncInsert(handle, &hmap, i, i + 11);
  }
  shad::rt::waitForCompletion(handle);
  size_t toinsert = kToInsert;
  ASSERT_EQ(hmap.Size(), toinsert);
  // Lookup
  Value *values;
  for (i = 1; i <= kToInsert; i++) {
    ASSERT_TRUE(DoLookup(&hmap, i, &values));
    CheckValue(values, i + 11);
  }
  ASSERT_FALSE(DoLookup(&hmap, 1234567890, &values));
}

TEST_F(LocalHashmapTest, AsyncInsertAsyncLookupTest) {
  HashmapType hmap(kNumBuckets);
  uint64_t i;
  shad::rt::Handle handle;
  for (i = 1; i <= kToInsert; i++) {
    DoAsyncInsert(handle, &hmap, i, i + 11);
  }
  shad::rt::waitForCompletion(handle);
  // Lookup
  Value **values = new Value *[kToInsert];
  for (i = 1; i < kToInsert; i++) {
    DoAsyncLookup(handle, &hmap, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (i = 1; i < kToInsert; i++) {
    ASSERT_NE(values[i], nullptr);
    CheckValue(values[i], i + 11);
  }
  delete[] values;
}

TEST_F(LocalHashmapTest, AsyncInsertAsyncLookup2Test) {
  HashmapType hmap(kNumBuckets);
  uint64_t i;
  shad::rt::Handle handle;
  for (i = 1; i <= kToInsert; i++) {
    DoAsyncInsert(handle, &hmap, i, i + 11);
  }
  shad::rt::waitForCompletion(handle);
  // Lookup
  HashmapType::LookupResult *values = new HashmapType::LookupResult[kToInsert];
  for (i = 1; i < kToInsert; i++) {
    DoAsyncLookup2(handle, &hmap, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (i = 1; i < kToInsert; i++) {
    CheckValue(&values[i].value, i + 11);
  }
  delete[] values;
}

TEST_F(LocalHashmapTest, InsertLookupParallel1) {
  HashmapType hmap(kNumBuckets);
  size_t it_chunk = 1;
  shad::rt::Handle handle;
  for (size_t i = 0; i < kToInsert;) {
    auto args = std::make_tuple(&hmap, i);
    shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                             InsertTestParallelFunc, args,
                             kToInsert / it_chunk);
    i += (kToInsert / it_chunk);
  }
  shad::rt::waitForCompletion(handle);
  size_t toinsert = kToInsert;
  ASSERT_EQ(hmap.Size(), toinsert);
  for (size_t i = 0; i < kToInsert;) {
    auto args = std::make_tuple(&hmap, i);
    shad::rt::forEachAt(shad::rt::thisLocality(), LookupTestParallelFunc, args,
                        kToInsert / it_chunk);
    i += (kToInsert / it_chunk);
  }
}

TEST_F(LocalHashmapTest, Erase) {
  HashmapType hmap(kNumBuckets);
  size_t it_chunk = 1;
  shad::rt::Handle handle;
  for (size_t i = 0; i < kToInsert;) {
    auto args = std::make_tuple(&hmap, i);
    shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                             InsertTestParallelFunc, args,
                             kToInsert / it_chunk);
    i += (kToInsert / it_chunk);
  }
  shad::rt::waitForCompletion(handle);
  size_t currSize = hmap.Size();
  size_t i;
  for (i = 0; i < kToInsert; i++) {
    if ((i % 3) != 0u) {
      Key k;
      FillKey(&k, i);
      hmap.Erase(k);
      currSize--;
    }
  }
  ASSERT_EQ(hmap.Size(), currSize);
  for (i = 0; i < kToInsert; i++) {
    Key k;
    FillKey(&k, i);
    Value *res = hmap.Lookup(k);
    if ((i % 3) != 0u) {
      ASSERT_EQ(res, nullptr);
    } else {
      ASSERT_NE(res, nullptr);
      CheckValue(res, i);
    }
  }
}

TEST_F(LocalHashmapTest, AsyncErase) {
  HashmapType hmap(kNumBuckets);
  size_t it_chunk = 1;
  shad::rt::Handle handle;
  for (size_t i = 0; i < kToInsert;) {
    auto args = std::make_tuple(&hmap, i);
    shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                             InsertTestParallelFunc, args,
                             kToInsert / it_chunk);
    i += (kToInsert / it_chunk);
  }
  shad::rt::waitForCompletion(handle);
  size_t currSize = hmap.Size();
  size_t i;
  for (i = 0; i < kToInsert; i++) {
    if ((i % 3) != 0u) {
      Key k;
      FillKey(&k, i);
      hmap.AsyncErase(handle, k);
      currSize--;
    }
  }
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(hmap.Size(), currSize);
  // hmap.Print(printfun);
  for (i = 0; i < kToInsert; i++) {
    Key k;
    FillKey(&k, i);
    Value *res = hmap.Lookup(k);
    if ((i % 3) != 0u) {
      ASSERT_EQ(res, nullptr);
    } else {
      ASSERT_NE(res, nullptr);
      CheckValue(res, i);
    }
  }
}

TEST_F(LocalHashmapTest, ForEachEntry) {
  HashmapType hmap(kNumBuckets);
  auto args = std::make_tuple(&hmap, 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  uint64_t cnt = 0;
  auto VisitLambda0args = [](const Key &key, Value &value) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
  };
  auto VisitLambda1arg = [](const Key &key, Value &value, uint64_t *&cntPtr) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
    __sync_fetch_and_add(cntPtr, 1);
  };
  auto VisitLambda = [](const Key &key, Value &value, uint64_t &magicValue,
                        uint64_t *&cntPtr) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
    __sync_fetch_and_add(cntPtr, 1);
  };
  uint64_t magicValue = kMagicValue;
  uint64_t *cntPtr = &cnt;
  hmap.ForEachEntry(VisitLambda0args);
  hmap.ForEachEntry(VisitLambda1arg, cntPtr);
  hmap.ForEachEntry(VisitLambda, magicValue, cntPtr);
  auto toinsert = kToInsert;
  ASSERT_EQ(cnt, toinsert * 2);
}

TEST_F(LocalHashmapTest, AsyncForEachEntry) {
  HashmapType hmap(kNumBuckets);
  auto args = std::make_tuple(&hmap, 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  uint64_t cnt = 0;
  auto AsyncVisitLambda0args = [](shad::rt::Handle &, const Key &key,
                                  Value &value) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
  };
  auto AsyncVisitLambda1arg = [](shad::rt::Handle &, const Key &key,
                                 Value &value, uint64_t *&cntPtr) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
    __sync_fetch_and_add(cntPtr, 1);
  };
  auto AsyncVisitLambda = [](shad::rt::Handle &, const Key &key, Value &value,
                             uint64_t &magicValue, uint64_t *&cntPtr) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
    __sync_fetch_and_add(cntPtr, 1);
  };
  uint64_t magicValue = kMagicValue;
  uint64_t *cntPtr = &cnt;
  hmap.AsyncForEachEntry(handle, AsyncVisitLambda0args);
  hmap.AsyncForEachEntry(handle, AsyncVisitLambda1arg, cntPtr);
  hmap.AsyncForEachEntry(handle, AsyncVisitLambda, magicValue, cntPtr);
  shad::rt::waitForCompletion(handle);
  auto toinsert = kToInsert;
  ASSERT_EQ(cnt, toinsert * 2);
}

TEST_F(LocalHashmapTest, ForEachKey) {
  HashmapType hmap(kNumBuckets);
  auto args = std::make_tuple(&hmap, 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  uint64_t cnt = 0;
  auto ForEachKeyLambda0args = [](const Key &key) {
    CheckKey(&key, GetSeed(&key));
  };
  auto ForEachKeyLambda1arg = [](const Key &key, uint64_t *&cntPtr) {
    CheckKey(&key, GetSeed(&key));
    __sync_fetch_and_add(cntPtr, 1);
  };
  auto ForEachKeyLambda = [](const Key &key, uint64_t &magicValue,
                             uint64_t *&cntPtr) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    __sync_fetch_and_add(cntPtr, 1);
  };
  uint64_t magicValue = kMagicValue;
  uint64_t *cntPtr = &cnt;
  hmap.ForEachKey(ForEachKeyLambda0args);
  hmap.ForEachKey(ForEachKeyLambda1arg, cntPtr);
  hmap.ForEachKey(ForEachKeyLambda, magicValue, cntPtr);
  auto toinsert = kToInsert;
  ASSERT_EQ(cnt, toinsert * 2);
}

TEST_F(LocalHashmapTest, AsyncForEachKey) {
  HashmapType hmap(kNumBuckets);
  auto args = std::make_tuple(&hmap, 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  uint64_t cnt = 0;
  uint64_t magicValue = kMagicValue;
  uint64_t *cntPtr = &cnt;
  auto AsyncForEachKeyLambda0args = [](shad::rt::Handle &, const Key &key) {
    CheckKey(&key, GetSeed(&key));
  };
  auto AsyncForEachKeyLambda1arg = [](shad::rt::Handle &, const Key &key,
                                      uint64_t *&cntPtr) {
    CheckKey(&key, GetSeed(&key));
    __sync_fetch_and_add(cntPtr, 1);
  };
  auto AsyncForEachKeyLambda = [](shad::rt::Handle &, const Key &key,
                                  uint64_t &magicValue, uint64_t *&cntPtr) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    __sync_fetch_and_add(cntPtr, 1);
  };
  hmap.AsyncForEachKey(handle, AsyncForEachKeyLambda0args);
  hmap.AsyncForEachKey(handle, AsyncForEachKeyLambda1arg, cntPtr);
  hmap.AsyncForEachKey(handle, AsyncForEachKeyLambda, magicValue, cntPtr);
  shad::rt::waitForCompletion(handle);
  auto toinsert = kToInsert;
  ASSERT_EQ(cnt, toinsert * 2);
}

TEST_F(LocalHashmapTest, Apply) {
  HashmapType hmap(kNumBuckets);
  auto args = std::make_tuple(&hmap, 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);

  auto toinsert = kToInsert;
  ASSERT_EQ(hmap.Size(), toinsert);

  uint64_t cnt = 0;
  auto ApplyLambda0args = [](const Key &key, Value &value) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
  };
  auto ApplyLambda1arg = [](const Key &key, Value &value, uint64_t *&cntPtr) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
    __sync_fetch_and_add(cntPtr, 1);
  };
  auto ApplyLambda = [](const Key &key, Value &value, uint64_t &magicValue,
                        uint64_t *&cntPtr) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
    __sync_fetch_and_add(cntPtr, 1);
  };

  uint64_t magicValue = kMagicValue;
  uint64_t *cntPtr = &cnt;
  for (size_t i = 0; i < kToInsert; i++) {
    Key keys;
    FillKey(&keys, i);
    hmap.Apply(keys, ApplyLambda0args);
    hmap.Apply(keys, ApplyLambda1arg, cntPtr);
    hmap.Apply(keys, ApplyLambda, magicValue, cntPtr);
  }
  ASSERT_EQ(cnt, toinsert * 2);
}

TEST_F(LocalHashmapTest, AsyncApply) {
  HashmapType hmap(kNumBuckets);
  auto args = std::make_tuple(&hmap, 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);

  auto AsyncApplyLambda0args = [](shad::rt::Handle &, const Key &key,
                                  Value &value) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
  };
  auto AsyncApplyLambda1arg = [](shad::rt::Handle &, const Key &key,
                                 Value &value, uint64_t *&cntPtr) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
    __sync_fetch_and_add(cntPtr, 1);
  };
  auto AsyncApplyLambda = [](shad::rt::Handle &, const Key &key, Value &value,
                             uint64_t &magicValue, uint64_t *&cntPtr) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
    __sync_fetch_and_add(cntPtr, 1);
  };

  auto toinsert = kToInsert;
  ASSERT_EQ(toinsert, hmap.Size());

  uint64_t cnt = 0;
  uint64_t magicValue = kMagicValue;
  uint64_t *cntPtr = &cnt;
  for (size_t i = 0; i < kToInsert; i++) {
    Key keys;
    FillKey(&keys, i);
    hmap.AsyncApply(handle, keys, AsyncApplyLambda0args);
    hmap.AsyncApply(handle, keys, AsyncApplyLambda1arg, cntPtr);
    hmap.AsyncApply(handle, keys, AsyncApplyLambda, magicValue, cntPtr);
  }
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(cnt, toinsert * 2);
}
