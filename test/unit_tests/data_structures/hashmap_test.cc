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

#include "shad/data_structures/hashmap.h"
#include "shad/runtime/runtime.h"

static const size_t kToInsert = 10000;

class HashmapTest : public ::testing::Test {
 public:
  HashmapTest() {}
  void SetUp() {}
  void TearDown() {}
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

  typedef shad::Hashmap<Key, Value> HashmapType;

  static void FillKey(Key *keys, uint64_t key_seed) {
    uint64_t i;
    for (i = 0; i < kKeysPerEntry; ++i) {
      keys->key[i] = (key_seed + i);
    }
  }

  static void FillValue(Value *values, uint64_t value_seed) {
    uint64_t i;
    for (i = 0; i < kValuesPerEntry; ++i) {
      values->value[i] = (value_seed + i);
    }
  }

  static void CheckValue(const Value *values, const uint64_t value_seed) {
    uint64_t i;
    for (i = 0; i < kValuesPerEntry; ++i) {
      ASSERT_EQ(values->value[i], (value_seed + i));
    }
  }

  static void CheckKey(const Key *keys, const uint64_t key_seed) {
    uint64_t i;
    for (i = 0; i < kKeysPerEntry; ++i) {
      ASSERT_EQ(keys->key[i], (key_seed + i));
    }
  }

  // Returns the seed used for this key
  static uint64_t GetSeed(const Key *keys) { return keys->key[0]; }

  // Returns the seed used for this value
  static uint64_t GetSeed(const Value *values) { return values->value[0]; }

  static void DoInsert(HashmapType::ObjectID oid, const uint64_t key_seed,
                       const uint64_t value_seed) {
    auto h0 = HashmapType::GetPtr(oid);
    Key keys;
    Value values;
    FillKey(&keys, key_seed);
    FillValue(&values, value_seed);
    h0->Insert(keys, values);
  }

  static void DoBufferedInsert(HashmapType::ObjectID oid,
                               const uint64_t key_seed,
                               const uint64_t value_seed) {
    auto h0 = HashmapType::GetPtr(oid);
    Key keys;
    Value values;
    FillKey(&keys, key_seed);
    FillValue(&values, value_seed);
    h0->BufferedInsert(keys, values);
  }

  static void DoAsyncInsert(shad::rt::Handle &handle, HashmapType::ObjectID oid,
                            const uint64_t key_seed,
                            const uint64_t value_seed) {
    auto h0 = HashmapType::GetPtr(oid);
    Key keys;
    Value values;
    FillKey(&keys, key_seed);
    FillValue(&values, value_seed);
    h0->AsyncInsert(handle, keys, values);
  }

  static void DoBufferedAsyncInsert(shad::rt::Handle &handle,
                                    HashmapType::ObjectID oid,
                                    const uint64_t key_seed,
                                    const uint64_t value_seed) {
    auto h0 = HashmapType::GetPtr(oid);
    Key keys;
    Value values;
    FillKey(&keys, key_seed);
    FillValue(&values, value_seed);
    h0->BufferedAsyncInsert(handle, keys, values);
  }

  static bool DoLookup(HashmapType::ObjectID oid, const uint64_t key_seed,
                       Value *values) {
    auto h0 = HashmapType::GetPtr(oid);
    Key keys;
    FillKey(&keys, key_seed);
    Value ret;
    bool found = h0->Lookup(keys, &ret);
    if (found) {
      *values = ret;
    }
    return found;
  }

  static void DoAsyncLookup(shad::rt::Handle &handle, HashmapType::ObjectID oid,
                            const uint64_t key_seed,
                            HashmapType::LookupResult *values) {
    auto h0 = HashmapType::GetPtr(oid);
    Key keys;
    FillKey(&keys, key_seed);
    h0->AsyncLookup(handle, keys, values);
  }

  static void InsertTestParallelFunc(
      shad::rt::Handle &handle,
      const std::tuple<HashmapType::ObjectID, size_t> &t, const size_t iter) {
    HashmapType::ObjectID id = std::get<0>(t);
    const uint64_t start_it = std::get<1>(t);
    DoAsyncInsert(handle, id, start_it + iter, start_it + iter);
  }
};

TEST_F(HashmapTest, InsertLookupTest) {
  auto mapPtr = HashmapType::Create(kToInsert);
  uint64_t i;
  for (i = 1; i <= kToInsert; i++) {
    DoInsert(mapPtr->GetGlobalID(), i, i + 11);
  }
  ASSERT_EQ(mapPtr->Size(), kToInsert);
  Value values;
  for (i = 1; i <= kToInsert; i++) {
    ASSERT_TRUE(DoLookup(mapPtr->GetGlobalID(), i, &values));
    CheckValue(&values, i + 11);
  }
  ASSERT_FALSE(DoLookup(mapPtr->GetGlobalID(), 1234567890, &values));
  HashmapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(HashmapTest, AsyncInsertLookupTest) {
  auto mapPtr = HashmapType::Create(kToInsert);
  shad::rt::Handle handle;
  uint64_t i;
  for (i = 1; i <= kToInsert; i++) {
    DoAsyncInsert(handle, mapPtr->GetGlobalID(), i, i + 11);
  }
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(mapPtr->Size(), kToInsert);
  Value values;
  for (i = 1; i <= kToInsert; i++) {
    ASSERT_TRUE(DoLookup(mapPtr->GetGlobalID(), i, &values));
    CheckValue(&values, i + 11);
  }
  ASSERT_FALSE(DoLookup(mapPtr->GetGlobalID(), 1234567890, &values));
  HashmapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(HashmapTest, AsyncInsertAsyncLookupTest) {
  auto mapPtr = HashmapType::Create(kToInsert);
  shad::rt::Handle handle;
  uint64_t i;
  for (i = 1; i <= kToInsert; i++) {
    DoAsyncInsert(handle, mapPtr->GetGlobalID(), i, i + 11);
  }
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(mapPtr->Size(), kToInsert);
  HashmapType::LookupResult *values = new HashmapType::LookupResult[kToInsert];
  for (i = 1; i < kToInsert; i++) {
    DoAsyncLookup(handle, mapPtr->GetGlobalID(), i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (i = 1; i < kToInsert; i++) {
    CheckValue(&(values[i].value), i + 11);
  }
  delete[] values;
  HashmapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(HashmapTest, BufferedInsertAsyncLookupTest) {
  auto mapPtr = HashmapType::Create(kToInsert);
  uint64_t i;
  for (i = 0; i < kToInsert; i++) {
    DoBufferedInsert(mapPtr->GetGlobalID(), i, i + 11);
  }
  mapPtr->WaitForBufferedInsert();
  ASSERT_EQ(mapPtr->Size(), kToInsert);
  HashmapType::LookupResult *values = new HashmapType::LookupResult[kToInsert];
  shad::rt::Handle handle;
  for (i = 0; i < kToInsert; i++) {
    DoAsyncLookup(handle, mapPtr->GetGlobalID(), i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (i = 0; i < kToInsert; i++) {
    CheckValue(&(values[i].value), i + 11);
  }
  delete[] values;
  HashmapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(HashmapTest, BufferedAsyncInsertAsyncLookupTest) {
  auto mapPtr = HashmapType::Create(kToInsert);
  shad::rt::Handle handle;
  uint64_t i;
  for (i = 0; i < kToInsert; i++) {
    DoBufferedAsyncInsert(handle, mapPtr->GetGlobalID(), i, i + 11);
  }
  shad::rt::waitForCompletion(handle);
  mapPtr->WaitForBufferedInsert();
  ASSERT_EQ(mapPtr->Size(), kToInsert);
  HashmapType::LookupResult *values = new HashmapType::LookupResult[kToInsert];
  for (i = 0; i < kToInsert; i++) {
    DoAsyncLookup(handle, mapPtr->GetGlobalID(), i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (i = 0; i < kToInsert; i++) {
    CheckValue(&(values[i].value), i + 11);
  }
  delete[] values;
  HashmapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(HashmapTest, FEBufferedAsyncInsertAsyncLookupTest) {
  auto mapPtr = HashmapType::Create(kToInsert);
  shad::rt::Handle handle;
  auto insertLambda = [](shad::rt::Handle &handle,
                         const std::tuple<HashmapType::ObjectID> &t, size_t i) {
    uint64_t value = i;
    DoBufferedAsyncInsert(handle, std::get<0>(t), value, value + 11);
  };
  shad::rt::asyncForEachOnAll(
      handle, insertLambda, std::make_tuple(mapPtr->GetGlobalID()), kToInsert);
  shad::rt::waitForCompletion(handle);
  mapPtr->WaitForBufferedInsert();
  ASSERT_EQ(mapPtr->Size(), kToInsert);
  HashmapType::LookupResult *values = new HashmapType::LookupResult[kToInsert];
  for (size_t i = 0; i < kToInsert; i++) {
    DoAsyncLookup(handle, mapPtr->GetGlobalID(), i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (uint64_t i = 0; i < kToInsert; i++) {
    CheckValue(&(values[i].value), i + 11);
  }
  delete[] values;
  HashmapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(HashmapTest, FEBufferedInsertAsyncLookupTest) {
  auto mapPtr = HashmapType::Create(kToInsert);
  shad::rt::Handle handle;
  auto insertLambda = [](shad::rt::Handle &handle,
                         const std::tuple<HashmapType::ObjectID> &t, size_t i) {
    uint64_t value = i;
    DoBufferedInsert(std::get<0>(t), value, value + 11);
  };
  shad::rt::asyncForEachOnAll(
      handle, insertLambda, std::make_tuple(mapPtr->GetGlobalID()), kToInsert);
  shad::rt::waitForCompletion(handle);
  mapPtr->WaitForBufferedInsert();
  ASSERT_EQ(mapPtr->Size(), kToInsert);
  HashmapType::LookupResult *values = new HashmapType::LookupResult[kToInsert];
  for (size_t i = 0; i < kToInsert; i++) {
    DoAsyncLookup(handle, mapPtr->GetGlobalID(), i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (uint64_t i = 0; i < kToInsert; i++) {
    CheckValue(&(values[i].value), i + 11);
  }
  delete[] values;
  HashmapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(HashmapTest, Erase) {
  auto mapPtr = HashmapType::Create(kToInsert);
  shad::rt::Handle handle;
  for (uint64_t i = 0; i < kToInsert; i++) {
    DoAsyncInsert(handle, mapPtr->GetGlobalID(), i, i + 11);
  }
  shad::rt::waitForCompletion(handle);
  size_t currSize = mapPtr->Size();
  for (uint64_t i = 0; i < kToInsert; i++) {
    if ((i % 3) != 0u) {
      Key k;
      FillKey(&k, i);
      mapPtr->Erase(k);
      currSize--;
    }
  }
  ASSERT_EQ(mapPtr->Size(), currSize);
  for (uint64_t i = 0; i < kToInsert; i++) {
    Key k;
    FillKey(&k, i);
    Value res;
    bool found = mapPtr->Lookup(k, &res);
    if ((i % 3) != 0u) {
      ASSERT_FALSE(found);
    } else {
      ASSERT_TRUE(found);
      CheckValue(&res, i + 11);
    }
  }
  HashmapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(HashmapTest, AsyncErase) {
  auto mapPtr = HashmapType::Create(kToInsert);
  shad::rt::Handle handle;
  for (uint64_t i = 0; i < kToInsert; i++) {
    DoAsyncInsert(handle, mapPtr->GetGlobalID(), i, i + 11);
  }
  shad::rt::waitForCompletion(handle);
  size_t currSize = mapPtr->Size();
  for (uint64_t i = 0; i < kToInsert; i++) {
    if ((i % 3) != 0u) {
      Key k;
      FillKey(&k, i);
      mapPtr->AsyncErase(handle, k);
      currSize--;
    }
  }
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(mapPtr->Size(), currSize);
  for (uint64_t i = 0; i < kToInsert; i++) {
    Key k;
    FillKey(&k, i);
    Value res;
    bool found = mapPtr->Lookup(k, &res);
    if ((i % 3) != 0u) {
      ASSERT_FALSE(found);
    } else {
      ASSERT_TRUE(found);
      CheckValue(&res, i + 11);
    }
  }
  HashmapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(HashmapTest, ForEachEntry) {
  auto mapPtr = HashmapType::Create(kToInsert);
  auto args = std::make_tuple(mapPtr->GetGlobalID(), 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(handle, InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  auto VisitLambda0args = [](const Key &key, Value &value) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
  };

  auto VisitLambda1arg = [](const Key &key, Value &value,
                            uint64_t &magicValue) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
  };

  auto VisitLambda = [](const Key &key, Value &value, uint64_t &magicValue,
                        uint64_t &magicValue2) {
    ASSERT_TRUE(magicValue == kMagicValue);
    ASSERT_TRUE(magicValue2 == kMagicValue * 2);
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
  };
  uint64_t magicValue = kMagicValue;
  uint64_t magicValue2 = kMagicValue * 2;
  mapPtr->ForEachEntry(VisitLambda0args);
  mapPtr->ForEachEntry(VisitLambda1arg, magicValue);
  mapPtr->ForEachEntry(VisitLambda, magicValue, magicValue2);
  HashmapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(HashmapTest, AsyncForEachEntry) {
  auto mapPtr = HashmapType::Create(kToInsert);
  auto args = std::make_tuple(mapPtr->GetGlobalID(), 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(handle, InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  auto VisitLambda0args = [](shad::rt::Handle &, const Key &key, Value &value) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
  };
  auto VisitLambda1arg = [](shad::rt::Handle &, const Key &key, Value &value,
                            uint64_t &magicValue) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
  };
  auto VisitLambda = [](shad::rt::Handle &, const Key &key, Value &value,
                        uint64_t &magicValue, uint64_t &magicValue2) {
    ASSERT_TRUE(magicValue == kMagicValue);
    ASSERT_TRUE(magicValue2 == kMagicValue * 2);
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
  };
  uint64_t magicValue = kMagicValue;
  uint64_t magicValue2 = kMagicValue * 2;
  mapPtr->AsyncForEachEntry(handle, VisitLambda0args);
  mapPtr->AsyncForEachEntry(handle, VisitLambda1arg, magicValue);
  mapPtr->AsyncForEachEntry(handle, VisitLambda, magicValue, magicValue2);
  shad::rt::waitForCompletion(handle);
  HashmapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(HashmapTest, ForEachKey) {
  auto mapPtr = HashmapType::Create(kToInsert);
  auto args = std::make_tuple(mapPtr->GetGlobalID(), 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(handle, InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  auto VisitLambda0args = [](const Key &key) { CheckKey(&key, GetSeed(&key)); };
  auto VisitLambda1arg = [](const Key &key, uint64_t &magicValue) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
  };
  auto VisitLambda = [](const Key &key, uint64_t &magicValue,
                        uint64_t &magicValue2) {
    ASSERT_TRUE(magicValue == kMagicValue);
    ASSERT_TRUE(magicValue2 == kMagicValue * 2);
    CheckKey(&key, GetSeed(&key));
  };
  uint64_t magicValue = kMagicValue;
  uint64_t magicValue2 = kMagicValue * 2;
  mapPtr->ForEachKey(VisitLambda0args);
  mapPtr->ForEachKey(VisitLambda1arg, magicValue);
  mapPtr->ForEachKey(VisitLambda, magicValue, magicValue2);
  HashmapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(HashmapTest, AsyncForEachKey) {
  auto mapPtr = HashmapType::Create(kToInsert);
  auto args = std::make_tuple(mapPtr->GetGlobalID(), 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(handle, InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  auto VisitLambda0args = [](shad::rt::Handle &, const Key &key) {
    CheckKey(&key, GetSeed(&key));
  };
  auto VisitLambda1arg = [](shad::rt::Handle &, const Key &key,
                            uint64_t &magicValue) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
  };
  auto VisitLambda = [](shad::rt::Handle &, const Key &key,
                        uint64_t &magicValue, uint64_t &magicValue2) {
    ASSERT_TRUE(magicValue == kMagicValue);
    ASSERT_TRUE(magicValue2 == kMagicValue * 2);
    CheckKey(&key, GetSeed(&key));
  };
  uint64_t magicValue = kMagicValue;
  uint64_t magicValue2 = kMagicValue * 2;
  mapPtr->AsyncForEachKey(handle, VisitLambda0args);
  mapPtr->AsyncForEachKey(handle, VisitLambda1arg, magicValue);
  mapPtr->AsyncForEachKey(handle, VisitLambda, magicValue, magicValue2);
  shad::rt::waitForCompletion(handle);
  HashmapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(HashmapTest, Apply) {
  auto mapPtr = HashmapType::Create(kToInsert);
  auto args = std::make_tuple(mapPtr->GetGlobalID(), 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  auto ApplyLambda0args = [](const Key &key, Value &value) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
  };
  auto ApplyLambda1arg = [](const Key &key, Value &value,
                            uint64_t &magicValue) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
  };
  auto ApplyLambda = [](const Key &key, Value &value, uint64_t &magicValue,
                        uint64_t &magicValue2) {
    ASSERT_TRUE(magicValue == kMagicValue);
    ASSERT_TRUE(magicValue2 == kMagicValue * 2);
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
  };
  uint64_t magicValue = kMagicValue;
  uint64_t magicValue2 = kMagicValue * 2;
  for (size_t i = 0; i < kToInsert; i++) {
    Key keys;
    FillKey(&keys, i);
    mapPtr->Apply(keys, ApplyLambda0args);
    mapPtr->Apply(keys, ApplyLambda1arg, magicValue);
    mapPtr->Apply(keys, ApplyLambda, magicValue, magicValue2);
  }
  HashmapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(HashmapTest, AsyncApply) {
  auto mapPtr = HashmapType::Create(kToInsert);
  auto args = std::make_tuple(mapPtr->GetGlobalID(), 0lu);
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
                                 Value &value, uint64_t &magicValue) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
  };
  auto AsyncApplyLambda = [](shad::rt::Handle &, const Key &key, Value &value,
                             uint64_t &magicValue, uint64_t &magicValue2) {
    ASSERT_TRUE(magicValue == kMagicValue);
    ASSERT_TRUE(magicValue2 == kMagicValue * 2);
    CheckKey(&key, GetSeed(&key));
    CheckValue(&value, GetSeed(&value));
  };
  uint64_t magicValue = kMagicValue;
  uint64_t magicValue2 = kMagicValue * 2;
  for (size_t i = 0; i < kToInsert; i++) {
    Key keys;
    FillKey(&keys, i);
    mapPtr->AsyncApply(handle, keys, AsyncApplyLambda0args);
    mapPtr->AsyncApply(handle, keys, AsyncApplyLambda1arg, magicValue);
    mapPtr->AsyncApply(handle, keys, AsyncApplyLambda, magicValue, magicValue2);
  }
  shad::rt::waitForCompletion(handle);
  HashmapType::Destroy(mapPtr->GetGlobalID());
}
