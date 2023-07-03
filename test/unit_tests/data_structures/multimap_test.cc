#include <vector>

#include "gtest/gtest.h"
#include "shad/runtime/runtime.h"

#include "shad/data_structures/multimap.h"

static const size_t kToInsert = 1000;

class MultimapTest : public ::testing::Test {
 public:
  MultimapTest() {}
  void SetUp() {}
  void TearDown() {}
  static const uint64_t kKeysPerEntry = 3;
  static const uint64_t kValuesPerEntry = 5;
  static const uint64_t kMagicValue = 9999;

  struct Key {
    uint64_t key[kKeysPerEntry];
    friend std::ostream &operator<<(std::ostream &os, const Key &rhs) {
      return os << rhs.key;
    }
  };

  struct Value {
    uint64_t value[kValuesPerEntry];
    friend std::ostream &operator<<(std::ostream &os, const Value &rhs) {
      return os << rhs.value;
    }
  };

  typedef shad::Multimap <Key, Value> MultimapType;
  static void FillKey(Key *keys, uint64_t key_seed) {
    for (uint64_t i = 0; i < kKeysPerEntry; ++i) {
      keys->key[i] = (key_seed + i);
    }
    // keys->key=key_seed;
  }

  static void FillValue(Value *values, uint64_t value_seed) {
    for (uint64_t i = 0; i < kValuesPerEntry; ++i) {
      values->value[i]=(value_seed + i);
    }
  }

  static void CheckValue( std::vector<Value> values, const uint64_t value_seed) {
    bool found=false;
    for(uint64_t i=0; i< values.size();i++){
      for(uint64_t j=0 ;j<kValuesPerEntry;j++){
        if( values[i].value[j]==value_seed){
          found=true;
        }
      }
    }
    ASSERT_TRUE(found==true);
  }

  static void CheckKey(const Key *keys, const uint64_t key_seed) {
    for (uint64_t i = 0; i < kKeysPerEntry; ++i) {
      ASSERT_EQ(keys->key[i], (key_seed + i));
    }
  }

  static void CheckKeyValue(typename MultimapType::iterator entry,
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
  static uint64_t GetSeed(const Value * values) { return values->value[0]; }

  static void DoInsert(MultimapType::ObjectID oid, const uint64_t key_seed, const uint64_t value_seed) {
    auto m0 = MultimapType::GetPtr(oid);
    Key keys;
    Value values;

    FillKey(&keys, key_seed);
    FillValue(&values, value_seed);
    m0->Insert(keys, values);
  }
    static void DoBufferedInsert(MultimapType::ObjectID oid,
                               const uint64_t key_seed,
                               const uint64_t value_seed) {
    auto m0 = MultimapType::GetPtr(oid);
    Key keys;
    Value values;
    FillKey(&keys, key_seed);
    FillValue(&values, value_seed);
    m0->BufferedInsert(keys, values);
  }

  static void DoAsyncInsert(shad::rt::Handle &handle, MultimapType::ObjectID oid,
                            const uint64_t key_seed,
                            const uint64_t value_seed) {
     auto m0 = MultimapType::GetPtr(oid);
    Key keys;
    Value values;
    FillKey(&keys, key_seed);
    FillValue(&values, value_seed);
    m0->AsyncInsert(handle, keys, values);
  }

  static void DoBufferedAsyncInsert(shad::rt::Handle &handle,
                                    MultimapType::ObjectID oid,
                                    const uint64_t key_seed,
                                    const uint64_t value_seed) {
    auto m0 = MultimapType::GetPtr(oid);
    Key keys;
    Value values;
    FillKey(&keys, key_seed);
    FillValue(&values, value_seed);
    m0->BufferedAsyncInsert(handle, keys, values);
  }

  static bool DoLookup(MultimapType::ObjectID oid, const uint64_t key_seed,
                       std::vector<Value> * values) {
    auto m0 = MultimapType::GetPtr(oid);                  
    Key keys;
    FillKey(&keys, key_seed);
    MultimapType::LookupResult lr;
    bool LookupCheck= m0->Lookup(keys,&lr );
    if (LookupCheck){
    *values=lr.value;
    }
    return LookupCheck;
  } 

  static void DoAsyncLookup(shad::rt::Handle &handle, MultimapType::ObjectID oid,
                            const uint64_t key_seed,  MultimapType::LookupResult *lr) {
    auto m0 = MultimapType::GetPtr(oid);  
    Key keys;
    FillKey(&keys, key_seed);
    m0->AsyncLookup(handle, keys, lr);
    // m0->Lookup(keys, values);
  }


 static void InsertTestParallelFunc(
      shad::rt::Handle &handle,
      const std::tuple<MultimapType::ObjectID, size_t> &t, const size_t iter) {
    MultimapType::ObjectID id = std::get<0>(t);
    const uint64_t start_it = std::get<1>(t);
    DoAsyncInsert(handle, id, start_it + iter, start_it + iter);
  }

};

TEST_F(MultimapTest, InsertLookupTest) {
  auto mapPtr = MultimapType::Create(kToInsert);
  uint64_t i;
  for (i = 1; i <= kToInsert; i++) {
    DoInsert(mapPtr->GetGlobalID(), i, i + 11);
  }
  ASSERT_EQ(mapPtr->Size(), kToInsert);
  std::vector<Value> values;
  for (i = 1; i <= kToInsert; i++) {
    ASSERT_TRUE(DoLookup(mapPtr->GetGlobalID(), i, &values));
    CheckValue(values, i + 11);
  }
  ASSERT_FALSE(DoLookup(mapPtr->GetGlobalID(), 1234567890, &values));
  MultimapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(MultimapTest, AsyncInsertLookupTest) {
  auto mapPtr = MultimapType::Create(kToInsert);
  shad::rt::Handle handle;
  uint64_t i;
  for (i = 1; i <= kToInsert; i++) {
    DoAsyncInsert(handle, mapPtr->GetGlobalID(), i, i + 11);
  }
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(mapPtr->Size(), kToInsert);
  std::vector<Value> values;
  for (i = 1; i <= kToInsert; i++) {
    ASSERT_TRUE(DoLookup(mapPtr->GetGlobalID(), i, &values));
    CheckValue(values, i + 11);
  }
  ASSERT_FALSE(DoLookup(mapPtr->GetGlobalID(), 1234567890, &values));
  MultimapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(MultimapTest, AsyncInsertAsyncLookupTest) {
  auto mapPtr = MultimapType::Create(kToInsert);
  shad::rt::Handle handle;
  uint64_t i;
  for (i = 1; i <= kToInsert; i++) {
    DoAsyncInsert(handle, mapPtr->GetGlobalID(), i, i + 11);
  }
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(mapPtr->Size(), kToInsert);
  MultimapType::LookupResult *values = new MultimapType::LookupResult [kToInsert];
  for (i = 1; i < kToInsert; i++) {
    DoAsyncLookup(handle, mapPtr->GetGlobalID(), i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (i = 1; i < kToInsert; i++) {
    CheckValue((values[i].value), i + 11);
  }
  delete[] values;
  MultimapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(MultimapTest, BufferedInsertAsyncLookupTest) {
  auto mapPtr = MultimapType::Create(kToInsert);
  uint64_t i;
  for (i = 0; i < kToInsert; i++) {
    DoBufferedInsert(mapPtr->GetGlobalID(), i, i + 11);
  }
  mapPtr->WaitForBufferedInsert();
  ASSERT_EQ(mapPtr->Size(), kToInsert);
   MultimapType::LookupResult *values = new MultimapType::LookupResult [kToInsert];
  shad::rt::Handle handle;
  for (i = 0; i < kToInsert; i++) {
    DoAsyncLookup(handle, mapPtr->GetGlobalID(), i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (i = 0; i < kToInsert; i++) {
    CheckValue((values[i].value), i + 11);
  }
  delete[] values;
  MultimapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(MultimapTest, BufferedAsyncInsertAsyncLookupTest) {
  auto mapPtr = MultimapType::Create(kToInsert);
  shad::rt::Handle handle;
  uint64_t i;
  for (i = 0; i < kToInsert; i++) {
    DoBufferedAsyncInsert(handle, mapPtr->GetGlobalID(), i, i + 11);
  }
  shad::rt::waitForCompletion(handle);
  mapPtr->WaitForBufferedInsert();
  ASSERT_EQ(mapPtr->Size(), kToInsert);
   MultimapType::LookupResult *values = new MultimapType::LookupResult [kToInsert];
  for (i = 0; i < kToInsert; i++) {
    DoAsyncLookup(handle, mapPtr->GetGlobalID(), i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (i = 0; i < kToInsert; i++) {
    CheckValue((values[i].value), i + 11);
  }
  delete[] values;
  MultimapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(MultimapTest, FEBufferedAsyncInsertAsyncLookupTest) {
  auto mapPtr = MultimapType::Create(kToInsert);
  shad::rt::Handle handle;
  auto insertLambda = [](shad::rt::Handle &handle,
                         const std::tuple<MultimapType::ObjectID> &t, size_t i) {
    uint64_t value = i;
    DoBufferedAsyncInsert(handle, std::get<0>(t), value, value + 11);
  };
  shad::rt::asyncForEachOnAll(
      handle, insertLambda, std::make_tuple(mapPtr->GetGlobalID()), kToInsert);
  shad::rt::waitForCompletion(handle);
  mapPtr->WaitForBufferedInsert();
  ASSERT_EQ(mapPtr->Size(), kToInsert);
   MultimapType::LookupResult *values = new MultimapType::LookupResult [kToInsert];
  for (size_t i = 0; i < kToInsert; i++) {
    DoAsyncLookup(handle, mapPtr->GetGlobalID(), i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (uint64_t i = 0; i < kToInsert; i++) {
    CheckValue((values[i].value), i + 11);
  }
  delete[] values;
  MultimapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(MultimapTest, FEBufferedInsertAsyncLookupTest) {
  auto mapPtr = MultimapType::Create(kToInsert);
  shad::rt::Handle handle;
  auto insertLambda = [](shad::rt::Handle &handle,
                         const std::tuple<MultimapType::ObjectID> &t, size_t i) {
    uint64_t value = i;
    DoBufferedInsert(std::get<0>(t), value, value + 11);
  };
  shad::rt::asyncForEachOnAll(
      handle, insertLambda, std::make_tuple(mapPtr->GetGlobalID()), kToInsert);
  shad::rt::waitForCompletion(handle);
  mapPtr->WaitForBufferedInsert();
  ASSERT_EQ(mapPtr->Size(), kToInsert);
   MultimapType::LookupResult *values = new MultimapType::LookupResult [kToInsert];
  for (size_t i = 0; i < kToInsert; i++) {
    DoAsyncLookup(handle, mapPtr->GetGlobalID(), i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (uint64_t i = 0; i < kToInsert; i++) {
    CheckValue((values[i].value), i + 11);
  }
  delete[] values;
  MultimapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(MultimapTest, Erase) {
  auto mapPtr = MultimapType::Create(kToInsert);
  shad::rt::Handle handle;
  for (uint64_t i = 1; i < kToInsert; i++) {
    DoAsyncInsert(handle, mapPtr->GetGlobalID(), i, i + 11);
  }
  shad::rt::waitForCompletion(handle);
  size_t currSize = mapPtr->Size();

  Key k;
  FillKey(&k,1);
  mapPtr->Erase(k);
  currSize--;
    

  for (uint64_t i = 1; i < kToInsert; i++) {
     std::vector<Value> values;
    bool found = DoLookup(mapPtr->GetGlobalID(), i, &values);
    if (i ==1) {
      ASSERT_FALSE(found);
    } else {
      ASSERT_TRUE(found);
      CheckValue(values, i + 11);
    }
  }
  MultimapType::Destroy(mapPtr->GetGlobalID());
}



TEST_F(MultimapTest, AsyncErase) {
  auto mapPtr = MultimapType::Create(kToInsert);
  shad::rt::Handle handle;
  for (uint64_t i = 1; i < kToInsert; i++) {
    DoAsyncInsert(handle, mapPtr->GetGlobalID(), i, i + 11);
  }
  shad::rt::waitForCompletion(handle);
   ASSERT_EQ(mapPtr->Size(), kToInsert-1);
  size_t currSize = mapPtr->Size();
  Key k;
  FillKey(&k, 1);
  mapPtr->AsyncErase(handle, k);
  currSize--;

  shad::rt::waitForCompletion(handle);
  for (uint64_t i = 1; i < kToInsert; i++) {
    Key k;
    FillKey(&k, i);
    std::vector<Value> values;
    bool found = DoLookup(mapPtr->GetGlobalID(), i, &values);
    if (i==1) {
      ASSERT_FALSE(found);
    } else {
      ASSERT_TRUE(found);
      CheckValue(values, i + 11);
    }
  }
  MultimapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(MultimapTest, ForEachEntry) {
  auto mapPtr = MultimapType::Create(kToInsert);
  auto args = std::make_tuple(mapPtr->GetGlobalID(), 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(handle, InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  auto VisitLambda0args = [](const Key &key, std::vector<Value> &value) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
  };

  auto VisitLambda1arg = [](const Key &key, std::vector<Value> &value,
                            uint64_t &magicValue) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
  };

  auto VisitLambda = [](const Key &key, std::vector<Value> &value, uint64_t &magicValue,
                        uint64_t &magicValue2) {
    ASSERT_TRUE(magicValue == kMagicValue);
    ASSERT_TRUE(magicValue2 == kMagicValue * 2);
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
  };
  uint64_t magicValue = kMagicValue;
  uint64_t magicValue2 = kMagicValue * 2;
  mapPtr->ForEachEntry(VisitLambda0args);
  mapPtr->ForEachEntry(VisitLambda1arg, magicValue);
  mapPtr->ForEachEntry(VisitLambda, magicValue, magicValue2);
  MultimapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(MultimapTest, AsyncForEachEntry) {
  auto mapPtr = MultimapType::Create(kToInsert);
  auto args = std::make_tuple(mapPtr->GetGlobalID(), 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(handle, InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  auto VisitLambda0args = [](shad::rt::Handle &, const Key &key, std::vector<Value> &value) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
  };
  auto VisitLambda1arg = [](shad::rt::Handle &, const Key &key, std::vector<Value> &value,
                            uint64_t &magicValue) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
  };
  auto VisitLambda = [](shad::rt::Handle &, const Key &key, std::vector<Value> &value,
                        uint64_t &magicValue, uint64_t &magicValue2) {
    ASSERT_TRUE(magicValue == kMagicValue);
    ASSERT_TRUE(magicValue2 == kMagicValue * 2);
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
  };
  uint64_t magicValue = kMagicValue;
  uint64_t magicValue2 = kMagicValue * 2;
  mapPtr->AsyncForEachEntry(handle, VisitLambda0args);
  mapPtr->AsyncForEachEntry(handle, VisitLambda1arg, magicValue);
  mapPtr->AsyncForEachEntry(handle, VisitLambda, magicValue, magicValue2);
  shad::rt::waitForCompletion(handle);
  MultimapType::Destroy(mapPtr->GetGlobalID());
}


TEST_F(MultimapTest, ForEachKey) {
  auto mapPtr = MultimapType::Create(kToInsert);
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
  MultimapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(MultimapTest, AsyncForEachKey) {
  auto mapPtr = MultimapType::Create(kToInsert);
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
  MultimapType::Destroy(mapPtr->GetGlobalID());
}

TEST_F(MultimapTest, Apply) {
  auto mapPtr = MultimapType::Create(kToInsert);
  auto args = std::make_tuple(mapPtr->GetGlobalID(), 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  auto ApplyLambda0args = [](const Key &key, std::vector<Value> &value) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
  };
  auto ApplyLambda1arg = [](const Key &key, std::vector<Value> &value,
                            uint64_t &magicValue) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
  };
  auto ApplyLambda = [](const Key &key, std::vector<Value> &value, uint64_t &magicValue,
                        uint64_t &magicValue2) {
    ASSERT_TRUE(magicValue == kMagicValue);
    ASSERT_TRUE(magicValue2 == kMagicValue * 2);
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
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
  MultimapType::Destroy(mapPtr->GetGlobalID());
}


TEST_F(MultimapTest, AsyncApply) {
  auto mapPtr = MultimapType::Create(kToInsert);
  auto args = std::make_tuple(mapPtr->GetGlobalID(), 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  auto AsyncApplyLambda0args = [](shad::rt::Handle &, const Key &key,
                                  std::vector<Value> &value) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
  };
  auto AsyncApplyLambda1arg = [](shad::rt::Handle &, const Key &key,
                                 std::vector<Value> &value, uint64_t &magicValue) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
  };
  auto AsyncApplyLambda = [](shad::rt::Handle &, const Key &key, std::vector<Value> &value,
                             uint64_t &magicValue, uint64_t &magicValue2) {
    ASSERT_TRUE(magicValue == kMagicValue);
    ASSERT_TRUE(magicValue2 == kMagicValue * 2);
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
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
  MultimapType::Destroy(mapPtr->GetGlobalID());
}
