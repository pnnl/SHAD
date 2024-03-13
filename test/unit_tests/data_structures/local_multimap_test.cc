
#include <vector>

#include "gtest/gtest.h"
#include "shad/runtime/runtime.h"

#include "shad/data_structures/local_multimap.h"

class LocalMultimapTest : public ::testing::Test {
 public:
  LocalMultimapTest() {}
  void SetUp() {}
  void TearDown() {}
  static const uint64_t kToInsert = 128;
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

  typedef shad::LocalMultimap <Key, Value> MultimapType;
  static void FillKey(Key *keys, uint64_t key_seed) {
    for (uint64_t i = 0; i < kKeysPerEntry; ++i) {
      keys->key[i] = (key_seed + i);
    }
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

  static std::pair<typename MultimapType::iterator, bool> DoInsert(
      MultimapType *h0, const uint64_t key_seed, const uint64_t value_seed) {
    Key keys;
    Value values;
    FillKey(&keys, key_seed);
    FillValue(&values, value_seed);
    return (h0->Insert(keys, values));
  }

  static void DoAsyncInsert(shad::rt::Handle &handle, MultimapType *h0,
                            const uint64_t key_seed,
                            const uint64_t value_seed) {
    Key keys;
    Value values;
    FillKey(&keys, key_seed);
    FillValue(&values, value_seed);
    h0->AsyncInsert(handle, keys, values);
  }

  static bool DoLookup(MultimapType *h0, const uint64_t key_seed,
                       std::vector<Value> * values) {
    Key keys;
    FillKey(&keys, key_seed);
    MultimapType::LookupResult lr;
    bool LookupCheck= h0->Lookup(keys,&lr );
    *values=lr.value;
    return LookupCheck;
  } 

  static void DoAsyncLookup(shad::rt::Handle &handle, MultimapType *h0,
                            const uint64_t key_seed,  MultimapType::LookupResult *lr) {
    Key keys;
    FillKey(&keys, key_seed);
    h0->AsyncLookup(handle, keys, lr);
    // h0->Lookup(keys, values);
  }


  static void InsertTestParallelFunc(shad::rt::Handle & /*unused*/,
                                     const std::tuple<MultimapType *, size_t> &t,
                                     const size_t iter) {
    MultimapType *mm = std::get<0>(t);
    const uint64_t start_it = std::get<1>(t);
    DoInsert(mm, start_it + iter, start_it + iter);
  }

  static void
  LookupTestParallelFunc(
      const std::tuple<MultimapType *, size_t> &t, const size_t iter) {
    MultimapType *mm = std::get<0>(t);
    const uint64_t start_it = std::get<1>(t);
    std::vector<Value> values;
    ASSERT_TRUE(DoLookup(mm, start_it + iter, &values));
    CheckValue(values, start_it + iter);
  }
};
TEST_F(LocalMultimapTest, InsertLookupTest) {
  MultimapType mmap(kNumBuckets);
  uint64_t i;
  for (i = 1; i <= kToInsert; i++) {
    DoInsert(&mmap, i, i + 11);
  }
  size_t toinsert = kToInsert;
  ASSERT_EQ(mmap.Size(), toinsert);
  std::vector<Value> values;
  for (i = 1; i <= kToInsert; i++) {
    ASSERT_TRUE(DoLookup(&mmap, i, &values));
    CheckValue(values, i + 11);
  }
  ASSERT_FALSE(DoLookup(&mmap, 1234567890, &values));
}


TEST_F(LocalMultimapTest, InsertReturnTest) {
  MultimapType mmap(kNumBuckets);
  uint64_t i;
  // successful inserts
  for (i = 1; i <= kToInsert; i++) {
    auto res = DoInsert(&mmap, i, i + 11);
    ASSERT_TRUE(res.second);
    CheckKeyValue(res.first, i, i + 11);
  }

  // overwriting inserts
  for (i = 1; i <= kToInsert; i++) {
    auto res = DoInsert(&mmap, i, i + 11);
    ASSERT_TRUE(res.second);
    CheckKeyValue(res.first, i, i + 11);
  }
}

TEST_F(LocalMultimapTest, AsyncInsertLookupTest) {
  MultimapType mmap(kNumBuckets);
  uint64_t i;
  shad::rt::Handle handle;
  for (i = 1; i <= kToInsert; i++) {
    DoAsyncInsert(handle, &mmap, i, i + 11);
  }
  shad::rt::waitForCompletion(handle);
  size_t toinsert = kToInsert;
  std::vector<Value> values;
  for (i = 1; i <= kToInsert; i++) {
    ASSERT_TRUE(DoLookup(&mmap, i, &values));
    CheckValue(values, i + 11);
  }
  ASSERT_FALSE(DoLookup(&mmap, 1234567890, &values));
}

TEST_F(LocalMultimapTest, AsyncInsertAsyncLookupTest) {
  MultimapType mmap(kNumBuckets);
  uint64_t i;
  shad::rt::Handle handle;
  for (i = 1; i <= kToInsert; i++) {
    DoAsyncInsert(handle, &mmap, i, i + 11);
  }
  shad::rt::waitForCompletion(handle);
  // Lookup
  MultimapType::LookupResult *values = new MultimapType::LookupResult [kToInsert];

  for (i = 1; i < kToInsert; i++) {
    DoAsyncLookup(handle, &mmap, i, &values[i]);
  }

  shad::rt::waitForCompletion(handle);
  for (i = 1; i < kToInsert; i++) {
    ASSERT_NE(&values[i], nullptr);
    CheckValue(values[i].value, i + 11);
  }
   delete[] values;
}


TEST_F(LocalMultimapTest, InsertLookupParallel1) {
  MultimapType mmap(kNumBuckets);
  size_t it_chunk = 1;
  shad::rt::Handle handle;
  for (size_t i = 0; i < kToInsert;) {
    auto args = std::make_tuple(&mmap, i);
    shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                             InsertTestParallelFunc, args,
                             kToInsert / it_chunk);
    i += (kToInsert / it_chunk);
  }
  shad::rt::waitForCompletion(handle);
  size_t toinsert = kToInsert;
  ASSERT_EQ(mmap.Size(), toinsert);
  for (size_t i = 0; i < kToInsert;) {
    auto args = std::make_tuple(&mmap, i);
    shad::rt::forEachAt(shad::rt::thisLocality(), LookupTestParallelFunc, args,
                        kToInsert / it_chunk);
    i += (kToInsert / it_chunk);
  }
}

TEST_F(LocalMultimapTest, Erase) {
  MultimapType mmap(kNumBuckets);
  size_t it_chunk = 1;
  shad::rt::Handle handle;
  for (size_t i = 0; i < kToInsert;) {
    auto args = std::make_tuple(&mmap, i);
    shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                             InsertTestParallelFunc, args,
                             kToInsert / it_chunk);
    i += (kToInsert / it_chunk);
  }
  shad::rt::waitForCompletion(handle);
  size_t currSize = mmap.Size();
  size_t i;
  for (i = 0; i < kToInsert; i++) {
    if ((i % 3) != 0u) {
      Key k;
      FillKey(&k, i);
      mmap.Erase(k);
      currSize--;
    }
  }
  ASSERT_EQ(mmap.Size(), currSize);
  for (i = 0; i < kToInsert; i++) {
      std::vector<Value>values;

    if ((i % 3) != 0u) {
       ASSERT_FALSE(DoLookup(&mmap, i, &values));
    } else {
      DoLookup(&mmap, i, &values);
      ASSERT_NE(&values, nullptr);
      CheckValue(values, i);
    }
  }
}

TEST_F(LocalMultimapTest, AsyncErase) {
  MultimapType mmap(kNumBuckets);
  size_t it_chunk = 1;
  shad::rt::Handle handle;
  for (size_t i = 0; i < kToInsert;) {
    auto args = std::make_tuple(&mmap, i);
    shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                             InsertTestParallelFunc, args,
                             kToInsert / it_chunk);
    i += (kToInsert / it_chunk);
  }
  shad::rt::waitForCompletion(handle);
  size_t currSize = mmap.Size();
  size_t i;
  for (i = 0; i < kToInsert; i++) {
    if ((i % 3) != 0u) {
      Key k;
      FillKey(&k, i);
      mmap.AsyncErase(handle, k);
      currSize--;
    }
  }
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(mmap.Size(), currSize);

  for (i = 0; i < kToInsert; i++) {
    std::vector<Value>values;

    if ((i % 3) != 0u) {
      ASSERT_FALSE(DoLookup(&mmap, i, &values));
    } else {
      DoLookup(&mmap, i, &values);
      ASSERT_NE(&values, nullptr);
      CheckValue(values, i);
    }
  }
}

TEST_F(LocalMultimapTest, ForEachEntry) {
  MultimapType mmap(kNumBuckets);
  auto args = std::make_tuple(&mmap, 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  uint64_t cnt = 0;
  auto VisitLambda0args = [](const Key &key, std::vector<Value> &value) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
  };
  auto VisitLambda1arg = [](const Key &key, std::vector<Value> &value, uint64_t *&cntPtr) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
    __sync_fetch_and_add(cntPtr, 1);
  };
  auto VisitLambda = [](const Key &key,  std::vector<Value> &value, uint64_t &magicValue,
                        uint64_t *&cntPtr) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
    __sync_fetch_and_add(cntPtr, 1);
  };
  uint64_t magicValue = kMagicValue;
  uint64_t *cntPtr = &cnt;
  mmap.ForEachEntry(VisitLambda0args);
  mmap.ForEachEntry(VisitLambda1arg, cntPtr);
  mmap.ForEachEntry(VisitLambda, magicValue, cntPtr);
  auto toinsert = kToInsert;
  ASSERT_EQ(cnt, toinsert * 2);
}

TEST_F(LocalMultimapTest, AsyncForEachEntry) {
  MultimapType mmap(kNumBuckets);
  auto args = std::make_tuple(&mmap, 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);
  uint64_t cnt = 0;
  auto AsyncVisitLambda0args = [](shad::rt::Handle &, const Key &key,
                                  std::vector<Value> &value) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
  };
  auto AsyncVisitLambda1arg = [](shad::rt::Handle &, const Key &key,
                                 std::vector<Value> &value, uint64_t *&cntPtr) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
    __sync_fetch_and_add(cntPtr, 1);
  };
  auto AsyncVisitLambda = [](shad::rt::Handle &, const Key &key, std::vector<Value> &value,
                             uint64_t &magicValue, uint64_t *&cntPtr) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
    __sync_fetch_and_add(cntPtr, 1);
  };
  uint64_t magicValue = kMagicValue;
  uint64_t *cntPtr = &cnt;
  mmap.AsyncForEachEntry(handle, AsyncVisitLambda0args);
  mmap.AsyncForEachEntry(handle, AsyncVisitLambda1arg, cntPtr);
  mmap.AsyncForEachEntry(handle, AsyncVisitLambda, magicValue, cntPtr);
  shad::rt::waitForCompletion(handle);
  auto toinsert = kToInsert;
  ASSERT_EQ(cnt, toinsert * 2);
}

TEST_F(LocalMultimapTest, ForEachKey) {
  MultimapType mmap(kNumBuckets);
  auto args = std::make_tuple(&mmap, 0lu);
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
  mmap.ForEachKey(ForEachKeyLambda0args);
  mmap.ForEachKey(ForEachKeyLambda1arg, cntPtr);
  mmap.ForEachKey(ForEachKeyLambda, magicValue, cntPtr);
  auto toinsert = kToInsert;
  ASSERT_EQ(cnt, toinsert * 2);
}

TEST_F(LocalMultimapTest, AsyncForEachKey) {
  MultimapType mmap(kNumBuckets);
  auto args = std::make_tuple(&mmap, 0lu);
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
  mmap.AsyncForEachKey(handle, AsyncForEachKeyLambda0args);
  mmap.AsyncForEachKey(handle, AsyncForEachKeyLambda1arg, cntPtr);
  mmap.AsyncForEachKey(handle, AsyncForEachKeyLambda, magicValue, cntPtr);
  shad::rt::waitForCompletion(handle);
  auto toinsert = kToInsert;
  ASSERT_EQ(cnt, toinsert * 2);
}

TEST_F(LocalMultimapTest, Apply) {
  MultimapType mmap(kNumBuckets);
  auto args = std::make_tuple(&mmap, 0lu);
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
                           InsertTestParallelFunc, args, kToInsert);
  shad::rt::waitForCompletion(handle);

  auto toinsert = kToInsert;
  ASSERT_EQ(mmap.Size(), toinsert);

  uint64_t cnt = 0;
  auto ApplyLambda0args = [](const Key &key, std::vector<Value> &value) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
  };
  auto ApplyLambda1arg = [](const Key &key, std::vector<Value> &value, uint64_t *&cntPtr) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
    __sync_fetch_and_add(cntPtr, 1);
  };
  auto ApplyLambda = [](const Key &key, std::vector<Value> &value, uint64_t &magicValue,
                        uint64_t *&cntPtr) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
    __sync_fetch_and_add(cntPtr, 1);
  };
  uint64_t magicValue = kMagicValue;
  uint64_t *cntPtr = &cnt;
  for (size_t i = 0; i < kToInsert; i++) {
    Key keys;
    FillKey(&keys, i); 
    mmap.Apply(keys, ApplyLambda0args);
    mmap.Apply(keys, ApplyLambda1arg, cntPtr);
    mmap.Apply(keys, ApplyLambda, magicValue, cntPtr);
  }
  ASSERT_EQ(cnt, toinsert * 2);
}

TEST_F(LocalMultimapTest, AsyncApply) {
  MultimapType mmap(kNumBuckets);
  auto args = std::make_tuple(&mmap, 0lu);
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
                                 std::vector<Value> &value, uint64_t *&cntPtr) {
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
    __sync_fetch_and_add(cntPtr, 1);
  };
  auto AsyncApplyLambda = [](shad::rt::Handle &, const Key &key, std::vector<Value> &value,
                             uint64_t &magicValue, uint64_t *&cntPtr) {
    ASSERT_TRUE(magicValue == kMagicValue);
    CheckKey(&key, GetSeed(&key));
    CheckValue(value, GetSeed(&value[0]));
    __sync_fetch_and_add(cntPtr, 1);
  };

  auto toinsert = kToInsert;
  ASSERT_EQ(toinsert, mmap.Size());

  uint64_t cnt = 0;
  uint64_t magicValue = kMagicValue;
  uint64_t *cntPtr = &cnt;
  for (size_t i = 0; i < kToInsert; i++) {
    Key keys;
    FillKey(&keys, i);
    mmap.AsyncApply(handle, keys, AsyncApplyLambda0args);
    mmap.AsyncApply(handle, keys, AsyncApplyLambda1arg, cntPtr);
    mmap.AsyncApply(handle, keys, AsyncApplyLambda, magicValue, cntPtr);
  }
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(cnt, toinsert * 2);
}

