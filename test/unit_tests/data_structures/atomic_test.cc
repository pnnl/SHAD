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

#include <algorithm>
#include <vector>

#include "gtest/gtest.h"

#include "shad/data_structures/atomic.h"
#include "shad/runtime/runtime.h"

static int64_t kInitValue = 42;
static int64_t kNumIter = 42;
using atomicPtr = shad::Atomic<int64_t>::SharedPtr;


class AtomicTest : public ::testing::Test {
 public:
  AtomicTest() {}

  void SetUp() {}

  void TearDown() {}
};

void destroy(std::vector<atomicPtr>& ptrs) {
  for (auto ptr : ptrs) {
    shad::Atomic<int64_t>::Destroy(ptr->GetGlobalID());
  }
}

TEST_F(AtomicTest, SyncLoad) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  size_t cnt = 0;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<int64_t>::Create(kInitValue+cnt, loc);
    ++cnt;
  }
  cnt = 0;
  for (auto ptr : ptrs) {
    auto value = ptr->Load();
    ASSERT_EQ(value, kInitValue + cnt);
    ++cnt;
  }
  destroy(ptrs);
}

TEST_F(AtomicTest, AsyncLoad) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  size_t cnt = 0;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<int64_t>::Create(kInitValue+cnt, loc);
    ++cnt;
  }
  cnt = 0;
  shad::rt::Handle h;
  std::vector<int64_t> values(locs.size());
  for (auto ptr : ptrs) {
    ptr->AsyncLoad(h, &values[cnt]);
    ++cnt;
  }
  shad::rt::waitForCompletion(h);
  cnt = 0;
  for (auto value : values) {
    ASSERT_EQ(value, kInitValue + cnt);
    ++cnt;
  }
  destroy(ptrs);
}

TEST_F(AtomicTest, SyncStore) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  size_t cnt = 0;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<int64_t>::Create(0lu, loc);
    ptrs[cnt]->Store(kInitValue+cnt);
    ++cnt;
  }
  cnt = 0;
  shad::rt::Handle h;
  std::vector<int64_t> values(locs.size());
  for (auto ptr : ptrs) {
    ptr->AsyncLoad(h, &values[cnt]);
    ++cnt;
  }
  shad::rt::waitForCompletion(h);
  cnt = 0;
  for (auto value : values) {
    ASSERT_EQ(value, kInitValue + cnt);
    ++cnt;
  }
  destroy(ptrs);
}

TEST_F(AtomicTest, SyncStoreCustom) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  size_t cnt = 0;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<int64_t>::Create(1lu, loc);
    bool done = ptrs[cnt]->Store(kInitValue+cnt, std::plus<int64_t>{});
    ASSERT_EQ(done, true);
    ++cnt;
  }
  cnt = 0;
  shad::rt::Handle h;
  std::vector<int64_t> values(locs.size());
  for (auto ptr : ptrs) {
    ptr->AsyncLoad(h, &values[cnt]);
    ++cnt;
  }
  shad::rt::waitForCompletion(h);
  cnt = 0;
  for (auto value : values) {
    ASSERT_EQ(value, kInitValue + cnt + 1);
    ++cnt;
  }
  destroy(ptrs);
}

TEST_F(AtomicTest, AsyncStore) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  size_t cnt = 0;
  shad::rt::Handle h;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<int64_t>::Create(0lu, loc);
    ptrs[cnt]->AsyncStore(h, kInitValue+cnt);
    ++cnt;
  }
  shad::rt::waitForCompletion(h);
  cnt = 0;
  std::vector<int64_t> values(locs.size());
  for (auto ptr : ptrs) {
    ptr->AsyncLoad(h, &values[cnt]);
    ++cnt;
  }
  shad::rt::waitForCompletion(h);
  cnt = 0;
  for (auto value : values) {
    ASSERT_EQ(value, kInitValue + cnt);
    ++cnt;
  }
  destroy(ptrs);
}

TEST_F(AtomicTest, AsyncStoreCustom) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  std::deque<bool> results(locs.size());
  size_t cnt = 0;
  shad::rt::Handle h;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<int64_t>::Create(1l, loc);
    ptrs[cnt]->AsyncStore(h, kInitValue+cnt, std::minus<int64_t>{}, &results[cnt]);
    ++cnt;
  }
  shad::rt::waitForCompletion(h);
  cnt = 0;
  std::vector<int64_t> values(locs.size());
  for (auto ptr : ptrs) {
    ptr->AsyncLoad(h, &values[cnt]);
    ++cnt;
  }
  shad::rt::waitForCompletion(h);
  cnt = 0;
  for (int64_t value : values) {
    ASSERT_EQ(value, 1l-(kInitValue + cnt));
    ++cnt;
  }
  for (auto r : results) {
    ASSERT_EQ(r, true);
  }
  destroy(ptrs);
}

TEST_F(AtomicTest, SyncFetchAdd) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  std::vector<atomicPtr> fetchedPtrs(locs.size());
  size_t cnt = 0;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<int64_t>::Create(kInitValue+cnt, loc);
    fetchedPtrs[cnt] = shad::Atomic<int64_t>::Create(0lu, loc);
    ++cnt;
  }
  shad::rt::Handle h;
  cnt = 0;
  using argsTuple_t = std::tuple<shad::Atomic<int64_t>::ObjectID,
                                 shad::Atomic<int64_t>::ObjectID>;
  for (auto loc : locs) {
    auto lambda = [](shad::rt::Handle&, const argsTuple_t &args) {
      uint64_t sum = 0;
      auto ptr = shad::Atomic<int64_t>::GetPtr(std::get<0>(args));
      auto ptr2 = shad::Atomic<int64_t>::GetPtr(std::get<1>(args));
      for (size_t i = 0; i < kNumIter; ++i) {
        sum += ptr->FetchAdd(1lu);
      }
      ptr2->FetchAdd(sum);
    };
    argsTuple_t args(ptrs[cnt]->GetGlobalID(), fetchedPtrs[cnt]->GetGlobalID());
    shad::rt::asyncExecuteOnAll(h, lambda, args);
    ++cnt;
  }
  shad::rt::waitForCompletion(h);
  cnt = 0;
  for (auto ptr : ptrs) {
    std::atomic<int64_t> temp(kInitValue+cnt);
    int64_t expected = 0;
    for (size_t i = 0; i < shad::rt::numLocalities(); ++i) {
      for (size_t j = 0; j < kNumIter; ++j) {
        expected += temp.fetch_add(1lu);
      }
    }
    auto value = ptr->Load();
    auto fetchedValue = fetchedPtrs[cnt]->Load();
    ASSERT_EQ(value, kInitValue + cnt + kNumIter*shad::rt::numLocalities());
    ASSERT_EQ(fetchedValue, expected);
    ++cnt;
  }
  destroy(ptrs);
  destroy(fetchedPtrs);
}

TEST_F(AtomicTest, AsyncFetchAdd) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  size_t cnt = 0;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<int64_t>::Create(kInitValue+cnt, loc);
    ++cnt;
  }
  shad::rt::Handle h;
  cnt = 0;
  for (auto loc : locs) {
    auto lambda = [](shad::rt::Handle& h, const shad::Atomic<int64_t>::ObjectID &oid) {
      auto ptr = shad::Atomic<int64_t>::GetPtr(oid);
      for (size_t i = 0; i < kNumIter; ++i) {
        ptr->AsyncFetchAdd(h, 1lu);
      }
    };
    shad::rt::asyncExecuteOnAll(h, lambda, ptrs[cnt]->GetGlobalID());
    ++cnt;
  }
  shad::rt::waitForCompletion(h);
  cnt = 0;
  for (auto ptr : ptrs) {
    auto value = ptr->Load();
    ASSERT_EQ(value, kInitValue + cnt + kNumIter*shad::rt::numLocalities());
    ++cnt;
  }
  destroy(ptrs);
}

TEST_F(AtomicTest, AsyncFetchAddWithRet) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  std::vector<atomicPtr> fetchedPtrs(locs.size());
  size_t cnt = 0;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<int64_t>::Create(kInitValue+cnt, loc);
    fetchedPtrs[cnt] = shad::Atomic<int64_t>::Create(0lu, loc);
    ++cnt;
  }
  shad::rt::Handle h;
  cnt = 0;
  using argsTuple_t = std::tuple<shad::Atomic<int64_t>::ObjectID,
                                 shad::Atomic<int64_t>::ObjectID>;
  for (auto loc : locs) {
    auto lambda = [](shad::rt::Handle& h, const argsTuple_t &args) {
      std::vector<int64_t> sums(kNumIter, 0lu);
      auto ptr = shad::Atomic<int64_t>::GetPtr(std::get<0>(args));
      auto ptr2 = shad::Atomic<int64_t>::GetPtr(std::get<1>(args));
      shad::rt::Handle h2;
      for (size_t i = 0; i < kNumIter; ++i) {
        ptr->AsyncFetchAdd(h2, 1lu, &sums[i]);
      }
      shad::rt::waitForCompletion(h2);
      int64_t sum = 0;
      for (auto s : sums) {
        sum += s;
      }
      ptr2->AsyncFetchAdd(h, sum);
    };
    argsTuple_t args(ptrs[cnt]->GetGlobalID(), fetchedPtrs[cnt]->GetGlobalID());
    shad::rt::asyncExecuteOnAll(h, lambda, args);
    ++cnt;
  }
  shad::rt::waitForCompletion(h);
  cnt = 0;
  for (auto ptr : ptrs) {
    std::atomic<int64_t> temp(kInitValue+cnt);
    int64_t expected = 0;
    for (size_t i = 0; i < shad::rt::numLocalities(); ++i) {
      for (size_t j = 0; j < kNumIter; ++j) {
        expected += temp.fetch_add(1lu);
      }
    }
    auto value = ptr->Load();
    auto fetchedValue = fetchedPtrs[cnt]->Load();
    ASSERT_EQ(value, kInitValue + cnt + kNumIter*shad::rt::numLocalities());
    ASSERT_EQ(fetchedValue, expected);
    ++cnt;
  }
  destroy(ptrs);
  destroy(fetchedPtrs);
}


TEST_F(AtomicTest, SyncFetchSub) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  std::vector<atomicPtr> fetchedPtrs(locs.size());
  size_t cnt = 0;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<int64_t>::Create(kInitValue+cnt, loc);
    fetchedPtrs[cnt] = shad::Atomic<int64_t>::Create(0lu, loc);
    ++cnt;
  }
  shad::rt::Handle h;
  cnt = 0;
  using argsTuple_t = std::tuple<shad::Atomic<int64_t>::ObjectID,
                                 shad::Atomic<int64_t>::ObjectID>;
  for (auto loc : locs) {
    auto lambda = [](shad::rt::Handle&, const argsTuple_t &args) {
      uint64_t sum = 0;
      auto ptr = shad::Atomic<int64_t>::GetPtr(std::get<0>(args));
      auto ptr2 = shad::Atomic<int64_t>::GetPtr(std::get<1>(args));
      for (size_t i = 0; i < kNumIter; ++i) {
        sum += ptr->FetchSub(-1l);
      }
      ptr2->FetchSub(sum);
    };
    argsTuple_t args(ptrs[cnt]->GetGlobalID(), fetchedPtrs[cnt]->GetGlobalID());
    shad::rt::asyncExecuteOnAll(h, lambda, args);
    ++cnt;
  }
  shad::rt::waitForCompletion(h);
  cnt = 0;
  for (auto ptr : ptrs) {
    std::atomic<int64_t> temp(kInitValue+cnt);
    int64_t expected = 0;
    for (size_t i = 0; i < shad::rt::numLocalities(); ++i) {
      for (size_t j = 0; j < kNumIter; ++j) {
        expected -= temp.fetch_sub(-1l);
      }
    }
    auto value = ptr->Load();
    auto fetchedValue = fetchedPtrs[cnt]->Load();
    ASSERT_EQ(value, kInitValue + cnt + kNumIter*shad::rt::numLocalities());
    ASSERT_EQ(fetchedValue, expected);
    ++cnt;
  }
  destroy(ptrs);
  destroy(fetchedPtrs);
}

TEST_F(AtomicTest, AsyncFetchSub) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  size_t cnt = 0;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<int64_t>::Create(kInitValue+cnt, loc);
    ++cnt;
  }
  shad::rt::Handle h;
  cnt = 0;
  for (auto loc : locs) {
    auto lambda = [](shad::rt::Handle& h, const shad::Atomic<int64_t>::ObjectID &oid) {
      auto ptr = shad::Atomic<int64_t>::GetPtr(oid);
      for (size_t i = 0; i < kNumIter; ++i) {
        ptr->AsyncFetchSub(h, -1l);
      }
    };
    shad::rt::asyncExecuteOnAll(h, lambda, ptrs[cnt]->GetGlobalID());
    ++cnt;
  }
  shad::rt::waitForCompletion(h);
  cnt = 0;
  for (auto ptr : ptrs) {
    auto value = ptr->Load();
    ASSERT_EQ(value, kInitValue + cnt + kNumIter*shad::rt::numLocalities());
    ++cnt;
  }
  destroy(ptrs);
}

TEST_F(AtomicTest, AsyncFetchSubWithRet) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  std::vector<atomicPtr> fetchedPtrs(locs.size());
  size_t cnt = 0;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<int64_t>::Create(kInitValue+cnt, loc);
    fetchedPtrs[cnt] = shad::Atomic<int64_t>::Create(0lu, loc);
    ++cnt;
  }
  shad::rt::Handle h;
  cnt = 0;
  using argsTuple_t = std::tuple<shad::Atomic<int64_t>::ObjectID,
                                 shad::Atomic<int64_t>::ObjectID>;
  for (auto loc : locs) {
    auto lambda = [](shad::rt::Handle& h, const argsTuple_t &args) {
      std::vector<int64_t> sums(kNumIter, 0lu);
      auto ptr = shad::Atomic<int64_t>::GetPtr(std::get<0>(args));
      auto ptr2 = shad::Atomic<int64_t>::GetPtr(std::get<1>(args));
      shad::rt::Handle h2;
      for (size_t i = 0; i < kNumIter; ++i) {
        ptr->AsyncFetchSub(h2, -1l, &sums[i]);
      }
      shad::rt::waitForCompletion(h2);
      int64_t sum = 0;
      for (auto s : sums) {
        sum += s;
      }
      ptr2->AsyncFetchSub(h, sum);
    };
    argsTuple_t args(ptrs[cnt]->GetGlobalID(), fetchedPtrs[cnt]->GetGlobalID());
    shad::rt::asyncExecuteOnAll(h, lambda, args);
    ++cnt;
  }
  shad::rt::waitForCompletion(h);
  cnt = 0;
  for (auto ptr : ptrs) {
    std::atomic<int64_t> temp(kInitValue+cnt);
    int64_t expected = 0;
    for (size_t i = 0; i < shad::rt::numLocalities(); ++i) {
      for (size_t j = 0; j < kNumIter; ++j) {
        expected -= temp.fetch_sub(-1l);
      }
    }
    auto value = ptr->Load();
    auto fetchedValue = fetchedPtrs[cnt]->Load();
    ASSERT_EQ(value, kInitValue + cnt + kNumIter*shad::rt::numLocalities());
    ASSERT_EQ(fetchedValue, expected);
    ++cnt;
  }
  destroy(ptrs);
  destroy(fetchedPtrs);
}

TEST_F(AtomicTest, FetchAndTests) {
  auto locs = shad::rt::allLocalities();
  auto it = locs.begin();
  std::advance(it, shad::rt::numLocalities()/2);
  shad::rt::Locality tgt = *it;
  shad::rt::Handle h;
  auto ptr = shad::Atomic<int64_t>::Create(kInitValue, tgt);
  std::atomic<int64_t>expected(kInitValue);
  auto fetched = ptr->FetchAnd(0l);
  auto exp_fetched = expected.fetch_and(0l);
  auto loaded = ptr->Load();
  auto exp_loaded = expected.load();
  ASSERT_EQ(fetched, exp_fetched);
  ASSERT_EQ(loaded, exp_loaded);
  ptr->Store(kInitValue);
  expected.store(kInitValue);
  ptr->AsyncFetchAnd(h, 0l, &fetched);
  shad::rt::waitForCompletion(h);
  exp_fetched = expected.fetch_and(0l);
  loaded = ptr->Load();
  exp_loaded = expected.load();
  ASSERT_EQ(fetched, exp_fetched);
  ASSERT_EQ(loaded, exp_loaded);
  ptr->AsyncFetchAnd(h, 1l);
  shad::rt::waitForCompletion(h);
  expected.fetch_and(1l);
  loaded = ptr->Load();
  exp_loaded = expected.load();
  ASSERT_EQ(loaded, exp_loaded);
  shad::Atomic<int64_t>::Destroy(ptr->GetGlobalID());
}

TEST_F(AtomicTest, FetchOrTests) {
  auto locs = shad::rt::allLocalities();
  auto it = locs.begin();
  std::advance(it, shad::rt::numLocalities()/2);
  shad::rt::Locality tgt = *it;
  shad::rt::Handle h;
  auto ptr = shad::Atomic<int64_t>::Create(kInitValue, tgt);
  std::atomic<int64_t>expected(kInitValue);
  auto fetched = ptr->FetchOr(0l);
  auto exp_fetched = expected.fetch_or(0l);
  auto loaded = ptr->Load();
  auto exp_loaded = expected.load();
  ASSERT_EQ(fetched, exp_fetched);
  ASSERT_EQ(loaded, exp_loaded);
  ptr->Store(kInitValue);
  expected.store(kInitValue);
  ptr->AsyncFetchOr(h, 0l, &fetched);
  shad::rt::waitForCompletion(h);
  exp_fetched = expected.fetch_or(0l);
  loaded = ptr->Load();
  exp_loaded = expected.load();
  ASSERT_EQ(fetched, exp_fetched);
  ASSERT_EQ(loaded, exp_loaded);
  ptr->AsyncFetchOr(h, 1l);
  shad::rt::waitForCompletion(h);
  expected.fetch_or(1l);
  loaded = ptr->Load();
  exp_loaded = expected.load();
  ASSERT_EQ(loaded, exp_loaded);
  shad::Atomic<int64_t>::Destroy(ptr->GetGlobalID());
}

TEST_F(AtomicTest, FetchXorTests) {
  auto locs = shad::rt::allLocalities();
  auto it = locs.begin();
  std::advance(it, shad::rt::numLocalities()/2);
  shad::rt::Locality tgt = *it;
  shad::rt::Handle h;
  auto ptr = shad::Atomic<int64_t>::Create(kInitValue, tgt);
  std::atomic<int64_t>expected(kInitValue);
  auto fetched = ptr->FetchXor(0l);
  auto exp_fetched = expected.fetch_xor(0l);
  auto loaded = ptr->Load();
  auto exp_loaded = expected.load();
  ASSERT_EQ(fetched, exp_fetched);
  ASSERT_EQ(loaded, exp_loaded);
  ptr->Store(kInitValue);
  expected.store(kInitValue);
  ptr->AsyncFetchXor(h, 0l, &fetched);
  shad::rt::waitForCompletion(h);
  exp_fetched = expected.fetch_xor(0l);
  loaded = ptr->Load();
  exp_loaded = expected.load();
  ASSERT_EQ(fetched, exp_fetched);
  ASSERT_EQ(loaded, exp_loaded);
  ptr->AsyncFetchXor(h, 1l);
  shad::rt::waitForCompletion(h);
  expected.fetch_xor(1l);
  loaded = ptr->Load();
  exp_loaded = expected.load();
  ASSERT_EQ(loaded, exp_loaded);
  shad::Atomic<int64_t>::Destroy(ptr->GetGlobalID());
}

TEST_F(AtomicTest, CASTest) {
  auto locs = shad::rt::allLocalities();
  auto it = locs.begin();
  std::advance(it, shad::rt::numLocalities()/2);
  shad::rt::Locality tgt = *it;
  auto ptr = shad::Atomic<float>::Create(kInitValue, tgt);
  bool ret = ptr->CompareExchange(kInitValue, kInitValue*2);
  auto fetched = ptr->Load();
  ASSERT_EQ(fetched, kInitValue*2);
  ASSERT_EQ(ret, true);
  ret = ptr->CompareExchange(kInitValue, 0);
  fetched = ptr->Load();
  ASSERT_EQ(fetched, kInitValue*2);
  ASSERT_EQ(ret, false);
}

TEST_F(AtomicTest, AsyncCASTest) {
  auto locs = shad::rt::allLocalities();
  auto it = locs.begin();
  std::advance(it, shad::rt::numLocalities()/2);
  shad::rt::Locality tgt = *it;
  auto ptr = shad::Atomic<float>::Create(kInitValue, tgt);
  shad::rt::Handle h;
  bool ret;
  ptr->AsyncCompareExchange(h, kInitValue, kInitValue*2, &ret);
  shad::rt::waitForCompletion(h);
  auto fetched = ptr->Load();
  ASSERT_EQ(fetched, kInitValue*2);
  ASSERT_EQ(ret, true);
  ptr->AsyncCompareExchange(h, kInitValue, 0, &ret);
  shad::rt::waitForCompletion(h);
  fetched = ptr->Load();
  ASSERT_EQ(fetched, kInitValue*2);
  ASSERT_EQ(ret, false);
}
