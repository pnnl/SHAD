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

static uint64_t kInitValue = 42;
static uint64_t kNumIter = 42;
using atomicPtr = shad::Atomic<uint64_t>::SharedPtr;

class AtomicTest : public ::testing::Test {
 public:
  AtomicTest() {}

  void SetUp() {}

  void TearDown() {}
};

void destroy(std::vector<atomicPtr>& ptrs) {
  for (auto ptr : ptrs) {
    shad::Atomic<uint64_t>::Destroy(ptr->GetGlobalID());
  }
}

TEST_F(AtomicTest, SyncLoad) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  size_t cnt = 0;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<uint64_t>::Create(kInitValue+cnt, loc);
    ++cnt;
  }
  cnt = 0;
  for (auto ptr : ptrs) {
    auto value = ptr->Load();
    // std::cout << "loc" << cnt << ": " << value << std::endl; 
    ASSERT_EQ(value, kInitValue + cnt);
    ++cnt;
  }
  destroy(ptrs);
}

TEST_F(AtomicTest, AsyncSyncLoad) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  size_t cnt = 0;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<uint64_t>::Create(kInitValue+cnt, loc);
    ++cnt;
  }
  cnt = 0;
  shad::rt::Handle h;
  std::vector<uint64_t> values(locs.size());
  for (auto ptr : ptrs) {
    ptr->AsyncLoad(h, &values[cnt]);
    ++cnt;
  }
  shad::rt::waitForCompletion(h);
  cnt = 0;
  for (auto value : values) {
    // std::cout << "loc" << cnt << ": " << value << std::endl; 
    ASSERT_EQ(value, kInitValue + cnt);
    ++cnt;
  }
  destroy(ptrs);
}

TEST_F(AtomicTest, SyncFetchAdd) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  std::vector<atomicPtr> fetchedPtrs(locs.size());
  size_t cnt = 0;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<uint64_t>::Create(kInitValue+cnt, loc);
    fetchedPtrs[cnt] = shad::Atomic<uint64_t>::Create(0lu, loc);
    ++cnt;
  }
  shad::rt::Handle h;
  cnt = 0;
  using argsTuple_t = std::tuple<shad::Atomic<uint64_t>::ObjectID,
                                 shad::Atomic<uint64_t>::ObjectID>;
  for (auto loc : locs) {
    auto lambda = [](shad::rt::Handle&, const argsTuple_t &args) {
      uint64_t sum = 0;
      auto ptr = shad::Atomic<uint64_t>::GetPtr(std::get<0>(args));
      auto ptr2 = shad::Atomic<uint64_t>::GetPtr(std::get<1>(args));
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
    std::atomic<uint64_t> temp(kInitValue+cnt);
    uint64_t expected = 0;
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
    ptrs[cnt] = shad::Atomic<uint64_t>::Create(kInitValue+cnt, loc);
    ++cnt;
  }
  shad::rt::Handle h;
  cnt = 0;
  for (auto loc : locs) {
    auto lambda = [](shad::rt::Handle& h, const shad::Atomic<uint64_t>::ObjectID &oid) {
      auto ptr = shad::Atomic<uint64_t>::GetPtr(oid);
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

#if 0
TEST_F(AtomicTest, AsyncFetchAdd) {
  auto locs = shad::rt::allLocalities();
  std::vector<atomicPtr> ptrs(locs.size());
  std::vector<atomicPtr> fetchedPtrs(locs.size());
  size_t cnt = 0;
  for (auto loc : locs) {
    ptrs[cnt] = shad::Atomic<uint64_t>::Create(kInitValue+cnt, loc);
    fetchedPtrs[cnt] = shad::Atomic<uint64_t>::Create(0lu, loc);
    ++cnt;
  }
  shad::rt::Handle h;
  cnt = 0;
  using argsTuple_t = std::tuple<shad::Atomic<uint64_t>::ObjectID,
                                 shad::Atomic<uint64_t>::ObjectID>;
  for (auto loc : locs) {
    auto lambda = [](shad::rt::Handle&, const argsTuple_t &args) {
      uint64_t sum = 0;
      auto ptr = shad::Atomic<uint64_t>::GetPtr(std::get<0>(args));
      auto ptr2 = shad::Atomic<uint64_t>::GetPtr(std::get<1>(args));
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
    std::atomic<uint64_t> temp(kInitValue+cnt);
    uint64_t expected = 0;
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
#endif