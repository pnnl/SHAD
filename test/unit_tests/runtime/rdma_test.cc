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

#include <cstring>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

#include "gtest/gtest.h"

#include "shad/runtime/runtime.h"

static const size_t _n_elements = 10000000; 
struct myelement_t {
  uint8_t first;
  uint16_t second;
  uint32_t third;
};
std::vector<myelement_t> remoteData(_n_elements, {0, 0, 0});

class RDMATest : public ::testing::Test {
 public:
  RDMATest() {}

  void SetUp() {}

  void TearDown() {}
};

TEST_F(RDMATest, synch_dmas) {
  myelement_t val{8, 24, 42};
  std::vector<myelement_t>localData(_n_elements, val);
  for (auto loc : shad::rt::allLocalities()) {
    myelement_t* raddress;
    shad::rt::executeAtWithRet(
        loc,
        [](const size_t &, myelement_t**addr) {
          // std::cout << "this Loc " << shad::rt::thisLocality() << std::endl;
           *addr = remoteData.data();
        },
        size_t(0), &raddress);
    shad::rt::dma(loc, raddress, localData.data(), _n_elements);
    std::tuple<size_t, size_t, size_t> acc(0, 0, 0);
    shad::rt::executeAtWithRet(
        loc,
        [](const size_t &, std::tuple<size_t, size_t, size_t>*addr) {
          size_t acc1(0), acc2(0), acc3(0);
          for (auto el : remoteData){
            acc1 += el.first;
            acc2 += el.second;
            acc3 += el.third;
          }
          std::tuple<size_t, size_t, size_t> acc(acc1, acc2, acc3);
          *addr = acc;
        },
        size_t(0), &acc);
    ASSERT_EQ(std::get<0>(acc), 8*_n_elements);
    ASSERT_EQ(std::get<1>(acc), 24*_n_elements);
    ASSERT_EQ(std::get<2>(acc), 42*_n_elements);

    // cleared vector should be filled with correct data
    // for next iteration by the dma 
    localData = std::vector<myelement_t>(_n_elements, {0, 0, 0});
    shad::rt::dma(localData.data(), loc, raddress, _n_elements);
    size_t acc1(0), acc2(0), acc3(0);
    for (auto el : localData){
      acc1 += el.first;
      acc2 += el.second;
      acc3 += el.third;
    }
    ASSERT_EQ(acc1, 8*_n_elements);
    ASSERT_EQ(acc2, 24*_n_elements);
    ASSERT_EQ(acc3, 42*_n_elements);
  }
}

TEST_F(RDMATest, async_put_sync_get) {
  myelement_t val{8, 24, 42};
  std::vector<myelement_t>localData(_n_elements, val);
  std::vector<myelement_t*> raddresses(shad::rt::numLocalities());
  shad::rt::Handle handle;
  for (auto loc : shad::rt::allLocalities()) {
    shad::rt::executeAtWithRet(
        loc,
        [](const size_t &, myelement_t**addr) {
          // std::cout << "this Loc " << shad::rt::thisLocality() << std::endl;
           *addr = remoteData.data();
        },
        size_t(0), &(raddresses[static_cast<uint32_t>(loc)]));
    
    shad::rt::asyncDma(handle, loc, raddresses[static_cast<uint32_t>(loc)],
                       localData.data(), _n_elements);
  }
  shad::rt::waitForCompletion(handle);
  for (auto loc : shad::rt::allLocalities()) {
    std::tuple<size_t, size_t, size_t> acc(0, 0, 0);
    shad::rt::executeAtWithRet(
        loc,
        [](const size_t &, std::tuple<size_t, size_t, size_t>*addr) {
          size_t acc1(0), acc2(0), acc3(0);
          for (auto el : remoteData){
            acc1 += el.first;
            acc2 += el.second;
            acc3 += el.third;
          }
          std::tuple<size_t, size_t, size_t> acc(acc1, acc2, acc3);
          *addr = acc;
        },
        size_t(0), &acc);
    ASSERT_EQ(std::get<0>(acc), 8*_n_elements);
    ASSERT_EQ(std::get<1>(acc), 24*_n_elements);
    ASSERT_EQ(std::get<2>(acc), 42*_n_elements);
    
    localData = std::vector<myelement_t>(_n_elements, {0, 0, 0});
    shad::rt::dma(localData.data(), loc,
                  raddresses[static_cast<uint32_t>(loc)], _n_elements);
    size_t acc1(0), acc2(0), acc3(0);
    for (auto el : localData) {
      acc1 += el.first;
      acc2 += el.second;
      acc3 += el.third;
    }
    ASSERT_EQ(acc1, 8*_n_elements);
    ASSERT_EQ(acc2, 24*_n_elements);
    ASSERT_EQ(acc3, 42*_n_elements);
  }
}

TEST_F(RDMATest, async_put_async_get) {
  myelement_t val{8, 24, 42};
  std::vector<myelement_t>localData(_n_elements, val);
  std::vector<myelement_t*> raddresses(shad::rt::numLocalities());
  shad::rt::Handle handle;
  for (auto loc : shad::rt::allLocalities()) {
    shad::rt::executeAtWithRet(
        loc,
        [](const size_t &, myelement_t**addr) {
          // std::cout << "this Loc " << shad::rt::thisLocality() << std::endl;
           *addr = remoteData.data();
        },
        size_t(0), &(raddresses[static_cast<uint32_t>(loc)]));
    
    shad::rt::asyncDma(handle, loc, raddresses[static_cast<uint32_t>(loc)],
                       localData.data(), _n_elements);
  }
  shad::rt::waitForCompletion(handle);
  std::vector<std::vector<myelement_t>>getData(shad::rt::numLocalities(),
                                               std::vector<myelement_t>(_n_elements, val));
  for (auto loc : shad::rt::allLocalities()) {
    const myelement_t* laddr = getData[static_cast<uint32_t>(loc)].data();
    shad::rt::asyncDma(handle, laddr, loc,
                       raddresses[static_cast<uint32_t>(loc)], _n_elements);
  }
  shad::rt::waitForCompletion(handle);
  for (auto loc : shad::rt::allLocalities()) {
    size_t acc1(0), acc2(0), acc3(0);
    for (auto el : getData[static_cast<uint32_t>(loc)]) {
      acc1 += el.first;
      acc2 += el.second;
      acc3 += el.third;
    }
    ASSERT_EQ(acc1, 8*_n_elements);
    ASSERT_EQ(acc2, 24*_n_elements);
    ASSERT_EQ(acc3, 42*_n_elements);
  }
}

  

#if 0
TEST_F(ExecuteAtTest, AsyncExecuteAtExplicit) {
  shad::rt::Handle handle;

  for (auto &loc : shad::rt::allLocalities()) {
    size_t value = kValue + static_cast<uint32_t>(loc);
    exData data = {value, loc};
    std::shared_ptr<uint8_t> args(new uint8_t[sizeof(exData)],
                                  std::default_delete<uint8_t[]>());

    std::memcpy(args.get(), &data, sizeof(exData));
    for (size_t i = 0; i < kNumIters; i++) {
      shad::rt::asyncExecuteAt(handle, loc, asyncIncrFunExplicit, args,
                               sizeof(exData));
    }
  }

  ASSERT_FALSE(handle.IsNull());
  shad::rt::waitForCompletion(handle);
  for (auto &loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, check, nullptr, 0);
  }
}

TEST_F(ExecuteAtTest, AsyncExecuteAt) {
  shad::rt::Handle handle;
  std::vector<exData> argv(shad::rt::numLocalities());
  for (auto loc : shad::rt::allLocalities()) {
    size_t value = kValue + static_cast<uint32_t>(loc);
    exData data = {value, loc};
    argv[static_cast<uint32_t>(loc)] = data;
    for (size_t i = 0; i < kNumIters; i++) {
      shad::rt::asyncExecuteAt(handle, loc, asyncIncrFun,
                               argv[static_cast<uint32_t>(loc)]);
    }
  }
  ASSERT_FALSE(handle.IsNull());
  shad::rt::waitForCompletion(handle);
  for (auto loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, check, nullptr, 0);
  }
}

TEST_F(ExecuteAtTest, SyncExecuteAt) {
  for (auto loc : shad::rt::allLocalities()) {
    size_t value = kValue + static_cast<uint32_t>(loc);
    exData data = {value, loc};
    for (size_t i = 0; i < kNumIters; i++) {
      shad::rt::executeAt(loc, incrFun, data);
    }
  }
  for (auto loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, check, nullptr, 0);
  }
}

TEST_F(ExecuteAtTest, SyncExecuteAtWithRetBuffExplicit) {
  for (auto loc : shad::rt::allLocalities()) {
    size_t value = kValue + static_cast<uint32_t>(loc);
    exData data = {value, loc};

    std::shared_ptr<uint8_t> argsBuffer(new uint8_t[sizeof(exData)],
                                        std::default_delete<uint8_t[]>());
    std::memcpy(argsBuffer.get(), &data, sizeof(exData));

    exData retData;
    uint32_t retSize;
    for (size_t i = 0; i < kNumIters; i++) {
      shad::rt::executeAtWithRetBuff(
          loc, incrFunWithRetBuffExplicit, argsBuffer, sizeof(data),
          reinterpret_cast<uint8_t *>(&retData), &retSize);
      ASSERT_EQ(retSize, sizeof(exData));
      ASSERT_EQ(retData.locality, loc);
      ASSERT_EQ(retData.counter,
                (kValue + static_cast<uint32_t>(loc)) * (i + 1));
    }
  }
  for (auto loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, check, nullptr, 0);
  }
}

TEST_F(ExecuteAtTest, AsyncExecuteAtWithRetBuffExplicit) {
  std::vector<exData> retData(shad::rt::numLocalities() * kNumIters);
  std::vector<uint32_t> retSizes(shad::rt::numLocalities() * kNumIters, 0);

  shad::rt::Handle handle;

  for (auto &locality : shad::rt::allLocalities()) {
    uint32_t localityNumber = static_cast<uint32_t>(locality);
    size_t value = kValue + localityNumber;
    exData data = {value, locality};
    std::shared_ptr<uint8_t> argsBuffer(new uint8_t[sizeof(exData)],
                                        std::default_delete<uint8_t[]>());
    std::memcpy(argsBuffer.get(), &data, sizeof(exData));

    for (size_t i = 0; i < kNumIters; ++i) {
      size_t idx = i * shad::rt::numLocalities() + localityNumber;
      shad::rt::asyncExecuteAtWithRetBuff(
          handle, locality, asynIncrFunWithRetBuffExplicit, argsBuffer,
          sizeof(exData), reinterpret_cast<uint8_t *>(&retData[idx]),
          &retSizes[idx]);
    }
  }

  ASSERT_FALSE(handle.IsNull());
  shad::rt::waitForCompletion(handle);

  for (auto &locality : shad::rt::allLocalities()) {
    uint32_t localityNumber = static_cast<uint32_t>(locality);

    for (size_t i = 0; i < kNumIters; ++i) {
      size_t idx = i * shad::rt::numLocalities() + localityNumber;

      ASSERT_EQ(retSizes[idx], sizeof(exData));
      ASSERT_EQ(retData[idx].counter, kValue + localityNumber + 1);
    }
  }

  for (auto &loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, check, nullptr, 0);
  }
}

TEST_F(ExecuteAtTest, SyncExecuteAtWithRetBuff) {
  for (auto loc : shad::rt::allLocalities()) {
    size_t value = kValue + static_cast<uint32_t>(loc);
    exData data = {value, loc};
    exData retData;
    uint32_t retSize = sizeof(retData);
    for (size_t i = 0; i < kNumIters; i++) {
      shad::rt::executeAtWithRetBuff(loc, incrFunWithRetBuff, data,
                                     reinterpret_cast<uint8_t *>(&retData),
                                     &retSize);
      ASSERT_EQ(retSize, sizeof(exData));
      ASSERT_EQ(retData.locality, loc);
      ASSERT_EQ(retData.counter,
                (kValue + static_cast<uint32_t>(loc)) * (i + 1));
    }
  }
  for (auto loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, check, nullptr, 0);
  }
}

TEST_F(ExecuteAtTest, SyncExecuteAtWithRetExplicit) {
  for (auto loc : shad::rt::allLocalities()) {
    size_t value = kValue + static_cast<uint32_t>(loc);
    exData data = {value, loc};
    std::shared_ptr<uint8_t> argsBuffer(new uint8_t[sizeof(exData)],
                                        std::default_delete<uint8_t[]>());
    std::memcpy(argsBuffer.get(), &data, sizeof(exData));

    exData retData;
    for (size_t i = 0; i < kNumIters; i++) {
      shad::rt::executeAtWithRet(loc, incrFunWithRetExplicit, argsBuffer,
                                 sizeof(data), &retData);
      ASSERT_EQ(retData.locality, loc);
      ASSERT_EQ(retData.counter,
                (kValue + static_cast<uint32_t>(loc)) * (i + 1));
    }
  }
  for (auto loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, check, nullptr, 0);
  }
}
#endif

