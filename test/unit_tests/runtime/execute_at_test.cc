//===------------------------------------------------------------*- C++ -*-===//
//
//                                     SHAD
//
//      The Scalable High-performance Algorithms and Data Structure Library
//
//===----------------------------------------------------------------------===//
//
// Copyright 2017 Pacific Northwest National Laboratory
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
#include <vector>

#include "gtest/gtest.h"

#include "shad/runtime/runtime.h"

struct exData {
  size_t counter;
  shad::rt::Locality locality;
  size_t extra;
  void reset() {
    counter = 0;
    locality = shad::rt::Locality();
  }
};

static exData globalData = {0, shad::rt::Locality(), 0ul};
static const unsigned kNumIters = 100;
static const size_t kValue = 3;

static void incrFun(const exData &data) {
  globalData.counter += data.counter;
  globalData.locality = data.locality;
};

static void asyncIncrFun(shad::rt::Handle & /*unused*/, const exData &data) {
  __sync_fetch_and_add(&globalData.counter, data.counter);
  globalData.locality = data.locality;
};

static void incrFunWithRetBuff(const exData &data, uint8_t *result,
                               uint32_t *resSize) {
  globalData.counter += data.counter;
  globalData.locality = data.locality;
  *resSize = sizeof(globalData);
  memcpy(result, &globalData, *resSize);
};

static void incrFunWithRetBuffExplicit(const uint8_t *argsBuffer,
                                       const uint32_t /*bufferSize*/,
                                       uint8_t *result, uint32_t *resSize) {
  const exData data = *reinterpret_cast<const exData *>(argsBuffer);
  globalData.counter += data.counter;
  globalData.locality = data.locality;
  *resSize = sizeof(globalData);
  memcpy(result, &globalData, *resSize);
};

static void asynIncrFunWithRetBuffExplicit(shad::rt::Handle & /*unused*/,
                                           const uint8_t *argsBuffer,
                                           const uint32_t /*bufferSize*/,
                                           uint8_t *result, uint32_t *resSize) {
  const exData &data = *reinterpret_cast<const exData *>(argsBuffer);
  __sync_fetch_and_add(&globalData.counter, data.counter);
  globalData.locality = data.locality;
  exData res{data.counter + 1, data.locality};

  *resSize = sizeof(res);
  *reinterpret_cast<exData *>(result) = res;
};

static void incrFunWithRetExplicit(const uint8_t *argsBuffer,
                                   const uint32_t /*bufferSize*/,
                                   exData *result) {
  const exData data = *reinterpret_cast<const exData *>(argsBuffer);
  globalData.counter += data.counter;
  globalData.locality = data.locality;
  *result = globalData;
};

static void incrFunWithRet(const exData &data, exData *result) {
  globalData.counter += data.counter;
  globalData.locality = data.locality;
  *result = globalData;
};

static void incrFunExplicit(const uint8_t *argsBuffer,
                            const uint32_t /*bufferSize*/) {
  const exData data = *reinterpret_cast<const exData *>(argsBuffer);
  globalData.counter += data.counter;
  globalData.locality = data.locality;
};

static void asyncIncrFunExplicit(shad::rt::Handle & /*unused*/,
                                 const uint8_t *argsBuffer,
                                 const uint32_t /*bufferSize*/) {
  const exData data = *reinterpret_cast<const exData *>(argsBuffer);
  __sync_fetch_and_add(&globalData.counter, data.counter);

  globalData.locality = data.locality;
}

static void check(const uint8_t * /*unused*/, const uint32_t /*unused*/) {
  std::cout << globalData.locality << "counter: " << globalData.counter
            << std::endl;
  ASSERT_EQ(globalData.locality, shad::rt::thisLocality());
  ASSERT_EQ(globalData.counter,
            (kValue + static_cast<uint32_t>(globalData.locality)) * kNumIters);
};

static void resetLocalityData(const uint8_t * /*unused*/,
                              const uint32_t /*unused*/) {
  globalData.reset();
};

static void resetGlobalData() {
  std::shared_ptr<uint8_t> EmptyArray;
  for (auto loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, resetLocalityData, EmptyArray, 0);
  }
};

class ExecuteAtTest : public ::testing::Test {
 public:
  ExecuteAtTest() {}

  void SetUp() { resetGlobalData(); }

  void TearDown() {}
};

TEST_F(ExecuteAtTest, SyncExecuteAtExplicit) {
  for (auto loc : shad::rt::allLocalities()) {
    size_t value = kValue + static_cast<uint32_t>(loc);
    exData data = {value, loc};
    std::shared_ptr<uint8_t> argsBuffer(new uint8_t[sizeof(exData)],
                                        std::default_delete<uint8_t[]>());

    std::memcpy(argsBuffer.get(), &data, sizeof(exData));

    for (size_t i = 0; i < kNumIters; i++) {
      shad::rt::executeAt(loc, incrFunExplicit, argsBuffer, sizeof(data));
    }
  }

  std::shared_ptr<uint8_t> EmptyArray;
  for (auto loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, check, EmptyArray, 0);
  }
}

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

TEST_F(ExecuteAtTest, SyncExecuteAtWithRet) {
  for (auto loc : shad::rt::allLocalities()) {
    size_t value = kValue + static_cast<uint32_t>(loc);
    exData data = {value, loc};
    exData retData;
    for (size_t i = 0; i < kNumIters; i++) {
      shad::rt::executeAtWithRet(loc, incrFunWithRet, data, &retData);
      ASSERT_EQ(retData.locality, loc);
      ASSERT_EQ(retData.counter,
                (kValue + static_cast<uint32_t>(loc)) * (i + 1));
    }
  }
  for (auto loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, check, nullptr, 0);
  }
}

TEST_F(ExecuteAtTest, AsyncExecuteAtWithRet) {
  shad::rt::Handle handle;

  std::vector<exData> argv(shad::rt::numLocalities());
  for (auto &locality : shad::rt::allLocalities()) {
    std::vector<exData> retData(kNumIters);

    uint32_t localityNumber = static_cast<uint32_t>(locality);
    size_t value = kValue + localityNumber;
    exData data = {value, locality};
    argv[localityNumber] = data;

    for (size_t i = 0; i < kNumIters; ++i) {
      shad::rt::asyncExecuteAtWithRet(
          handle, locality,
          [](shad::rt::Handle &handle, const exData &data, exData *result) {
            ASSERT_FALSE(handle.IsNull());
            ASSERT_EQ(data.counter,
                      kValue + static_cast<uint32_t>(shad::rt::thisLocality()));

            globalData.locality = data.locality;

            ASSERT_GE(static_cast<uint32_t>(globalData.locality), 0);
            ASSERT_LT(static_cast<uint32_t>(globalData.locality),
                      shad::rt::numLocalities());

            __sync_fetch_and_add(&globalData.counter, data.counter);
            *result = data;
          },
          argv[localityNumber], &retData[i]);
    }

    ASSERT_FALSE(handle.IsNull());

    shad::rt::waitForCompletion(handle);

    shad::rt::executeAt(locality, check, nullptr, 0);

    for (size_t i = 0; i < kNumIters; ++i) {
      ASSERT_EQ(retData[i].locality, locality);
      ASSERT_EQ(retData[i].counter, value);
    }
  }
}

TEST_F(ExecuteAtTest, AsyncExecuteAtWithRetExplicit) {
  for (auto &loc : shad::rt::allLocalities()) {
    size_t value = kValue + static_cast<uint32_t>(loc);
    std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)],
                                  std::default_delete<uint8_t[]>());
    new (data.get()) exData{value, loc};

    shad::rt::Handle handle;
    std::vector<uint32_t> retSizes(kNumIters);
    std::vector<exData> retData(kNumIters);

    for (size_t i = 0; i < kNumIters; ++i) {
      shad::rt::asyncExecuteAtWithRet(
          handle, loc,
          [](shad::rt::Handle &, const uint8_t *data, const uint32_t size,
             exData *result) {
            const exData *ptr = reinterpret_cast<const exData *>(data);
            ASSERT_EQ(ptr->counter,
                      kValue + static_cast<uint32_t>(shad::rt::thisLocality()));

            globalData.locality = ptr->locality;

            ASSERT_GE(static_cast<uint32_t>(globalData.locality), 0);
            ASSERT_LT(static_cast<uint32_t>(globalData.locality),
                      shad::rt::numLocalities());

            __sync_fetch_and_add(&globalData.counter, ptr->counter);
            *result = *ptr;
          },
          data, sizeof(exData), &retData[i]);
    }

    ASSERT_FALSE(handle.IsNull());

    shad::rt::waitForCompletion(handle);

    shad::rt::executeAt(loc, check, nullptr, 0);

    for (size_t i = 0; i < kNumIters; ++i) {
      ASSERT_EQ(retData[i].locality, loc);
      ASSERT_EQ(retData[i].counter, value);
    }
  }
}

TEST_F(ExecuteAtTest, AsyncExecuteAtWithRetDifferentSizes) {
  struct A {
    size_t a;
    size_t b;
    char c;
  };

  struct B {
    size_t a;
    size_t b;
    char c[10];
    double d;
  };

  shad::rt::Handle handle;
  std::vector<A> retData1(kNumIters * shad::rt::numLocalities());

  for (auto &loc : shad::rt::allLocalities()) {
    uint32_t localityIdx = static_cast<uint32_t>(loc);
    size_t value = kValue + localityIdx;
    std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)],
                                  std::default_delete<uint8_t[]>());
    new (data.get()) exData{value, loc};

    for (size_t i = 0; i < kNumIters; ++i) {
      size_t j = static_cast<uint32_t>(shad::rt::thisLocality()) *
                     shad::rt::numLocalities() +
                 i;

      shad::rt::asyncExecuteAtWithRet(
          handle, loc,
          [](shad::rt::Handle &, const uint8_t *data, const uint32_t size,
             A *result) {
            const exData *ptr = reinterpret_cast<const exData *>(data);

            ASSERT_EQ(ptr->counter,
                      kValue + static_cast<uint32_t>(shad::rt::thisLocality()));

            globalData.locality = ptr->locality;

            ASSERT_GE(static_cast<uint32_t>(globalData.locality), 0);
            ASSERT_LT(static_cast<uint32_t>(globalData.locality),
                      shad::rt::numLocalities());

            __sync_fetch_and_add(&globalData.counter, ptr->counter);
            result->a = ptr->counter;
          },
          data, sizeof(exData), &retData1[j]);
    }
  }

  std::vector<B> retData2(kNumIters * shad::rt::numLocalities());

  for (auto &loc : shad::rt::allLocalities()) {
    uint32_t localityIdx = static_cast<uint32_t>(loc);
    size_t value = kValue + localityIdx;
    std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)],
                                  std::default_delete<uint8_t[]>());
    new (data.get()) exData{value, loc};

    for (size_t i = 0; i < kNumIters; ++i) {
      shad::rt::asyncExecuteAtWithRet(
          handle, loc,
          [](shad::rt::Handle &, const uint8_t *data, const uint32_t size,
             B *result) {
            const exData *ptr = reinterpret_cast<const exData *>(data);
            ASSERT_EQ(ptr->counter,
                      kValue + static_cast<uint32_t>(shad::rt::thisLocality()));

            globalData.locality = ptr->locality;

            ASSERT_GE(static_cast<uint32_t>(globalData.locality), 0);
            ASSERT_LT(static_cast<uint32_t>(globalData.locality),
                      shad::rt::numLocalities());
            __sync_fetch_and_add(&globalData.counter, ptr->counter);
            result->a = ptr->counter;
          },
          data, sizeof(exData), &retData2[i]);
    }
  }

  shad::rt::waitForCompletion(handle);
}

TEST_F(ExecuteAtTest, NotExistingLocality) {
  shad::rt::Locality badLocality(shad::rt::numLocalities());

#define TEST_BAD_LOCALITY(stmt)                             \
  do {                                                      \
    try {                                                   \
      stmt;                                                 \
      FAIL();                                               \
    } catch (const std::exception &e) {                     \
      std::string message = e.what();                       \
      std::stringstream ss;                                 \
      ss << "The system does not include " << badLocality;  \
      ASSERT_NE(std::string::npos, message.find(ss.str())); \
    }                                                       \
  } while (0)

  std::shared_ptr<uint8_t> EmptyArray;

  size_t ret;
  uint8_t retBuffer[10];
  uint32_t retSize;

  TEST_BAD_LOCALITY(
      shad::rt::executeAt(badLocality, [](const size_t &) {}, size_t(0)));

  TEST_BAD_LOCALITY(shad::rt::executeAt(
      badLocality, [](const uint8_t *, uint32_t) {}, EmptyArray, 0));

  TEST_BAD_LOCALITY(shad::rt::executeAtWithRet(
      badLocality, [](const size_t &, size_t *) {}, size_t(0), &ret));

  TEST_BAD_LOCALITY(shad::rt::executeAtWithRet(
      badLocality, [](const uint8_t *, uint32_t, size_t *) {}, nullptr, 0,
      &ret));

  TEST_BAD_LOCALITY(shad::rt::executeAtWithRetBuff(
      badLocality, [](const size_t &, uint8_t *, uint32_t *) {}, size_t(0),
      retBuffer, &retSize));

  TEST_BAD_LOCALITY(shad::rt::executeAtWithRetBuff(
      badLocality, [](const uint8_t *, uint32_t, uint8_t *, uint32_t *) {},
      nullptr, 0, retBuffer, &retSize));

  shad::rt::Handle handle;

  TEST_BAD_LOCALITY(shad::rt::asyncExecuteAt(
      handle, badLocality, [](shad::rt::Handle &, const size_t &) {},
      size_t(0)));

  TEST_BAD_LOCALITY(shad::rt::asyncExecuteAt(
      handle, badLocality, [](shad::rt::Handle &, const uint8_t *, uint32_t) {},
      nullptr, 0));

  TEST_BAD_LOCALITY(shad::rt::asyncExecuteAtWithRet(
      handle, badLocality, [](shad::rt::Handle &, const size_t &, size_t *) {},
      size_t(0), &ret));

  TEST_BAD_LOCALITY(shad::rt::asyncExecuteAtWithRet(
      handle, badLocality,
      [](shad::rt::Handle &, const uint8_t *, uint32_t, size_t *) {}, nullptr,
      0, &ret));

  TEST_BAD_LOCALITY(shad::rt::asyncExecuteAtWithRetBuff(
      handle, badLocality,
      [](shad::rt::Handle &, const size_t &, uint8_t *, uint32_t *) {},
      size_t(0), retBuffer, &retSize));

  TEST_BAD_LOCALITY(shad::rt::asyncExecuteAtWithRetBuff(
      handle, badLocality,
      [](shad::rt::Handle &, const uint8_t *, uint32_t, uint8_t *, uint32_t *) {
      },
      nullptr, 0, retBuffer, &retSize));

  shad::rt::waitForCompletion(handle);

#undef TEST_BAD_LOCALITY
}
