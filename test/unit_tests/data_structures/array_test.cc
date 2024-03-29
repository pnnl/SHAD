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

#include "shad/data_structures/array.h"
#include "shad/runtime/runtime.h"

constexpr static size_t kArraySize = 10001;

static size_t kInitValue = 42;

class ArrayTest : public ::testing::Test {
 public:
  ArrayTest() : kSmallArraySize_(std::max<size_t>(1lu, shad::rt::numLocalities()/2)) { 
    inputData_ = std::vector<size_t>(kArraySize, 42);
    smallInputData_ = std::vector<size_t>(kSmallArraySize_, 42);
  }

  void SetUp() {
    for (size_t i = 0; i < kArraySize; i++) {
      inputData_[i] = i + 1;
    }
    for (size_t i = 0; i < kSmallArraySize_; i++) {
      smallInputData_[i] = i + 1;
    }
  }

  void TearDown() {}
  std::vector<size_t> inputData_;
  std::vector<size_t> smallInputData_;
  size_t kSmallArraySize_;
};

TEST_F(ArrayTest, SyncInsertAndSyncGet) {
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->InsertAt(i, i + 1);
  }
  for (size_t i = 0; i < kArraySize; i++) {
    size_t value = edsPtr->At(i);
    ASSERT_EQ(value, i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, AsyncInsertAndSyncGet) {
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    size_t value = edsPtr->At(i);
    ASSERT_EQ(value, i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, AsyncInsertAndAsyncGet) {
  std::vector<size_t> values(kArraySize);
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);

  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);

  shad::rt::Handle handle2;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle2, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle2);

  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, RangedSyncInsertAndAsyncGet) {
  std::vector<size_t> values(kArraySize);

  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  edsPtr->InsertAt(0, inputData_.data(), kArraySize);

  shad::rt::Handle handle2;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle2, i, &values[i]);
  }

  shad::rt::waitForCompletion(handle2);
  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1);
  }

  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, RangedAsyncInsertAndAsyncGet) {
  std::vector<size_t> values(kArraySize);

  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);

  shad::rt::Handle handle;
  edsPtr->AsyncInsertAt(handle, 0, inputData_.data(), kArraySize);
  shad::rt::waitForCompletion(handle);

  shad::rt::Handle handle2;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle2, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle2);

  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, RangedAsyncInsertAndAsyncGetElements) {
  std::vector<size_t> values(kArraySize);

  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  edsPtr->FillPtrs();
  shad::rt::Handle handle;
  edsPtr->AsyncInsertAt(handle, 0, inputData_.data(), kArraySize);
  shad::rt::waitForCompletion(handle);

  shad::rt::Handle handle2;
  edsPtr->AsyncGetElements(handle2, values.data(), 0, kArraySize);
  shad::rt::waitForCompletion(handle2);

  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1);
  }

  uint64_t to_insert2 = kArraySize/2;
  uint64_t idx2 = kArraySize/6;
  std::vector<size_t> values2(to_insert2);

  edsPtr->AsyncGetElements(handle2, values2.data(), idx2, to_insert2);
  shad::rt::waitForCompletion(handle2);

  for (size_t i = 0; i < to_insert2; i++) {
    ASSERT_EQ(values2[i], i + idx2 + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, BufferedSyncInsertAndSyncGet) {
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->BufferedInsertAt(i, i + 1);
  }
  edsPtr->WaitForBufferedInsert();
  for (size_t i = 0; i < kArraySize; i++) {
    size_t value = edsPtr->At(i);
    ASSERT_EQ(value, i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, BufferedAsyncInsertAndSyncGet) {
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->BufferedAsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->WaitForBufferedInsert();
  for (size_t i = 0; i < kArraySize; i++) {
    size_t value = edsPtr->At(i);
    ASSERT_EQ(value, i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

static void applyFun(size_t i, size_t &elem, size_t &incr) {
  ASSERT_EQ(incr, kInitValue);
  ASSERT_EQ(elem, i + 1);
  elem += kInitValue;
}

static void applyFunNoArgs(size_t i, size_t &elem) {
  ASSERT_EQ(elem, i + kInitValue + 1);
  elem += kInitValue;
}

static void applyFunTwoArgs(size_t /*i*/, size_t & /*elem*/, size_t &arg1,
                            size_t &arg2) {
  ASSERT_EQ(arg1, kInitValue);
  ASSERT_EQ(arg2, kInitValue + 2);
}

TEST_F(ArrayTest, AsyncInsertSyncApplyAndAsyncGet) {
  std::vector<size_t> values(kArraySize);
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->Apply(i, applyFun, kInitValue);
  }
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->Apply(i, applyFunNoArgs);
  }
  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->Apply(i, applyFunTwoArgs, arg1, arg2);
  }
  shad::rt::Handle handle2;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle2, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle2);
  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

static void asyncApplyFun(shad::rt::Handle & /*unused*/, size_t i, size_t &elem,
                          size_t &incr) {
  ASSERT_EQ(incr, kInitValue);
  ASSERT_EQ(elem, i + 1);
  elem += kInitValue;
}

static void asyncApplyWRBFun(shad::rt::Handle & /*unused*/, size_t i, size_t &elem, size_t &incr,
                             uint8_t* result, uint32_t* resultSize) {
  ASSERT_EQ(incr, kInitValue);
  ASSERT_EQ(elem, i + 1);
  elem += kInitValue;
  *resultSize = sizeof(elem);
  memcpy(result, &elem, sizeof(elem)); 
}

static void asyncApplyFunNoArgs(shad::rt::Handle & /*unused*/, size_t i,
                                size_t &elem) {
  ASSERT_EQ(elem, i + kInitValue + 1);
  elem += kInitValue;
}

static void asyncApplyFunTwoArgs(shad::rt::Handle & /*unused*/, size_t /*i*/,
                                 size_t & /*elem*/, size_t &arg1,
                                 size_t &arg2) {
  ASSERT_EQ(arg1, kInitValue);
  ASSERT_EQ(arg2, kInitValue + 2);
}

TEST_F(ArrayTest, AsyncInsertAsyncApplyAndAsyncGet) {
  std::vector<size_t> values(kArraySize);
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);

  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);

  shad::rt::Handle handle2;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncApply(handle2, i, asyncApplyFun, kInitValue);
  }
  shad::rt::waitForCompletion(handle2);

  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncApply(handle2, i, asyncApplyFunNoArgs);
  }
  shad::rt::waitForCompletion(handle2);

  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncApply(handle2, i, asyncApplyFunTwoArgs, arg1, arg2);
  }
  shad::rt::waitForCompletion(handle2);
  shad::rt::Handle handle3;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle3, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle3);
  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, AsyncInsertAsyncApplyWRBAndAsyncGet) {
  std::vector<size_t> values(kArraySize);
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);

  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);

  shad::rt::Handle handle2;
  std::vector<size_t> ret_values(kArraySize);
  std::vector<uint32_t> ret_sizes(kArraySize);
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncApplyWithRetBuff(handle2, i, asyncApplyWRBFun,
                                  (uint8_t*)(&ret_values[i]),
                                  &ret_sizes[i], kInitValue);
  }
  shad::rt::waitForCompletion(handle2);

  shad::rt::Handle handle3;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle3, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle3);
  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1 + kInitValue);
    ASSERT_EQ(ret_values[i], i + 1 + kInitValue);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, AsyncInsertSyncForEachInRangeAndAsyncGet) {
  std::vector<size_t> values(kArraySize);
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->ForEachInRange(0lu, kArraySize, applyFun, kInitValue);
  edsPtr->ForEachInRange(0lu, kArraySize, applyFunNoArgs);
  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  edsPtr->ForEachInRange(0lu, kArraySize, applyFunTwoArgs, arg1, arg2);
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, AsyncInsertSyncForEachAndAsyncGet) {
  std::vector<size_t> values(kArraySize);
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->ForEach(applyFun, kInitValue);
  edsPtr->ForEach(applyFunNoArgs);
  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  edsPtr->ForEach(applyFunTwoArgs, arg1, arg2);
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, AsyncInsertAsyncForEachInRangeAndAsyncGet) {
  std::vector<size_t> values(kArraySize);
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEachInRange(handle, 0lu, kArraySize, asyncApplyFun,
                              kInitValue);
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEachInRange(handle, 0lu, kArraySize, asyncApplyFunNoArgs);
  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEachInRange(handle, 0lu, kArraySize, asyncApplyFunTwoArgs,
                              arg1, arg2);
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, AsyncInsertAsyncForEachAndAsyncGet) {
  std::vector<size_t> values(kArraySize);
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEach(handle, asyncApplyFun, kInitValue);
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEach(handle, asyncApplyFunNoArgs);
  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEach(handle, asyncApplyFunTwoArgs, arg1, arg2);
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, SmallSyncInsertAndSyncGet) {
  auto edsPtr = shad::Array<size_t>::Create(kSmallArraySize_, kInitValue);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->InsertAt(i, i + 1);
  }
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    size_t value = edsPtr->At(i);
    ASSERT_EQ(value, i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, SmallAsyncInsertAndSyncGet) {
  auto edsPtr = shad::Array<size_t>::Create(kSmallArraySize_, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    size_t value = edsPtr->At(i);
    ASSERT_EQ(value, i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, SmallAsyncInsertAndAsyncGet) {
  std::vector<size_t> values(kSmallArraySize_);
  auto edsPtr = shad::Array<size_t>::Create(kSmallArraySize_, kInitValue);

  shad::rt::Handle handle;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);

  shad::rt::Handle handle2;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncAt(handle2, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle2);

  for (size_t i = 0; i < kSmallArraySize_; i++) {
    ASSERT_EQ(values[i], i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, SmallRangedSyncInsertAndAsyncGet) {
  std::vector<size_t> values(kSmallArraySize_);

  auto edsPtr = shad::Array<size_t>::Create(kSmallArraySize_, kInitValue);
  edsPtr->InsertAt(0, smallInputData_.data(), kSmallArraySize_);

  shad::rt::Handle handle2;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncAt(handle2, i, &values[i]);
  }

  shad::rt::waitForCompletion(handle2);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    ASSERT_EQ(values[i], i + 1);
  }

  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, SmallRangedAsyncInsertAndAsyncGet) {
  std::vector<size_t> values(kSmallArraySize_);

  auto edsPtr = shad::Array<size_t>::Create(kSmallArraySize_, kInitValue);

  shad::rt::Handle handle;
  edsPtr->AsyncInsertAt(handle, 0, smallInputData_.data(), kSmallArraySize_);
  shad::rt::waitForCompletion(handle);

  shad::rt::Handle handle2;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncAt(handle2, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle2);

  for (size_t i = 0; i < kSmallArraySize_; i++) {
    ASSERT_EQ(values[i], i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, SmallRangedAsyncInsertAndAsyncGetElements) {
  std::vector<size_t> values(kSmallArraySize_);

  auto edsPtr = shad::Array<size_t>::Create(kSmallArraySize_, kInitValue);
  edsPtr->FillPtrs();
  shad::rt::Handle handle;
  edsPtr->AsyncInsertAt(handle, 0, smallInputData_.data(), kSmallArraySize_);
  shad::rt::waitForCompletion(handle);

  shad::rt::Handle handle2;
  edsPtr->AsyncGetElements(handle2, values.data(), 0, kSmallArraySize_);
  shad::rt::waitForCompletion(handle2);

  for (size_t i = 0; i < kSmallArraySize_; i++) {
    ASSERT_EQ(values[i], i + 1);
  }

  uint64_t to_insert2 = kSmallArraySize_/2;
  uint64_t idx2 = kSmallArraySize_/6;
  std::vector<size_t> values2(to_insert2);

  edsPtr->AsyncGetElements(handle2, values2.data(), idx2, to_insert2);
  shad::rt::waitForCompletion(handle2);

  for (size_t i = 0; i < to_insert2; i++) {
    ASSERT_EQ(values2[i], i + idx2 + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, SmallBufferedSyncInsertAndSyncGet) {
  auto edsPtr = shad::Array<size_t>::Create(kSmallArraySize_, kInitValue);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->BufferedInsertAt(i, i + 1);
  }
  edsPtr->WaitForBufferedInsert();
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    size_t value = edsPtr->At(i);
    ASSERT_EQ(value, i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, SmallBufferedAsyncInsertAndSyncGet) {
  auto edsPtr = shad::Array<size_t>::Create(kSmallArraySize_, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->BufferedAsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->WaitForBufferedInsert();
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    size_t value = edsPtr->At(i);
    ASSERT_EQ(value, i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, SmallAsyncInsertSyncApplyAndAsyncGet) {
  std::vector<size_t> values(kSmallArraySize_);
  auto edsPtr = shad::Array<size_t>::Create(kSmallArraySize_, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->Apply(i, applyFun, kInitValue);
  }
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->Apply(i, applyFunNoArgs);
  }
  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->Apply(i, applyFunTwoArgs, arg1, arg2);
  }
  shad::rt::Handle handle2;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncAt(handle2, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle2);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, SmallAsyncInsertAsyncApplyAndAsyncGet) {
  std::vector<size_t> values(kSmallArraySize_);
  auto edsPtr = shad::Array<size_t>::Create(kSmallArraySize_, kInitValue);

  shad::rt::Handle handle;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);

  shad::rt::Handle handle2;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncApply(handle2, i, asyncApplyFun, kInitValue);
  }
  shad::rt::waitForCompletion(handle2);

  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncApply(handle2, i, asyncApplyFunNoArgs);
  }
  shad::rt::waitForCompletion(handle2);

  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncApply(handle2, i, asyncApplyFunTwoArgs, arg1, arg2);
  }
  shad::rt::waitForCompletion(handle2);
  shad::rt::Handle handle3;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncAt(handle3, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle3);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, SmallAsyncInsertAsyncApplyWRBAndAsyncGet) {
  std::vector<size_t> values(kSmallArraySize_);
  auto edsPtr = shad::Array<size_t>::Create(kSmallArraySize_, kInitValue);

  shad::rt::Handle handle;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);

  shad::rt::Handle handle2;
  std::vector<size_t> ret_values(kSmallArraySize_);
  std::vector<uint32_t> ret_sizes(kSmallArraySize_);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncApplyWithRetBuff(handle2, i, asyncApplyWRBFun,
                                  (uint8_t*)(&ret_values[i]),
                                  &ret_sizes[i], kInitValue);
  }
  shad::rt::waitForCompletion(handle2);

  shad::rt::Handle handle3;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncAt(handle3, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle3);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    ASSERT_EQ(values[i], i + 1 + kInitValue);
    ASSERT_EQ(ret_values[i], i + 1 + kInitValue);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, SmallAsyncInsertSyncForEachInRangeAndAsyncGet) {
  std::vector<size_t> values(kSmallArraySize_);
  auto edsPtr = shad::Array<size_t>::Create(kSmallArraySize_, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->ForEachInRange(0lu, kSmallArraySize_, applyFun, kInitValue);
  edsPtr->ForEachInRange(0lu, kSmallArraySize_, applyFunNoArgs);
  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  edsPtr->ForEachInRange(0lu, kSmallArraySize_, applyFunTwoArgs, arg1, arg2);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, SmallAsyncInsertSyncForEachAndAsyncGet) {
  std::vector<size_t> values(kSmallArraySize_);
  auto edsPtr = shad::Array<size_t>::Create(kSmallArraySize_, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->ForEach(applyFun, kInitValue);
  edsPtr->ForEach(applyFunNoArgs);
  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  edsPtr->ForEach(applyFunTwoArgs, arg1, arg2);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, SmallAsyncInsertAsyncForEachInRangeAndAsyncGet) {
  std::vector<size_t> values(kSmallArraySize_);
  auto edsPtr = shad::Array<size_t>::Create(kSmallArraySize_, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEachInRange(handle, 0lu, kSmallArraySize_, asyncApplyFun,
                              kInitValue);
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEachInRange(handle, 0lu, kSmallArraySize_, asyncApplyFunNoArgs);
  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEachInRange(handle, 0lu, kSmallArraySize_, asyncApplyFunTwoArgs,
                              arg1, arg2);
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, SmallAsyncInsertAsyncForEachAndAsyncGet) {
  std::vector<size_t> values(kSmallArraySize_);
  auto edsPtr = shad::Array<size_t>::Create(kSmallArraySize_, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEach(handle, asyncApplyFun, kInitValue);
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEach(handle, asyncApplyFunNoArgs);
  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEach(handle, asyncApplyFunTwoArgs, arg1, arg2);
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kSmallArraySize_; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}
