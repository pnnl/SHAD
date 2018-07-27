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
  ArrayTest() { inputData_ = std::vector<size_t>(kArraySize, 42); }

  void SetUp() {
    for (size_t i = 0; i < kArraySize; i++) {
      inputData_[i] = i + 1;
    }
  }

  void TearDown() {}
  std::vector<size_t> inputData_;
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
  ;

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

TEST_F(ArrayTest, CreateNewArray) {
  auto arrayPtr = shad::array<std::size_t, kArraySize>::Create();

  ASSERT_EQ(arrayPtr->size(), kArraySize);
  ASSERT_EQ(arrayPtr->max_size(), kArraySize);
  using value_type = typename shad::array<std::size_t, 123456>::value_type;
  ASSERT_TRUE((std::is_same<std::size_t, value_type>::value));

  arrayPtr->Destroy(arrayPtr->GetGlobalID());
}

TEST_F(ArrayTest, FillArray) {
  auto arrayPtr = shad::array<std::size_t, kArraySize>::Create();

  arrayPtr->fill(0xdeadc0d3);

  ASSERT_EQ(arrayPtr->front(), 0xdeadc0d3);
  ASSERT_EQ(arrayPtr->back(), 0xdeadc0d3);

  for (size_t i = 0; i < arrayPtr->size(); ++i) {
    ASSERT_EQ(arrayPtr->at(i), 0xdeadc0d3) << i;
    arrayPtr->at(i) = i;
  }

  arrayPtr->Destroy(arrayPtr->GetGlobalID());
}

TEST_F(ArrayTest, ArrayIterator) {
  auto arrayPtr = shad::array<std::size_t, kArraySize>::Create();

  std::size_t splitPoint =
      kArraySize / std::max<std::size_t>(shad::rt::numLocalities(), 2) + 1;

  for (size_t i = 0; i < arrayPtr->size(); ++i) {
    arrayPtr->at(i) = i;
  }
  size_t i = 0;
  for (auto v : *arrayPtr) {
    ASSERT_EQ(v, i) << i;
    ++i;
  }

  auto itr = arrayPtr->begin();
  ASSERT_EQ(*itr, 0);

  itr += splitPoint - 10;
  ASSERT_EQ(*itr, splitPoint - 10) << static_cast<std::size_t>(*itr);

  itr -= 10;
  ASSERT_EQ(*itr, splitPoint - 20) << static_cast<std::size_t>(*itr);

  itr += 20;
  ASSERT_EQ(*itr, splitPoint) << static_cast<std::size_t>(*itr);

  itr -= splitPoint - 20;
  ASSERT_EQ(*itr, 20) << static_cast<std::size_t>(*itr);
}

TEST_F(ArrayTest, ArrayIteratorTraitTest) {
  using array_type = shad::array<size_t, kArraySize>;
  using iterator_traits = std::iterator_traits<typename array_type::iterator>;

  ASSERT_TRUE((std::is_same<typename iterator_traits::difference_type,
                            typename array_type::difference_type>::value));
  ASSERT_TRUE((std::is_same<typename iterator_traits::value_type,
                            typename array_type::value_type>::value));
  ASSERT_TRUE((std::is_same<typename iterator_traits::pointer,
                            typename array_type::pointer>::value));
  ASSERT_TRUE((std::is_same<typename iterator_traits::reference,
                            typename array_type::reference>::value));
  ASSERT_TRUE((std::is_same<typename iterator_traits::iterator_category,
                            std::random_access_iterator_tag>::value));

  auto array = array_type::Create();

  array->fill(1);
  array->at(kArraySize / 2) = 0;

  auto e = std::min_element(array->begin(), array->end());
  ASSERT_EQ(*e, 0);
}
