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

#include <algorithm>
#include <type_traits>

#include "gtest/gtest.h"

#include "shad/data_structures/vector.h"

class VectorTest : public ::testing::Test {
 public:
  void SetUp() {}
  void TearDown() {}

  static const size_t kNumElements;
};

const size_t VectorTest::kNumElements = 10001;

template <typename Int>
struct GenerateSequence {
  Int current;
  explicit GenerateSequence(Int start) : current(start) {}
  Int operator()() { return current++; }
};

TEST_F(VectorTest, Types) {
  ASSERT_TRUE((std::is_same<shad::Vector<int>::value_type, int>::value));
}

TEST_F(VectorTest, Creation) {
  auto vectorPtr = shad::Vector<int>::Create(100);

  ASSERT_EQ(vectorPtr->Size(), 100);
  ASSERT_GE(vectorPtr->Capacity(), 100);

  shad::Vector<int>::Destroy(vectorPtr->GetGlobalID());
}

TEST_F(VectorTest, Capacity) {
  auto vectorPtr = shad::Vector<int>::Create(100);

  size_t oldCapacity = vectorPtr->Capacity();
  vectorPtr->Reserve(100);
  size_t newCapacity = vectorPtr->Capacity();

  ASSERT_EQ(oldCapacity, newCapacity);

  vectorPtr->Reserve(oldCapacity + 1000);
  newCapacity = vectorPtr->Capacity();
  ASSERT_GT(newCapacity, oldCapacity);
  ASSERT_GE(newCapacity, oldCapacity + 1000);

  vectorPtr->Clear();
  ASSERT_EQ(vectorPtr->Size(), 0);
  ASSERT_EQ(vectorPtr->Capacity(), 0);

  shad::Vector<int>::Destroy(vectorPtr->GetGlobalID());
}

TEST_F(VectorTest, SingleElementInsert) {
  auto vectorPtr = shad::Vector<int>::Create(0);

  ASSERT_EQ(vectorPtr->Size(), 0);
  ASSERT_GE(vectorPtr->Capacity(), 0);
  ASSERT_TRUE(vectorPtr->Empty());

  vectorPtr->Resize(200);
  ASSERT_EQ(vectorPtr->Size(), 200);
  ASSERT_GE(vectorPtr->Capacity(), 200);
  ASSERT_FALSE(vectorPtr->Empty());

  vectorPtr->InsertAt(0, 100);
  ASSERT_EQ(vectorPtr->At(0), 100);
  ASSERT_EQ(vectorPtr->Front(), 100);

  for (size_t i = 0; i < 200; i++) {
    vectorPtr->InsertAt(i, i + 1);
    ASSERT_EQ(vectorPtr->At(i), i + 1);
  }
  ASSERT_EQ(vectorPtr->Front(), 1);
  ASSERT_EQ(vectorPtr->Back(), 200);
  ASSERT_EQ(vectorPtr->Size(), 200);
  ASSERT_GE(vectorPtr->Capacity(), 200);

  shad::Vector<int>::Destroy(vectorPtr->GetGlobalID());
}

TEST_F(VectorTest, BlockInsert) {
  auto vectorPtr = shad::Vector<int>::Create(5);

  ASSERT_EQ(vectorPtr->Size(), 5);

  std::vector<int> input(100);

  for (size_t i = 0; i < 100; i++) {
    input[i] = i;
  }

  vectorPtr->InsertAt(0, input.begin(), input.end());
  ASSERT_EQ(vectorPtr->Size(), input.size());
  ASSERT_GE(vectorPtr->Capacity(), input.size());
  for (size_t i = 0; i < 100; i++) {
    ASSERT_EQ(vectorPtr->At(i), i);
  }
}

TEST_F(VectorTest, PushBack) {
  auto vectorPtr = shad::Vector<int>::Create(0);

  ASSERT_EQ(vectorPtr->Size(), 0);
  ASSERT_GE(vectorPtr->Capacity(), 0);
  ASSERT_TRUE(vectorPtr->Empty());

  vectorPtr->PushBack(100);
  ASSERT_FALSE(vectorPtr->Empty());
  ASSERT_EQ(vectorPtr->Size(), 1);
  ASSERT_EQ(vectorPtr->At(0), 100);
  ASSERT_EQ(vectorPtr->Front(), 100);
  ASSERT_EQ(vectorPtr->Back(), 100);

  vectorPtr->Clear();
  for (size_t i = 0; i < 200; i++) {
    vectorPtr->PushBack(i + 1);
    ASSERT_EQ(vectorPtr->Size(), i + 1);
    ASSERT_GE(vectorPtr->Capacity(), i + 1);
    ASSERT_EQ(vectorPtr->At(i), i + 1);
  }
  ASSERT_EQ(vectorPtr->Front(), 1);
  ASSERT_EQ(vectorPtr->Back(), 200);
}

TEST_F(VectorTest, InsertAndAsyncAt) {
  auto edsPtr = shad::Vector<size_t>::Create(kNumElements);
  for (size_t i = 0; i < kNumElements; i++) {
    edsPtr->InsertAt(i, i + 1);
  }

  std::vector<size_t> values(kNumElements, 0);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kNumElements; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);

  for (size_t i = 0; i < kNumElements; i++) {
    ASSERT_EQ(values[i], i + 1);
  }

  shad::Vector<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(VectorTest, BufferedSyncInsertAndSyncGet) {
  auto edsPtr = shad::Vector<size_t>::Create(kNumElements);
  for (size_t i = 0; i < kNumElements; i++) {
    edsPtr->BufferedInsertAt(i, i + 1);
  }
  edsPtr->WaitForBufferedInsert();
  for (size_t i = 0; i < kNumElements; i++) {
    size_t value = edsPtr->At(i);
    ASSERT_EQ(value, i + 1);
  }
  shad::Vector<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(VectorTest, BufferedAsyncInsertAndSyncGet) {
  auto edsPtr = shad::Vector<size_t>::Create(kNumElements);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kNumElements; i++) {
    edsPtr->BufferedAsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->WaitForBufferedInsert();
  for (size_t i = 0; i < kNumElements; i++) {
    size_t value = edsPtr->At(i);
    ASSERT_EQ(value, i + 1);
  }
  shad::Vector<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(VectorTest, RangedAsyncInsertAndAsyncGet) {
  std::vector<size_t> values(kNumElements);
  auto edsPtr = shad::Vector<size_t>::Create(kNumElements);

  std::generate(std::begin(values), std::end(values),
                GenerateSequence<size_t>(1));

  shad::rt::Handle handle;
  edsPtr->AsyncInsertAt(handle, 0, std::begin(values), std::end(values));
  shad::rt::waitForCompletion(handle);

  std::fill(std::begin(values), std::end(values), 0);

  for (size_t i = 0; i < kNumElements; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);

  for (size_t i = 0; i < kNumElements; i++) {
    ASSERT_EQ(values[i], i + 1);
  }
  shad::Vector<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(VectorTest, AsyncInsertAndAsyncGet) {
  std::vector<size_t> values(kNumElements);
  auto edsPtr = shad::Vector<size_t>::Create(kNumElements);

  std::generate(std::begin(values), std::end(values),
                GenerateSequence<size_t>(1));

  shad::rt::Handle handle;
  for (auto value : values) {
    edsPtr->AsyncInsertAt(handle, value - 1, value);
  }
  shad::rt::waitForCompletion(handle);

  std::fill(std::begin(values), std::end(values), 0);

  for (size_t i = 0; i < kNumElements; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);

  for (size_t i = 0; i < kNumElements; i++) {
    ASSERT_EQ(values[i], i + 1);
  }

  shad::Vector<size_t>::Destroy(edsPtr->GetGlobalID());
}

static void applyFun(shad::Vector<size_t>::size_type /*i*/, size_t &elem,
                     const size_t &incr) {
  ASSERT_EQ(incr, VectorTest::kNumElements);
  elem += VectorTest::kNumElements;
}

static void applyFunNoArgs(shad::Vector<size_t>::size_type /*i*/,
                           size_t &elem) {
  elem += VectorTest::kNumElements;
}

static void applyFunTwoArgs(shad::Vector<size_t>::size_type /*i*/, size_t &elem,
                            const size_t &arg1, size_t &arg2) {
  ASSERT_EQ(arg1, VectorTest::kNumElements);
  ASSERT_EQ(arg2, VectorTest::kNumElements + 1);
  elem += VectorTest::kNumElements;
}

TEST_F(VectorTest, AsyncInsertSyncApplyAndAsyncGet) {
  std::vector<size_t> values(kNumElements);
  auto edsPtr = shad::Vector<size_t>::Create(kNumElements);

  std::generate(std::begin(values), std::end(values),
                GenerateSequence<size_t>(1));

  shad::rt::Handle handle;
  for (auto value : values) {
    edsPtr->AsyncInsertAt(handle, value - 1, value);
  }
  shad::rt::waitForCompletion(handle);

  std::fill(std::begin(values), std::end(values), 0);

  size_t arg2 = kNumElements + 1;
  for (size_t i = 0; i < kNumElements; i++) {
    edsPtr->Apply(i, applyFunNoArgs);
    edsPtr->Apply(i, applyFun, kNumElements);
    edsPtr->Apply(i, applyFunTwoArgs, kNumElements, arg2);
  }

  for (size_t i = 0; i < kNumElements; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);

  for (size_t i = 0; i < kNumElements; i++) {
    ASSERT_EQ(values[i], i + 1 + (3 * kNumElements));
  }
}

static void asyncApplyFun(shad::rt::Handle & /*unused*/,
                          shad::Vector<size_t>::size_type /*i*/, size_t &elem,
                          const size_t &incr) {
  ASSERT_EQ(incr, VectorTest::kNumElements);
  elem += VectorTest::kNumElements;
}

static void asyncApplyFunNoArgs(shad::rt::Handle & /*unused*/,
                                shad::Vector<size_t>::size_type /*i*/,
                                size_t &elem) {
  elem += VectorTest::kNumElements;
}

static void asyncApplyFunTwoArgs(shad::rt::Handle & /*unused*/,
                                 shad::Vector<size_t>::size_type /*i*/,
                                 size_t &elem, const size_t &arg1,
                                 size_t &arg2) {
  ASSERT_EQ(arg1, VectorTest::kNumElements);
  ASSERT_EQ(arg2, VectorTest::kNumElements + 1);
  elem += VectorTest::kNumElements;
}

TEST_F(VectorTest, AsyncInsertAsyncApplyAndAsyncGet) {
  std::vector<size_t> values(kNumElements);
  auto edsPtr = shad::Vector<size_t>::Create(kNumElements);

  std::generate(std::begin(values), std::end(values),
                GenerateSequence<size_t>(1));

  shad::rt::Handle handle;
  for (auto value : values) {
    edsPtr->AsyncInsertAt(handle, value - 1, value);
  }
  shad::rt::waitForCompletion(handle);

  std::fill(std::begin(values), std::end(values), 0);

  size_t arg2 = kNumElements + 1;
  for (size_t i = 0; i < kNumElements; i++) {
    edsPtr->AsyncApply(handle, i, asyncApplyFunNoArgs);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kNumElements; i++) {
    edsPtr->AsyncApply(handle, i, asyncApplyFun, kNumElements);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kNumElements; i++) {
    edsPtr->AsyncApply(handle, i, asyncApplyFunTwoArgs, kNumElements, arg2);
  }
  shad::rt::waitForCompletion(handle);

  for (size_t i = 0; i < kNumElements; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);

  for (size_t i = 0; i < kNumElements; i++) {
    ASSERT_EQ(values[i], i + 1 + (3 * kNumElements));
  }
}

TEST_F(VectorTest, AsyncInsertSyncForEachInRangeAndAsyncGet) {
  std::vector<size_t> values(kNumElements);
  auto edsPtr = shad::Vector<size_t>::Create(kNumElements);

  std::generate(std::begin(values), std::end(values),
                GenerateSequence<size_t>(1));

  shad::rt::Handle handle;
  for (auto value : values) {
    edsPtr->AsyncInsertAt(handle, value - 1, value);
  }
  shad::rt::waitForCompletion(handle);

  std::fill(std::begin(values), std::end(values), 0);

  edsPtr->ForEachInRange(0, kNumElements, applyFunNoArgs);
  edsPtr->ForEachInRange(0, kNumElements, applyFun, kNumElements);
  size_t arg2 = kNumElements + 1;
  edsPtr->ForEachInRange(0, kNumElements, applyFunTwoArgs, kNumElements, arg2);

  for (size_t i = 0; i < kNumElements; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);

  for (size_t i = 0; i < kNumElements; i++) {
    ASSERT_EQ(values[i], i + 1 + (3 * kNumElements));
  }
}

TEST_F(VectorTest, AsyncInsertAsyncForEachInRangeAndAsyncGet) {
  std::vector<size_t> values(kNumElements);
  auto edsPtr = shad::Vector<size_t>::Create(kNumElements);

  std::generate(std::begin(values), std::end(values),
                GenerateSequence<size_t>(1));

  shad::rt::Handle handle;
  for (auto value : values) {
    edsPtr->AsyncInsertAt(handle, value - 1, value);
  }
  shad::rt::waitForCompletion(handle);

  std::fill(std::begin(values), std::end(values), 0);

  edsPtr->AsyncForEachInRange(handle, 0, kNumElements, asyncApplyFunNoArgs);
  edsPtr->AsyncForEachInRange(handle, 0, kNumElements, asyncApplyFun,
                              kNumElements);
  size_t arg2 = kNumElements + 1;
  edsPtr->AsyncForEachInRange(handle, 0, kNumElements, asyncApplyFunTwoArgs,
                              kNumElements, arg2);
  shad::rt::waitForCompletion(handle);

  for (size_t i = 0; i < kNumElements; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);

  for (size_t i = 0; i < kNumElements; i++) {
    ASSERT_EQ(values[i], i + 1 + (3 * kNumElements));
  }
}
