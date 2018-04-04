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

#include <atomic>

#include "gtest/gtest.h"

#include "shad/runtime/runtime.h"

class ExecuteOnAllTest : public ::testing::Test {
 public:
  static std::atomic<int> Counter;

  void SetUp() {
    for (auto &loc : shad::rt::allLocalities()) {
      shad::rt::executeAt(loc, [](const bool &) { Counter = 0; }, false);
    }
  }

  void TearDown() {}
};

std::atomic<int> ExecuteOnAllTest::Counter(0);

struct TestStruct {
  int valueA;
  int valueB;
};

TEST_F(ExecuteOnAllTest, ExecuteOnAllWithStruct) {
  for (auto &loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, [](const bool &) { ASSERT_EQ(Counter, 0); },
                        false);
  }

  shad::rt::executeOnAll([](const bool &) { Counter++; }, false);

  for (auto &loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, [](const bool &) { ASSERT_EQ(Counter, 1); },
                        false);
  }

  shad::rt::executeOnAll(
      [](const TestStruct &S) { Counter = S.valueA + S.valueB; },
      TestStruct{5, 5});

  for (auto &loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, [](const bool &) { ASSERT_EQ(Counter, 10); },
                        false);
  }
}

TEST_F(ExecuteOnAllTest, ExecuteOnAllWithBuffer) {
  for (auto &loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, [](const bool &) { ASSERT_EQ(Counter, 0); },
                        false);
  }

  shad::rt::executeOnAll([](const uint8_t *, const uint32_t) { Counter++; },
                         nullptr, 0);

  for (auto &loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, [](const bool &) { ASSERT_EQ(Counter, 1); },
                        false);
  }

  std::shared_ptr<uint8_t> buffer(new uint8_t[2]{5, 5},
                                  std::default_delete<uint8_t[]>());
  shad::rt::executeOnAll(
      [](const uint8_t *B, const uint32_t S) {
        ASSERT_EQ(S, sizeof(uint8_t) << 1);
        Counter = B[0] + B[1];
      },
      buffer, sizeof(uint8_t) << 1);

  for (auto &loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, [](const bool &) { ASSERT_EQ(Counter, 10); },
                        false);
  }
}

TEST_F(ExecuteOnAllTest, AsyncExecuteOnAllWithStruct) {
  for (auto &loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, [](const bool &) { ASSERT_EQ(Counter, 0); },
                        false);
  }

  {
    shad::rt::Handle handle;
    shad::rt::asyncExecuteOnAll(
        handle, [](shad::rt::Handle &, const bool &) { Counter++; }, false);

    shad::rt::waitForCompletion(handle);
  }

  for (auto &loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, [](const bool &) { ASSERT_EQ(Counter, 1); },
                        false);
  }

  {
    shad::rt::Handle handle;
    shad::rt::asyncExecuteOnAll(handle,
                                [](shad::rt::Handle &, const TestStruct &S) {
                                  Counter = S.valueA + S.valueB;
                                },
                                TestStruct{5, 5});

    shad::rt::waitForCompletion(handle);
  }

  for (auto &loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, [](const bool &) { ASSERT_EQ(Counter, 10); },
                        false);
  }
}

TEST_F(ExecuteOnAllTest, AsyncExecuteOnAllWithBuffer) {
  for (auto &loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, [](const bool &) { ASSERT_EQ(Counter, 0); },
                        false);
  }

  {
    shad::rt::Handle handle;
    shad::rt::asyncExecuteOnAll(
        handle,
        [](shad::rt::Handle &, const uint8_t *, const uint32_t) { Counter++; },
        nullptr, 0);

    shad::rt::waitForCompletion(handle);
  }

  for (auto &loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, [](const bool &) { ASSERT_EQ(Counter, 1); },
                        false);
  }

  {
    shad::rt::Handle handle;

    std::shared_ptr<uint8_t> buffer(new uint8_t[2]{5, 5},
                                    std::default_delete<uint8_t[]>());

    shad::rt::asyncExecuteOnAll(
        handle,
        [](shad::rt::Handle &, const uint8_t *B, const uint32_t S) {
          ASSERT_EQ(S, sizeof(uint8_t) << 1);
          Counter = B[0] + B[1];
        },
        buffer, 2);

    shad::rt::waitForCompletion(handle);
  }

  for (auto &loc : shad::rt::allLocalities()) {
    shad::rt::executeAt(loc, [](const bool &) { ASSERT_EQ(Counter, 10); },
                        false);
  }
}
