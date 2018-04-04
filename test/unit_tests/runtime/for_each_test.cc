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

class ForEachTest : public ::testing::Test {
 public:
  static std::atomic<int> Counter;

  void SetUp() {
    for (auto &loc : shad::rt::allLocalities()) {
      shad::rt::executeAt(loc, [](const bool &) { Counter = 0; }, false);
    }
  }

  void TearDown() {}
};

std::atomic<int> ForEachTest::Counter(0);

struct TestStruct {
  int valueA;
  int valueB;
};

TEST_F(ForEachTest, ForEachOnAllWithStruct) {
  shad::rt::executeOnAll([](const bool &) { ASSERT_EQ(Counter, 0); }, false);

  shad::rt::forEachOnAll(
      [](const TestStruct &args, size_t i) {
        ASSERT_GE(i, 0);
        ASSERT_LT(i,
                  shad::rt::numLocalities() * shad::rt::impl::getConcurrency());
        ASSERT_EQ(args.valueA, 5);
        ASSERT_EQ(args.valueB, 5);
        Counter++;
      },
      TestStruct{5, 5},
      shad::rt::numLocalities() * shad::rt::impl::getConcurrency());

  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, shad::rt::impl::getConcurrency());
      },
      false);

  shad::rt::forEachOnAll(
      [](const TestStruct &args, size_t i) {
        ASSERT_GE(i, 0);
        ASSERT_LT(i,
                  shad::rt::numLocalities() * shad::rt::impl::getConcurrency());
        ASSERT_EQ(args.valueA, 5);
        ASSERT_EQ(args.valueB, 5);
        Counter += args.valueA + args.valueB;
      },
      TestStruct{5, 5},
      shad::rt::numLocalities() * shad::rt::impl::getConcurrency());

  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, 11 * shad::rt::impl::getConcurrency());
      },
      false);
}

TEST_F(ForEachTest, ForEachOnAllWithBuffer) {
  shad::rt::executeOnAll([](const bool &) { ASSERT_EQ(Counter, 0); }, false);

  std::shared_ptr<uint8_t> buffer(new uint8_t[2]{5, 5},
                                  std::default_delete<uint8_t[]>());

  shad::rt::forEachOnAll(
      [](const uint8_t *input, const uint32_t size, size_t i) {
        ASSERT_EQ(size, 2);
        ASSERT_GE(i, 0);
        ASSERT_LT(i,
                  shad::rt::numLocalities() * shad::rt::impl::getConcurrency());
        ASSERT_EQ(input[0], 5);
        ASSERT_EQ(input[1], 5);
        Counter++;
      },
      buffer, 2, shad::rt::numLocalities() * shad::rt::impl::getConcurrency());

  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, shad::rt::impl::getConcurrency());
      },
      false);

  shad::rt::forEachOnAll(
      [](const uint8_t *input, const uint32_t size, size_t i) {
        ASSERT_EQ(size, 2);
        ASSERT_GE(i, 0);
        ASSERT_LT(i,
                  shad::rt::numLocalities() * shad::rt::impl::getConcurrency());
        ASSERT_EQ(input[0], 5);
        ASSERT_EQ(input[1], 5);
        Counter += input[0] + input[1];
      },
      buffer, 2, shad::rt::numLocalities() * shad::rt::impl::getConcurrency());

  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, 11 * shad::rt::impl::getConcurrency());
      },
      false);
}

TEST_F(ForEachTest, ForEachAtWithStruct) {
  shad::rt::executeOnAll([](const bool &) { ASSERT_EQ(Counter, 0); }, false);

  for (auto &locality : shad::rt::allLocalities()) {
    shad::rt::forEachAt(locality,
                        [](const TestStruct &args, size_t i) {
                          ASSERT_GE(i, 0);
                          ASSERT_LT(i, shad::rt::numLocalities() *
                                           shad::rt::impl::getConcurrency());
                          ASSERT_EQ(args.valueA, 5);
                          ASSERT_EQ(args.valueB, 5);
                          Counter++;
                        },
                        TestStruct{5, 5}, shad::rt::impl::getConcurrency());
  }

  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, shad::rt::impl::getConcurrency());
      },
      false);

  for (auto &locality : shad::rt::allLocalities()) {
    shad::rt::forEachAt(locality,
                        [](const TestStruct &args, size_t i) {
                          ASSERT_GE(i, 0);
                          ASSERT_LT(i, shad::rt::numLocalities() *
                                           shad::rt::impl::getConcurrency());
                          ASSERT_EQ(args.valueA, 5);
                          ASSERT_EQ(args.valueB, 5);
                          Counter += args.valueA + args.valueB;
                        },
                        TestStruct{5, 5}, shad::rt::impl::getConcurrency());
  }
  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, 11 * shad::rt::impl::getConcurrency());
      },
      false);
}

TEST_F(ForEachTest, ForEachAtWithBuffer) {
  shad::rt::executeOnAll([](const bool &) { ASSERT_EQ(Counter, 0); }, false);

  std::shared_ptr<uint8_t> buffer(new uint8_t[2]{5, 5},
                                  std::default_delete<uint8_t[]>());

  for (auto &locality : shad::rt::allLocalities()) {
    shad::rt::forEachAt(
        locality,
        [](const uint8_t *input, const uint32_t size, size_t i) {
          ASSERT_EQ(size, 2);
          ASSERT_GE(i, 0);
          ASSERT_LT(i, shad::rt::impl::getConcurrency());
          ASSERT_EQ(input[0], 5);
          ASSERT_EQ(input[1], 5);

          Counter++;
        },
        buffer, 2, shad::rt::impl::getConcurrency());
  }

  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, shad::rt::impl::getConcurrency());
      },
      false);

  for (auto &locality : shad::rt::allLocalities()) {
    shad::rt::forEachAt(
        locality,
        [](const uint8_t *input, const uint32_t size, size_t i) {
          ASSERT_EQ(size, 2);
          ASSERT_GE(i, 0);
          ASSERT_LT(i, shad::rt::impl::getConcurrency());
          ASSERT_EQ(input[0], 5);
          ASSERT_EQ(input[1], 5);

          Counter += input[0] + input[1];
        },
        buffer, 2, shad::rt::impl::getConcurrency());
  }

  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, 11 * shad::rt::impl::getConcurrency());
      },
      false);
}

TEST_F(ForEachTest, AsyncForEachOnAllWithStruct) {
  shad::rt::executeOnAll([](const bool &) { ASSERT_EQ(Counter, 0); }, false);

  {
    shad::rt::Handle handle;
    shad::rt::asyncForEachOnAll(
        handle,
        [](shad::rt::Handle &handle, const TestStruct &args, size_t i) {
          ASSERT_FALSE(handle.IsNull());
          ASSERT_GE(i, 0);
          ASSERT_LT(
              i, shad::rt::numLocalities() * shad::rt::impl::getConcurrency());
          ASSERT_EQ(args.valueA, 5);
          ASSERT_EQ(args.valueB, 5);
          Counter++;
        },
        TestStruct{5, 5},
        shad::rt::numLocalities() * shad::rt::impl::getConcurrency());

    shad::rt::waitForCompletion(handle);
  }

  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, shad::rt::impl::getConcurrency());
      },
      false);

  {
    shad::rt::Handle handle;
    shad::rt::asyncForEachOnAll(
        handle,
        [](shad::rt::Handle &handle, const TestStruct &args, size_t i) {
          ASSERT_FALSE(handle.IsNull());
          ASSERT_GE(i, 0);
          ASSERT_LT(
              i, shad::rt::numLocalities() * shad::rt::impl::getConcurrency());
          ASSERT_EQ(args.valueA, 5);
          ASSERT_EQ(args.valueB, 5);
          Counter += args.valueA + args.valueB;
        },
        TestStruct{5, 5},
        shad::rt::numLocalities() * shad::rt::impl::getConcurrency());

    shad::rt::waitForCompletion(handle);
  }

  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, 11 * shad::rt::impl::getConcurrency());
      },
      false);
}

TEST_F(ForEachTest, AsyncForEachOnAllWithBuffer) {
  shad::rt::executeOnAll([](const bool &) { ASSERT_EQ(Counter, 0); }, false);

  std::shared_ptr<uint8_t> buffer(new uint8_t[2]{5, 5},
                                  std::default_delete<uint8_t[]>());
  {
    shad::rt::Handle handle;
    shad::rt::asyncForEachOnAll(
        handle,
        [](shad::rt::Handle &handle, const uint8_t *input, const uint32_t size,
           size_t i) {
          ASSERT_FALSE(handle.IsNull());
          ASSERT_EQ(size, 2);
          ASSERT_GE(i, 0);
          ASSERT_LT(
              i, shad::rt::numLocalities() * shad::rt::impl::getConcurrency());
          ASSERT_EQ(input[0], 5);
          ASSERT_EQ(input[1], 5);
          Counter++;
        },
        buffer, 2,
        shad::rt::numLocalities() * shad::rt::impl::getConcurrency());

    shad::rt::waitForCompletion(handle);
  }

  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, shad::rt::impl::getConcurrency());
      },
      false);

  {
    shad::rt::Handle handle;
    shad::rt::asyncForEachOnAll(
        handle,
        [](shad::rt::Handle &handle, const uint8_t *input, const uint32_t size,
           size_t i) {
          ASSERT_FALSE(handle.IsNull());
          ASSERT_EQ(size, 2);
          ASSERT_GE(i, 0);
          ASSERT_LT(
              i, shad::rt::numLocalities() * shad::rt::impl::getConcurrency());
          ASSERT_EQ(input[0], 5);
          ASSERT_EQ(input[1], 5);
          Counter += input[0] + input[1];
        },
        buffer, 2,
        shad::rt::numLocalities() * shad::rt::impl::getConcurrency());

    shad::rt::waitForCompletion(handle);
  }

  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, 11 * shad::rt::impl::getConcurrency());
      },
      false);
}

TEST_F(ForEachTest, AsyncForEachAtWithStruct) {
  shad::rt::executeOnAll([](const bool &) { ASSERT_EQ(Counter, 0); }, false);

  {
    shad::rt::Handle handle;
    for (auto &locality : shad::rt::allLocalities()) {
      shad::rt::asyncForEachAt(
          handle, locality,
          [](shad::rt::Handle &handle, const TestStruct &args, size_t i) {
            ASSERT_FALSE(handle.IsNull());
            ASSERT_GE(i, 0);
            ASSERT_LT(i, shad::rt::numLocalities() *
                             shad::rt::impl::getConcurrency());
            ASSERT_EQ(args.valueA, 5);
            ASSERT_EQ(args.valueB, 5);
            Counter++;
          },
          TestStruct{5, 5}, shad::rt::impl::getConcurrency());
    }

    shad::rt::waitForCompletion(handle);
  }

  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, shad::rt::impl::getConcurrency());
      },
      false);

  {
    shad::rt::Handle handle;

    for (auto &locality : shad::rt::allLocalities()) {
      shad::rt::asyncForEachAt(
          handle, locality,
          [](shad::rt::Handle &handle, const TestStruct &args, size_t i) {
            ASSERT_FALSE(handle.IsNull());
            ASSERT_GE(i, 0);
            ASSERT_LT(i, shad::rt::numLocalities() *
                             shad::rt::impl::getConcurrency());
            ASSERT_EQ(args.valueA, 5);
            ASSERT_EQ(args.valueB, 5);
            Counter += args.valueA + args.valueB;
          },
          TestStruct{5, 5}, shad::rt::impl::getConcurrency());
    }

    shad::rt::waitForCompletion(handle);
  }

  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, 11 * shad::rt::impl::getConcurrency());
      },
      false);
}

TEST_F(ForEachTest, AsyncForEachAtWithBuffer) {
  shad::rt::executeOnAll([](const bool &) { ASSERT_EQ(Counter, 0); }, false);

  std::shared_ptr<uint8_t> buffer(new uint8_t[2]{5, 5},
                                  std::default_delete<uint8_t[]>());

  {
    shad::rt::Handle handle;

    for (auto &locality : shad::rt::allLocalities()) {
      shad::rt::asyncForEachAt(
          handle, locality,
          [](shad::rt::Handle &handle, const uint8_t *input,
             const uint32_t size, size_t i) {
            ASSERT_FALSE(handle.IsNull());
            ASSERT_EQ(size, 2);
            ASSERT_GE(i, 0);
            ASSERT_LT(i, shad::rt::impl::getConcurrency());
            ASSERT_EQ(input[0], 5);
            ASSERT_EQ(input[1], 5);

            Counter++;
          },
          buffer, 2, shad::rt::impl::getConcurrency());
    }

    shad::rt::waitForCompletion(handle);
  }

  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, shad::rt::impl::getConcurrency())
            << shad::rt::thisLocality();
      },
      false);

  {
    shad::rt::Handle handle;

    for (auto &locality : shad::rt::allLocalities()) {
      shad::rt::asyncForEachAt(
          handle, locality,
          [](shad::rt::Handle &handle, const uint8_t *input,
             const uint32_t size, size_t i) {
            ASSERT_EQ(size, 2);
            ASSERT_GE(i, 0);
            ASSERT_LT(i, shad::rt::impl::getConcurrency());
            ASSERT_EQ(input[0], 5);
            ASSERT_EQ(input[1], 5);

            Counter += input[0] + input[1];
          },
          buffer, 2, shad::rt::impl::getConcurrency());
    }

    shad::rt::waitForCompletion(handle);
  }

  shad::rt::executeOnAll(
      [](const bool &) {
        ASSERT_EQ(Counter, 11 * shad::rt::impl::getConcurrency());
      },
      false);
}

TEST_F(ForEachTest, SyncZeroIterations) {
  std::shared_ptr<uint8_t> data(new uint8_t[2]{5, 5},
                                std::default_delete<uint8_t[]>());

  shad::rt::executeOnAll([](const bool &) { ASSERT_EQ(Counter, 0); }, false);

  for (auto &locality : shad::rt::allLocalities()) {
    shad::rt::forEachAt(locality,
                        [](const TestStruct &args, size_t i) {
                          ASSERT_GE(i, 0);
                          ASSERT_LT(i, shad::rt::numLocalities() *
                                           shad::rt::impl::getConcurrency());
                          ASSERT_EQ(args.valueA, 5);
                          ASSERT_EQ(args.valueB, 5);
                          Counter++;
                        },
                        TestStruct{5, 5}, 0);
  }

  for (auto &locality : shad::rt::allLocalities()) {
    shad::rt::forEachAt(
        locality,
        [](const uint8_t *input, const uint32_t size, size_t i) {
          ASSERT_EQ(size, 2);
          ASSERT_GE(i, 0);
          ASSERT_LT(i, shad::rt::impl::getConcurrency());
          ASSERT_EQ(input[0], 5);
          ASSERT_EQ(input[1], 5);

          Counter++;
        },
        data, 2, 0);
  }

  shad::rt::forEachOnAll(
      [](const uint8_t *input, const uint32_t size, size_t i) {
        ASSERT_EQ(size, 2);
        ASSERT_GE(i, 0);
        ASSERT_LT(i,
                  shad::rt::numLocalities() * shad::rt::impl::getConcurrency());
        ASSERT_EQ(input[0], 5);
        ASSERT_EQ(input[1], 5);
        Counter += input[0] + input[1];
      },
      data, 2, 0);

  shad::rt::forEachOnAll(
      [](const TestStruct &args, size_t i) {
        ASSERT_GE(i, 0);
        ASSERT_LT(i,
                  shad::rt::numLocalities() * shad::rt::impl::getConcurrency());
        ASSERT_EQ(args.valueA, 5);
        ASSERT_EQ(args.valueB, 5);
        Counter += args.valueA + args.valueB;
      },
      TestStruct{5, 5}, 0);

  shad::rt::executeOnAll([](const bool &) { ASSERT_EQ(Counter, 0); }, false);
}

TEST_F(ForEachTest, AsyncZeroIterations) {
  std::shared_ptr<uint8_t> buffer(new uint8_t[2]{5, 5},
                                  std::default_delete<uint8_t[]>());

  shad::rt::executeOnAll([](const bool &) { ASSERT_EQ(Counter, 0); }, false);

  shad::rt::Handle handle;

  for (auto &locality : shad::rt::allLocalities()) {
    shad::rt::asyncForEachAt(
        handle, locality,
        [](shad::rt::Handle &, const TestStruct &args, size_t i) {
          ASSERT_GE(i, 0);
          ASSERT_LT(
              i, shad::rt::numLocalities() * shad::rt::impl::getConcurrency());
          ASSERT_EQ(args.valueA, 5);
          ASSERT_EQ(args.valueB, 5);
          Counter += args.valueA + args.valueB;
        },
        TestStruct{5, 5}, 0);
  }

  for (auto &locality : shad::rt::allLocalities()) {
    shad::rt::asyncForEachAt(handle, locality,
                             [](shad::rt::Handle &handle, const uint8_t *input,
                                const uint32_t size, size_t i) {
                               ASSERT_EQ(size, 2);
                               ASSERT_GE(i, 0);
                               ASSERT_LT(i, shad::rt::impl::getConcurrency());
                               ASSERT_EQ(input[0], 5);
                               ASSERT_EQ(input[1], 5);

                               Counter += input[0] + input[1];
                             },
                             buffer, 2, 0);
  }

  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &, const uint8_t *input, const uint32_t size,
         size_t i) {
        ASSERT_EQ(size, 2);
        ASSERT_GE(i, 0);
        ASSERT_LT(i,
                  shad::rt::numLocalities() * shad::rt::impl::getConcurrency());
        ASSERT_EQ(input[0], 5);
        ASSERT_EQ(input[1], 5);
        Counter += input[0] + input[1];
      },
      buffer, 2, 0);

  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &, const TestStruct &args, size_t i) {
        ASSERT_GE(i, 0);
        ASSERT_LT(i,
                  shad::rt::numLocalities() * shad::rt::impl::getConcurrency());
        ASSERT_EQ(args.valueA, 5);
        ASSERT_EQ(args.valueB, 5);
        Counter++;
      },
      TestStruct{5, 5}, 0);

  shad::rt::waitForCompletion(handle);

  shad::rt::executeOnAll([](const bool &) { ASSERT_EQ(Counter, 0); }, false);
}

TEST_F(ForEachTest, NotExistingLocality) {
  shad::rt::Locality badLocality(shad::rt::numLocalities() + 1);

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

  size_t ret;
  uint8_t retBuffer[10];
  uint32_t retSize;

  TEST_BAD_LOCALITY(shad::rt::forEachAt(
      badLocality, [](const size_t &, size_t) {}, size_t(0), 10));

  TEST_BAD_LOCALITY(shad::rt::forEachAt(
      badLocality, [](const uint8_t *input, const uint32_t, size_t) {}, nullptr,
      0, 10));

  shad::rt::Handle handle;

  TEST_BAD_LOCALITY(shad::rt::asyncForEachAt(
      handle, badLocality, [](shad::rt::Handle &, const size_t &, size_t) {},
      size_t(0), 10));

  TEST_BAD_LOCALITY(shad::rt::asyncForEachAt(
      handle, badLocality,
      [](shad::rt::Handle &, const uint8_t *input, const uint32_t, size_t) {},
      nullptr, 0, 10));

  shad::rt::waitForCompletion(handle);

#undef TEST_BAD_LOCALITY
}
