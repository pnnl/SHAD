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

#include "gtest/gtest.h"
#include "gtest/gtest-spi.h"

#include "shad/runtime/runtime.h"

namespace testing {
namespace internal {

// Check for at least a failure of the expected type matching substr.
AssertionResult HasRemoteFailure(const char * /*result_expr */,
                                 const char * /* type_expr */,
                                 const char * /* substr_expr*/,
                                 const TestPartResultArray &results,
                                 TestPartResult::Type type,
                                 const string &substr) {
  const std::string expected(type == TestPartResult::kFatalFailure
                                 ? "a fatal failure"
                                 : "a non-fatal failure");
  Message msg;
  for (int i = 0; i < results.size(); ++i) {
    const TestPartResult &r = results.GetTestPartResult(i);
    if (r.type() != type) {
      continue;
    }

    if (strstr(r.message(), substr.c_str()) != nullptr) {
      return AssertionSuccess();
    }
  }

  return AssertionFailure()
         << "Expected: " << expected << " containing \"" << substr << "\"\n"
         << "  None found\n";
}

// Run the HasRemoteFailure check.
class RemoteFailureChecker {
 public:
  RemoteFailureChecker(const TestPartResultArray *results,
                       TestPartResult::Type type, const string &substr)
      : results_(results), type_(type), substr_(substr) {}

  ~RemoteFailureChecker() {
    EXPECT_PRED_FORMAT3(HasRemoteFailure, *results_, type_, substr_);
  }

  RemoteFailureChecker(RemoteFailureChecker &&) = delete;
  RemoteFailureChecker operator=(RemoteFailureChecker &&) = delete;

 private:
  const TestPartResultArray *const results_;
  const TestPartResult::Type type_;
  const string substr_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(RemoteFailureChecker);
};

}  // namespace internal
}  // namespace testing

// Implement an equivalent version of EXPECT_FATAL_FAILURE_ON_ALL_THREADS
// to check that remote failures have been propagate in the rank 0 node.
#define EXPECT_REMOTE_FATAL_FAILURE_ON_ALL_THREADS(statement, substr)         \
  do {                                                                        \
    class GTestExpectFatalFailureHelper {                                     \
     public:                                                                  \
      static void Execute() { statement; }                                    \
    };                                                                        \
    ::testing::TestPartResultArray gtest_failures;                            \
    ::testing::internal::RemoteFailureChecker gtest_checker(                  \
        &gtest_failures, ::testing::TestPartResult::kFatalFailure, (substr)); \
    {                                                                         \
      ::testing::ScopedFakeTestPartResultReporter gtest_reporter(             \
          ::testing::ScopedFakeTestPartResultReporter::INTERCEPT_ALL_THREADS, \
          &gtest_failures);                                                   \
      GTestExpectFatalFailureHelper::Execute();                               \
    }                                                                         \
  } while (::testing::internal::AlwaysFalse())

static void test1(const size_t & /* unused */) {
  if (shad::rt::thisLocality() == shad::rt::Locality(0)) {
    SUCCEED();
  } else {
    FAIL() << "Failing on remote locality : " << shad::rt::thisLocality();
  }
}

auto test1Lambda = []() {
  for (auto &locality : shad::rt::allLocalities()) {
    shad::rt::executeAt(locality, test1, size_t(0));
  }
};

TEST(TestingRemoteFailure, remoteFailsLocalSuccedes) {
  if (shad::rt::numLocalities() == 1) {
    SUCCEED();
    return;
  }

  EXPECT_REMOTE_FATAL_FAILURE_ON_ALL_THREADS(test1Lambda(), "Remote failure");
}

static void test2(const size_t & /* unused */) {
  if (shad::rt::thisLocality() != shad::rt::Locality(0)) {
    SUCCEED();
  } else {
    FAIL() << "Failing on locality 0";
  }
}

auto test2Lambda = []() {
  for (auto &locality : shad::rt::allLocalities()) {
    shad::rt::executeAt(locality, test2, size_t(0));
  }
};

TEST(TestingRemoteFailure, remoteSuccedesLocalFails) {
  if (shad::rt::numLocalities() == 1) {
    SUCCEED();
    return;
  }

  EXPECT_FATAL_FAILURE_ON_ALL_THREADS(test2Lambda(), "Failing on locality 0");
}

static void test3(const size_t & /* unused */) {
  if (shad::rt::thisLocality() != shad::rt::Locality(0)) {
    SUCCEED();
  } else {
    ADD_FAILURE() << "Failing on locality 0";
  }
}

auto test3Lambda = []() {
  for (auto &locality : shad::rt::allLocalities()) {
    shad::rt::executeAt(locality, test3, size_t(0));
  }
};

TEST(TestingRemoteNonFatalFailure, remoteSuccedesLocalFails) {
  if (shad::rt::numLocalities() == 1) {
    SUCCEED();
    return;
  }

  EXPECT_NONFATAL_FAILURE_ON_ALL_THREADS(test3Lambda(),
                                         "Failing on locality 0");
}

// Implement an equivalent version of EXPECT_NONFATAL_FAILURE_ON_ALL_THREADS
// to check that remote failures have been propagate in the rank 0 node.
#define EXPECT_REMOTE_NONFATAL_FAILURE_ON_ALL_THREADS(statement, substr)      \
  do {                                                                        \
    class GTestExpectFatalFailureHelper {                                     \
     public:                                                                  \
      static void Execute() { statement; }                                    \
    };                                                                        \
    ::testing::TestPartResultArray gtest_failures;                            \
    ::testing::internal::RemoteFailureChecker gtest_checker(                  \
        &gtest_failures, ::testing::TestPartResult::kNonFatalFailure,         \
        (substr));                                                            \
    {                                                                         \
      ::testing::ScopedFakeTestPartResultReporter gtest_reporter(             \
          ::testing::ScopedFakeTestPartResultReporter::INTERCEPT_ALL_THREADS, \
          &gtest_failures);                                                   \
      GTestExpectFatalFailureHelper::Execute();                               \
    }                                                                         \
  } while (::testing::internal::AlwaysFalse())

static void test4(const size_t & /* unused */) {
  if (shad::rt::numLocalities() == 1) {
    SUCCEED();
    return;
  }

  if (shad::rt::thisLocality() == shad::rt::Locality(0)) {
    SUCCEED();
  } else {
    ADD_FAILURE() << "Failing on locality : " << shad::rt::thisLocality();
  }
}

auto test4Lambda = []() {
  for (auto &locality : shad::rt::allLocalities()) {
    shad::rt::executeAt(locality, test4, size_t(0));
  }
};

TEST(TestingRemoteNonFatalFailure, remoteFailsLocalSucceedes) {
  if (shad::rt::numLocalities() == 1) {
    SUCCEED();
    return;
  }

  EXPECT_REMOTE_NONFATAL_FAILURE_ON_ALL_THREADS(test4Lambda(),
                                                "Remote non-fatal failure");
}
