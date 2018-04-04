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

#include "shad/config/config.h"
#include "shad/runtime/runtime.h"

namespace shad {

namespace test {

// Report failing asserts on locality 0 to make test fails when
// failing asserts triggers or remote nodes.
class DistributedSystemCheck : public testing::EmptyTestEventListener {
 private:
  void OnTestPartResult(const ::testing::TestPartResult &result) override {
    // When the result is passed() or the locality is 0 we don't need
    // to do anything.
    if (result.passed() || rt::thisLocality() == rt::Locality(0)) {
      return;
    }

    shad::rt::executeAt(rt::Locality(0),
                        [](const bool &fatal) {
                          if (fatal) {
                            FAIL() << "Remote failure";
                          }

                          ADD_FAILURE() << "Remote non-fatal failure";
                        },
                        result.fatally_failed());
  }
};

}  // namespace test

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  if (rt::numLocalities() > 1) {
    // Add the handler that deals with remote asserts when
    // we have more than one locality.
    shad::rt::executeOnAll(
        [](const size_t &) {
          testing::TestEventListeners &listeners =
              testing::UnitTest::GetInstance()->listeners();
          listeners.Append(new test::DistributedSystemCheck);
        },
        size_t(0));
  }

  return RUN_ALL_TESTS();
}

}  // namespace shad
