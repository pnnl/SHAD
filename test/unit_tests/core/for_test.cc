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

#include <array>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"

#include "shad/core/array.h"
#include "shad/core/unordered_map.h"
#include "shad/core/unordered_set.h"

#include "common.h"

///////////////////////////////////////
//
// std::vector
//
///////////////////////////////////////
template <typename T>
using VTF = shad_test_stl::VectorTestFixture<T>;

using VTF_TestTypes = ::testing::Types<std::vector<int>>;
TYPED_TEST_CASE(VTF, VTF_TestTypes);

TYPED_TEST(VTF, for_deref) {
  int64_t obs_checksum = 0;
  for (auto it = this->in->begin(); it != this->in->end(); ++it)
    obs_checksum += *it;
  ASSERT_EQ(obs_checksum, this->expected_checksum());
}

TYPED_TEST(VTF, for_const_deref) {
  int64_t obs_checksum = 0;
  for (auto it = this->in->cbegin(); it != this->in->cend(); ++it)
    obs_checksum += *it;
  ASSERT_EQ(obs_checksum, this->expected_checksum());
}

TYPED_TEST(VTF, for_inc_deref) {
  int64_t obs_checksum = 0;
  auto it = this->in->cbegin();
  while (it != this->in->cend()) obs_checksum += *it++;
  ASSERT_EQ(obs_checksum, this->expected_checksum());
}

// todo test arrow dereferencing

///////////////////////////////////////
//
// std::array, shad::array
//
///////////////////////////////////////
template <typename T>
using ATF = shad_test_stl::ArrayTestFixture<T>;

using ATF_TestTypes =
    ::testing::Types<std::array<int, shad_test_stl::kNumElements>,
                     shad::array<int, shad_test_stl::kNumElements>>;
TYPED_TEST_CASE(ATF, ATF_TestTypes);

TYPED_TEST(ATF, for_deref) {
  int64_t obs_checksum = 0;
  for (auto it = this->in->begin(); it != this->in->end(); ++it)
    obs_checksum += *it;
  ASSERT_EQ(obs_checksum, this->expected_checksum());
}

TYPED_TEST(ATF, for_const_deref) {
  int64_t obs_checksum = 0;
  for (auto it = this->in->cbegin(); it != this->in->cend(); ++it)
    obs_checksum += *it;
  ASSERT_EQ(obs_checksum, this->expected_checksum());
}

TYPED_TEST(ATF, for_inc_deref) {
  int64_t obs_checksum = 0;
  auto it = this->in->cbegin();
  while (it != this->in->cend()) obs_checksum += *it++;
  ASSERT_EQ(obs_checksum, this->expected_checksum());
}

///////////////////////////////////////
//
// std::unordered_set, shad::unordered_set
//
///////////////////////////////////////
template <typename T>
using STF = shad_test_stl::SetTestFixture<T>;

using STF_TestTypes =
    ::testing::Types<std::unordered_set<int>, shad::unordered_set<int>>;
TYPED_TEST_CASE(STF, STF_TestTypes);

TYPED_TEST(STF, for_deref) {
  int64_t obs_checksum = 0;
  for (auto it = this->in->begin(); it != this->in->end(); ++it)
    obs_checksum += *it;
  ASSERT_EQ(obs_checksum, this->expected_checksum());
}

TYPED_TEST(STF, for_const_deref) {
  int64_t obs_checksum = 0;
  for (auto it = this->in->cbegin(); it != this->in->cend(); ++it)
    obs_checksum += *it;
  ASSERT_EQ(obs_checksum, this->expected_checksum());
}

TYPED_TEST(STF, for_inc_deref) {
  int64_t obs_checksum = 0;
  auto it = this->in->cbegin();
  while (it != this->in->cend()) obs_checksum += *it++;
  ASSERT_EQ(obs_checksum, this->expected_checksum());
}

///////////////////////////////////////
//
// std::unordered_map, shad::unordered_map
//
///////////////////////////////////////
template <typename T>
using MTF = shad_test_stl::MapTestFixture<T>;

using MTF_TestTypes = ::testing::Types<std::unordered_map<int, int>,
                                       shad::unordered_map<int, int>>;
TYPED_TEST_CASE(MTF, MTF_TestTypes);

TYPED_TEST(MTF, for_deref) {
  int64_t obs_checksum = 0;
  for (auto it = this->in->begin(); it != this->in->end(); ++it)
    obs_checksum += (*it).second;
  ASSERT_EQ(obs_checksum, this->expected_checksum());
}

TYPED_TEST(MTF, for_const_deref) {
  int64_t obs_checksum = 0;
  for (auto it = this->in->cbegin(); it != this->in->cend(); ++it)
    obs_checksum += (*it).second;
  ASSERT_EQ(obs_checksum, this->expected_checksum());
}

TYPED_TEST(MTF, for_inc_deref) {
  int64_t obs_checksum = 0;
  auto it = this->in->cbegin();
  while (it != this->in->cend()) obs_checksum += (*it++).second;
  ASSERT_EQ(obs_checksum, this->expected_checksum());
}

// todo test arrow dereferencing
