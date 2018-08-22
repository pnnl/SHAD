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
#include <numeric>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"

#include "shad/core/array.h"
#include "shad/core/unordered_map.h"
#include "shad/core/unordered_set.h"

#include "common.h"
#include "stl_emulation/numeric.h"

///////////////////////////////////////
//
// std::vector
//
///////////////////////////////////////
template <typename T>
using VTF = shad_test_stl::VectorTestFixture<T>;

using VTF_TestTypes = ::testing::Types<std::vector<int>>;
TYPED_TEST_CASE(VTF, VTF_TestTypes);

TYPED_TEST(VTF, accumulate) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  this->test(std::accumulate<it_t, val_t>,
             shad_test_stl::accumulate_<it_t, val_t>, 0);
}

#ifdef STD_REDUCE_TEST
TYPED_TEST(VTF, std_reduce) {
  using it_t = typeof(this->in->begin());
  this->test(std::reduce<it_t>, shad_test_stl::reduce_<it_t>);
}
#endif

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

TYPED_TEST(ATF, accumulate) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  this->test(std::accumulate<it_t, val_t>,
             shad_test_stl::accumulate_<it_t, val_t>, 0);
}

#ifdef STD_REDUCE_TEST
TYPED_TEST(ATF, std_reduce) {
  using it_t = typeof(this->in->begin());
  this->test(std::reduce<it_t>, shad_test_stl::reduce_<it_t>);
}
#endif

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

TYPED_TEST(STF, accumulate) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  this->test(std::accumulate<it_t, val_t>,
             shad_test_stl::accumulate_<it_t, val_t>, 0);
}

#ifdef STD_REDUCE_TEST
TYPED_TEST(STF, std_reduce) {
  using it_t = typeof(this->in->begin());
  this->test(std::reduce<it_t>, shad_test_stl::reduce_<it_t>);
}
#endif

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

TYPED_TEST(MTF, accumulate) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using op_t = shad_test_stl::pair_acc<int, val_t>;
  this->test(std::accumulate<it_t, int, op_t>,
             shad_test_stl::accumulate_<it_t, int, op_t>, 0, op_t{});
}

#ifdef STD_REDUCE_TEST
TYPED_TEST(MTF, std_reduce) {
  using it_t = typeof(this->in->begin());
  this->test(std::reduce<it_t>, shad_test_stl::reduce_<it_t>);
}
#endif
