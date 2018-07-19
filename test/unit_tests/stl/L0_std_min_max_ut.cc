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
#include <array>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"

#include "shad/data_structures/array.h"
#include "shad/data_structures/hashmap.h"
#include "shad/data_structures/set.h"

#include "common.h"
#include "stl_emulation/algorithm.h"

///////////////////////////////////////
//
// std::vector
//
///////////////////////////////////////
template <typename T>
using VTF = shad_test_stl::VectorTestFixture<T>;

using VTF_TestTypes = ::testing::Types<std::vector<int>>;
TYPED_TEST_CASE(VTF, VTF_TestTypes);

TYPED_TEST(VTF, min_element) {
  using it_t = typeof(this->in->begin());
  this->test(std::min_element<it_t>, shad_test_stl::min_element_<it_t>);
}

TYPED_TEST(VTF, max_element) {
  using it_t = typeof(this->in->begin());
  this->test(std::max_element<it_t>, shad_test_stl::max_element_<it_t>);
}

TYPED_TEST(VTF, minmax_element) {
  using it_t = typeof(this->in->begin());
  this->test(std::minmax_element<it_t>, shad_test_stl::minmax_element_<it_t>);
}

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

TYPED_TEST(ATF, min_element) {
  using it_t = typeof(this->in->begin());
  this->test(std::min_element<it_t>, shad_test_stl::min_element_<it_t>);
}

TYPED_TEST(ATF, max_element) {
  using it_t = typeof(this->in->begin());
  this->test(std::max_element<it_t>, shad_test_stl::max_element_<it_t>);
}

TYPED_TEST(ATF, minmax_element) {
  using it_t = typeof(this->in->begin());
  this->test(std::minmax_element<it_t>, shad_test_stl::minmax_element_<it_t>);
}

///////////////////////////////////////
//
// std::unordered_set, shad::Set
//
///////////////////////////////////////
template <typename T>
using STF = shad_test_stl::SetTestFixture<T>;

using STF_TestTypes = ::testing::Types<std::unordered_set<int>, shad::Set<int>>;
TYPED_TEST_CASE(STF, STF_TestTypes);

TYPED_TEST(STF, min_element) {
  using it_t = typeof(this->in->begin());
  this->test(std::min_element<it_t>, shad_test_stl::min_element_<it_t>);
}

TYPED_TEST(STF, max_element) {
  using it_t = typeof(this->in->begin());
  this->test(std::max_element<it_t>, shad_test_stl::max_element_<it_t>);
}

TYPED_TEST(STF, minmax_element) {
  using it_t = typeof(this->in->begin());
  this->test(std::minmax_element<it_t>, shad_test_stl::minmax_element_<it_t>);
}

///////////////////////////////////////
//
// std::unordered_map, shad::Hashmap
//
///////////////////////////////////////
template <typename T>
using MTF = shad_test_stl::MapTestFixture<T>;

using MTF_TestTypes = ::testing::Types<std::unordered_map<int, int>, shad::Hashmap<int, int>>;
TYPED_TEST_CASE(MTF, MTF_TestTypes);

TYPED_TEST(MTF, min_element) {
  using it_t = typeof(this->in->begin());
  this->test(std::min_element<it_t>, shad_test_stl::min_element_<it_t>);
}

TYPED_TEST(MTF, max_element) {
  using it_t = typeof(this->in->begin());
  this->test(std::max_element<it_t>, shad_test_stl::max_element_<it_t>);
}

TYPED_TEST(MTF, minmax_element) {
  using it_t = typeof(this->in->begin());
  this->test(std::minmax_element<it_t>, shad_test_stl::minmax_element_<it_t>);
}

