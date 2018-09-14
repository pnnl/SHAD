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

TYPED_TEST(VTF, iota) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  this->test_void(std::iota<it_t, val_t>, shad_test_stl::iota_<it_t, val_t>,
                  shad_test_stl::ordered_checksum<it_t>, 0);
}

TYPED_TEST(VTF, accumulate) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  this->test(std::accumulate<it_t, val_t, std::plus<int>>,
             shad_test_stl::accumulate_<it_t, val_t, std::plus<int>>, 0,
             std::plus<int>{});
}

TYPED_TEST(VTF, inner_product) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using combine_f = std::multiplies<val_t>;
  using reduce_f = std::plus<val_t>;
  auto other = shad_test_stl::create_vector_<TypeParam, false>{}(
      shad_test_stl::kNumElements);
  this->test(
      std::inner_product<it_t, it_t, val_t, reduce_f, combine_f>,
      shad_test_stl::inner_product_<it_t, it_t, val_t, reduce_f, combine_f>,
      other->begin(), 0, reduce_f{}, combine_f{});
}

TYPED_TEST(VTF, adjacent_difference) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using diff_f = std::minus<val_t>;
  this->test_io_assignment(
      std::adjacent_difference<it_t, it_t, diff_f>,
      shad_test_stl::adjacent_difference_<it_t, it_t, diff_f>, diff_f{});
}

TYPED_TEST(VTF, partial_sum) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using sum_f = std::plus<val_t>;
  this->test_io_assignment(std::partial_sum<it_t, it_t, sum_f>,
                           shad_test_stl::partial_sum_<it_t, it_t, sum_f>,
                           sum_f{});
}

#ifndef PARTIAL_STD_TESTS
// todo double-check STL parameter ordering for exclusive/inclusive_scan
TYPED_TEST(VTF, inclusive_scan) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using sum_f = std::plus<val_t>;
  this->test_io_assignment(
      std::inclusive_scan<it_t, it_t, val_t, sum_f>,
      shad_test_stl::inclusive_scan_<it_t, it_t, val_t, sum_f>, sum_f{}, 0);
}

TYPED_TEST(VTF, exclusive_scan) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using sum_f = std::plus<val_t>;
  this->test_io_assignment(
      std::exclusive_scan<it_t, it_t, val_t, sum_f>,
      shad_test_stl::exclusive_scan_<it_t, it_t, val_t, sum_f>, 0, sum_f{});
}

TYPED_TEST(VTF, transform_reduce_two_containers) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using combine_f = std::multiplies<val_t>;
  using reduce_f = std::plus<val_t>;
  auto other = shad_test_stl::create_vector_<TypeParam, false>{}(
      shad_test_stl::kNumElements);
  this->test(
      std::transform_reduce<it_t, it_t, val_t, reduce_f, combine_f>,
      shad_test_stl::transform_reduce_<it_t, it_t, val_t, reduce_f, combine_f>,
      other->begin(), 0, reduce_f{}, combine_f{});
}

TYPED_TEST(VTF, transform_reduce_one_container) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  using reduce_f = std::plus<val_t>;
  this->test(std::transform_reduce<it_t, val_t, reduce_f, map_f>,
             shad_test_stl::transform_reduce_<it_t, val_t, reduce_f, map_f>, 0,
             reduce_f{}, map_f{});
}

TYPED_TEST(VTF, transform_inclusive_scan) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  using reduce_f = std::plus<val_t>;
  this->test_io_assignment(
      std::transform_inclusive_scan<it_t, it_t, val_t, reduce_f, map_f>,
      shad_test_stl::transform_inclusive_scan_<it_t, it_t, val_t, reduce_f,
                                               map_f>,
      reduce_f{}, map_f{}, 0);
}

TYPED_TEST(VTF, transform_exclusive_scan) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  using reduce_f = std::plus<val_t>;
  this->test_io_assignment(
      std::transform_exclusive_scan<it_t, it_t, val_t, reduce_f, map_f>,
      shad_test_stl::transform_exclusive_scan_<it_t, it_t, val_t, reduce_f,
                                               map_f>,
      0, reduce_f{}, map_f{});
}

TYPED_TEST(VTF, std_reduce) {
  using it_t = typeof(this->in->begin());
  using val_t = typename it_t::value_type;
  using reduce_f = std::plus<val_t>;
  this->test(std::reduce<it_t, val_t, reduce_f>,
             shad_test_stl::reduce_<it_t, val_t, reduce_f>, 0, reduce_f{});
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

TYPED_TEST(ATF, iota) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  this->test_void(std::iota<it_t, val_t>, shad_test_stl::iota_<it_t, val_t>,
                  shad_test_stl::ordered_checksum<it_t>, 0);
}

TYPED_TEST(ATF, accumulate) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  this->test(std::accumulate<it_t, val_t, std::plus<int>>,
             shad_test_stl::accumulate_<it_t, val_t, std::plus<int>>, 0,
             std::plus<int>{});
}

TYPED_TEST(ATF, inner_product) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using combine_f = std::multiplies<val_t>;
  using reduce_f = std::plus<val_t>;
  auto other = shad_test_stl::create_array_<TypeParam, false>{}();
  this->test(
      std::inner_product<it_t, it_t, val_t, reduce_f, combine_f>,
      shad_test_stl::inner_product_<it_t, it_t, val_t, reduce_f, combine_f>,
      other->begin(), 0, reduce_f{}, combine_f{});
}

TYPED_TEST(ATF, adjacent_difference) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using diff_f = std::minus<val_t>;
  this->test_io_assignment(
      std::adjacent_difference<it_t, it_t, diff_f>,
      shad_test_stl::adjacent_difference_<it_t, it_t, diff_f>, diff_f{});
}

TYPED_TEST(ATF, partial_sum) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using sum_f = std::plus<val_t>;
  this->test_io_assignment(std::partial_sum<it_t, it_t, sum_f>,
                           shad_test_stl::partial_sum_<it_t, it_t, sum_f>,
                           sum_f{});
}

// todo double-check STL parameter ordering for exclusive/inclusive_scan
#ifndef PARTIAL_STD_TESTS
TYPED_TEST(ATF, inclusive_scan) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using sum_f = std::plus<val_t>;
  this->test_io_assignment(
      std::inclusive_scan<it_t, it_t, val_t, sum_f>,
      shad_test_stl::inclusive_scan_<it_t, it_t, val_t, sum_f>, sum_f{}, 0);
}

TYPED_TEST(ATF, exclusive_scan) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using sum_f = std::plus<val_t>;
  this->test_io_assignment(
      std::exclusive_scan<it_t, it_t, val_t, sum_f>,
      shad_test_stl::exclusive_scan_<it_t, it_t, val_t, sum_f>, 0, sum_f{});
}

TYPED_TEST(ATF, transform_reduce_two_containers) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using combine_f = std::multiplies<val_t>;
  using reduce_f = std::plus<val_t>;
  auto other = shad_test_stl::create_array_<TypeParam, false>{}();
  this->test(
      std::transform_reduce<it_t, it_t, val_t, reduce_f, combine_f>,
      shad_test_stl::transform_reduce_<it_t, it_t, val_t, reduce_f, combine_f>,
      other->begin(), 0, reduce_f{}, combine_f{});
}

TYPED_TEST(ATF, transform_reduce_one_container) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  using reduce_f = std::plus<val_t>;
  this->test(std::transform_reduce<it_t, val_t, reduce_f, map_f>,
             shad_test_stl::transform_reduce_<it_t, val_t, reduce_f, map_f>, 0,
             reduce_f{}, map_f{});
}

TYPED_TEST(ATF, transform_inclusive_scan) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  using reduce_f = std::plus<val_t>;
  this->test_io_assignment(
      std::transform_inclusive_scan<it_t, it_t, val_t, reduce_f, map_f>,
      shad_test_stl::transform_inclusive_scan_<it_t, it_t, val_t, reduce_f,
                                               map_f>,
      reduce_f{}, map_f{}, 0);
}

TYPED_TEST(ATF, transform_exclusive_scan) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  using reduce_f = std::plus<val_t>;
  this->test_io_assignment(
      std::transform_exclusive_scan<it_t, it_t, val_t, reduce_f, map_f>,
      shad_test_stl::transform_exclusive_scan_<it_t, it_t, val_t, reduce_f,
                                               map_f>,
      0, reduce_f{}, map_f{});
}

TYPED_TEST(ATF, std_reduce) {
  using it_t = typeof(this->in->begin());
  using reduce_f = std::plus<int>;
  this->test(std::reduce<it_t, int, reduce_f>,
             shad_test_stl::reduce_<it_t, int, reduce_f>, 0, reduce_f{});
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
  this->test(std::accumulate<it_t, val_t, std::plus<int>>,
             shad_test_stl::accumulate_<it_t, val_t, std::plus<int>>, 0,
             std::plus<int>{});
}

TYPED_TEST(STF, inner_product) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using combine_f = std::multiplies<val_t>;
  using reduce_f = std::plus<val_t>;
  auto other = shad_test_stl::create_set_<TypeParam, false>{}(
      shad_test_stl::kNumElements);
  this->test(
      std::inner_product<it_t, it_t, val_t, reduce_f, combine_f>,
      shad_test_stl::inner_product_<it_t, it_t, val_t, reduce_f, combine_f>,
      other->begin(), 0, reduce_f{}, combine_f{});
}

TYPED_TEST(STF, adjacent_difference) {
  using it_t = typename TypeParam::iterator;
  using out_it_t = std::insert_iterator<TypeParam>;
  using val_t = typename TypeParam::value_type;
  using diff_f = std::minus<val_t>;
  this->test_io_inserters(
      std::adjacent_difference<it_t, out_it_t, diff_f>,
      shad_test_stl::adjacent_difference_<it_t, out_it_t, diff_f>, diff_f{});
}

TYPED_TEST(STF, partial_sum) {
  using it_t = typeof(this->in->begin());
  using out_it_t = std::insert_iterator<TypeParam>;
  using val_t = typename TypeParam::value_type;
  using sum_f = std::plus<val_t>;
  this->test_io_inserters(std::partial_sum<it_t, out_it_t, sum_f>,
                          shad_test_stl::partial_sum_<it_t, out_it_t, sum_f>,
                          sum_f{});
}

// todo double-check STL parameter ordering for exclusive/inclusive_scan
#ifndef PARTIAL_STD_TESTS
TYPED_TEST(STF, inclusive_scan) {
  using it_t = typeof(this->in->begin());
  using out_it_t = std::insert_iterator<TypeParam>;
  using val_t = typename TypeParam::value_type;
  using sum_f = std::plus<val_t>;
  this->test_io_inserters(
      std::inclusive_scan<it_t, out_it_t, val_t, sum_f>,
      shad_test_stl::inclusive_scan_<it_t, out_it_t, val_t, sum_f>, sum_f{}, 0);
}

TYPED_TEST(STF, exclusive_scan) {
  using it_t = typeof(this->in->begin());
  using out_it_t = std::insert_iterator<TypeParam>;
  using val_t = typename TypeParam::value_type;
  using sum_f = std::plus<val_t>;
  this->test_io_inserters(
      std::exclusive_scan<it_t, out_it_t, val_t, sum_f>,
      shad_test_stl::exclusive_scan_<it_t, out_it_t, val_t, sum_f>, 0, sum_f{});
}

TYPED_TEST(STF, transform_reduce_two_containers) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using combine_f = std::multiplies<val_t>;
  using reduce_f = std::plus<val_t>;
  auto other = shad_test_stl::create_set_<TypeParam, false>{}(
      shad_test_stl::kNumElements);
  this->test(
      std::transform_reduce<it_t, it_t, val_t, reduce_f, combine_f>,
      shad_test_stl::transform_reduce_<it_t, it_t, val_t, reduce_f, combine_f>,
      other->begin(), 0, reduce_f{}, combine_f{});
}

TYPED_TEST(STF, transform_reduce_one_container) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  using reduce_f = std::plus<val_t>;
  this->test(std::transform_reduce<it_t, val_t, reduce_f, map_f>,
             shad_test_stl::transform_reduce_<it_t, val_t, reduce_f, map_f>, 0,
             reduce_f{}, map_f{});
}

TYPED_TEST(STF, transform_inclusive_scan) {
  using it_t = typeof(this->in->begin());
  using out_it_t = std::insert_iterator<TypeParam>;
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  using reduce_f = std::plus<val_t>;
  this->test_io_inserters(
      std::transform_inclusive_scan<it_t, out_it_t, val_t, reduce_f, map_f>,
      shad_test_stl::transform_inclusive_scan_<it_t, out_it_t, val_t, reduce_f,
                                               map_f>,
      reduce_f{}, map_f{}, 0);
}

TYPED_TEST(STF, transform_exclusive_scan) {
  using it_t = typeof(this->in->begin());
  using out_it_t = std::insert_iterator<TypeParam>;
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  using reduce_f = std::plus<val_t>;
  this->test_io_inserters(
      std::transform_exclusive_scan<it_t, out_it_t, val_t, reduce_f, map_f>,
      shad_test_stl::transform_exclusive_scan_<it_t, out_it_t, val_t, reduce_f,
                                               map_f>,
      0, reduce_f{}, map_f{});
}

TYPED_TEST(STF, std_reduce) {
  using it_t = typeof(this->in->begin());
  using reduce_f = std::plus<int>;
  this->test(std::reduce<it_t, int, reduce_f>,
             shad_test_stl::reduce_<it_t, int, reduce_f>, 0, reduce_f{});
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
  auto op_t = [](int64_t acc, const val_t &p) {
    return acc + shad_test_stl::to_int64<val_t>{}(p);
  };
  this->test(std::accumulate<it_t, int64_t, typeof(op_t)>,
             shad_test_stl::accumulate_<it_t, int64_t, typeof(op_t)>, 0, op_t);
}

TYPED_TEST(MTF, inner_product) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using acc_t = std::pair<int, int>;
  using combine_f = std::multiplies<val_t>;
  using reduce_f = std::plus<val_t>;
  auto other = shad_test_stl::create_map_<TypeParam, false>{}(
      shad_test_stl::kNumElements);
  this->test(
      std::inner_product<it_t, it_t, acc_t, reduce_f, combine_f>,
      shad_test_stl::inner_product_<it_t, it_t, acc_t, reduce_f, combine_f>,
      this->in->begin(), std::make_pair(0, 0), reduce_f{}, combine_f{});
}

#ifndef PARTIAL_STD_TESTS
// todo adjacent_difference - not compiling
// todo partial sum - not compiling
// todo exclusive_scan - not compiling
// todo inclusive_scan - not compiling

TYPED_TEST(MTF, transform_reduce_two_containers) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using acc_t = std::pair<int, int>;
  using combine_f = std::multiplies<val_t>;
  using reduce_f = std::plus<val_t>;
  auto other = shad_test_stl::create_map_<TypeParam, false>{}(
      shad_test_stl::kNumElements);
  this->test(
      std::transform_reduce<it_t, it_t, acc_t, reduce_f, combine_f>,
      shad_test_stl::transform_reduce_<it_t, it_t, acc_t, reduce_f, combine_f>,
      other->begin(), std::make_pair(0, 0), reduce_f{}, combine_f{});
}
#endif

#ifndef PARTIAL_STD_TESTS
TYPED_TEST(MTF, transform_reduce_one_container) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using acc_t = std::pair<int, int>;
  using map_f = std::negate<val_t>;
  using reduce_f = std::plus<val_t>;
  this->test(std::transform_reduce<it_t, acc_t, reduce_f, map_f>,
             shad_test_stl::transform_reduce_<it_t, acc_t, reduce_f, map_f>,
             std::make_pair(0, 0), reduce_f{}, map_f{});
}
#endif

// todo transform_exclusive_scan
// todo transform_inclusive_scan

#ifndef PARTIAL_STD_TESTS
TYPED_TEST(MTF, std_reduce) {
  using it_t = typeof(this->in->begin());
  using acc_t = std::pair<int, int>;
  using reduce_f = std::plus<acc_t>;
  this->test(std::reduce<it_t, acc_t, reduce_f>,
             shad_test_stl::reduce_<it_t, acc_t, reduce_f>,
             std::make_pair(0, 0), reduce_f{});
}
#endif
