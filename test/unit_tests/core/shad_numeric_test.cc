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

#include <functional>

#include "gtest/gtest.h"

#include "shad/core/array.h"
#include "shad/core/execution.h"
#include "shad/core/numeric.h"
#include "shad/core/unordered_map.h"
#include "shad/core/unordered_set.h"

#include "common.h"
#include "stl_emulation/numeric.h"

///////////////////////////////////////
//
// shad::array
//
///////////////////////////////////////
template <typename T>
using ATF = shad_test_stl::ArrayTestFixture<T>;

using ATF_TestTypes =
    ::testing::Types<shad::array<int, shad_test_stl::kNumElements>>;
TYPED_TEST_CASE(ATF, ATF_TestTypes);

TYPED_TEST(ATF, iota) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  this->test_void(shad::iota<it_t, val_t>, shad_test_stl::iota_<it_t, val_t>,
                  shad_test_stl::ordered_checksum<it_t>, 0);
}

TYPED_TEST(ATF, accumulate) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using acc_f = std::plus<int>;
  this->test(shad::accumulate<it_t, val_t, acc_f>,
             shad_test_stl::accumulate_<it_t, val_t, acc_f>, 0, acc_f{});
}

TYPED_TEST(ATF, inner_product) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using combine_f = std::multiplies<val_t>;
  using reduce_f = std::plus<val_t>;
  auto other = shad_test_stl::create_array_<TypeParam, false>{}();
  this->test(
      shad::inner_product<it_t, it_t, val_t, reduce_f, combine_f>,
      shad_test_stl::inner_product_<it_t, it_t, val_t, reduce_f, combine_f>,
      other->begin(), 0, reduce_f{}, combine_f{});
}

TYPED_TEST(ATF, partial_sum) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using sum_f = std::plus<val_t>;
  this->test_io_assignment(shad::partial_sum<it_t, it_t, sum_f>,
                           shad_test_stl::partial_sum_<it_t, it_t, sum_f>,
                           shad_test_stl::ordered_checksum<it_t>, sum_f{});
}

TYPED_TEST(ATF, adjacent_difference) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using diff_f = std::minus<val_t>;
  this->test_io_assignment_with_policy(
      shad::distributed_sequential_tag{},
      shad::adjacent_difference<shad::distributed_sequential_tag, it_t, it_t,
                                diff_f>,
      shad_test_stl::adjacent_difference_<it_t, it_t, diff_f>,
      shad_test_stl::ordered_checksum<it_t>, diff_f{});
  this->test_io_assignment_with_policy(
      shad::distributed_parallel_tag{},
      shad::adjacent_difference<shad::distributed_parallel_tag, it_t, it_t,
                                diff_f>,
      shad_test_stl::adjacent_difference_<it_t, it_t, diff_f>,
      shad_test_stl::ordered_checksum<it_t>, diff_f{});
}

TYPED_TEST(ATF, inclusive_scan) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using sum_f = std::plus<val_t>;
  this->test_io_assignment_with_policy(
      shad::distributed_sequential_tag{},
      shad::inclusive_scan<shad::distributed_sequential_tag, it_t, it_t, sum_f,
                           val_t>,
      shad_test_stl::inclusive_scan_<it_t, it_t, sum_f, val_t>,
      shad_test_stl::ordered_checksum<it_t>, sum_f{}, 0);
  this->test_io_assignment_with_policy(
      shad::distributed_parallel_tag{},
      shad::inclusive_scan<shad::distributed_parallel_tag, it_t, it_t, sum_f,
                           val_t>,
      shad_test_stl::inclusive_scan_<it_t, it_t, sum_f, val_t>,
      shad_test_stl::ordered_checksum<it_t>, sum_f{}, 0);
}

TYPED_TEST(ATF, exclusive_scan) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using sum_f = std::plus<val_t>;
  this->test_io_assignment_with_policy(
      shad::distributed_sequential_tag{},
      shad::exclusive_scan<shad::distributed_sequential_tag, it_t, it_t, val_t,
                           sum_f>,
      shad_test_stl::exclusive_scan_<it_t, it_t, val_t, sum_f>,
      shad_test_stl::ordered_checksum<it_t>, 0, sum_f{});
  this->test_io_assignment_with_policy(
      shad::distributed_parallel_tag{},
      shad::exclusive_scan<shad::distributed_parallel_tag, it_t, it_t, val_t,
                           sum_f>,
      shad_test_stl::exclusive_scan_<it_t, it_t, val_t, sum_f>,
      shad_test_stl::ordered_checksum<it_t>, 0, sum_f{});
}

TYPED_TEST(ATF, transform_reduce_two_containers) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using combine_f = std::multiplies<val_t>;
  using reduce_f = std::plus<val_t>;
  auto other = shad_test_stl::create_array_<TypeParam, false>{}();
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::transform_reduce<shad::distributed_sequential_tag, it_t, it_t,
                             val_t, reduce_f, combine_f>,
      shad_test_stl::transform_reduce_<it_t, it_t, val_t, reduce_f, combine_f>,
      other->begin(), 0, reduce_f{}, combine_f{});
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::transform_reduce<shad::distributed_parallel_tag, it_t, it_t, val_t,
                             reduce_f, combine_f>,
      shad_test_stl::transform_reduce_<it_t, it_t, val_t, reduce_f, combine_f>,
      other->begin(), 0, reduce_f{}, combine_f{});
}

TYPED_TEST(ATF, transform_reduce_one_container) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  using reduce_f = std::plus<val_t>;
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::transform_reduce<shad::distributed_sequential_tag, it_t, val_t,
                             reduce_f, map_f>,
      shad_test_stl::transform_reduce_<it_t, val_t, reduce_f, map_f>, 0,
      reduce_f{}, map_f{});
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::transform_reduce<shad::distributed_parallel_tag, it_t, val_t,
                             reduce_f, map_f>,
      shad_test_stl::transform_reduce_<it_t, val_t, reduce_f, map_f>, 0,
      reduce_f{}, map_f{});
}

TYPED_TEST(ATF, transform_inclusive_scan) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  using reduce_f = std::plus<val_t>;
  this->test_io_assignment_with_policy(
      shad::distributed_sequential_tag{},
      shad::transform_inclusive_scan<shad::distributed_sequential_tag, it_t,
                                     it_t, reduce_f, map_f, val_t>,
      shad_test_stl::transform_inclusive_scan_<it_t, it_t, reduce_f, map_f,
                                               val_t>,
      shad_test_stl::ordered_checksum<it_t>, reduce_f{}, map_f{}, 0);
  this->test_io_assignment_with_policy(
      shad::distributed_parallel_tag{},
      shad::transform_inclusive_scan<shad::distributed_parallel_tag, it_t, it_t,
                                     reduce_f, map_f, val_t>,
      shad_test_stl::transform_inclusive_scan_<it_t, it_t, reduce_f, map_f,
                                               val_t>,
      shad_test_stl::ordered_checksum<it_t>, reduce_f{}, map_f{}, 0);
}

TYPED_TEST(ATF, transform_exclusive_scan) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  using reduce_f = std::plus<val_t>;
  this->test_io_assignment_with_policy(
      shad::distributed_sequential_tag{},
      shad::transform_exclusive_scan<shad::distributed_sequential_tag, it_t,
                                     it_t, val_t, reduce_f, map_f>,
      shad_test_stl::transform_exclusive_scan_<it_t, it_t, val_t, reduce_f,
                                               map_f>,
      shad_test_stl::ordered_checksum<it_t>, 0, reduce_f{}, map_f{});
  this->test_io_assignment_with_policy(
      shad::distributed_parallel_tag{},
      shad::transform_exclusive_scan<shad::distributed_parallel_tag, it_t, it_t,
                                     val_t, reduce_f, map_f>,
      shad_test_stl::transform_exclusive_scan_<it_t, it_t, val_t, reduce_f,
                                               map_f>,
      shad_test_stl::ordered_checksum<it_t>, 0, reduce_f{}, map_f{});
}

TYPED_TEST(ATF, std_reduce) {
  using it_t = typeof(this->in->begin());
  using reduce_f = std::plus<int>;
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::reduce<shad::distributed_sequential_tag, it_t, int, reduce_f>,
      shad_test_stl::reduce_<it_t, int, reduce_f>, 0, reduce_f{});
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::reduce<shad::distributed_parallel_tag, it_t, int, reduce_f>,
      shad_test_stl::reduce_<it_t, int, reduce_f>, 0, reduce_f{});
}

///////////////////////////////////////
//
// shad::unordered_set
//
///////////////////////////////////////
template <typename T>
using STF = shad_test_stl::SetTestFixture<T>;

using STF_TestTypes = ::testing::Types<shad::unordered_set<int>>;
TYPED_TEST_CASE(STF, STF_TestTypes);

TYPED_TEST(STF, accumulate) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using acc_f = std::plus<int>;
  this->test(shad::accumulate<it_t, val_t, acc_f>,
             shad_test_stl::accumulate_<it_t, val_t, acc_f>, 0, acc_f{});
}

TYPED_TEST(STF, transform_reduce_one_container) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  using reduce_f = std::plus<val_t>;
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::transform_reduce<shad::distributed_sequential_tag, it_t, val_t,
                             reduce_f, map_f>,
      shad_test_stl::transform_reduce_<it_t, val_t, reduce_f, map_f>, 0,
      reduce_f{}, map_f{});
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::transform_reduce<shad::distributed_parallel_tag, it_t, val_t,
                             reduce_f, map_f>,
      shad_test_stl::transform_reduce_<it_t, val_t, reduce_f, map_f>, 0,
      reduce_f{}, map_f{});
}

TYPED_TEST(STF, reduce) {
  using it_t = typeof(this->in->begin());
  using reduce_f = std::plus<int>;
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::reduce<shad::distributed_sequential_tag, it_t, int, reduce_f>,
      shad_test_stl::reduce_<it_t, int, reduce_f>, 0, reduce_f{});
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::reduce<shad::distributed_parallel_tag, it_t, int, reduce_f>,
      shad_test_stl::reduce_<it_t, int, reduce_f>, 0, reduce_f{});
}

///////////////////////////////////////
//
// shad::unordered_map
//
///////////////////////////////////////
template <typename T>
using MTF = shad_test_stl::MapTestFixture<T>;

using MTF_TestTypes = ::testing::Types<shad::unordered_map<int, int>>;
TYPED_TEST_CASE(MTF, MTF_TestTypes);

TYPED_TEST(MTF, accumulate) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  auto op_t = [](int64_t acc, const val_t &p) {
    return acc + shad_test_stl::to_int64<val_t>{}(p);
  };
  this->test(shad::accumulate<it_t, int64_t, typeof(op_t)>,
             shad_test_stl::accumulate_<it_t, int64_t, typeof(op_t)>, 0, op_t);
}

TYPED_TEST(MTF, transform_reduce_one_container) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using acc_t = std::pair<int, int>;
  using map_f = std::negate<val_t>;
  using reduce_f = std::plus<val_t>;
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::transform_reduce<shad::distributed_sequential_tag, it_t, acc_t,
                             reduce_f, map_f>,
      shad_test_stl::transform_reduce_<it_t, acc_t, reduce_f, map_f>,
      std::make_pair(0, 0), reduce_f{}, map_f{});
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::transform_reduce<shad::distributed_parallel_tag, it_t, acc_t,
                             reduce_f, map_f>,
      shad_test_stl::transform_reduce_<it_t, acc_t, reduce_f, map_f>,
      std::make_pair(0, 0), reduce_f{}, map_f{});
}

TYPED_TEST(MTF, reduce) {
  using it_t = typeof(this->in->begin());
  using acc_t = std::pair<int, int>;
  using reduce_f = std::plus<acc_t>;
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::reduce<shad::distributed_sequential_tag, it_t, acc_t, reduce_f>,
      shad_test_stl::reduce_<it_t, acc_t, reduce_f>, std::make_pair(0, 0),
      reduce_f{});
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::reduce<shad::distributed_parallel_tag, it_t, acc_t, reduce_f>,
      shad_test_stl::reduce_<it_t, acc_t, reduce_f>, std::make_pair(0, 0),
      reduce_f{});
}
