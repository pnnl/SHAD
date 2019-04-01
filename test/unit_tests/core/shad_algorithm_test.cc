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

#include "shad/core/algorithm.h"
#include "shad/core/array.h"
#include "shad/core/unordered_map.h"
#include "shad/core/unordered_set.h"

#include "common.h"
#include "stl_emulation/algorithm.h"

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

// min_element, max_element, minmax_element
TYPED_TEST(ATF, shad_min_element) {
  using it_t = typename TypeParam::iterator;

  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::min_element<shad::distributed_sequential_tag, it_t>,
      shad_test_stl::min_element_<it_t>);

  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::min_element<shad::distributed_parallel_tag, it_t>,
      shad_test_stl::min_element_<it_t>);
}

TYPED_TEST(ATF, shad_max_element) {
  using it_t = typename TypeParam::iterator;

  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::max_element<shad::distributed_sequential_tag, it_t>,
      shad_test_stl::max_element_<it_t>);

  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::max_element<shad::distributed_parallel_tag, it_t>,
      shad_test_stl::max_element_<it_t>);
}

TYPED_TEST(ATF, shad_minmax_element) {
  using it_t = typename TypeParam::iterator;

  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::minmax_element<shad::distributed_sequential_tag, it_t>,
      shad_test_stl::minmax_element_<it_t>);

  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::minmax_element<shad::distributed_parallel_tag, it_t>,
      shad_test_stl::minmax_element_<it_t>);
}

// find_if, find_if_not
TYPED_TEST(ATF, shad_find_if) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // found
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find_if<shad::distributed_sequential_tag, it_t,
                    shad_test_stl::is_even<value_t>>,
      shad_test_stl::find_if_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // not found
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find_if<shad::distributed_sequential_tag, it_t,
                    shad_test_stl::is_odd<value_t>>,
      shad_test_stl::find_if_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});

  // found
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find_if<shad::distributed_parallel_tag, it_t,
                    shad_test_stl::is_even<value_t>>,
      shad_test_stl::find_if_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // not found
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find_if<shad::distributed_parallel_tag, it_t,
                    shad_test_stl::is_odd<value_t>>,
      shad_test_stl::find_if_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(ATF, shad_find_if_not) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;
  auto even_pred_t = shad_test_stl::is_even_wrapper<value_t>;
  auto odd_pred_t = shad_test_stl::is_odd_wrapper<value_t>;
  using pred_t = typeof(even_pred_t);

  // not found
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find_if_not<shad::distributed_sequential_tag, it_t, pred_t>,
      shad_test_stl::find_if_not_<it_t, pred_t>, even_pred_t);

  // found
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find_if_not<shad::distributed_sequential_tag, it_t, pred_t>,
      shad_test_stl::find_if_not_<it_t, pred_t>, odd_pred_t);

  // not found
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find_if_not<shad::distributed_parallel_tag, it_t, pred_t>,
      shad_test_stl::find_if_not_<it_t, pred_t>, even_pred_t);

  // found
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find_if_not<shad::distributed_parallel_tag, it_t, pred_t>,
      shad_test_stl::find_if_not_<it_t, pred_t>, odd_pred_t);
}

// all_of, any_of, none_of
TYPED_TEST(ATF, shad_all_of) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // true
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::all_of<shad::distributed_sequential_tag, it_t,
                   shad_test_stl::is_even<value_t>>,
      shad_test_stl::all_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // false
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::all_of<shad::distributed_sequential_tag, it_t,
                   shad_test_stl::is_odd<value_t>>,
      shad_test_stl::all_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});

  // true
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::all_of<shad::distributed_parallel_tag, it_t,
                   shad_test_stl::is_even<value_t>>,
      shad_test_stl::all_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // false
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::all_of<shad::distributed_parallel_tag, it_t,
                   shad_test_stl::is_odd<value_t>>,
      shad_test_stl::all_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(ATF, shad_any_of) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // true
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::any_of<shad::distributed_sequential_tag, it_t,
                   shad_test_stl::is_even<value_t>>,
      shad_test_stl::any_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // false
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::any_of<shad::distributed_sequential_tag, it_t,
                   shad_test_stl::is_odd<value_t>>,
      shad_test_stl::any_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});

  // true
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::any_of<shad::distributed_parallel_tag, it_t,
                   shad_test_stl::is_even<value_t>>,
      shad_test_stl::any_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // false
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::any_of<shad::distributed_parallel_tag, it_t,
                   shad_test_stl::is_odd<value_t>>,
      shad_test_stl::any_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(ATF, shad_none_of) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // false
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::none_of<shad::distributed_sequential_tag, it_t,
                    shad_test_stl::is_even<value_t>>,
      shad_test_stl::none_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // true
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::none_of<shad::distributed_sequential_tag, it_t,
                    shad_test_stl::is_odd<value_t>>,
      shad_test_stl::none_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});

  // false
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::none_of<shad::distributed_parallel_tag, it_t,
                    shad_test_stl::is_even<value_t>>,
      shad_test_stl::none_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // true
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::none_of<shad::distributed_parallel_tag, it_t,
                    shad_test_stl::is_odd<value_t>>,
      shad_test_stl::none_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});
}

// for_each, for_each_n - todo

// count, count_if
TYPED_TEST(ATF, shad_count) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // occurring
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::count<shad::distributed_sequential_tag, it_t, value_t>,
      shad_test_stl::count_<it_t, value_t>, 0);

  // not occurring
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::count<shad::distributed_sequential_tag, it_t, value_t>,
      shad_test_stl::count_<it_t, value_t>, 1);

  // occurring
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::count<shad::distributed_parallel_tag, it_t, value_t>,
      shad_test_stl::count_<it_t, value_t>, 0);

  // not occurring
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::count<shad::distributed_parallel_tag, it_t, value_t>,
      shad_test_stl::count_<it_t, value_t>, 1);
}

TYPED_TEST(ATF, shad_count_if) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // all
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::count_if<shad::distributed_sequential_tag, it_t,
                     shad_test_stl::is_even<value_t>>,
      shad_test_stl::count_if_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // none
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::count_if<shad::distributed_sequential_tag, it_t,
                     shad_test_stl::is_odd<value_t>>,
      shad_test_stl::count_if_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});

  // all
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::count_if<shad::distributed_parallel_tag, it_t,
                     shad_test_stl::is_even<value_t>>,
      shad_test_stl::count_if_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // none
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::count_if<shad::distributed_parallel_tag, it_t,
                     shad_test_stl::is_odd<value_t>>,
      shad_test_stl::count_if_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});
}

// mismatch - todo

// find
TYPED_TEST(ATF, shad_find) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // occurring
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find<shad::distributed_sequential_tag, it_t, value_t>,
      shad_test_stl::find_<it_t, value_t>, 0);

  // not occurring
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find<shad::distributed_sequential_tag, it_t, value_t>,
      shad_test_stl::find_<it_t, value_t>, 1);

  // occurring
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find<shad::distributed_parallel_tag, it_t, value_t>,
      shad_test_stl::find_<it_t, value_t>, 0);

  // not occurring
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find<shad::distributed_parallel_tag, it_t, value_t>,
      shad_test_stl::find_<it_t, value_t>, 1);
}

// todo find_end
// todo find_first_of
// todo adjacent_find
// todo search

// search_n - todo

// fill
TYPED_TEST(ATF, shad_fill) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  this->test_void_with_policy(
      shad::distributed_sequential_tag{},
      shad::fill<shad::distributed_sequential_tag, it_t, val_t>,
      shad_test_stl::fill_<it_t, val_t>, shad_test_stl::ordered_checksum<it_t>,
      42);
  this->test_void_with_policy(
      shad::distributed_parallel_tag{},
      shad::fill<shad::distributed_parallel_tag, it_t, val_t>,
      shad_test_stl::fill_<it_t, val_t>, shad_test_stl::ordered_checksum<it_t>,
      42);
}

// transform
TYPED_TEST(ATF, shad_transform) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  // ditributed sequential - aligned
  this->test_io_assignment_with_policy(
      shad::distributed_sequential_tag{},
      shad::transform<shad::distributed_sequential_tag, it_t, it_t, map_f>,
      shad_test_stl::transform_<it_t, it_t, map_f>,
      shad_test_stl::ordered_checksum<it_t>, map_f{});
  // ditributed parallel - aligned
  this->test_io_assignment_with_policy(
      shad::distributed_parallel_tag{},
      shad::transform<shad::distributed_parallel_tag, it_t, it_t, map_f>,
      shad_test_stl::transform_<it_t, it_t, map_f>,
      shad_test_stl::ordered_checksum<it_t>, map_f{});
  // ditributed sequential - unaligned
  this->test_io_unaligned_assignment_with_policy(
      shad::distributed_sequential_tag{},
      shad::transform<shad::distributed_sequential_tag, it_t,
                      typename TestFixture::unaligned_iterator, map_f>,
      shad_test_stl::transform_<it_t, it_t, map_f>, map_f{});
  // ditributed parallel - unaligned
  this->test_io_unaligned_assignment_with_policy(
      shad::distributed_parallel_tag{},
      shad::transform<shad::distributed_parallel_tag, it_t,
                      typename TestFixture::unaligned_iterator, map_f>,
      shad_test_stl::transform_<it_t, it_t, map_f>, map_f{});
}

// generate
TYPED_TEST(ATF, shad_generate) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  auto flip_f = [n = 42]() mutable { return n = std::negate<val_t>{}(n); };
  this->test_void_with_policy(
      shad::distributed_sequential_tag{},
      shad::generate<shad::distributed_sequential_tag, it_t, typeof(flip_f)>,
      shad_test_stl::generate_<it_t, typeof(flip_f)>,
      shad_test_stl::ordered_checksum<it_t>, flip_f);
  auto const_f = [n = 42]() mutable { return n; };
  this->test_void_with_policy(
      shad::distributed_parallel_tag{},
      shad::generate<shad::distributed_parallel_tag, it_t, typeof(const_f)>,
      shad_test_stl::generate_<it_t, typeof(const_f)>,
      shad_test_stl::ordered_checksum<it_t>, const_f);
}

// replace
TYPED_TEST(ATF, shad_replace) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  this->test_void_with_policy(
      shad::distributed_sequential_tag{},
      shad::replace<shad::distributed_sequential_tag, it_t, val_t>,
      shad_test_stl::replace_<it_t, val_t>,
      shad_test_stl::ordered_checksum<it_t>, 42, 43);
  this->test_void_with_policy(
      shad::distributed_parallel_tag{},
      shad::replace<shad::distributed_parallel_tag, it_t, val_t>,
      shad_test_stl::replace_<it_t, val_t>,
      shad_test_stl::ordered_checksum<it_t>, 42, 43);
}

// replace_if
TYPED_TEST(ATF, shad_replace_if) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  uint64_t exp_sum = 0, obs_sum = 0, pos;
  auto pred = [](int x) { return (x % 3 == 0); };
  this->test_void_with_policy(
      shad::distributed_sequential_tag{},
      shad::replace_if<shad::distributed_sequential_tag, it_t, typeof(pred),
                       val_t>,
      shad_test_stl::replace_if_<it_t, typeof(pred), val_t>,
      shad_test_stl::ordered_checksum<it_t>, pred, 3);
  this->test_void_with_policy(
      shad::distributed_parallel_tag{},
      shad::replace_if<shad::distributed_parallel_tag, it_t, typeof(pred),
                       val_t>,
      shad_test_stl::replace_if_<it_t, typeof(pred), val_t>,
      shad_test_stl::ordered_checksum<it_t>, pred, 3);
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

// min_element, max_element, minmax_element
TYPED_TEST(STF, shad_min_element) {
  using it_t = typename TypeParam::iterator;

  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::min_element<shad::distributed_sequential_tag, it_t>,
      shad_test_stl::min_element_<it_t>);

  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::min_element<shad::distributed_parallel_tag, it_t>,
      shad_test_stl::min_element_<it_t>);
}

TYPED_TEST(STF, shad_max_element) {
  using it_t = typename TypeParam::iterator;

  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::max_element<shad::distributed_sequential_tag, it_t>,
      shad_test_stl::max_element_<it_t>);

  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::max_element<shad::distributed_parallel_tag, it_t>,
      shad_test_stl::max_element_<it_t>);
}

TYPED_TEST(STF, shad_minmax_element) {
  using it_t = typename TypeParam::iterator;

  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::minmax_element<shad::distributed_sequential_tag, it_t>,
      shad_test_stl::minmax_element_<it_t>);

  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::minmax_element<shad::distributed_parallel_tag, it_t>,
      shad_test_stl::minmax_element_<it_t>);
}

// find_if, find_if_not
TYPED_TEST(STF, shad_find_if) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // found
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find_if<shad::distributed_sequential_tag, it_t,
                    shad_test_stl::is_even<value_t>>,
      shad_test_stl::find_if_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // not found
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find_if<shad::distributed_sequential_tag, it_t,
                    shad_test_stl::is_odd<value_t>>,
      shad_test_stl::find_if_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});

  // found
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find_if<shad::distributed_parallel_tag, it_t,
                    shad_test_stl::is_even<value_t>>,
      shad_test_stl::find_if_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // not found
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find_if<shad::distributed_parallel_tag, it_t,
                    shad_test_stl::is_odd<value_t>>,
      shad_test_stl::find_if_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(STF, shad_find_if_not) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;
  auto even_pred_t = shad_test_stl::is_even_wrapper<value_t>;
  auto odd_pred_t = shad_test_stl::is_odd_wrapper<value_t>;
  using pred_t = typeof(even_pred_t);

  // not found
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find_if_not<shad::distributed_sequential_tag, it_t, pred_t>,
      shad_test_stl::find_if_not_<it_t, pred_t>, even_pred_t);

  // found
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find_if_not<shad::distributed_sequential_tag, it_t, pred_t>,
      shad_test_stl::find_if_not_<it_t, pred_t>, odd_pred_t);

  // not found
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find_if_not<shad::distributed_parallel_tag, it_t, pred_t>,
      shad_test_stl::find_if_not_<it_t, pred_t>, even_pred_t);

  // found
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find_if_not<shad::distributed_parallel_tag, it_t, pred_t>,
      shad_test_stl::find_if_not_<it_t, pred_t>, odd_pred_t);
}

// all_of, any_of, none_of
TYPED_TEST(STF, shad_all_of) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // true
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::all_of<shad::distributed_sequential_tag, it_t,
                   shad_test_stl::is_even<value_t>>,
      shad_test_stl::all_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // false
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::all_of<shad::distributed_sequential_tag, it_t,
                   shad_test_stl::is_odd<value_t>>,
      shad_test_stl::all_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});

  // true
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::all_of<shad::distributed_parallel_tag, it_t,
                   shad_test_stl::is_even<value_t>>,
      shad_test_stl::all_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // false
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::all_of<shad::distributed_parallel_tag, it_t,
                   shad_test_stl::is_odd<value_t>>,
      shad_test_stl::all_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(STF, shad_any_of) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // true
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::any_of<shad::distributed_sequential_tag, it_t,
                   shad_test_stl::is_even<value_t>>,
      shad_test_stl::any_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // false
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::any_of<shad::distributed_sequential_tag, it_t,
                   shad_test_stl::is_odd<value_t>>,
      shad_test_stl::any_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});

  // true
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::any_of<shad::distributed_parallel_tag, it_t,
                   shad_test_stl::is_even<value_t>>,
      shad_test_stl::any_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // false
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::any_of<shad::distributed_parallel_tag, it_t,
                   shad_test_stl::is_odd<value_t>>,
      shad_test_stl::any_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(STF, shad_none_of) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // false
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::none_of<shad::distributed_sequential_tag, it_t,
                    shad_test_stl::is_even<value_t>>,
      shad_test_stl::none_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // true
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::none_of<shad::distributed_sequential_tag, it_t,
                    shad_test_stl::is_odd<value_t>>,
      shad_test_stl::none_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});

  // false
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::none_of<shad::distributed_parallel_tag, it_t,
                    shad_test_stl::is_even<value_t>>,
      shad_test_stl::none_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // true
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::none_of<shad::distributed_parallel_tag, it_t,
                    shad_test_stl::is_odd<value_t>>,
      shad_test_stl::none_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});
}

// for_each, for_each_n - todo

// count, count_if
TYPED_TEST(STF, shad_count) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // occurring
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::count<shad::distributed_sequential_tag, it_t, value_t>,
      shad_test_stl::count_<it_t, value_t>, 0);

  // not occurring
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::count<shad::distributed_sequential_tag, it_t, value_t>,
      shad_test_stl::count_<it_t, value_t>, 1);

  // occurring
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::count<shad::distributed_parallel_tag, it_t, value_t>,
      shad_test_stl::count_<it_t, value_t>, 0);

  // not occurring
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::count<shad::distributed_parallel_tag, it_t, value_t>,
      shad_test_stl::count_<it_t, value_t>, 1);
}

TYPED_TEST(STF, shad_count_if) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // all
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::count_if<shad::distributed_sequential_tag, it_t,
                     shad_test_stl::is_even<value_t>>,
      shad_test_stl::count_if_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // none
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::count_if<shad::distributed_sequential_tag, it_t,
                     shad_test_stl::is_odd<value_t>>,
      shad_test_stl::count_if_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});

  // all
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::count_if<shad::distributed_parallel_tag, it_t,
                     shad_test_stl::is_even<value_t>>,
      shad_test_stl::count_if_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // none
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::count_if<shad::distributed_parallel_tag, it_t,
                     shad_test_stl::is_odd<value_t>>,
      shad_test_stl::count_if_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});
}

// mismatch - todo

// find
TYPED_TEST(STF, shad_find) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // occurring
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find<shad::distributed_sequential_tag, it_t, value_t>,
      shad_test_stl::find_<it_t, value_t>, 0);

  // not occurring
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find<shad::distributed_sequential_tag, it_t, value_t>,
      shad_test_stl::find_<it_t, value_t>, 1);

  // occurring
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find<shad::distributed_parallel_tag, it_t, value_t>,
      shad_test_stl::find_<it_t, value_t>, 0);

  // not occurring
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find<shad::distributed_parallel_tag, it_t, value_t>,
      shad_test_stl::find_<it_t, value_t>, 1);
}

// todo find_end
// todo find_first_of
// todo adjacent_find
// todo search

// search_n - todo

// transform
TYPED_TEST(STF, shad_transform) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  using aligned_map_f = std::negate<val_t>;
  using unaligned_map_f = shad_test_stl::inc<val_t>;

  // sequential - aligned
  this->template test_io_inserters_with_policy<
      shad::insert_iterator<TypeParam>>(
      shad::distributed_sequential_tag{},
      shad::transform<shad::distributed_sequential_tag, it_t,
                      shad::insert_iterator<TypeParam>, aligned_map_f>,
      shad_test_stl::transform_<it_t, std::insert_iterator<TypeParam>,
                                aligned_map_f>,
      shad_test_stl::checksum<it_t>, aligned_map_f{});

  // parallel - aligned
  this->template test_io_inserters_with_policy<
      shad::insert_iterator<TypeParam>>(
      shad::distributed_parallel_tag{},
      shad::transform<shad::distributed_parallel_tag, it_t,
                      shad::insert_iterator<TypeParam>, aligned_map_f>,
      shad_test_stl::transform_<it_t, std::insert_iterator<TypeParam>,
                                aligned_map_f>,
      shad_test_stl::checksum<it_t>, aligned_map_f{});

  // sequential - aligned - buffered insert iterator
  this->template test_io_inserters_with_policy<
      shad::buffered_insert_iterator<TypeParam>>(
      shad::distributed_sequential_tag{},
      shad::transform<shad::distributed_sequential_tag, it_t,
                      shad::buffered_insert_iterator<TypeParam>, aligned_map_f>,
      shad_test_stl::transform_<it_t, std::insert_iterator<TypeParam>,
                                aligned_map_f>,
      shad_test_stl::checksum<it_t>, aligned_map_f{});

  // parallel - aligned - buffered insert iterator
  this->template test_io_inserters_with_policy<
      shad::buffered_insert_iterator<TypeParam>>(
      shad::distributed_parallel_tag{},
      shad::transform<shad::distributed_parallel_tag, it_t,
                      shad::buffered_insert_iterator<TypeParam>, aligned_map_f>,
      shad_test_stl::transform_<it_t, std::insert_iterator<TypeParam>,
                                aligned_map_f>,
      shad_test_stl::checksum<it_t>, aligned_map_f{});

  // sequential - unaligned - buffered insert iterator
  this->template test_io_inserters_with_policy<
      shad::buffered_insert_iterator<TypeParam>>(
      shad::distributed_sequential_tag{},
      shad::transform<shad::distributed_sequential_tag, it_t,
                      shad::buffered_insert_iterator<TypeParam>,
                      unaligned_map_f>,
      shad_test_stl::transform_<it_t, std::insert_iterator<TypeParam>,
                                unaligned_map_f>,
      shad_test_stl::checksum<it_t>, unaligned_map_f{});

  // parallel - unaligned - buffered insert iterator
  this->template test_io_inserters_with_policy<
      shad::buffered_insert_iterator<TypeParam>>(
      shad::distributed_parallel_tag{},
      shad::transform<shad::distributed_parallel_tag, it_t,
                      shad::buffered_insert_iterator<TypeParam>,
                      unaligned_map_f>,
      shad_test_stl::transform_<it_t, std::insert_iterator<TypeParam>,
                                unaligned_map_f>,
      shad_test_stl::checksum<it_t>, unaligned_map_f{});
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

// min_element, max_element, minmax_element
TYPED_TEST(MTF, shad_min_element) {
  using it_t = typename TypeParam::iterator;

  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::min_element<shad::distributed_sequential_tag, it_t>,
      shad_test_stl::min_element_<it_t>);

  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::min_element<shad::distributed_parallel_tag, it_t>,
      shad_test_stl::min_element_<it_t>);
}

TYPED_TEST(MTF, shad_max_element) {
  using it_t = typename TypeParam::iterator;

  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::max_element<shad::distributed_sequential_tag, it_t>,
      shad_test_stl::max_element_<it_t>);

  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::max_element<shad::distributed_parallel_tag, it_t>,
      shad_test_stl::max_element_<it_t>);
}

TYPED_TEST(MTF, shad_minmax_element) {
  using it_t = typename TypeParam::iterator;

  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::minmax_element<shad::distributed_sequential_tag, it_t>,
      shad_test_stl::minmax_element_<it_t>);

  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::minmax_element<shad::distributed_parallel_tag, it_t>,
      shad_test_stl::minmax_element_<it_t>);
}

// find_if, find_if_not
TYPED_TEST(MTF, shad_find_if) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // found
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find_if<shad::distributed_sequential_tag, it_t,
                    shad_test_stl::is_even<value_t>>,
      shad_test_stl::find_if_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // not found
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find_if<shad::distributed_sequential_tag, it_t,
                    shad_test_stl::is_odd<value_t>>,
      shad_test_stl::find_if_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});

  // found
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find_if<shad::distributed_parallel_tag, it_t,
                    shad_test_stl::is_even<value_t>>,
      shad_test_stl::find_if_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // not found
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find_if<shad::distributed_parallel_tag, it_t,
                    shad_test_stl::is_odd<value_t>>,
      shad_test_stl::find_if_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(MTF, shad_find_if_not) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;
  auto even_pred_t = shad_test_stl::is_even_wrapper<value_t>;
  auto odd_pred_t = shad_test_stl::is_odd_wrapper<value_t>;
  using pred_t = typeof(even_pred_t);

  // not found
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find_if_not<shad::distributed_sequential_tag, it_t, pred_t>,
      shad_test_stl::find_if_not_<it_t, pred_t>, even_pred_t);

  // found
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find_if_not<shad::distributed_sequential_tag, it_t, pred_t>,
      shad_test_stl::find_if_not_<it_t, pred_t>, odd_pred_t);

  // not found
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find_if_not<shad::distributed_parallel_tag, it_t, pred_t>,
      shad_test_stl::find_if_not_<it_t, pred_t>, even_pred_t);

  // found
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find_if_not<shad::distributed_parallel_tag, it_t, pred_t>,
      shad_test_stl::find_if_not_<it_t, pred_t>, odd_pred_t);
}

// all_of, any_of, none_of
TYPED_TEST(MTF, shad_all_of) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // true
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::all_of<shad::distributed_sequential_tag, it_t,
                   shad_test_stl::is_even<value_t>>,
      shad_test_stl::all_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // false
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::all_of<shad::distributed_sequential_tag, it_t,
                   shad_test_stl::is_odd<value_t>>,
      shad_test_stl::all_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});

  // true
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::all_of<shad::distributed_parallel_tag, it_t,
                   shad_test_stl::is_even<value_t>>,
      shad_test_stl::all_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // false
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::all_of<shad::distributed_parallel_tag, it_t,
                   shad_test_stl::is_odd<value_t>>,
      shad_test_stl::all_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(MTF, shad_any_of) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // true
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::any_of<shad::distributed_sequential_tag, it_t,
                   shad_test_stl::is_even<value_t>>,
      shad_test_stl::any_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // false
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::any_of<shad::distributed_sequential_tag, it_t,
                   shad_test_stl::is_odd<value_t>>,
      shad_test_stl::any_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});

  // true
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::any_of<shad::distributed_parallel_tag, it_t,
                   shad_test_stl::is_even<value_t>>,
      shad_test_stl::any_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // false
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::any_of<shad::distributed_parallel_tag, it_t,
                   shad_test_stl::is_odd<value_t>>,
      shad_test_stl::any_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(MTF, shad_none_of) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // false
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::none_of<shad::distributed_sequential_tag, it_t,
                    shad_test_stl::is_even<value_t>>,
      shad_test_stl::none_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // true
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::none_of<shad::distributed_sequential_tag, it_t,
                    shad_test_stl::is_odd<value_t>>,
      shad_test_stl::none_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});

  // false
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::none_of<shad::distributed_parallel_tag, it_t,
                    shad_test_stl::is_even<value_t>>,
      shad_test_stl::none_of_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // true
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::none_of<shad::distributed_parallel_tag, it_t,
                    shad_test_stl::is_odd<value_t>>,
      shad_test_stl::none_of_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});
}

// for_each, for_each_n - todo

// count, count_if
TYPED_TEST(MTF, shad_count) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // occurring
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::count<shad::distributed_sequential_tag, it_t, value_t>,
      shad_test_stl::count_<it_t, value_t>, std::make_pair(0, 0));

  // not occurring
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::count<shad::distributed_sequential_tag, it_t, value_t>,
      shad_test_stl::count_<it_t, value_t>, std::make_pair(0, 1));

  // occurring
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::count<shad::distributed_parallel_tag, it_t, value_t>,
      shad_test_stl::count_<it_t, value_t>, std::make_pair(0, 0));

  // not occurring
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::count<shad::distributed_parallel_tag, it_t, value_t>,
      shad_test_stl::count_<it_t, value_t>, std::make_pair(0, 1));
}

TYPED_TEST(MTF, shad_count_if) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // all
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::count_if<shad::distributed_sequential_tag, it_t,
                     shad_test_stl::is_even<value_t>>,
      shad_test_stl::count_if_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // none
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::count_if<shad::distributed_sequential_tag, it_t,
                     shad_test_stl::is_odd<value_t>>,
      shad_test_stl::count_if_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});

  // all
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::count_if<shad::distributed_parallel_tag, it_t,
                     shad_test_stl::is_even<value_t>>,
      shad_test_stl::count_if_<it_t, shad_test_stl::is_even<value_t>>,
      shad_test_stl::is_even<value_t>{});

  // none
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::count_if<shad::distributed_parallel_tag, it_t,
                     shad_test_stl::is_odd<value_t>>,
      shad_test_stl::count_if_<it_t, shad_test_stl::is_odd<value_t>>,
      shad_test_stl::is_odd<value_t>{});
}

// mismatch - todo

// find
TYPED_TEST(MTF, shad_find) {
  using it_t = typename TypeParam::iterator;
  using value_t = typename TypeParam::value_type;

  // occurring
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find<shad::distributed_sequential_tag, it_t, value_t>,
      shad_test_stl::find_<it_t, value_t>, std::make_pair(0, 0));

  // not occurring
  this->test_with_policy(
      shad::distributed_sequential_tag{},
      shad::find<shad::distributed_sequential_tag, it_t, value_t>,
      shad_test_stl::find_<it_t, value_t>, std::make_pair(0, 1));

  // occurring
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find<shad::distributed_parallel_tag, it_t, value_t>,
      shad_test_stl::find_<it_t, value_t>, std::make_pair(0, 0));

  // not occurring
  this->test_with_policy(
      shad::distributed_parallel_tag{},
      shad::find<shad::distributed_parallel_tag, it_t, value_t>,
      shad_test_stl::find_<it_t, value_t>, std::make_pair(0, 1));
}

// todo find_end
// todo find_first_of
// todo adjacent_find
// todo search

// search_n - todo

// transform
TYPED_TEST(MTF, shad_transform) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  using aligned_map_f = std::negate<val_t>;
  using unaligned_map_f = shad_test_stl::inc<val_t>;

  // sequential - aligned
  this->template test_io_inserters_with_policy<
      shad::insert_iterator<TypeParam>>(
      shad::distributed_sequential_tag{},
      shad::transform<shad::distributed_sequential_tag, it_t,
                      shad::insert_iterator<TypeParam>, aligned_map_f>,
      shad_test_stl::transform_<it_t, std::insert_iterator<TypeParam>,
                                aligned_map_f>,
      shad_test_stl::checksum<it_t>, aligned_map_f{});

  // parallel - aligned
  this->template test_io_inserters_with_policy<
      shad::insert_iterator<TypeParam>>(
      shad::distributed_parallel_tag{},
      shad::transform<shad::distributed_parallel_tag, it_t,
                      shad::insert_iterator<TypeParam>, aligned_map_f>,
      shad_test_stl::transform_<it_t, std::insert_iterator<TypeParam>,
                                aligned_map_f>,
      shad_test_stl::checksum<it_t>, aligned_map_f{});

  // sequential - aligned - buffered insert iterator
  this->template test_io_inserters_with_policy<
      shad::buffered_insert_iterator<TypeParam>>(
      shad::distributed_sequential_tag{},
      shad::transform<shad::distributed_sequential_tag, it_t,
                      shad::buffered_insert_iterator<TypeParam>, aligned_map_f>,
      shad_test_stl::transform_<it_t, std::insert_iterator<TypeParam>,
                                aligned_map_f>,
      shad_test_stl::checksum<it_t>, aligned_map_f{});

  // parallel - aligned - buffered insert iterator
  this->template test_io_inserters_with_policy<
      shad::buffered_insert_iterator<TypeParam>>(
      shad::distributed_parallel_tag{},
      shad::transform<shad::distributed_parallel_tag, it_t,
                      shad::buffered_insert_iterator<TypeParam>, aligned_map_f>,
      shad_test_stl::transform_<it_t, std::insert_iterator<TypeParam>,
                                aligned_map_f>,
      shad_test_stl::checksum<it_t>, aligned_map_f{});

  // sequential - unaligned - buffered insert iterator
  this->template test_io_inserters_with_policy<
      shad::buffered_insert_iterator<TypeParam>>(
      shad::distributed_sequential_tag{},
      shad::transform<shad::distributed_sequential_tag, it_t,
                      shad::buffered_insert_iterator<TypeParam>,
                      unaligned_map_f>,
      shad_test_stl::transform_<it_t, std::insert_iterator<TypeParam>,
                                unaligned_map_f>,
      shad_test_stl::checksum<it_t>, unaligned_map_f{});

  // parallel - unaligned - buffered insert iterator
  this->template test_io_inserters_with_policy<
      shad::buffered_insert_iterator<TypeParam>>(
      shad::distributed_parallel_tag{},
      shad::transform<shad::distributed_parallel_tag, it_t,
                      shad::buffered_insert_iterator<TypeParam>,
                      unaligned_map_f>,
      shad_test_stl::transform_<it_t, std::insert_iterator<TypeParam>,
                                unaligned_map_f>,
      shad_test_stl::checksum<it_t>, unaligned_map_f{});
}
