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

#include "shad/core/array.h"
#include "shad/core/unordered_map.h"
#include "shad/core/unordered_set.h"

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

// min_element, max_element, minmax_element
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

// find_if, find_if_not
TYPED_TEST(VTF, std_find_if) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // found
  this->test(std::find_if<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::find_if_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // not found
  this->test(std::find_if<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::find_if_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(VTF, std_find_if_not) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // not found
  this->test(std::find_if_not<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::find_if_not_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // found
  this->test(std::find_if_not<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::find_if_not_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

// all_of, any_of, none_of
TYPED_TEST(VTF, std_all_of) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // true
  this->test(std::all_of<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::all_of_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // false
  this->test(std::all_of<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::all_of_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(VTF, std_any_of) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // true
  this->test(std::any_of<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::any_of_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // false
  this->test(std::any_of<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::any_of_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(VTF, std_none_of) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // false
  this->test(std::none_of<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::none_of_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // true
  this->test(std::none_of<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::none_of_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

// for_each, for_each_n - todo

// count, count_if
TYPED_TEST(VTF, std_count) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // occurring
  this->test(std::count<it_t, value_t>, shad_test_stl::count_<it_t, value_t>,
             0);

  // not occurring
  this->test(std::count<it_t, value_t>, shad_test_stl::count_<it_t, value_t>,
             1);
}

TYPED_TEST(VTF, std_count_if) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // all
  this->test(std::count_if<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::count_if_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // none
  this->test(std::count_if<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::count_if_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

// mismatch - todo

// find
TYPED_TEST(VTF, std_find) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // occurring
  this->test(std::find<it_t, value_t>, shad_test_stl::find_<it_t, value_t>, 0);

  // not occurring
  this->test(std::find<it_t, value_t>, shad_test_stl::find_<it_t, value_t>, 1);
}

// find_end
TYPED_TEST(VTF, std_find_end) {
  using it_t = typeof(this->in->begin());

  // occurring
  auto s = shad_test_stl::subseq_from_<TypeParam>{}(
      this->in, shad_test_stl::kNumElements - shad_test_stl::substr_len,
      shad_test_stl::substr_len);
  this->test(std::find_end<it_t, it_t>, shad_test_stl::find_end_<it_t, it_t>,
             s->begin(), s->end());

  // not occurring
  s = shad_test_stl::create_vector_<TypeParam, false>{}(
      shad_test_stl::substr_len);
  this->test(std::find_end<it_t, it_t>, shad_test_stl::find_end_<it_t, it_t>,
             s->begin(), s->end());
}

// find_first_of
TYPED_TEST(VTF, std_find_first_of) {
  using it_t = typeof(this->in->begin());

  // occurring
  auto s = shad_test_stl::subseq_from_<TypeParam>{}(
      this->in, shad_test_stl::kNumElements - shad_test_stl::substr_len,
      shad_test_stl::substr_len);
  this->test(std::find_first_of<it_t, it_t>,
             shad_test_stl::find_first_of_<it_t, it_t>, s->begin(), s->end());

  // not occurring
  s = shad_test_stl::create_vector_<TypeParam, false>{}(
      shad_test_stl::substr_len);
  this->test(std::find_first_of<it_t, it_t>,
             shad_test_stl::find_first_of_<it_t, it_t>, s->begin(), s->end());
}

// adjacent_find
TYPED_TEST(VTF, std_adjacent_find) {
  using it_t = typeof(this->in->begin());

  // none
  this->test(std::adjacent_find<it_t>, shad_test_stl::adjacent_find_<it_t>);

  // some - todo
}

// search
TYPED_TEST(VTF, std_search) {
  using it_t = typeof(this->in->begin());

  // occurring
  auto s = shad_test_stl::subseq_from_<TypeParam>{}(
      this->in, shad_test_stl::kNumElements - shad_test_stl::substr_len,
      shad_test_stl::substr_len);
  this->test(std::search<it_t, it_t>, shad_test_stl::search_<it_t, it_t>,
             s->begin(), s->end());

  // not occurring
  s = shad_test_stl::create_vector_<TypeParam, false>{}(
      shad_test_stl::substr_len);
  this->test(std::search<it_t, it_t>, shad_test_stl::search_<it_t, it_t>,
             s->begin(), s->end());
}

// search_n - todo

// fill
TYPED_TEST(VTF, std_fill) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  this->test_void(std::fill<it_t, val_t>, shad_test_stl::fill_<it_t, val_t>,
                  shad_test_stl::ordered_checksum<it_t>, 42);
}

// transform
TYPED_TEST(VTF, std_transform) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  this->test_io_assignment(std::transform<it_t, it_t, map_f>,
                           shad_test_stl::transform_<it_t, it_t, map_f>,
                           shad_test_stl::ordered_checksum<it_t>, map_f{});
}

// generate
TYPED_TEST(VTF, std_generate) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  int x = 42;
  auto generator = [&x]() { return x = std::negate<val_t>{}(x); };
  this->test_void(std::generate<it_t, typeof(generator)>,
                  shad_test_stl::generate_<it_t, typeof(generator)>,
                  shad_test_stl::ordered_checksum<it_t>, generator);
}

// replace
TYPED_TEST(VTF, std_replace) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  this->test_void(std::replace<it_t, val_t>,
                  shad_test_stl::replace_<it_t, val_t>,
                  shad_test_stl::ordered_checksum<it_t>, 42, 43);
}

// replace_if
TYPED_TEST(VTF, std_replace_if) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  auto pred = [](int x) { return (x % 3 == 0); };

  this->test_void(std::replace_if<it_t, typeof(pred), val_t>,
                  shad_test_stl::replace_if_<it_t, typeof(pred), val_t>,
                  shad_test_stl::ordered_checksum<it_t>, pred, 3);
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

// min_element, max_element, minmax_element
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

// find_if, find_if_not
TYPED_TEST(ATF, std_find_if) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // found
  this->test(std::find_if<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::find_if_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // not found
  this->test(std::find_if<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::find_if_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(ATF, std_find_if_not) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // not found
  this->test(std::find_if_not<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::find_if_not_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // found
  this->test(std::find_if_not<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::find_if_not_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

// all_of, any_of, none_of
TYPED_TEST(ATF, std_all_of) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // true
  this->test(std::all_of<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::all_of_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // false
  this->test(std::all_of<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::all_of_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(ATF, std_any_of) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // true
  this->test(std::any_of<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::any_of_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // false
  this->test(std::any_of<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::any_of_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(ATF, std_none_of) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // false
  this->test(std::none_of<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::none_of_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // true
  this->test(std::none_of<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::none_of_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

// for_each, for_each_n - todo

// count, count_if
TYPED_TEST(ATF, std_count) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // occurring
  this->test(std::count<it_t, value_t>, shad_test_stl::count_<it_t, value_t>,
             0);

  // not occurring
  this->test(std::count<it_t, value_t>, shad_test_stl::count_<it_t, value_t>,
             1);
}

TYPED_TEST(ATF, std_count_if) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // all
  this->test(std::count_if<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::count_if_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // none
  this->test(std::count_if<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::count_if_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

// mismatch - todo

// find
TYPED_TEST(ATF, std_find) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // occurring
  this->test(std::find<it_t, value_t>, shad_test_stl::find_<it_t, value_t>, 0);

  // not occurring
  this->test(std::find<it_t, value_t>, shad_test_stl::find_<it_t, value_t>, 1);
}

// find_end
TYPED_TEST(ATF, std_find_end) {
  using it_t = typeof(this->in->begin());

  // occurring
  auto s = shad_test_stl::static_subseq_from_<TypeParam,
                                              shad_test_stl::substr_len>{}(
      this->in, shad_test_stl::kNumElements - shad_test_stl::substr_len);
  using s_it_t = typeof(s->begin());
  this->test(std::find_end<it_t, s_it_t>,
             shad_test_stl::find_end_<it_t, s_it_t>, s->begin(), s->end());

  // not occurring
  using res_t = typeof(s);
  s = shad_test_stl::create_array_<typename res_t::element_type, false>{}();
  this->test(std::find_end<it_t, s_it_t>,
             shad_test_stl::find_end_<it_t, s_it_t>, s->begin(), s->end());
}

// find_first_of
TYPED_TEST(ATF, std_find_first_of) {
  using it_t = typeof(this->in->begin());

  // occurring
  auto s = shad_test_stl::static_subseq_from_<TypeParam,
                                              shad_test_stl::substr_len>{}(
      this->in, shad_test_stl::kNumElements - shad_test_stl::substr_len);
  using s_it_t = typeof(s->begin());
  this->test(std::find_first_of<it_t, s_it_t>,
             shad_test_stl::find_first_of_<it_t, s_it_t>, s->begin(), s->end());

  // not occurring
  using res_t = typeof(s);
  s = shad_test_stl::create_array_<typename res_t::element_type, false>{}();
  this->test(std::find_first_of<it_t, s_it_t>,
             shad_test_stl::find_first_of_<it_t, s_it_t>, s->begin(), s->end());
}

// adjacent_find
TYPED_TEST(ATF, std_adjacent_find) {
  using it_t = typeof(this->in->begin());

  // none
  this->test(std::adjacent_find<it_t>, shad_test_stl::adjacent_find_<it_t>);

  // some - todo
}

// search
TYPED_TEST(ATF, std_search) {
  using it_t = typeof(this->in->begin());

  // occurring
  auto s = shad_test_stl::static_subseq_from_<TypeParam,
                                              shad_test_stl::substr_len>{}(
      this->in, shad_test_stl::kNumElements - shad_test_stl::substr_len);
  using s_it_t = typeof(s->begin());
  this->test(std::search<it_t, s_it_t>, shad_test_stl::search_<it_t, s_it_t>,
             s->begin(), s->end());

  // not occurring
  using res_t = typeof(s);
  s = shad_test_stl::create_array_<typename res_t::element_type, false>{}();
  this->test(std::search<it_t, s_it_t>, shad_test_stl::search_<it_t, s_it_t>,
             s->begin(), s->end());
}

// search_n - todo

// fill
TYPED_TEST(ATF, std_fill) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  this->test_void(std::fill<it_t, val_t>, shad_test_stl::fill_<it_t, val_t>,
                  shad_test_stl::ordered_checksum<it_t>, 42);
}

// transform
TYPED_TEST(ATF, std_transform) {
  using it_t = typeof(this->in->begin());
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  this->test_io_assignment(std::transform<it_t, it_t, map_f>,
                           shad_test_stl::transform_<it_t, it_t, map_f>,
                           shad_test_stl::ordered_checksum<it_t>, map_f{});
}

// generate
TYPED_TEST(ATF, std_generate) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  int x = 42;
  auto generator = [&x]() { return x = std::negate<val_t>{}(x); };
  this->test_void(std::generate<it_t, typeof(generator)>,
                  shad_test_stl::generate_<it_t, typeof(generator)>,
                  shad_test_stl::ordered_checksum<it_t>, generator);
}

// replace
TYPED_TEST(ATF, std_replace) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  this->test_void(std::replace<it_t, val_t>,
                  shad_test_stl::replace_<it_t, val_t>,
                  shad_test_stl::ordered_checksum<it_t>, 42, 43);
}

// replace_if
TYPED_TEST(ATF, std_replace_if) {
  using it_t = typename TypeParam::iterator;
  using val_t = typename TypeParam::value_type;
  uint64_t exp_sum = 0, obs_sum = 0, pos;
  auto pred = [](int x) { return (x % 3 == 0); };
  this->test_void(std::replace_if<it_t, typeof(pred), val_t>,
                  shad_test_stl::replace_if_<it_t, typeof(pred), val_t>,
                  shad_test_stl::ordered_checksum<it_t>, pred, 3);
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

// min_element, max_element, minmax_element
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

// find_if, find_if_not
TYPED_TEST(STF, std_find_if) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // found
  this->test(std::find_if<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::find_if_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // not found
  this->test(std::find_if<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::find_if_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(STF, std_find_if_not) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // not found
  this->test(std::find_if_not<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::find_if_not_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // found
  this->test(std::find_if_not<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::find_if_not_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

// all_of, any_of, none_of
TYPED_TEST(STF, std_all_of) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // true
  this->test(std::all_of<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::all_of_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // false
  this->test(std::all_of<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::all_of_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(STF, std_any_of) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // true
  this->test(std::any_of<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::any_of_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // false
  this->test(std::any_of<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::any_of_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(STF, std_none_of) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // false
  this->test(std::none_of<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::none_of_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // true
  this->test(std::none_of<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::none_of_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

// for_each, for_each_n - todo

// count, count_if
TYPED_TEST(STF, std_count) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // occurring
  this->test(std::count<it_t, value_t>, shad_test_stl::count_<it_t, value_t>,
             0);

  // not occurring
  this->test(std::count<it_t, value_t>, shad_test_stl::count_<it_t, value_t>,
             1);
}

TYPED_TEST(STF, std_count_if) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // all
  this->test(std::count_if<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::count_if_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // none
  this->test(std::count_if<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::count_if_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

// mismatch - todo

// find
TYPED_TEST(STF, std_find) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // occurring
  this->test(std::find<it_t, value_t>, shad_test_stl::find_<it_t, value_t>, 0);

  // not occurring
  this->test(std::find<it_t, value_t>, shad_test_stl::find_<it_t, value_t>, 1);
}

// find_end
TYPED_TEST(STF, std_find_end) {
  using it_t = typeof(this->in->begin());

  // occurring
  auto s = shad_test_stl::subseq_from_<TypeParam>{}(
      this->in, shad_test_stl::kNumElements - shad_test_stl::substr_len,
      shad_test_stl::substr_len);
  this->test(std::find_end<it_t, it_t>, shad_test_stl::find_end_<it_t, it_t>,
             s->begin(), s->end());

  // not occurring
  s = shad_test_stl::create_set_<TypeParam, false>{}(shad_test_stl::substr_len);
  this->test(std::find_end<it_t, it_t>, shad_test_stl::find_end_<it_t, it_t>,
             s->begin(), s->end());
}

// find_first_of
TYPED_TEST(STF, std_find_first_of) {
  using it_t = typeof(this->in->begin());

  // occurring
  auto s = shad_test_stl::subseq_from_<TypeParam>{}(
      this->in, shad_test_stl::kNumElements - shad_test_stl::substr_len,
      shad_test_stl::substr_len);
  this->test(std::find_first_of<it_t, it_t>,
             shad_test_stl::find_first_of_<it_t, it_t>, s->begin(), s->end());

  // not occurring
  s = shad_test_stl::create_set_<TypeParam, false>{}(shad_test_stl::substr_len);
  this->test(std::find_first_of<it_t, it_t>,
             shad_test_stl::find_first_of_<it_t, it_t>, s->begin(), s->end());
}

// adjacent_find
TYPED_TEST(STF, std_adjacent_find) {
  using it_t = typeof(this->in->begin());

  // none
  this->test(std::adjacent_find<it_t>, shad_test_stl::adjacent_find_<it_t>);

  // some - todo
}

// search
TYPED_TEST(STF, std_search) {
  using it_t = typeof(this->in->begin());

  // occurring
  auto s = shad_test_stl::subseq_from_<TypeParam>{}(
      this->in, shad_test_stl::kNumElements - shad_test_stl::substr_len,
      shad_test_stl::substr_len);
  this->test(std::search<it_t, it_t>, shad_test_stl::search_<it_t, it_t>,
             s->begin(), s->end());

  // not occurring
  s = shad_test_stl::create_set_<TypeParam, false>{}(shad_test_stl::substr_len);
  this->test(std::search<it_t, it_t>, shad_test_stl::search_<it_t, it_t>,
             s->begin(), s->end());
}

// search_n - todo

// transform
TYPED_TEST(STF, std_transform) {
  using it_t = typeof(this->in->begin());
  using out_it_t = std::insert_iterator<TypeParam>;
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  this->test_io_inserters(std::transform<it_t, out_it_t, map_f>,
                          shad_test_stl::transform_<it_t, out_it_t, map_f>,
                          shad_test_stl::checksum<it_t>, map_f{});
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

// min_element, max_element, minmax_element
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

// find_if, find_if_not
TYPED_TEST(MTF, std_find_if) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // found
  this->test(std::find_if<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::find_if_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // not found
  this->test(std::find_if<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::find_if_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(MTF, std_find_if_not) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // not found
  this->test(std::find_if_not<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::find_if_not_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // found
  this->test(std::find_if_not<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::find_if_not_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

// all_of, any_of, none_of
TYPED_TEST(MTF, std_all_of) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // true
  this->test(std::all_of<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::all_of_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // false
  this->test(std::all_of<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::all_of_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(MTF, std_any_of) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // true
  this->test(std::any_of<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::any_of_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // false
  this->test(std::any_of<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::any_of_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

TYPED_TEST(MTF, std_none_of) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // false
  this->test(std::none_of<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::none_of_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // true
  this->test(std::none_of<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::none_of_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

// for_each, for_each_n - todo

// count, count_if
TYPED_TEST(MTF, std_count) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // occurring
  this->test(std::count<it_t, value_t>, shad_test_stl::count_<it_t, value_t>,
             std::make_pair(0, 0));

  // not occurring
  this->test(std::count<it_t, value_t>, shad_test_stl::count_<it_t, value_t>,
             std::make_pair(0, 1));
}

TYPED_TEST(MTF, std_count_if) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // all
  this->test(std::count_if<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::count_if_<it_t, shad_test_stl::is_even<value_t>>,
             shad_test_stl::is_even<value_t>{});

  // none
  this->test(std::count_if<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::count_if_<it_t, shad_test_stl::is_odd<value_t>>,
             shad_test_stl::is_odd<value_t>{});
}

// mismatch - todo

// find
TYPED_TEST(MTF, std_find) {
  using it_t = typeof(this->in->begin());
  using value_t = typename TypeParam::value_type;

  // occurring
  this->test(std::find<it_t, value_t>, shad_test_stl::find_<it_t, value_t>,
             std::make_pair(0, 0));

  // not occurring
  this->test(std::find<it_t, value_t>, shad_test_stl::find_<it_t, value_t>,
             std::make_pair(0, 1));
}

// find_end
TYPED_TEST(MTF, std_find_end) {
  using it_t = typeof(this->in->begin());

  // occurring
  auto s = shad_test_stl::subseq_from_<TypeParam>{}(
      this->in, shad_test_stl::kNumElements - shad_test_stl::substr_len,
      shad_test_stl::substr_len);
  this->test(std::find_end<it_t, it_t>, shad_test_stl::find_end_<it_t, it_t>,
             s->begin(), s->end());

  // not occurring
  s = shad_test_stl::create_map_<TypeParam, false>{}(shad_test_stl::substr_len);
  this->test(std::find_end<it_t, it_t>, shad_test_stl::find_end_<it_t, it_t>,
             s->begin(), s->end());
}

// find_first_of
TYPED_TEST(MTF, std_find_first_of) {
  using it_t = typeof(this->in->begin());

  // occurring
  auto s = shad_test_stl::subseq_from_<TypeParam>{}(
      this->in, shad_test_stl::kNumElements - shad_test_stl::substr_len,
      shad_test_stl::substr_len);
  this->test(std::find_first_of<it_t, it_t>,
             shad_test_stl::find_first_of_<it_t, it_t>, s->begin(), s->end());

  // not occurring
  s = shad_test_stl::create_map_<TypeParam, false>{}(shad_test_stl::substr_len);
  this->test(std::find_first_of<it_t, it_t>,
             shad_test_stl::find_first_of_<it_t, it_t>, s->begin(), s->end());
}

// adjacent_find
TYPED_TEST(MTF, std_adjacent_find) {
  using it_t = typeof(this->in->begin());

  // none
  this->test(std::adjacent_find<it_t>, shad_test_stl::adjacent_find_<it_t>);

  // some - todo
}

// search
TYPED_TEST(MTF, std_search) {
  using it_t = typeof(this->in->begin());

  // occurring
  auto s = shad_test_stl::subseq_from_<TypeParam>{}(
      this->in, shad_test_stl::kNumElements - shad_test_stl::substr_len,
      shad_test_stl::substr_len);
  this->test(std::search<it_t, it_t>, shad_test_stl::search_<it_t, it_t>,
             s->begin(), s->end());

  // not occurring
  s = shad_test_stl::create_map_<TypeParam, false>{}(shad_test_stl::substr_len);
  this->test(std::search<it_t, it_t>, shad_test_stl::search_<it_t, it_t>,
             s->begin(), s->end());
}

// search_n - todo

// transform
TYPED_TEST(MTF, std_transform) {
  using it_t = typeof(this->in->begin());
  using out_it_t = std::insert_iterator<TypeParam>;
  using val_t = typename TypeParam::value_type;
  using map_f = std::negate<val_t>;
  this->test_io_inserters(std::transform<it_t, out_it_t, map_f>,
                          shad_test_stl::transform_<it_t, out_it_t, map_f>,
                          shad_test_stl::checksum<it_t>, map_f{});
}
