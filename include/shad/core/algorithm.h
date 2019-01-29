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

#ifndef INCLUDE_SHAD_CORE_ALGORITHM_H
#define INCLUDE_SHAD_CORE_ALGORITHM_H

#include <algorithm>
#include <functional>
#include <iterator>
#include <numeric>
#include <tuple>
#include <utility>
#include <vector>

#include "shad/core/execution.h"
#include "shad/core/impl/comparison_ops.h"
#include "shad/core/impl/minimum_maximum_ops.h"
#include "shad/core/impl/modifyng_sequence_ops.h"
#include "shad/core/impl/non_modifyng_sequence_ops.h"
#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {

//  TODO non_modifyng_sequence_ops/for_each_n
//  TODO non_modifyng_sequence_ops/mismatch
//  TODO non_modifyng_sequence_ops/find_end
//  TODO non_modifyng_sequence_ops/find_first_of
//  TODO non_modifyng_sequence_ops/adjacent_find
//  TODO non_modifyng_sequence_ops/search
//  TODO non_modifyng_sequence_ops/search_n

// ---------------------------------------------//
//                                              //
//          non_modifyng_sequence_ops           //
//                                              //
// ---------------------------------------------//

template <typename ExecutionPolicy, typename ForwardItr,
          typename UnaryPredicate>
bool all_of(ExecutionPolicy&& policy, ForwardItr first, ForwardItr last,
            UnaryPredicate p) {
  return impl::all_of(std::forward<ExecutionPolicy>(policy), first, last, p);
}

template <typename ExecutionPolicy, typename ForwardItr,
          typename UnaryPredicate>
bool any_of(ExecutionPolicy&& policy, ForwardItr first, ForwardItr last,
            UnaryPredicate p) {
  return impl::any_of(std::forward<ExecutionPolicy>(policy), first, last, p);
}

template <typename ExecutionPolicy, typename ForwardItr,
          typename UnaryPredicate>
bool none_of(ExecutionPolicy&& policy, ForwardItr first, ForwardItr last,
             UnaryPredicate p) {
  return !any_of(std::forward<ExecutionPolicy>(policy), first, last, p);
}

template <typename ExecutionPolicy, typename ForwardItr, typename T>
ForwardItr find(ExecutionPolicy&& policy, ForwardItr first, ForwardItr last,
                const T& value) {
  return impl::find(std::forward<ExecutionPolicy>(policy), first, last, value);
}

template <typename ExecutionPolicy, typename ForwardItr,
          typename UnaryPredicate>
ForwardItr find_if(ExecutionPolicy&& policy, ForwardItr first, ForwardItr last,
                   UnaryPredicate p) {
  return impl::find_if(std::forward<ExecutionPolicy>(policy), first, last, p);
}

template <typename ExecutionPolicy, typename ForwardItr,
          typename UnaryPredicate>
ForwardItr find_if_not(ExecutionPolicy&& policy, ForwardItr first,
                       ForwardItr last, UnaryPredicate p) {
  return impl::find_if_not(std::forward<ExecutionPolicy>(policy), first, last,
                           p);
}

template <typename ExecutionPolicy, typename ForwardItr,
          typename UnaryPredicate>
void for_each(ExecutionPolicy&& policy, ForwardItr first, ForwardItr last,
              UnaryPredicate p) {
  impl::for_each(std::forward<ExecutionPolicy>(policy), first, last, p);
}

template <typename ExecutionPolicy, typename InputItr, typename T>
typename shad::distributed_iterator_traits<InputItr>::difference_type count(
    ExecutionPolicy&& policy, InputItr first, InputItr last, const T& value) {
  return impl::count(std::forward<ExecutionPolicy>(policy), first, last, value);
}

template <typename ExecutionPolicy, typename InputItr, typename UnaryPredicate>
typename shad::distributed_iterator_traits<InputItr>::difference_type count_if(
    ExecutionPolicy&& policy, InputItr first, InputItr last, UnaryPredicate p) {
  return impl::count_if(std::forward<ExecutionPolicy>(policy), first, last, p);
}

// ---------------------------------------------//
//                                              //
//              minimum_maximum_ops             //
//                                              //
// ---------------------------------------------//

//  ---------------  //
//  | max_element |  //
//  ---------------  //

template <class ForwardIt>
ForwardIt max_element(ForwardIt first, ForwardIt last) {
  return impl::max_element(distributed_sequential_tag{}, first, last,
                           std::greater<>());
}

template <class ExecutionPolicy, class ForwardIt>
std::enable_if_t<shad::is_execution_policy<ExecutionPolicy>::value, ForwardIt>
max_element(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last) {
  return max_element(std::forward<ExecutionPolicy>(policy), first, last,
                     std::less<typename ForwardIt::value_type>());
}

template <class ForwardIt, class Compare>
std::enable_if_t<!shad::is_execution_policy<ForwardIt>::value, ForwardIt>
max_element(ForwardIt first, ForwardIt last, Compare comp) {
  return impl::max_element(distributed_sequential_tag{}, first, last, comp);
}

template <class ExecutionPolicy, class ForwardIt, class Compare>
ForwardIt max_element(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last,
                      Compare comp) {
  return impl::max_element(std::forward<ExecutionPolicy>(policy), first, last,
                           comp);
}

//  ---------------  //
//  | min_element |  //
//  ---------------  //

template <class ForwardIt>
ForwardIt min_element(ForwardIt first, ForwardIt last) {
  return impl::min_element(distributed_sequential_tag{}, first, last,
                           std::less<>());
}

template <class ExecutionPolicy, class ForwardIt>
std::enable_if_t<shad::is_execution_policy<ExecutionPolicy>::value, ForwardIt>
min_element(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last) {
  return min_element(std::forward<ExecutionPolicy>(policy), first, last,
                     std::less<typename ForwardIt::value_type>());
}

template <class ForwardIt, class Compare>
std::enable_if_t<!shad::is_execution_policy<ForwardIt>::value, ForwardIt>
min_element(ForwardIt first, ForwardIt last, Compare comp) {
  return impl::min_element(distributed_sequential_tag{}, first, last, comp);
}

template <class ExecutionPolicy, class ForwardIt, class Compare>
ForwardIt min_element(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last,
                      Compare comp) {
  return impl::min_element(std::forward<ExecutionPolicy>(policy), first, last,
                           comp);
}

//  ------------------  //
//  | minmax_element |  //
//  ------------------  //

template <class ForwardIt>
std::pair<ForwardIt, ForwardIt> minmax_element(ForwardIt first,
                                               ForwardIt last) {
  return impl::minmax_element(distributed_sequential_tag{}, first, last);
}

template <class ExecutionPolicy, class ForwardIt>
std::enable_if_t<shad::is_execution_policy<ExecutionPolicy>::value,
                 std::pair<ForwardIt, ForwardIt>>
minmax_element(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last) {
  return minmax_element(std::forward<ExecutionPolicy>(policy), first, last,
                        std::less<typename ForwardIt::value_type>());
}

template <class ForwardIt, class Compare>
std::enable_if_t<!shad::is_execution_policy<ForwardIt>::value,
                 std::pair<ForwardIt, ForwardIt>>
minmax_element(ForwardIt first, ForwardIt last, Compare comp) {
  return impl::minmax_element(distributed_sequential_tag{}, first, last, comp);
}

template <class ExecutionPolicy, class ForwardIt, class Compare>
std::pair<ForwardIt, ForwardIt> minmax_element(ExecutionPolicy&& policy,
                                               ForwardIt first, ForwardIt last,
                                               Compare comp) {
  return impl::minmax_element(std::forward<ExecutionPolicy>(policy), first,
                              last, comp);
}

// ---------------------------------------------//
//                                              //
//            modifyng_sequence_ops             //
//                                              //
// ---------------------------------------------//

template <class ExecutionPolicy, class ForwardIt, class T>
void fill(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last,
          const T& value) {
  impl::fill(std::forward<ExecutionPolicy>(policy), first, last, value);
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2,
          class UnaryOperation>
ForwardIt2 transform(ExecutionPolicy&& policy, ForwardIt1 first1,
                     ForwardIt1 last1, ForwardIt2 d_first,
                     UnaryOperation unary_op) {
  return impl::transform(std::forward<ExecutionPolicy>(policy), first1, last1,
                         d_first, unary_op);
}

template <class ExecutionPolicy, class ForwardIt, class Generator>
void generate(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last,
              Generator g) {
  impl::generate(std::forward<ExecutionPolicy>(policy), first, last, g);
}

template <class ExecutionPolicy, class ForwardIt, class T>
void replace(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last,
             const T& old_value, const T& new_value) {
  impl::replace(std::forward<ExecutionPolicy>(policy), first, last, old_value,
                new_value);
}

template <class ExecutionPolicy, class ForwardIt, class UnaryPredicate, class T>
void replace_if(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last,
                UnaryPredicate p, const T& new_value) {
  impl::replace_if(std::forward<ExecutionPolicy>(policy), first, last, p,
                   new_value);
}

// ---------------------------------------------//
//                                              //
//                 comparison_ops               //
//                                              //
// ---------------------------------------------//

//  ------------------  //
//  |      equal     |  //
//  ------------------  //

template <class InputIt1, class InputIt2>
bool equal(InputIt1 first1, InputIt1 last1, InputIt2 first2) {
  return impl::equal(distributed_sequential_tag{}, first1, last1, first2,
                     std::equal_to<>());
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2>
std::enable_if_t<shad::is_execution_policy<ExecutionPolicy>::value, bool> equal(
    ExecutionPolicy&& policy, ForwardIt1 first1, ForwardIt1 last1,
    ForwardIt2 first2) {
  return impl::equal(std::forward<ExecutionPolicy>(policy), first1, last1,
                     first2, std::equal_to<>());
}

template <class InputIt1, class InputIt2, class BinaryPredicate>
std::enable_if_t<!std::is_same<InputIt2, BinaryPredicate>::value, bool> equal(
    InputIt1 first1, InputIt1 last1, InputIt2 first2, BinaryPredicate p) {
  return impl::equal(distributed_sequential_tag{}, first1, last1, first2, p);
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2,
          class BinaryPredicate>
std::enable_if_t<(shad::is_execution_policy<ExecutionPolicy>::value &&
                  !std::is_same<ForwardIt2, BinaryPredicate>::value),
                 bool>
equal(ExecutionPolicy&& policy, ForwardIt1 first1, ForwardIt1 last1,
      ForwardIt2 first2, BinaryPredicate p) {
  return impl::equal(std::forward<ExecutionPolicy>(policy), first1, last1,
                     first2, p);
}

template <class InputIt1, class InputIt2>
bool equal(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2) {
  if (std::distance(first1, last1) != std::distance(first2, last2)) {
    return false;
  }
  return impl::equal(distributed_sequential_tag{}, first1, last1, first2,
                     std::equal_to<>());
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2>
std::enable_if_t<shad::is_execution_policy<ExecutionPolicy>::value, bool> equal(
    ExecutionPolicy&& policy, ForwardIt1 first1, ForwardIt1 last1,
    ForwardIt2 first2, ForwardIt2 last2) {
  if (std::distance(first1, last1) != std::distance(first2, last2)) {
    return false;
  }
  return impl::equal(std::forward<ExecutionPolicy>(policy), first1, last1,
                     first2, std::equal_to<>());
}

template <class InputIt1, class InputIt2, class BinaryPredicate>
std::enable_if_t<!std::is_same<InputIt2, BinaryPredicate>::value, bool> equal(
    InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
    BinaryPredicate p) {
  if (std::distance(first1, last1) != std::distance(first2, last2)) {
    return false;
  }
  return impl::equal(distributed_sequential_tag{}, first1, last1, first2, p);
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2,
          class BinaryPredicate>
bool equal(ExecutionPolicy&& policy, ForwardIt1 first1, ForwardIt1 last1,
           ForwardIt2 first2, ForwardIt2 last2, BinaryPredicate p) {
  if (std::distance(first1, last1) != std::distance(first2, last2)) {
    return false;
  }
  return impl::equal(std::forward<ExecutionPolicy>(policy), first1, last1,
                     first2, p);
}

//  -----------------------------  //
//  |  lexicographical_compare  |  //
//  -----------------------------  //

template <class InputIt1, class InputIt2>
bool lexicographical_compare(InputIt1 first1, InputIt1 last1, InputIt2 first2,
                             InputIt2 last2) {
  return impl::lexicographical_compare(distributed_sequential_tag{}, first1,
                                       last1, first2, last2, std::less<>());
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2>
std::enable_if_t<shad::is_execution_policy<ExecutionPolicy>::value, bool>
lexicographical_compare(ExecutionPolicy&& policy, ForwardIt1 first1,
                        ForwardIt1 last1, ForwardIt2 first2, ForwardIt2 last2) {
  return impl::lexicographical_compare(std::forward<ExecutionPolicy>(policy),
                                       first1, last1, first2, last2,
                                       std::less<>());
};

template <class InputIt1, class InputIt2, class Compare>
std::enable_if_t<!shad::is_execution_policy<InputIt1>::value, bool>
lexicographical_compare(InputIt1 first1, InputIt1 last1, InputIt2 first2,
                        InputIt2 last2, Compare comp) {
  return impl::lexicographical_compare(distributed_sequential_tag{}, first1,
                                       last1, first2, last2, comp);
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2,
          class Compare>
bool lexicographical_compare(ExecutionPolicy&& policy, ForwardIt1 first1,
                             ForwardIt1 last1, ForwardIt2 first2,
                             ForwardIt2 last2, Compare comp) {
  return impl::lexicographical_compare(std::forward<ExecutionPolicy>(policy),
                                       first1, last1, first2, last2, comp);
}
}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_ALGORITHM_H */
