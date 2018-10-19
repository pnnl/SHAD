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

#ifndef INCLUDE_SHAD_CORE_IMPL_COMPARISON_OPS_H
#define INCLUDE_SHAD_CORE_IMPL_COMPARISON_OPS_H

#include <algorithm>
#include <functional>
#include <iterator>
#include "shad/core/execution.h"
#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {
namespace impl {

namespace equal_impl {
template <class ForwardIt1, class ForwardIt2, class BinaryOperation>
bool fw_kernel(std::forward_iterator_tag, ForwardIt1 begin1, ForwardIt1 end1,
               ForwardIt2 begin2, BinaryOperation op) {
  using itr_traits1 = distributed_iterator_traits<ForwardIt1>;
  using itr_traits2 = distributed_iterator_traits<ForwardIt2>;

  // lfirst1, llast1: local range for the input1-range portion
  auto local_range = itr_traits1::local_range(begin1, end1);
  auto lfirst1 = local_range.begin();
  auto llast1 = local_range.end();

  // first2, last2: input2-range portion corresponding to the
  // input-range portion
  auto first1 = itr_traits1::iterator_from_local(begin1, end1, lfirst1);
  auto last1 = itr_traits1::iterator_from_local(begin1, end1, llast1);
  auto first2 = begin2;
  std::advance(first2, std::distance(begin1, first1));
  auto last2 = first2;
  std::advance(last2, std::distance(lfirst1, llast1));

  // loc_lfirst2, loc_llast2: local input2-range portion
  auto loc_range2 = itr_traits2::local_range(first2, last2);
  auto loc_lfirst2 = loc_range2.begin();
  auto loc_llast2 = loc_range2.end();

  // loc_first2, loc_last2: global range for the local input2-range
  // portion
  auto loc_first2 =
      itr_traits2::iterator_from_local(first2, last2, loc_lfirst2);
  auto loc_last2 = itr_traits2::iterator_from_local(first2, last2, loc_llast2);

  // process portions
  auto in_it1 = lfirst1, in_it2 = lfirst1;
  auto prefix_len = std::distance(first2, loc_first2);
  std::advance(in_it2, prefix_len);
  if (!std::equal(in_it1, in_it2, first2, op)) return false;

  auto local_len = std::distance(loc_first2, loc_last2);
  in_it1 = in_it2;
  std::advance(in_it2, local_len);
  if (!std::equal(in_it1, in_it2, loc_lfirst2, op)) return false;

  auto suffix_len = std::distance(loc_last2, last2);
  in_it1 = in_it2;
  std::advance(in_it2, suffix_len);
  return std::equal(in_it1, in_it2, loc_last2, op);
}

// distributed_sequential, forward_iterator_tag
template <class ForwardIt1, class ForwardIt2, class BinaryOperation>
bool equal(std::forward_iterator_tag, distributed_sequential_tag&& policy,
           ForwardIt1 begin1, ForwardIt1 end1, ForwardIt2 begin2,
           BinaryOperation op) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  auto localities = itr_traits::localities(begin1, end1);
  bool res = true;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(locality,
                         [](const std::tuple<ForwardIt1, ForwardIt1, ForwardIt2,
                                             BinaryOperation>& args,
                            bool* res_ptr) {
                           *res_ptr =
                               fw_kernel(std::forward_iterator_tag{},
                                         std::get<0>(args), std::get<1>(args),
                                         std::get<2>(args), std::get<3>(args));
                         },
                         std::make_tuple(begin1, end1, begin2, op), &res);
  }
  return res;
}

// distributed_parallel, forward_iterator_tag
template <class ForwardIt1, class ForwardIt2, class BinaryOperation>
bool equal(std::forward_iterator_tag, distributed_parallel_tag&& policy,
           ForwardIt1 begin1, ForwardIt1 end1, ForwardIt2 begin2,
           BinaryOperation op) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  auto localities = itr_traits::localities(begin1, end1);
  bool* partial = (bool*)malloc(localities.size() * sizeof(bool));
  size_t i = 0;
  rt::Handle h;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&,
           const std::tuple<ForwardIt1, ForwardIt1, ForwardIt2,
                            BinaryOperation>& args,
           bool* res_ptr) {
          *res_ptr = fw_kernel(std::forward_iterator_tag{}, std::get<0>(args),
                               std::get<1>(args), std::get<2>(args),
                               std::get<3>(args));
        },
        std::make_tuple(begin1, end1, begin2, op), partial + i);
  }
  rt::waitForCompletion(h);
  bool res = true;
  for (i = 0; i < localities.size(); ++i) {
    if (!partial[i]) {
      res = false;
      break;
    }
  }
  free(partial);
  return res;
}
}  // namespace equal_impl

template <class Policy, class ForwardIt1, class ForwardIt2,
          class BinaryPredicate>
bool equal(Policy&& policy, ForwardIt1 first1, ForwardIt1 last1,
           ForwardIt2 first2, BinaryPredicate p) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  return equal_impl::equal(
      typename std::iterator_traits<ForwardIt2>::iterator_category{},
      std::forward<Policy>(policy), first1, last1, first2, p);
}

namespace lexicographical_compare_impl {
template <class ForwardIt1, class ForwardIt2, class BinaryOperation>
bool fw_kernel(std::forward_iterator_tag, ForwardIt1 begin1, ForwardIt1 end1,
               ForwardIt2 begin2, ForwardIt2 end2, BinaryOperation op) {
  using itr_traits1 = distributed_iterator_traits<ForwardIt1>;
  using itr_traits2 = distributed_iterator_traits<ForwardIt2>;

  // lfirst1, llast1: local range for the input1-range portion
  auto local_range = itr_traits1::local_range(begin1, end1);
  auto lfirst1 = local_range.begin();
  auto llast1 = local_range.end();

  // first2, last2: input2-range portion corresponding to the
  // input-range portion
  auto first1 = itr_traits1::iterator_from_local(begin1, end1, lfirst1);
  auto last1 = itr_traits1::iterator_from_local(begin1, end1, llast1);
  auto first2 = begin2;
  std::advance(first2, std::distance(begin1, first1));
  auto last2 = first2;
  std::advance(last2, std::distance(lfirst1, llast1));

  // TODO optimize by returning last1
  if (first2 > end2) return false;
  if (last2 > end2) last2 = end2;

  // loc_lfirst2, loc_llast2: local input2-range portion
  auto loc_range2 = itr_traits2::local_range(first2, last2);
  auto loc_lfirst2 = loc_range2.begin();
  auto loc_llast2 = loc_range2.end();

  // loc_first2, loc_last2: global range for the local input2-range
  // portion
  auto loc_first2 =
      itr_traits2::iterator_from_local(first2, last2, loc_lfirst2);
  auto loc_last2 = itr_traits2::iterator_from_local(first2, last2, loc_llast2);

  // process portions
  auto in_it1 = lfirst1, in_it2 = lfirst1;
  auto prefix_len = std::distance(first2, loc_first2);
  std::advance(in_it2, prefix_len);
  if (!std::lexicographical_compare(in_it1, in_it2, first2, loc_first2, op))
    return false;

  auto local_len = std::distance(loc_first2, loc_last2);
  in_it1 = in_it2;
  std::advance(in_it2, local_len);
  if (!std::lexicographical_compare(in_it1, in_it2, loc_lfirst2, loc_llast2,
                                    op))
    return false;

  auto suffix_len = std::distance(loc_last2, last2);
  in_it1 = in_it2;
  std::advance(in_it2, suffix_len);
  return std::lexicographical_compare(in_it1, in_it2, loc_last2, last2, op);
}

// distributed_sequential, forward_iterator_tag
template <class ForwardIt1, class ForwardIt2, class BinaryOperation>
bool lexicographical_compare(std::forward_iterator_tag,
                             distributed_sequential_tag&& policy,
                             ForwardIt1 begin1, ForwardIt1 end1,
                             ForwardIt2 begin2, ForwardIt2 end2,
                             BinaryOperation op) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  auto localities = itr_traits::localities(begin1, end1);
  bool res = true;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(locality,
                         [](const std::tuple<ForwardIt1, ForwardIt1, ForwardIt2,
                                             ForwardIt2, BinaryOperation>& args,
                            bool* res_ptr) {
                           *res_ptr = fw_kernel(
                               std::forward_iterator_tag{}, std::get<0>(args),
                               std::get<1>(args), std::get<2>(args),
                               std::get<3>(args), std::get<4>(args));
                         },
                         std::make_tuple(begin1, end1, begin2, end2, op), &res);
  }
  return res;
}

// distributed_parallel, forward_iterator_tag
template <class ForwardIt1, class ForwardIt2, class BinaryOperation>
bool lexicographical_compare(std::forward_iterator_tag,
                             distributed_parallel_tag&& policy,
                             ForwardIt1 begin1, ForwardIt1 end1,
                             ForwardIt2 begin2, ForwardIt2 end2,
                             BinaryOperation op) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  auto localities = itr_traits::localities(begin1, end1);
  bool* partial = (bool*)malloc(localities.size() * sizeof(bool));
  size_t i = 0;
  rt::Handle h;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&,
           const std::tuple<ForwardIt1, ForwardIt1, ForwardIt2, ForwardIt2,
                            BinaryOperation>& args,
           bool* res_ptr) {
          *res_ptr = fw_kernel(std::forward_iterator_tag{}, std::get<0>(args),
                               std::get<1>(args), std::get<2>(args),
                               std::get<3>(args), std::get<4>(args));
        },
        std::make_tuple(begin1, end1, begin2, end2, op), partial + i);
  }
  rt::waitForCompletion(h);
  bool res = true;
  for (i = 0; i < localities.size(); ++i) {
    if (!partial[i]) {
      res = false;
      break;
    }
  }
  free(partial);
  return res;
}
}  // namespace lexicographical_compare_impl

template <class Policy, class ForwardIt1, class ForwardIt2,
          class BinaryPredicate>
bool lexicographical_compare(Policy&& policy, ForwardIt1 first1,
                             ForwardIt1 last1, ForwardIt2 first2,
                             ForwardIt2 last2, BinaryPredicate p) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  return lexicographical_compare_impl::lexicographical_compare(
      typename std::iterator_traits<ForwardIt2>::iterator_category{},
      std::forward<Policy>(policy), first1, last1, first2, last2, p);
}

}  // namespace impl
}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_IMPL_COMPARISON_OPS_H */
