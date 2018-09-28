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

template <class ForwardIt1, class ForwardIt2, class BinaryPredicate>
bool equal(distributed_sequential_tag&&, ForwardIt1 first1, ForwardIt1 last1,
           ForwardIt2 first2, BinaryPredicate p) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  auto localities = itr_traits::localities(first1, last1);
  auto res = std::make_pair(first2, true);
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<ForwardIt1, ForwardIt1, ForwardIt2,
                            BinaryPredicate>& args,
           std::pair<ForwardIt2, bool>* result) {
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto lfirst1 = local_range.begin();
          auto llast1 = local_range.end();
          auto first2 = std::get<2>(args);
          if (lfirst1 == llast1) {
            *result = std::make_pair(first2, true);
            return;
          }
          auto op = std::get<3>(args);
          for (; lfirst1 != llast1; ++lfirst1, ++first2) {
            if (!op(*lfirst1, *first2)) {
              *result = std::make_pair(first2, false);
              return;
            }
          }
          *result = std::make_pair(first2, true);
        },
        std::make_tuple(first1, last1, res.first, p), &res);
    if (!res.second) return false;
  }
  return true;
}

template <class ForwardIt1, class ForwardIt2, class BinaryPredicate>
bool equal(distributed_parallel_tag&&, ForwardIt1 first1, ForwardIt1 last1,
           ForwardIt2 first2, BinaryPredicate p) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  auto localities = itr_traits::localities(first1, last1);
  auto ls = localities.size();
  bool res[ls];
  uint32_t i = 0;
  rt::Handle h;
  auto args = std::make_tuple(first1, last1, first2, p);
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&,
           const std::tuple<ForwardIt1, ForwardIt1, ForwardIt2,
                            BinaryPredicate>& args, bool* result) {
          auto gfirst = std::get<0>(args);
          auto glast = std::get<1>(args);
          auto local_range = itr_traits::local_range(gfirst, glast);
          auto lfirst1 = local_range.begin();
          auto llast1 = local_range.end();
          if (lfirst1 == llast1) {
            *result = true;
            return;
          }
          auto first2 = std::get<2>(args);
          auto it = itr_traits::iterator_from_local(gfirst, glast, lfirst1);
          first2 += (std::distance(gfirst, it));
          auto op = std::get<3>(args);
          for (; lfirst1 != llast1; ++lfirst1, ++first2) {
            if (!op(*lfirst1, *first2)) {
              *result = false;
              return;
            }
          }
          *result = true;
        },
        args, &res[i]);
  }
  rt::waitForCompletion(h);
  std::all_of(&res[0], &res[ls], [](bool v) -> bool { return v; });
  return true;
}

template <class ForwardIt1, class ForwardIt2, class BinaryPredicate>
bool lexicographical_compare(distributed_sequential_tag&&,
                             ForwardIt1 first1, ForwardIt1 last1,
                             ForwardIt2 first2, ForwardIt2 last2,
                             BinaryPredicate p) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  auto localities = itr_traits::localities(first1, last1);
  auto res = std::make_pair(first2, true);
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<ForwardIt1, ForwardIt1,
                            ForwardIt2, ForwardIt2,
                            BinaryPredicate>& args,
           std::pair<ForwardIt2, bool>* result) {
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto lfirst1 = local_range.begin();
          auto llast1 = local_range.end();
          auto first2 = std::get<2>(args);
          auto last2 = std::get<3>(args);
          if (lfirst1 == llast1) {
            bool ret = (first2 != last2);
            *result = std::make_pair(first2, ret);
            return;
          }
          auto op = std::get<4>(args);
          for (; (lfirst1 != llast1) && (first2 != last2);
               ++lfirst1, ++first2) {
            if (op(*lfirst1, *first2)) {
              *result = std::make_pair(first2, true);
              return;
            }
            if (op(*first2, *lfirst1)) {
              *result = std::make_pair(first2, false);
              return;
            }
          }
          bool ret = (first2 != last2);
          *result = std::make_pair(first2, ret);
        },
        std::make_tuple(first1, last1, res.first, last2,  p), &res);
    if (!res.second) return false;
  }
  return true;
}

template <class ForwardIt1, class ForwardIt2, class BinaryPredicate>
bool lexicographical_compare(distributed_parallel_tag&&,
                             ForwardIt1 first1, ForwardIt1 last1,
                             ForwardIt2 first2, ForwardIt2 last2,
                             BinaryPredicate p) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  auto localities = itr_traits::localities(first1, last1);
  auto ls = localities.size();
  bool res[ls];
  uint32_t i = 0;
  rt::Handle h;
  auto args = std::make_tuple(first1, last1, first2, last2, p);
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&,
           const std::tuple<ForwardIt1, ForwardIt1, ForwardIt2,
                            ForwardIt2, BinaryPredicate>& args,
           bool* result) {
          auto gfirst = std::get<0>(args);
          auto glast = std::get<1>(args);
          auto local_range = itr_traits::local_range(gfirst, glast);
          auto lfirst1 = local_range.begin();
          auto llast1 = local_range.end();
          auto first2 = std::get<2>(args);
          auto last2 = std::get<3>(args);
          auto it = itr_traits::iterator_from_local(gfirst, glast, lfirst1);
          std::advance(first2, std::distance(gfirst, it));
          if (lfirst1 == llast1) {
            *result = (first2 != last2);
            return;
          }
          auto op = std::get<4>(args);
          for (; (lfirst1 != llast1) && (first2 != last2);
               ++lfirst1, ++first2) {
            if (op(*lfirst1, *first2)) {
              *result = true;
              return;
            }
            if (op(*first2, *lfirst1)) {
              *result = false;
              return;
            }
          }
          *result = (first2 != last2);
        },
        args, &res[i]);
  }
  rt::waitForCompletion(h);
  std::all_of(&res[0], &res[ls], [](bool v) -> bool { return v; });
  return true;
}

}  // namespace impl
}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_IMPL_COMPARISON_OPS_H */
