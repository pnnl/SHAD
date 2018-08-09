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
#include <iterator>
#include <tuple>
#include <utility>
#include <vector>

#include "shad/core/execution.h"
#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {

namespace impl {

template <typename ForwardItr, typename T>
ForwardItr find(distributed_parallel_tag&& policy, ForwardItr first,
                ForwardItr last, const T& value) {
  using itr_traits = distributed_iterator_traits<ForwardItr>;
  auto localities = itr_traits::localities(first, last);

  rt::Handle H;

  std::vector<ForwardItr> results(localities.size());
  size_t i = 0;

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::asyncExecuteAtWithRet(
        H, locality,
        [](rt::Handle&, const std::tuple<ForwardItr, ForwardItr, T>& args,
           ForwardItr* result) {
          auto begin = std::get<0>(args);
          *result = std::get<1>(args);
          auto value = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, *result);
          auto local_res =
              std::find(local_range.begin(), local_range.end(), value);

          if (local_res != local_range.end()) {
            *result = std::move(itr_traits::iterator_from_local(
                std::get<0>(args), std::get<1>(args), local_res));
          }
        },
        std::make_tuple(first, last, value), &results[i]);
    ++i;
  }

  rt::waitForCompletion(H);

  auto resultPos =
      std::find_if(std::begin(results), std::end(results),
                   [&](const ForwardItr& o) -> bool { return last != o; });
  if (resultPos != results.end()) last = std::move(*resultPos);

  return last;
}

template <typename ForwardItr, typename T>
ForwardItr find(distributed_sequential_tag&& policy, ForwardItr first,
                ForwardItr last, const T& value) {
  using itr_traits = distributed_iterator_traits<ForwardItr>;
  auto localities = itr_traits::localities(first, last);

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    ForwardItr result;

    rt::executeAtWithRet(
        locality,
        [](const std::tuple<ForwardItr, ForwardItr, T>& args,
           ForwardItr* result) {
          auto begin = std::get<0>(args);
          *result = std::get<1>(args);
          auto value = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, *result);
          auto local_res =
              std::find(local_range.begin(), local_range.end(), value);

          if (local_res != local_range.end()) {
            *result = std::move(itr_traits::iterator_from_local(
                std::get<0>(args), std::get<1>(args), local_res));
          }
        },
        std::make_tuple(first, last, value), &result);

    if (result != last) {
      return result;
    }
  }

  return last;
}

}  // namespace impl

template <typename ExecutionPolicy, typename ForwardItr, typename T>
ForwardItr find(ExecutionPolicy&& policy, ForwardItr first, ForwardItr last,
                const T& value) {
  return impl::find(std::forward<ExecutionPolicy>(policy), first, last, value);
}

namespace impl {

template <typename ForwardItr, typename UnaryPredicate>
ForwardItr find_if(distributed_sequential_tag&& policy, ForwardItr first,
                   ForwardItr last, UnaryPredicate p) {
  using itr_traits = distributed_iterator_traits<ForwardItr>;
  auto localities = itr_traits::localities(first, last);

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    ForwardItr result;

    rt::executeAtWithRet(
        locality,
        [](const std::tuple<ForwardItr, ForwardItr, UnaryPredicate>& args,
           ForwardItr* result) {
          auto begin = std::get<0>(args);
          *result = std::get<1>(args);
          auto predicate = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, *result);
          auto local_res =
              std::find_if(local_range.begin(), local_range.end(), predicate);

          if (local_res != local_range.end()) {
            *result = std::move(itr_traits::iterator_from_local(
                std::get<0>(args), std::get<1>(args), local_res));
          }
        },
        std::make_tuple(first, last, p), &result);

    if (result != last) {
      return result;
    }
  }

  return last;
}

template <typename ForwardItr, typename UnaryPredicate>
ForwardItr find_if(distributed_parallel_tag&& policy, ForwardItr first,
                   ForwardItr last, UnaryPredicate p) {
  using itr_traits = distributed_iterator_traits<ForwardItr>;
  auto localities = itr_traits::localities(first, last);

  rt::Handle H;

  std::vector<ForwardItr> results(localities.size());
  size_t i = 0;

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::asyncExecuteAtWithRet(
        H, locality,
        [](rt::Handle&,
           const std::tuple<ForwardItr, ForwardItr, UnaryPredicate>& args,
           ForwardItr* result) {
          auto begin = std::get<0>(args);
          *result = std::get<1>(args);
          auto predicate = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, *result);
          auto local_res =
              std::find_if(local_range.begin(), local_range.end(), predicate);

          if (local_res != local_range.end()) {
            *result = std::move(itr_traits::iterator_from_local(
                std::get<0>(args), std::get<1>(args), local_res));
          }
        },
        std::make_tuple(first, last, p), &results[i]);

    ++i;
  }

  rt::waitForCompletion(H);

  auto resultPos =
      std::find_if(std::begin(results), std::end(results),
                   [&](const ForwardItr& o) -> bool { return last != o; });
  if (resultPos != results.end()) last = std::move(*resultPos);

  return last;
}

}  // namespace impl

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
  return impl::find_if(std::forward<ExecutionPolicy>(policy), first, last,
                       std::unary_negate<UnaryPredicate>(p));
}

namespace impl {

template <typename ForwardItr, typename UnaryPredicate>
void for_each(distributed_sequential_tag&& policy, ForwardItr first,
              ForwardItr last, UnaryPredicate p) {
  using itr_traits = distributed_iterator_traits<ForwardItr>;
  auto localities = itr_traits::localities(first, last);

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAt(
        locality,
        [](const std::tuple<ForwardItr, ForwardItr, UnaryPredicate>& args) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto predicate = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, end);
          std::for_each(local_range.begin(), local_range.end(), predicate);
        },
        std::make_tuple(first, last, p));
  }
}

template <typename ForwardItr, typename UnaryPredicate>
void for_each(distributed_parallel_tag&& policy, ForwardItr first,
              ForwardItr last, UnaryPredicate p) {
  using itr_traits = distributed_iterator_traits<ForwardItr>;
  auto localities = itr_traits::localities(first, last);

  rt::Handle H;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::asyncExecuteAt(
        H, locality,
        [](rt::Handle&,
           const std::tuple<ForwardItr, ForwardItr, UnaryPredicate>& args) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto predicate = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, end);
          std::for_each(local_range.begin(), local_range.end(), predicate);
        },
        std::make_tuple(first, last, p));
  }

  rt::waitForCompletion(H);
}

}  // namespace impl

template <typename ExecutionPolicy, typename ForwardItr,
          typename UnaryPredicate>
void for_each(ExecutionPolicy&& policy, ForwardItr first, ForwardItr last,
              UnaryPredicate p) {
  impl::for_each(std::forward<ExecutionPolicy>(policy), first, last, p);
}

}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_ALGORITHM_H */
