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
#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {

// Utility functions
namespace impl {
// given two global iterators, it returns a vector with the lengths of each
// local range spanned by the global range
template <typename ForwardIt>
std::vector<typename std::iterator_traits<ForwardIt>::difference_type>
local_range_lenghts(const ForwardIt first, const ForwardIt last) {
  using difference_type =
      typename std::iterator_traits<ForwardIt>::difference_type;
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  rt::Handle h;

  std::vector<difference_type> res;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality)
    res.emplace_back();
  auto it = res.begin();

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++it) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&, const std::tuple<ForwardIt, ForwardIt>& args,
           difference_type* res_ptr) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto local_range = itr_traits::local_range(begin, end);
          *res_ptr = std::distance(local_range.begin(), local_range.end());
        },
        std::make_tuple(first, last), &(*it));
  }

  rt::waitForCompletion(h);
  return res;
}
}  // namespace impl

// Non-modifying sequence operations

namespace impl {

template <typename ForwardItr, typename UnaryPredicate>
bool all_of(distributed_sequential_tag&& policy, ForwardItr first,
            ForwardItr last, UnaryPredicate p) {
  using itr_traits = distributed_iterator_traits<ForwardItr>;
  auto localities = itr_traits::localities(first, last);

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    bool result;

    rt::executeAtWithRet(
        locality,
        [](const std::tuple<ForwardItr, ForwardItr, UnaryPredicate>& args,
           bool* result) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto predicate = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, end);
          *result =
              std::all_of(local_range.begin(), local_range.end(), predicate);
        },
        std::make_tuple(first, last, p), &result);

    if (!result) {
      return false;
    }
  }

  return true;
}

template <typename ForwardItr, typename UnaryPredicate>
bool all_of(distributed_parallel_tag&& policy, ForwardItr first,
            ForwardItr last, UnaryPredicate p) {
  using itr_traits = distributed_iterator_traits<ForwardItr>;
  auto localities = itr_traits::localities(first, last);

  rt::Handle H;

  std::vector<char> results(localities.size());
  size_t i = 0;

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::asyncExecuteAtWithRet(
        H, locality,
        [](rt::Handle&,
           const std::tuple<ForwardItr, ForwardItr, UnaryPredicate>& args,
           char* result) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto predicate = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, end);
          *result =
              std::all_of(local_range.begin(), local_range.end(), predicate)
                  ? 1
                  : 0;
        },
        std::make_tuple(first, last, p), &results[i]);
    ++i;
  }

  rt::waitForCompletion(H);

  return std::all_of(results.begin(), results.end(),
                     [](char v) -> bool { return v == 1; });
}

}  // namespace impl

template <typename ExecutionPolicy, typename ForwardItr,
          typename UnaryPredicate>
bool all_of(ExecutionPolicy&& policy, ForwardItr first, ForwardItr last,
            UnaryPredicate p) {
  return impl::all_of(std::forward<ExecutionPolicy>(policy), first, last, p);
}

namespace impl {

template <typename ForwardItr, typename UnaryPredicate>
bool any_of(distributed_sequential_tag&& policy, ForwardItr first,
            ForwardItr last, UnaryPredicate p) {
  using itr_traits = distributed_iterator_traits<ForwardItr>;
  auto localities = itr_traits::localities(first, last);

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    bool result;

    rt::executeAtWithRet(
        locality,
        [](const std::tuple<ForwardItr, ForwardItr, UnaryPredicate>& args,
           bool* result) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto predicate = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, end);
          *result =
              std::any_of(local_range.begin(), local_range.end(), predicate);
        },
        std::make_tuple(first, last, p), &result);

    if (result) {
      return true;
    }
  }

  return false;
}

template <typename ForwardItr, typename UnaryPredicate>
bool any_of(distributed_parallel_tag&& policy, ForwardItr first,
            ForwardItr last, UnaryPredicate p) {
  using itr_traits = distributed_iterator_traits<ForwardItr>;
  auto localities = itr_traits::localities(first, last);

  rt::Handle H;

  std::vector<char> results(localities.size());
  size_t i = 0;

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::asyncExecuteAtWithRet(
        H, locality,
        [](rt::Handle&,
           const std::tuple<ForwardItr, ForwardItr, UnaryPredicate>& args,
           char* result) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto predicate = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, end);
          *result =
              std::any_of(local_range.begin(), local_range.end(), predicate)
                  ? 1
                  : 0;
        },
        std::make_tuple(first, last, p), &results[i]);
    ++i;
  }

  rt::waitForCompletion(H);

  return std::any_of(results.begin(), results.end(),
                     [](char v) -> bool { return v == 1; });
}

}  // namespace impl

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

namespace impl {

template <typename ForwardItr, typename T>
ForwardItr find(distributed_parallel_tag&& policy, ForwardItr first,
                ForwardItr last, const T& value) {
  using itr_traits = distributed_iterator_traits<ForwardItr>;
  auto localities = itr_traits::localities(first, last);

  rt::Handle H;

  std::vector<ForwardItr> results(localities.size(), last);
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
    ForwardItr result = last;

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
    ForwardItr result = last;

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

  std::vector<ForwardItr> results(localities.size(), last);
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

namespace impl {

template <typename InputItr, typename T>
typename shad::distributed_iterator_traits<InputItr>::difference_type count(
    distributed_parallel_tag&& policy, InputItr first, InputItr last,
    const T& value) {
  using itr_traits = distributed_iterator_traits<InputItr>;
  using difference_type = typename itr_traits::difference_type;
  auto localities = itr_traits::localities(first, last);

  rt::Handle H;

  std::vector<difference_type> results(localities.size());
  size_t i = 0;

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::asyncExecuteAtWithRet(
        H, locality,
        [](rt::Handle&, const std::tuple<InputItr, InputItr, T>& args,
           difference_type* result) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto value = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, end);
          *result = std::count(local_range.begin(), local_range.end(), value);
        },
        std::make_tuple(first, last, value), &results[i]);

    ++i;
  }

  rt::waitForCompletion(H);

  return std::accumulate(results.begin(), results.end(), difference_type(0));
}

template <typename InputItr, typename T>
typename shad::distributed_iterator_traits<InputItr>::difference_type count(
    distributed_sequential_tag&& policy, InputItr first, InputItr last,
    const T& value) {
  using itr_traits = distributed_iterator_traits<InputItr>;
  using difference_type = typename itr_traits::difference_type;
  auto localities = itr_traits::localities(first, last);

  difference_type result = 0;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    difference_type delta = 0;

    rt::executeAtWithRet(
        locality,
        [](const std::tuple<InputItr, InputItr, T>& args,
           difference_type* result) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto value = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, end);
          *result = std::count(local_range.begin(), local_range.end(), value);
        },
        std::make_tuple(first, last, value), &delta);

    result += delta;
  }

  return result;
}

}  // namespace impl

template <typename ExecutionPolicy, typename InputItr, typename T>
typename shad::distributed_iterator_traits<InputItr>::difference_type count(
    ExecutionPolicy&& policy, InputItr first, InputItr last, const T& value) {
  return impl::count(std::forward<ExecutionPolicy>(policy), first, last, value);
}

namespace impl {

template <typename InputItr, typename UnaryPredicate>
typename shad::distributed_iterator_traits<InputItr>::difference_type count_if(
    distributed_parallel_tag&& policy, InputItr first, InputItr last,
    UnaryPredicate predicate) {
  using itr_traits = distributed_iterator_traits<InputItr>;
  using difference_type = typename itr_traits::difference_type;
  auto localities = itr_traits::localities(first, last);

  rt::Handle H;

  std::vector<difference_type> results(localities.size());
  size_t i = 0;

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::asyncExecuteAtWithRet(
        H, locality,
        [](rt::Handle&,
           const std::tuple<InputItr, InputItr, UnaryPredicate>& args,
           difference_type* result) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto predicate = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, end);
          *result =
              std::count_if(local_range.begin(), local_range.end(), predicate);
        },
        std::make_tuple(first, last, predicate), &results[i]);

    ++i;
  }

  rt::waitForCompletion(H);

  return std::accumulate(results.begin(), results.end(), difference_type(0));
}

template <typename InputItr, typename UnaryPredicate>
typename shad::distributed_iterator_traits<InputItr>::difference_type count_if(
    distributed_sequential_tag&& policy, InputItr first, InputItr last,
    UnaryPredicate predicate) {
  using itr_traits = distributed_iterator_traits<InputItr>;
  using difference_type = typename itr_traits::difference_type;
  auto localities = itr_traits::localities(first, last);

  difference_type result = 0;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    difference_type delta = 0;

    rt::executeAtWithRet(
        locality,
        [](const std::tuple<InputItr, InputItr, UnaryPredicate>& args,
           difference_type* result) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto predicate = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, end);
          *result =
              std::count_if(local_range.begin(), local_range.end(), predicate);
        },
        std::make_tuple(first, last, predicate), &delta);

    result += delta;
  }

  return result;
}

}  // namespace impl

template <typename ExecutionPolicy, typename InputItr, typename UnaryPredicate>
typename shad::distributed_iterator_traits<InputItr>::difference_type count_if(
    ExecutionPolicy&& policy, InputItr first, InputItr last, UnaryPredicate p) {
  return impl::count_if(std::forward<ExecutionPolicy>(policy), first, last, p);
}

// Modifying sequence operations

namespace impl {

template <typename ForwardIt, typename T>
void fill(distributed_parallel_tag&& policy, ForwardIt first, ForwardIt last,
          const T& value) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);

  rt::Handle H;

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::asyncExecuteAt(
        H, locality,
        [](rt::Handle&, const std::tuple<ForwardIt, ForwardIt, T>& args) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto value = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, end);
          std::fill(local_range.begin(), local_range.end(), value);
        },
        std::make_tuple(first, last, value));
  }

  rt::waitForCompletion(H);
}

template <typename ForwardIt, typename T>
void fill(distributed_sequential_tag&& policy, ForwardIt first, ForwardIt last,
          const T& value) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAt(locality,
                  [](const std::tuple<ForwardIt, ForwardIt, T>& args) {
                    auto begin = std::get<0>(args);
                    auto end = std::get<1>(args);
                    auto value = std::get<2>(args);

                    auto local_range = itr_traits::local_range(begin, end);
                    std::fill(local_range.begin(), local_range.end(), value);
                  },
                  std::make_tuple(first, last, value));
  }
}

}  // namespace impl

template <class ExecutionPolicy, class ForwardIt, class T>
void fill(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last,
          const T& value) {
  impl::fill(std::forward<ExecutionPolicy>(policy), first, last, value);
}

namespace impl {
namespace generate_impl {
template <typename ForwardIt>
std::pair<std::shared_ptr<uint8_t>, size_t> prepare_buffer(
    const ForwardIt first, const ForwardIt last,
    typename std::iterator_traits<ForwardIt>::difference_type range_len) {
  // prepare the argument buffer
  size_t val_size = sizeof(typename ForwardIt::value_type);
  size_t buf_len = range_len * val_size + 2 * sizeof(ForwardIt);
  std::shared_ptr<uint8_t> sp(new uint8_t[buf_len],
                              std::default_delete<uint8_t[]>());
  auto it_buf_ptr =
      reinterpret_cast<ForwardIt*>(sp.get() + range_len * val_size);
  it_buf_ptr[0] = first;
  it_buf_ptr[1] = last;

  return std::make_pair(sp, buf_len);
}

template <typename ForwardIt>
void kernel(const uint8_t* argsBuffer, const uint32_t buf_len) {
  using T = typename ForwardIt::value_type;
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  const auto value_buf_ptr = reinterpret_cast<const T*>(argsBuffer);
  const auto it_buf_ptr = reinterpret_cast<const ForwardIt*>(
      argsBuffer + buf_len - 2 * sizeof(ForwardIt));
  auto begin = it_buf_ptr[0];
  auto end = it_buf_ptr[1];
  auto local_range = itr_traits::local_range(begin, end);

  std::copy(value_buf_ptr, reinterpret_cast<const T*>(it_buf_ptr),
            local_range.begin());
}
}  // namespace generate_impl

template <typename ForwardIt, typename Generator>
void generate(distributed_parallel_tag&& policy, ForwardIt first,
              ForwardIt last, Generator generator) {
  using T = typename ForwardIt::value_type;
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  using difference_type = typename itr_traits::difference_type;

  auto range_lengths = local_range_lenghts(first, last);
  auto len_it = range_lengths.begin();

  rt::Handle H;
  std::vector<std::shared_ptr<uint8_t>> buf_ptrs;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++len_it) {
    // prepare the arguments
    auto args = generate_impl::prepare_buffer(first, last, *len_it);
    buf_ptrs.push_back(args.first);
    auto value_buf_ptr = reinterpret_cast<T*>(args.first.get());
    for (difference_type i = 0; i < *len_it; ++i)
      value_buf_ptr[i] = generator();

    // execute the remote store
    rt::asyncExecuteAt(
        H, locality,
        [](rt::Handle&, const uint8_t* argsBuffer, const uint32_t buf_len) {
          generate_impl::kernel<ForwardIt>(argsBuffer, buf_len);
        },
        args.first, args.second);
  }

  rt::waitForCompletion(H);
  buf_ptrs.clear();
}

template <typename ForwardIt, typename Generator>
void generate(distributed_sequential_tag&& policy, ForwardIt first,
              ForwardIt last, Generator generator) {
  using T = typename ForwardIt::value_type;
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  using difference_type = typename itr_traits::difference_type;
  difference_type range_len;

  auto range_lengths = local_range_lenghts(first, last);
  auto len_it = range_lengths.begin();

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++len_it) {
    // prepare the arguments
    auto args = generate_impl::prepare_buffer(first, last, *len_it);
    auto value_buf_ptr = reinterpret_cast<T*>(args.first.get());
    for (difference_type i = 0; i < *len_it; ++i)
      value_buf_ptr[i] = generator();

    // execute the remote store
    rt::executeAt(locality, generate_impl::kernel<ForwardIt>, args.first,
                  args.second);
  }
}
}  // namespace impl

template <class ExecutionPolicy, class ForwardIt, class Generator>
void generate(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last,
              Generator g) {
  impl::generate(std::forward<ExecutionPolicy>(policy), first, last, g);
}

namespace impl {

template <typename ForwardIt, typename T>
void replace(distributed_parallel_tag&& policy, ForwardIt first, ForwardIt last,
             const T& old_value, const T& new_value) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);

  rt::Handle H;

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::asyncExecuteAt(
        H, locality,
        [](rt::Handle&, const std::tuple<ForwardIt, ForwardIt, T, T>& args) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto old_value = std::get<2>(args);
          auto new_value = std::get<3>(args);

          auto local_range = itr_traits::local_range(begin, end);
          std::replace(local_range.begin(), local_range.end(), old_value,
                       new_value);
        },
        std::make_tuple(first, last, old_value, new_value));
  }

  rt::waitForCompletion(H);
}

template <typename ForwardIt, typename T>
void replace(distributed_sequential_tag&& policy, ForwardIt first,
             ForwardIt last, const T& old_value, const T& new_value) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAt(locality,
                  [](const std::tuple<ForwardIt, ForwardIt, T, T>& args) {
                    auto begin = std::get<0>(args);
                    auto end = std::get<1>(args);
                    auto old_value = std::get<2>(args);
                    auto new_value = std::get<3>(args);

                    auto local_range = itr_traits::local_range(begin, end);
                    std::replace(local_range.begin(), local_range.end(),
                                 old_value, new_value);
                  },
                  std::make_tuple(first, last, old_value, new_value));
  }
}

}  // namespace impl

template <class ExecutionPolicy, class ForwardIt, class T>
void replace(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last,
             const T& old_value, const T& new_value) {
  impl::replace(std::forward<ExecutionPolicy>(policy), first, last, old_value,
                new_value);
}

namespace impl {

template <typename ForwardIt, typename UnaryPredicate, typename T>
void replace_if(distributed_parallel_tag&& policy, ForwardIt first,
                ForwardIt last, UnaryPredicate p, const T& new_value) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);

  rt::Handle H;

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::asyncExecuteAt(
        H, locality,
        [](rt::Handle&,
           const std::tuple<ForwardIt, ForwardIt, UnaryPredicate, T>& args) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto p = std::get<2>(args);
          auto new_value = std::get<3>(args);

          auto local_range = itr_traits::local_range(begin, end);
          std::replace_if(local_range.begin(), local_range.end(), p, new_value);
        },
        std::make_tuple(first, last, p, new_value));
  }

  rt::waitForCompletion(H);
}

template <typename ForwardIt, typename UnaryPredicate, typename T>
void replace_if(distributed_sequential_tag&& policy, ForwardIt first,
                ForwardIt last, UnaryPredicate p, const T& new_value) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAt(
        locality,
        [](const std::tuple<ForwardIt, ForwardIt, UnaryPredicate, T>& args) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto p = std::get<2>(args);
          auto new_value = std::get<3>(args);

          auto local_range = itr_traits::local_range(begin, end);
          std::replace_if(local_range.begin(), local_range.end(), p, new_value);
        },
        std::make_tuple(first, last, p, new_value));
  }
}

}  // namespace impl

template <class ExecutionPolicy, class ForwardIt, class UnaryPredicate, class T>
void replace_if(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last,
                UnaryPredicate p, const T& new_value) {
  impl::replace_if(std::forward<ExecutionPolicy>(policy), first, last, p,
                   new_value);
}

}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_ALGORITHM_H */
