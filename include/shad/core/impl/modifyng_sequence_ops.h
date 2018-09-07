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

#ifndef INCLUDE_SHAD_CORE_IMPL_MODIFYING_SEQUENCE_OPS_H
#define INCLUDE_SHAD_CORE_IMPL_MODIFYING_SEQUENCE_OPS_H

#include <algorithm>
#include <functional>
#include <iterator>
#include <tuple>

#include "shad/core/execution.h"
#include "shad/core/impl/utils.h"
#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {
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
}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_IMPL_MODIFYING_SEQUENCE_OPS_H */
