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

#ifndef INCLUDE_SHAD_CORE_IMPL_IMPL_PATTERNS_H
#define INCLUDE_SHAD_CORE_IMPL_IMPL_PATTERNS_H

#include <cstddef>
#include <iterator>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {
namespace impl {

////////////////////////////////////////////////////////////////////////////////
//
// apply_from is an utility function that performs compile-time unpacking of
// elements [i,N) from a N-ary tuple and invokes a callable on the unpacked
// elements.
//
////////////////////////////////////////////////////////////////////////////////
template <size_t i, size_t N>
struct Apply {
  template <typename F, typename T, typename... A>
  static inline auto apply(F&& f, T&& t, A&&... a) {
    return Apply<i, N - 1>::apply(::std::forward<F>(f), ::std::forward<T>(t),
                                  ::std::get<N - 1>(::std::forward<T>(t)),
                                  ::std::forward<A>(a)...);
  }
};

template <size_t i>
struct Apply<i, i> {
  template <typename F, typename T, typename... A>
  static inline auto apply(F&& f, T&&, A&&... a) {
    return ::std::forward<F>(f)(::std::forward<A>(a)...);
  }
};

template <size_t i, typename F, typename T>
inline auto apply_from(F&& f, T&& t) {
  return Apply<i, ::std::tuple_size<::std::decay_t<T>>::value>::apply(
      ::std::forward<F>(f), ::std::forward<T>(t));
}

////////////////////////////////////////////////////////////////////////////////
//
// distributed_folding_map applies map_kernel sequentially to each local
// portion, forwarding the solution from portion i to portion i + 1.
//
// There is *no* guarantee that map_kernel is not invoked on an empty range.
//
////////////////////////////////////////////////////////////////////////////////
template <typename ForwardIt, typename MapF, typename S, typename... Args>
S distributed_folding_map(ForwardIt first, ForwardIt last, MapF&& map_kernel,
                          const S& init_sol, Args&&... args) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  auto res = init_sol;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    auto d_args = std::make_tuple(map_kernel, first, last, res, args...);
    rt::executeAtWithRet(
        locality,
        [](const typeof(d_args)& d_args, S* result) {
          *result = apply_from<1>(::std::get<0>(d_args),
                                  ::std::forward<typeof(d_args)>(d_args));
        },
        d_args, &res);
  }
  return res;
}

template <typename ForwardIt, typename MapF, typename... Args>
void distributed_folding_map_void(ForwardIt first, ForwardIt last,
                                  MapF&& map_kernel, Args&&... args) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    auto d_args = std::make_tuple(map_kernel, first, last, args...);
    rt::executeAt(
        locality,
        [](const typeof(d_args)& d_args) {
          apply_from<1>(::std::get<0>(d_args),
                        ::std::forward<typeof(d_args)>(d_args));
        },
        d_args);
  }
}

template <typename ForwardIt, typename MapF, typename HaltF, typename S,
          typename... Args>
S distributed_folding_map_early_termination(ForwardIt first, ForwardIt last,
                                            MapF&& map_kernel, HaltF&& halt,
                                            const S& init_sol, Args&&... args) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  auto res = init_sol;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    auto d_args = std::make_tuple(map_kernel, first, last, res, args...);
    rt::executeAtWithRet(
        locality,
        [](const typeof(d_args)& d_args, S* result) {
          *result = apply_from<1>(::std::get<0>(d_args),
                                  ::std::forward<typeof(d_args)>(d_args));
        },
        d_args, &res);
    if (halt(res)) return res;
  }
  return res;
}

////////////////////////////////////////////////////////////////////////////////
//
// distributed_map applies map_kernel to each local portion and returns an
// iterable collection of partial results (one for each locality that owns a
// portion of the input range).
//
// The return type of map_kernel must be DefaultConstructible.
//
////////////////////////////////////////////////////////////////////////////////
template <typename T>
struct optional_vector {
  struct entry_t {
    T value;
    bool valid;
  };
  optional_vector(size_t s) : data(s) {}
  std::vector<entry_t> data;
};

// TODO specialize mapped_t to support lambdas returning bool
template <typename ForwardIt, typename MapF, typename... Args>
std::vector<
    typename std::result_of<MapF&(ForwardIt, ForwardIt, Args&&...)>::type>
distributed_map(ForwardIt first, ForwardIt last, MapF&& map_kernel,
                Args&&... args) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  using mapped_t =
      typename std::result_of<MapF&(ForwardIt, ForwardIt, Args && ...)>::type;
  static_assert(std::is_default_constructible<mapped_t>::value,
                "distributed_map requires DefaultConstructible value type");
  static_assert(
      !std::is_same<mapped_t, bool>::value,
      "distributed-map kernels returning bool are not supported (yet)");
  using opt_mapped_t = typename optional_vector<mapped_t>::entry_t;

  auto localities = itr_traits::localities(first, last);
  size_t i = 0;
  rt::Handle h;
  auto d_args = std::make_tuple(map_kernel, first, last, args...);
  optional_vector<mapped_t> opt_res(localities.size());
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&, const typeof(d_args)& d_args, opt_mapped_t* result) {
          auto first = ::std::get<1>(d_args);
          auto last = ::std::get<2>(d_args);
          auto lrange = itr_traits::local_range(first, last);
          if (lrange.begin() != lrange.end()) {
            result->valid = true;
            result->value = apply_from<1>(
                ::std::get<0>(d_args), ::std::forward<typeof(d_args)>(d_args));
          } else {
            result->valid = false;
          }
        },
        d_args, &opt_res.data[i]);
  }
  rt::waitForCompletion(h);
  std::vector<mapped_t> res;
  for (auto& x : opt_res.data)
    if (x.valid)
      res.push_back(x.value);
  return res;
}

template <typename ForwardIt, typename MapF, typename... Args>
void distributed_map_void(ForwardIt first, ForwardIt last, MapF&& map_kernel,
                          Args&&... args) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  size_t i = 0;
  rt::Handle h;
  auto d_args = std::make_tuple(map_kernel, first, last, args...);
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAt(
        h, locality,
        [](rt::Handle&, const typeof(d_args)& d_args) {
          apply_from<1>(::std::get<0>(d_args),
                        ::std::forward<typeof(d_args)>(d_args));
        },
        d_args);
  }
  rt::waitForCompletion(h);
}

////////////////////////////////////////////////////////////////////////////////
//
// local_map applies map_kernel over a partitioning of a local portion and
// returns an iterable collection of partial results.
//
//  The return type of map_kernel must be DefaultConstructible.
//
////////////////////////////////////////////////////////////////////////////////
// TODO specialize mapped_t to support lambdas returning bool
template <typename ForwardIt, typename MapF>
std::vector<typename std::result_of<MapF&(ForwardIt, ForwardIt)>::type>
local_map(ForwardIt first, ForwardIt last, MapF&& map_kernel) {
  using mapped_t = typename std::result_of<MapF&(ForwardIt, ForwardIt)>::type;
  static_assert(std::is_default_constructible<mapped_t>::value,
                "local_map requires DefaultConstructible value type");
  static_assert(
      !std::is_same<mapped_t, bool>::value,
      "distributed-map kernels returning bool are not supported (yet)");

  // allocate partial results
  auto range_len = std::distance(first, last);
  auto n_blocks = std::min(rt::impl::getConcurrency(), (size_t)range_len);
  std::vector<mapped_t> map_res(n_blocks);

  if (n_blocks) {
    auto block_size = (range_len + n_blocks - 1) / n_blocks;

    rt::Handle map_h;
    for (size_t block_id = 0; block_id < n_blocks; ++block_id) {
      auto map_args = std::make_tuple(block_id, block_size, first, last,
                                      map_kernel, &map_res[block_id]);
      rt::asyncExecuteAt(
          map_h, rt::thisLocality(),
          [](rt::Handle&, const typeof(map_args)& map_args) {
            size_t block_id = std::get<0>(map_args);
            size_t block_size = std::get<1>(map_args);
            auto begin = std::get<2>(map_args);
            auto end = std::get<3>(map_args);
            auto map_kernel = std::get<4>(map_args);
            auto res_unit = std::get<5>(map_args);
            // iteration-block boundaries
            auto block_begin = begin;
            std::advance(block_begin, block_id * block_size);
            auto block_end = block_begin;
            if (std::distance(block_begin, end) < block_size)
              block_end = end;
            else
              std::advance(block_end, block_size);
            // map over the block
            auto m = map_kernel(block_begin, block_end);
            *res_unit = m;
          },
          map_args);
    }
    rt::waitForCompletion(map_h);
  }

  return map_res;
}

template <typename ForwardIt, typename MapF>
void local_map_void(ForwardIt first, ForwardIt last, MapF&& map_kernel) {
  // allocate partial results
  auto range_len = std::distance(first, last);
  auto n_blocks = std::min(rt::impl::getConcurrency(), (size_t)range_len);

  if (n_blocks) {
    auto block_size = (range_len + n_blocks - 1) / n_blocks;

    rt::Handle map_h;
    for (size_t block_id = 0; block_id < n_blocks; ++block_id) {
      auto map_args =
          std::make_tuple(block_id, block_size, first, last, map_kernel);
      rt::asyncExecuteAt(
          map_h, rt::thisLocality(),
          [](rt::Handle&, const typeof(map_args)& map_args) {
            size_t block_id = std::get<0>(map_args);
            size_t block_size = std::get<1>(map_args);
            auto begin = std::get<2>(map_args);
            auto end = std::get<3>(map_args);
            auto map_kernel = std::get<4>(map_args);
            // iteration-block boundaries
            auto block_begin = begin;
            std::advance(block_begin, block_id * block_size);
            auto block_end = block_begin;
            if (std::distance(block_begin, end) < block_size)
              block_end = end;
            else
              std::advance(block_end, block_size);
            // map over the block
            map_kernel(block_begin, block_end);
          },
          map_args);
    }
    rt::waitForCompletion(map_h);
  }
}

}  // namespace impl
}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_IMPL_IMPL_PATTERNS_H */
