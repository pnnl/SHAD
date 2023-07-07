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

/// @brief applies the folding-map pattern over a distributed range
///
/// Applies an operation sequentially to each sub-range (one for each locality
/// on which the range is physically mapped), forwarding the solution from
/// portion i to portion i + 1.
///
/// @tparam ForwardIt the type of the iterators in the input range
/// @tparam MapF the type of the operation function object
/// @tparam S the type of the solution
/// @tparam Args the type of operation's arguments
///
/// @param[in] first,last the input range
/// @param map_kernel the operation function object that will be applied
/// @param init_sol the initial solution
/// @param args operation's arguments
///
/// @return the last solution
template <typename ForwardIt, typename MapF, typename S, typename... Args>
S distributed_folding_map(ForwardIt first, ForwardIt last, MapF&& map_kernel,
                          const S& init_sol, Args&&... args) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  auto res = init_sol;
#ifdef HAVE_HPX
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    auto d_args = std::make_tuple(shad::rt::lambda_wrapper<std::decay_t<MapF>>(
                                      std::forward<MapF>(map_kernel)),
                                  first, last, res, args...);
    rt::executeAtWithRet(
        locality,
        [](const typeof(d_args)& d_args, S* result) {
          *result = apply_from<1>(::std::get<0>(d_args),
                                  ::std::forward<typeof(d_args)>(d_args));
        },
        d_args, &res);
  }
#else
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
#endif
  return res;
}

// distributed_folding_map variant with void operation
template <typename ForwardIt, typename MapF, typename... Args>
void distributed_folding_map_void(ForwardIt first, ForwardIt last,
                                  MapF&& map_kernel, Args&&... args) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
#ifdef HAVE_HPX
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    auto d_args = std::make_tuple(shad::rt::lambda_wrapper<std::decay_t<MapF>>(
                                      std::forward<MapF>(map_kernel)),
                                  first, last, args...);
    rt::executeAt(
        locality,
        [](const typeof(d_args)& d_args) {
          apply_from<1>(::std::get<0>(d_args),
                        ::std::forward<typeof(d_args)>(d_args));
        },
        d_args);
  }
#else
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
#endif
}

// distributed_folding_map variant testing for early termination
template <typename ForwardIt, typename MapF, typename HaltF, typename S,
          typename... Args>
S distributed_folding_map_early_termination(ForwardIt first, ForwardIt last,
                                            MapF&& map_kernel, HaltF&& halt,
                                            const S& init_sol, Args&&... args) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  auto res = init_sol;
#ifdef HAVE_HPX
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    auto d_args = std::make_tuple(shad::rt::lambda_wrapper<std::decay_t<MapF>>(
                                      std::forward<MapF>(map_kernel)),
                                  first, last, res, args...);
    rt::executeAtWithRet(
        locality,
        [](const typeof(d_args)& d_args, S* result) {
          *result = apply_from<1>(::std::get<0>(d_args),
                                  ::std::forward<typeof(d_args)>(d_args));
        },
        d_args, &res);
    if (halt(res)) return res;
  }
#else
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
#endif

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
  optional_vector(size_t s, const T& init) : data(s, entry_t{init, false}) {}
  std::vector<entry_t> data;
};

/// @brief applies the map pattern over a distributed range
///
/// Applies an operation in parallel to each sub-range (one for each locality on
/// which the range is physically mapped) and returns the collection of mapped
/// values (one for each sub-range).
///
/// @tparam ForwardIt the type of the iterators in the input range
/// @tparam MapF the type of the operation function object
/// @tparam Args the type of operation's arguments
///
/// @param[in] first,last the input range
/// @param map_kernel the operation function object that will be applied
/// @param init the initial mapped value
/// @param args operation's arguments
///
/// @return the collection of mapped values
///
/// @todo support operations returning bool
template <typename ForwardIt, typename MapF, typename... Args>
std::vector<
    typename std::result_of<MapF&(ForwardIt, ForwardIt, Args&&...)>::type>
distributed_map_init(
    ForwardIt first, ForwardIt last, MapF&& map_kernel,
    const typename std::result_of<MapF&(ForwardIt, ForwardIt, Args&&...)>::type&
        init,
    Args&&... args) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  using mapped_t =
      typename std::result_of<MapF&(ForwardIt, ForwardIt, Args && ...)>::type;
  static_assert(
      !std::is_same<mapped_t, bool>::value,
      "distributed-map kernels returning bool are not supported (yet)");
  using opt_mapped_t = typename optional_vector<mapped_t>::entry_t;

  auto localities = itr_traits::localities(first, last);
  size_t i = 0;
  rt::Handle h;
#ifdef HAVE_HPX
  auto d_args = std::make_tuple(shad::rt::lambda_wrapper<std::decay_t<MapF>>(
                                    std::forward<MapF>(map_kernel)),
                                first, last, args...);
#else
  auto d_args = std::make_tuple(map_kernel, first, last, args...);
#endif 
  optional_vector<mapped_t> opt_res(localities.size(), init);
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
    if (x.valid) res.push_back(x.value);
  return res;
}

// distributed_map_init variant with default-constructed initial value
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
  return distributed_map_init(first, last, map_kernel, mapped_t{},
                              std::forward<Args>(args)...);
}

// distributed_map_init variant with void operation
template <typename ForwardIt, typename MapF, typename... Args>
void distributed_map_void(ForwardIt first, ForwardIt last, MapF&& map_kernel,
                          Args&&... args) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  size_t i = 0;
  rt::Handle h;
#ifdef HAVE_HPX
  auto d_args = std::make_tuple(shad::rt::lambda_wrapper<std::decay_t<MapF>>(
                                    std::forward<MapF>(map_kernel)),
                                first, last, args...);
#else
  auto d_args = std::make_tuple(map_kernel, first, last, args...);
#endif
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

/// @brief applies the map pattern over a local range
///
/// Applies an operation in parallel to each partition of a local range and
/// returns the collection of mapped values (one for each partition).
///
/// @tparam ForwardIt the type of the iterators in the input range
/// @tparam MapF the type of the operation function object
///
/// @param[in] first,last the input range
/// @param map_kernel the operation function object that will be applied
/// @param init the initial mapped value
///
/// @return the collection of mapped values
///
/// @todo support operations returning bool
template <typename ForwardIt, typename MapF>
std::vector<typename std::result_of<MapF&(ForwardIt, ForwardIt)>::type>
local_map_init(
    ForwardIt first, ForwardIt last, MapF&& map_kernel,
    const typename std::result_of<MapF&(ForwardIt, ForwardIt)>::type& init) {
  using mapped_t = typename std::result_of<MapF&(ForwardIt, ForwardIt)>::type;
  static_assert(
      !std::is_same<mapped_t, bool>::value,
      "distributed-map kernels returning bool are not supported (yet)");

  // allocate partial results
  auto parts = local_iterator_traits<ForwardIt>::partitions(
      first, last, rt::impl::getConcurrency());

  std::vector<mapped_t> map_res(parts.size(), init);

  if (parts.size()) {
#ifdef HAVE_HPX
    auto map_args =
        std::make_tuple(parts.data(),
                        shad::rt::lambda_wrapper<std::decay_t<MapF>>(
                            std::forward<MapF>(map_kernel)),
                        map_res.data());
#else
    auto map_args = std::make_tuple(parts.data(), map_kernel, map_res.data());
#endif
    shad::rt::forEachAt(
        rt::thisLocality(),
        [](const typeof(map_args)& map_args, size_t iter) {
          auto pfirst = std::get<0>(map_args) + iter;
          auto map_kernel = std::get<1>(map_args);
          auto res_unit = std::get<2>(map_args) + iter;
          // map over the partition
          assert(pfirst->begin() != pfirst->end());
          *res_unit = map_kernel(pfirst->begin(), pfirst->end());
        },
        map_args, parts.size());
  }

  return map_res;
}

// local_map_init variant with default-constructed initial value
template <typename ForwardIt, typename MapF>
std::vector<typename std::result_of<MapF&(ForwardIt, ForwardIt)>::type>
local_map(ForwardIt first, ForwardIt last, MapF&& map_kernel) {
  using mapped_t = typename std::result_of<MapF&(ForwardIt, ForwardIt)>::type;
  static_assert(std::is_default_constructible<mapped_t>::value,
                "local_map requires DefaultConstructible value type");
  static_assert(
      !std::is_same<mapped_t, bool>::value,
      "distributed-map kernels returning bool are not supported (yet)");

  return local_map_init(first, last, map_kernel, mapped_t{});
}

// local_map_init variant with void operation
template <typename ForwardIt, typename MapF>
void local_map_void(ForwardIt first, ForwardIt last, MapF&& map_kernel) {
  auto parts = local_iterator_traits<ForwardIt>::partitions(
      first, last, rt::impl::getConcurrency());

  if (parts.size()) {
#ifdef HAVE_HPX
    auto map_args = std::make_tuple(
        parts.data(), shad::rt::lambda_wrapper<std::decay_t<MapF>>(
                          std::forward<MapF>(map_kernel)));
#else
    auto map_args = std::make_tuple(parts.data(), map_kernel);
#endif
    shad::rt::forEachAt(
        rt::thisLocality(),
        [](const typeof(map_args)& map_args, size_t iter) {
          auto pfirst = std::get<0>(map_args) + iter;
          auto map_kernel = std::get<1>(map_args);
          // map over the partition
          assert(pfirst->begin() != pfirst->end());
          map_kernel(pfirst->begin(), pfirst->end());
        },
        map_args, parts.size());
  }
}

// local_map_init variant with a void operation that takes in input the offset
// of the processed partition with respect to the input range
template <typename ForwardIt, typename MapF>
void local_map_void_offset(ForwardIt first, ForwardIt last, MapF&& map_kernel) {
  auto parts = local_iterator_traits<ForwardIt>::partitions(
      first, last, rt::impl::getConcurrency());

  if (parts.size()) {
#ifdef HAVE_HPX
    auto map_args =
        std::make_tuple(parts.data(),
                        shad::rt::lambda_wrapper<std::decay_t<MapF>>(
                            std::forward<MapF>(map_kernel)),
                        first);
#else
    auto map_args = std::make_tuple(parts.data(), map_kernel, first);
#endif
    shad::rt::forEachAt(
        rt::thisLocality(),
        [](const typeof(map_args)& map_args, size_t iter) {
          auto pfirst = std::get<0>(map_args) + iter;
          auto map_kernel = std::get<1>(map_args);
          auto first = std::get<2>(map_args);

          // map over the partition
          assert(pfirst->begin() != pfirst->end());
          map_kernel(pfirst->begin(), pfirst->end(),
                     std::distance(first, pfirst->begin()));
        },
        map_args, parts.size());
  }
}

}  // namespace impl
}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_IMPL_IMPL_PATTERNS_H */
