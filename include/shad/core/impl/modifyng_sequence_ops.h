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

  // distributed map
  distributed_map_void(
      // range
      first, last,
      // kernel
      [](ForwardIt first, ForwardIt last, const T& value) {
        using local_iterator_t = typename itr_traits::local_iterator_type;

        // local map
        auto lrange = itr_traits::local_range(first, last);

        local_map_void(
            // range
            lrange.begin(), lrange.end(),
            // kernel
            [&](local_iterator_t b, local_iterator_t e) {
              std::fill(b, e, value);
            });
      },
      // map arguments
      value);
}

template <typename ForwardIt, typename T>
void fill(distributed_sequential_tag&& policy, ForwardIt first, ForwardIt last,
          const T& value) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;

  distributed_folding_map_void(
      // range
      first, last,
      // kernel
      [](ForwardIt first, ForwardIt last, const T& value) {
        // local processing
        auto lrange = itr_traits::local_range(first, last);
        std::fill(lrange.begin(), lrange.end(), value);
      },
      // map arguments
      value);
}

template <class ForwardIt1, class ForwardIt2, class UnaryOperation>
ForwardIt2 transform(distributed_parallel_tag&& policy, ForwardIt1 first1,
                     ForwardIt1 last1, ForwardIt2 d_first,
                     UnaryOperation unary_op) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  auto localities = itr_traits::localities(first1, last1);
  std::vector<ForwardIt2> res(localities.size(), d_first);
  auto res_it = res.begin();
  rt::Handle h;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++res_it) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&,
           const std::tuple<ForwardIt1, ForwardIt1, ForwardIt2, UnaryOperation>&
               args,
           ForwardIt2* res_ptr) {
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto it = itr_traits::iterator_from_local(gbegin, gend, begin);
          auto d_first_ = std::get<2>(args);
          advance_output_iterator(d_first_, gbegin, it);
          auto op = std::get<3>(args);
          *res_ptr = std::transform(begin, end, d_first_, op);
          flush_iterator(*res_ptr);
        },
        std::make_tuple(first1, last1, d_first, unary_op), &(*res_it));
  }
  rt::waitForCompletion(h);
  return res.back();
}

template <class ForwardIt1, class ForwardIt2, class UnaryOperation>
ForwardIt2 transform(distributed_sequential_tag&& policy, ForwardIt1 first1,
                     ForwardIt1 last1, ForwardIt2 d_first,
                     UnaryOperation unary_op) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  auto localities = itr_traits::localities(first1, last1);
  ForwardIt2 res = d_first;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<ForwardIt1, ForwardIt1, ForwardIt2, UnaryOperation>&
               args,
           ForwardIt2* res_ptr) {
          auto d_first_ = std::get<2>(args);
          auto op = std::get<3>(args);
          auto local_range =
              itr_traits::local_range(std::get<0>(args), std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          *res_ptr = std::transform(begin, end, d_first_, op);
          flush_iterator(*res_ptr);
        },
        std::make_tuple(first1, last1, res, unary_op), &res);
  }
  return res;
}

template <typename ForwardIt, typename Generator>
void generate(distributed_parallel_tag&& policy, ForwardIt first,
              ForwardIt last, Generator generator) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;

  // distributed map
  distributed_map_void(
      // range
      first, last,
      // kernel
      [](ForwardIt first, ForwardIt last, Generator generator) {
        using local_iterator_t = typename itr_traits::local_iterator_type;

        // local map
        auto lrange = itr_traits::local_range(first, last);

        local_map_void(
            // range
            lrange.begin(), lrange.end(),
            // kernel
            [&](local_iterator_t b, local_iterator_t e) {
              std::generate(b, e, generator);
            });
      },
      // map arguments
      generator);
}

template <typename ForwardIt, typename Generator>
void generate(distributed_sequential_tag&& policy, ForwardIt first,
              ForwardIt last, Generator generator) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;

  distributed_folding_map_void(
      // range
      first, last,
      // kernel
      [](ForwardIt first, ForwardIt last, Generator generator) {
        // local processing
        auto lrange = itr_traits::local_range(first, last);
        std::generate(lrange.begin(), lrange.end(), generator);
      },
      // map arguments
      generator);
}

template <typename ForwardIt, typename T>
void replace(distributed_parallel_tag&& policy, ForwardIt first, ForwardIt last,
             const T& old_value, const T& new_value) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;

  // distributed map
  distributed_map_void(
      // range
      first, last,
      // kernel
      [](ForwardIt first, ForwardIt last, const T& old_value,
         const T& new_value) {
        using local_iterator_t = typename itr_traits::local_iterator_type;

        // local map
        auto lrange = itr_traits::local_range(first, last);

        local_map_void(
            // range
            lrange.begin(), lrange.end(),
            // kernel
            [&](local_iterator_t b, local_iterator_t e) {
              std::replace(b, e, old_value, new_value);
            });
      },
      // map arguments
      old_value, new_value);
}

template <typename ForwardIt, typename T>
void replace(distributed_sequential_tag&& policy, ForwardIt first,
             ForwardIt last, const T& old_value, const T& new_value) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;

  distributed_folding_map_void(
      // range
      first, last,
      // kernel
      [](ForwardIt first, ForwardIt last, const T& old_value,
         const T& new_value) {
        // local processing
        auto lrange = itr_traits::local_range(first, last);
        std::replace(lrange.begin(), lrange.end(), old_value, new_value);
      },
      // map arguments
      old_value, new_value);
}

template <typename ForwardIt, typename UnaryPredicate, typename T>
void replace_if(distributed_parallel_tag&& policy, ForwardIt first,
                ForwardIt last, UnaryPredicate p, const T& new_value) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;

  // distributed map
  distributed_map_void(
      // range
      first, last,
      // kernel
      [](ForwardIt first, ForwardIt last, UnaryPredicate p,
         const T& new_value) {
        using local_iterator_t = typename itr_traits::local_iterator_type;

        // local map
        auto lrange = itr_traits::local_range(first, last);

        local_map_void(
            // range
            lrange.begin(), lrange.end(),
            // kernel
            [&](local_iterator_t b, local_iterator_t e) {
              std::replace_if(b, e, p, new_value);
            });
      },
      // map arguments
      p, new_value);
}

template <typename ForwardIt, typename UnaryPredicate, typename T>
void replace_if(distributed_sequential_tag&& policy, ForwardIt first,
                ForwardIt last, UnaryPredicate p, const T& new_value) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;

  distributed_folding_map_void(
      // range
      first, last,
      // kernel
      [](ForwardIt first, ForwardIt last, UnaryPredicate p,
         const T& new_value) {
        // local processing
        auto lrange = itr_traits::local_range(first, last);
        std::replace_if(lrange.begin(), lrange.end(), p, new_value);
      },
      // map arguments
      p, new_value);
}

}  // namespace impl
}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_IMPL_MODIFYING_SEQUENCE_OPS_H */
