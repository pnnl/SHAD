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
#include <cstring>
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

namespace transform_impl {
template <typename ForwardIt>
struct gen_args_t {
  static constexpr size_t buf_size =
      (2 << 10) / sizeof(typename ForwardIt::value_type);
  typename ForwardIt::value_type buf[buf_size];
  ForwardIt w_first;
  size_t size;
};

template <class ForwardIt1, class ForwardIt2, class UnaryOperation>
ForwardIt2 block_contiguous_kernel(rt::Locality l, ForwardIt1 first,
                                   ForwardIt1 last, ForwardIt2 d_first,
                                   UnaryOperation op) {
  using itr_traits1 = std::iterator_traits<ForwardIt1>;
  using itr_traits2 = distributed_iterator_traits<ForwardIt2>;
  using args_t = gen_args_t<ForwardIt2>;
  auto size = std::distance(first, last);
  auto d_last = d_first;
  std::advance(d_last, size);

  // local assign
  if (rt::thisLocality() == l) {
    auto local_d_range = itr_traits2::local_range(d_first, d_last);
    auto loc_res = std::transform(first, last, local_d_range.begin(), op);
    return itr_traits2::iterator_from_local(d_first, d_last, loc_res - 1) + 1;
  }

  // remote assign
  std::shared_ptr<uint8_t> args_buf(new uint8_t[sizeof(args_t)],
                                    std::default_delete<uint8_t[]>());
  auto typed_args_buf = reinterpret_cast<args_t*>(args_buf.get());
  auto block_last = first;
  rt::Handle h;
  while (first != last) {
    typed_args_buf->w_first = d_first;
    typed_args_buf->size =
        std::min(args_t::buf_size, (size_t)std::distance(first, last));
    std::advance(block_last, typed_args_buf->size);
    std::transform(first, block_last, typed_args_buf->buf, op);
    rt::asyncExecuteAt(
        h, l,
        [](rt::Handle&, const uint8_t* args_buf, const uint32_t) {
          const args_t& args = *reinterpret_cast<const args_t*>(args_buf);
          using val_t = typename ForwardIt2::value_type;
          ForwardIt2 w_last = args.w_first;
          std::advance(w_last, args.size);
          auto w_range = itr_traits2::local_range(args.w_first, w_last);
          std::memcpy(w_range.begin(), args.buf, sizeof(val_t) * args.size);
        },
        args_buf, sizeof(args_t));
    std::advance(first, typed_args_buf->size);
    std::advance(d_first, typed_args_buf->size);
  }
  rt::waitForCompletion(h);
  return d_last;  // todo double check
}

// distributed-sequential kernel for non-block-contiguous output-iterators
template <class ForwardIt1, class ForwardIt2, class UnaryOperation>
void dseq_kernel(std::false_type, ForwardIt1 first, ForwardIt1 last,
                 ForwardIt2 d_first, ForwardIt2* res_ptr, UnaryOperation op) {
  using itr_traits1 = distributed_iterator_traits<ForwardIt1>;
  auto local_range = itr_traits1::local_range(first, last);
  auto begin = local_range.begin();
  auto end = local_range.end();
  *res_ptr = std::transform(begin, end, d_first, op);
}

// distributed-sequential kernel for block-contiguous output-iterators
template <class ForwardIt1, class ForwardIt2, class UnaryOperation>
void dseq_kernel(std::true_type, ForwardIt1 first, ForwardIt1 last,
                 ForwardIt2 d_first, ForwardIt2* res_ptr, UnaryOperation op) {
  using itr_traits1 = distributed_iterator_traits<ForwardIt1>;
  using itr_traits2 = distributed_random_access_iterator_trait<ForwardIt2>;
  auto loc_range = itr_traits1::local_range(first, last);
  auto loc_first = loc_range.begin();
  auto d_last = d_first;
  std::advance(d_last, std::distance(loc_first, loc_range.end()));
  auto dmap = itr_traits2::distribution(d_first, d_last);
  auto loc_last = loc_first;
  for (auto i : dmap) {
    auto l = i.first;
    std::advance(loc_last, i.second);
    d_last = transform_impl::block_contiguous_kernel(l, loc_first, loc_last,
                                                     d_first, op);
    std::advance(loc_first, i.second);
    std::advance(d_first, i.second);
  }
  *res_ptr = d_last;
}

// distributed-parallel kernel for non-block-contiguous output-iterators
template <class ForwardIt1, class ForwardIt2, class UnaryOperation>
void dpar_kernel(std::false_type, ForwardIt1 first, ForwardIt1 last,
                 ForwardIt2 d_first, ForwardIt2* res_ptr, UnaryOperation op) {
  using itr_traits1 = distributed_iterator_traits<ForwardIt1>;
  auto local_range = itr_traits1::local_range(first, last);
  auto begin = local_range.begin();
  auto end = local_range.end();
  auto it = itr_traits1::iterator_from_local(first, last, begin);
  *res_ptr = std::transform(begin, end, d_first, op);
}

// distributed-parallel kernel for block-contiguous output-iterators
template <class ForwardIt1, class ForwardIt2, class UnaryOperation>
void dpar_kernel(std::true_type, ForwardIt1 first, ForwardIt1 last,
                 ForwardIt2 d_first, ForwardIt2* res_ptr, UnaryOperation op) {
  using itr_traits1 = distributed_iterator_traits<ForwardIt1>;
  using itr_traits2 = distributed_random_access_iterator_trait<ForwardIt2>;
  auto loc_range = itr_traits1::local_range(first, last);
  auto loc_first = loc_range.begin();
  auto first_ = itr_traits1::iterator_from_local(first, last, loc_first);
  std::advance(d_first, std::distance(first, first_));
  auto d_last = d_first;
  std::advance(d_last, std::distance(loc_first, loc_range.end()));
  auto dmap = itr_traits2::distribution(d_first, d_last);
  auto loc_last = loc_first;
  for (auto i : dmap) {
    auto l = i.first;
    std::advance(loc_last, i.second);
    d_last = transform_impl::block_contiguous_kernel(l, loc_first, loc_last,
                                                     d_first, op);
    std::advance(loc_first, i.second);
    std::advance(d_first, i.second);
  }
  *res_ptr = d_last;
}

// dispatchers
template <class ForwardIt1, class ForwardIt2, class UnaryOperation>
void dseq_kernel(ForwardIt1 first, ForwardIt1 last, ForwardIt2 d_first,
                 ForwardIt2* res_ptr, UnaryOperation op) {
  dseq_kernel(is_block_contiguous<ForwardIt2>::value, first, last, d_first,
              res_ptr, op);
}

template <class ForwardIt1, class ForwardIt2, class UnaryOperation>
void dpar_kernel(ForwardIt1 first, ForwardIt1 last, ForwardIt2 d_first,
                 ForwardIt2* res_ptr, UnaryOperation op) {
  dpar_kernel(is_block_contiguous<ForwardIt2>::value, first, last, d_first,
              res_ptr, op);
}

}  // namespace transform_impl

template <class ForwardIt1, class ForwardIt2, class UnaryOperation>
ForwardIt2 transform(distributed_parallel_tag&& policy, ForwardIt1 first1,
                     ForwardIt1 last1, ForwardIt2 d_first,
                     UnaryOperation unary_op) {
  using itr_traits1 = distributed_iterator_traits<ForwardIt1>;
  using itr_traits2 = distributed_random_access_iterator_trait<ForwardIt2>;
  auto localities = itr_traits1::localities(first1, last1);
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
          auto first = std::get<0>(args);
          auto last = std::get<1>(args);
          auto d_first = std::get<2>(args);
          auto op = std::get<3>(args);
          transform_impl::dpar_kernel(first, last, d_first, res_ptr, op);
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
  using itr_traits1 = distributed_iterator_traits<ForwardIt1>;
  using itr_traits2 = distributed_random_access_iterator_trait<ForwardIt2>;
  auto localities = itr_traits1::localities(first1, last1);
  ForwardIt2 res = d_first;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<ForwardIt1, ForwardIt1, ForwardIt2, UnaryOperation>&
               args,
           ForwardIt2* res_ptr) {
          auto first = std::get<0>(args);
          auto last = std::get<1>(args);
          auto d_first = std::get<2>(args);
          auto op = std::get<3>(args);
          transform_impl::dseq_kernel(first, last, d_first, res_ptr, op);
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
