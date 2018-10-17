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

namespace transform_impl {
template <class ForwardIt1, class ForwardIt2, class UnaryOperation>
ForwardIt2 fw_kernel(std::forward_iterator_tag, ForwardIt1 begin1,
                     ForwardIt1 end1, ForwardIt2 d_begin, UnaryOperation op) {
  using itr_traits1 = distributed_iterator_traits<ForwardIt1>;
  using itr_traits2 = distributed_iterator_traits<ForwardIt2>;

  // lfirst1, llast1: local range for the input-range portion
  auto local_range = itr_traits1::local_range(begin1, end1);
  auto lfirst1 = local_range.begin();
  auto llast1 = local_range.end();

  // d_first, d_last: output-range portion corresponding to the
  // input-range portion
  auto first1 = itr_traits1::iterator_from_local(begin1, end1, lfirst1);
  auto last1 = itr_traits1::iterator_from_local(begin1, end1, llast1);
  auto d_first = d_begin;
  std::advance(d_first, std::distance(begin1, first1));
  auto d_last = d_first;
  std::advance(d_last, std::distance(lfirst1, llast1));

  // loc_d_lfirst, loc_d_llast: local output-range portion
  auto loc_d_range = itr_traits2::local_range(d_first, d_last);
  auto loc_d_lfirst = loc_d_range.begin();
  auto loc_d_llast = loc_d_range.end();

  // loc_d_first, loc_d_last: global range for the local output-range
  // portion
  auto loc_d_first =
      itr_traits2::iterator_from_local(d_first, d_last, loc_d_lfirst);
  auto loc_d_last =
      itr_traits2::iterator_from_local(d_first, d_last, loc_d_llast);

  // process portions
  auto in_it1 = lfirst1, in_it2 = lfirst1;
  auto prefix_len = std::distance(d_first, loc_d_first);
  std::advance(in_it2, prefix_len);
  std::transform(in_it1, in_it2, d_first, op);

  auto local_len = std::distance(loc_d_first, loc_d_last);
  in_it1 = in_it2;
  std::advance(in_it2, local_len);
  std::transform(in_it1, in_it2, loc_d_lfirst, op);

  auto suffix_len = std::distance(loc_d_last, d_last);
  in_it1 = in_it2;
  std::advance(in_it2, suffix_len);
  return std::transform(in_it1, in_it2, loc_d_last, op);
}

// distributed_sequential, output_iterator_tag
template <class ForwardIt1, class ForwardIt2, class UnaryOperation>
ForwardIt2 transform(std::output_iterator_tag,
                     distributed_sequential_tag&& policy, ForwardIt1 first1,
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
        },
        std::make_tuple(first1, last1, res, unary_op), &res);
  }
  return res;
}

// distributed_sequential, forward_iterator_tag
template <class ForwardIt1, class ForwardIt2, class UnaryOperation>
ForwardIt2 transform(std::forward_iterator_tag,
                     distributed_sequential_tag&& policy, ForwardIt1 begin1,
                     ForwardIt1 end1, ForwardIt2 d_begin,
                     UnaryOperation unary_op) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  auto localities = itr_traits::localities(begin1, end1);
  auto res = d_begin;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<ForwardIt1, ForwardIt1, ForwardIt2, UnaryOperation>&
               args,
           ForwardIt2* res_ptr) {
          *res_ptr = fw_kernel(std::forward_iterator_tag{}, std::get<0>(args),
                               std::get<1>(args), std::get<2>(args),
                               std::get<3>(args));
        },
        std::make_tuple(begin1, end1, d_begin, unary_op), &res);
  }
  return res;
}

// distributed_parallel, output_iterator_tag
template <class ForwardIt, class OutputIt, class UnaryOperation>
OutputIt transform(std::output_iterator_tag, distributed_parallel_tag&& policy,
                   ForwardIt first1, ForwardIt last1, OutputIt d_first,
                   UnaryOperation unary_op) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first1, last1);
  std::vector<OutputIt> res(localities.size(), d_first);
  auto res_it = res.begin();
  rt::Handle h;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++res_it) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&,
           const std::tuple<ForwardIt, ForwardIt, OutputIt, UnaryOperation>&
               args,
           OutputIt* res_ptr) {
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto it = itr_traits::iterator_from_local(gbegin, gend, begin);
          auto d_first_ = std::get<2>(args);
          auto op = std::get<3>(args);
          *res_ptr = std::transform(begin, end, d_first_, op);
        },
        std::make_tuple(first1, last1, d_first, unary_op), &(*res_it));
  }
  rt::waitForCompletion(h);
  return res.back();
}

// distributed_parallel, forward_iterator_tag
template <class ForwardIt1, class ForwardIt2, class UnaryOperation>
ForwardIt2 transform(std::forward_iterator_tag,
                     distributed_parallel_tag&& policy, ForwardIt1 begin1,
                     ForwardIt1 end1, ForwardIt2 d_begin,
                     UnaryOperation unary_op) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  auto localities = itr_traits::localities(begin1, end1);
  std::vector<ForwardIt2> res(localities.size(), d_begin);
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
          *res_ptr = fw_kernel(std::forward_iterator_tag{}, std::get<0>(args),
                               std::get<1>(args), std::get<2>(args),
                               std::get<3>(args));
        },
        std::make_tuple(begin1, end1, d_begin, unary_op), &(*res_it));
  }
  rt::waitForCompletion(h);
  return res.back();
}
}  // namespace transform_impl

template <class ForwardIt1, class OutputIt, class UnaryOperation>
OutputIt transform(distributed_sequential_tag&& policy, ForwardIt1 begin1,
                   ForwardIt1 end1, OutputIt d_begin, UnaryOperation unary_op) {
  return transform_impl::transform(
      typename std::iterator_traits<OutputIt>::iterator_category{},
      std::forward<distributed_sequential_tag>(policy), begin1, end1, d_begin,
      unary_op);
}

template <class ForwardIt1, class OutputIt, class UnaryOperation>
OutputIt transform(distributed_parallel_tag&& policy, ForwardIt1 begin1,
                   ForwardIt1 end1, OutputIt d_begin, UnaryOperation unary_op) {
  return transform_impl::transform(
      typename std::iterator_traits<OutputIt>::iterator_category{},
      std::forward<distributed_parallel_tag>(policy), begin1, end1, d_begin,
      unary_op);
}

template <typename ForwardIt, typename Generator>
void generate(distributed_parallel_tag&& policy, ForwardIt first,
              ForwardIt last, Generator generator) {
  using T = typename ForwardIt::value_type;
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);

  rt::Handle H;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::asyncExecuteAt(
        H, locality,
        [](rt::Handle&,
           const std::tuple<ForwardIt, ForwardIt, Generator>& args) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto generator = std::get<2>(args);
          auto local_range = itr_traits::local_range(begin, end);
          auto lbegin = local_range.begin();
          auto lend = local_range.end();
          std::generate(lbegin, lend, generator);
        },
        std::make_tuple(first, last, generator));
  }

  rt::waitForCompletion(H);
}

template <typename ForwardIt, typename Generator>
void generate(distributed_sequential_tag&& policy, ForwardIt first,
              ForwardIt last, Generator generator) {
  using T = typename ForwardIt::value_type;
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);

  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAt(locality,
                  [](const std::tuple<ForwardIt, ForwardIt, Generator>& args) {
                    auto begin = std::get<0>(args);
                    auto end = std::get<1>(args);
                    auto generator = std::get<2>(args);
                    auto local_range = itr_traits::local_range(begin, end);
                    auto lbegin = local_range.begin();
                    auto lend = local_range.end();
                    std::generate(lbegin, lend, generator);
                  },
                  std::make_tuple(first, last, generator));
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
