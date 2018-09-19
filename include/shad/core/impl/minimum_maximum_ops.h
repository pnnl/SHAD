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

#ifndef INCLUDE_SHAD_CORE_IMPL_MINIMUM_MAXIMUM_OPS_H
#define INCLUDE_SHAD_CORE_IMPL_MINIMUM_MAXIMUM_OPS_H

#include <algorithm>
#include <functional>
#include <iterator>
#include <tuple>
#include <type_traits>
#include <vector>

#include "shad/core/execution.h"
#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {
namespace impl {

template <class ForwardIt, class Compare>
ForwardIt max_element(distributed_sequential_tag&& policy, ForwardIt first,
                      ForwardIt last, Compare comp) {
  if (first == last) return last;
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  using value_t =
      typename std::remove_const<typename itr_traits::value_type>::type;
  using res_t = std::pair<ForwardIt, value_t>;
  auto args = std::make_tuple(first, last, comp);
  std::vector<res_t> res(localities.size());
  size_t i = 0;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++i) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<ForwardIt, ForwardIt, Compare>& args,
           res_t* result) {
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto lmax = std::max_element(begin, end, std::get<2>(args));
          ForwardIt gres = itr_traits::iterator_from_local(gbegin, gend, lmax);
          if (gres != gend)
            *result = std::make_pair(gres, *lmax);
          else
            *result = std::make_pair(gres, std::numeric_limits<value_t>::min());
        },
        args, &res[i]);
  }

  auto res_it = std::max_element(
      res.begin(), res.end(),
      [](const res_t& x, const res_t& y) { return x.second < y.second; });
  return res_it->first;
}

template <class ForwardIt, class Compare>
ForwardIt max_element(distributed_parallel_tag&& policy, ForwardIt first,
                      ForwardIt last, Compare comp) {
  if (first == last) return last;
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  using value_t =
      typename std::remove_const<typename itr_traits::value_type>::type;
  using res_t = std::pair<ForwardIt, value_t>;
  std::vector<res_t> res(localities.size());
  size_t i = 0;
  rt::Handle h;
  auto args = std::make_tuple(first, last, comp);
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&, const std::tuple<ForwardIt, ForwardIt, Compare>& args,
           res_t* result) {
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto lmax = std::max_element(begin, end, std::get<2>(args));
          ForwardIt gres = itr_traits::iterator_from_local(gbegin, gend, lmax);
          if (gres != gend)
            *result = std::make_pair(gres, *lmax);
          else
            *result = std::make_pair(gres, std::numeric_limits<value_t>::min());
        },
        args, &res[i]);
  }
  rt::waitForCompletion(h);
  auto res_it = std::max_element(
      res.begin(), res.end(),
      [](const res_t& x, const res_t& y) { return x.second < y.second; });
  return res_it->first;
}

template <class ForwardIt, class Compare>
ForwardIt min_element(distributed_sequential_tag&& policy, ForwardIt first,
                      ForwardIt last, Compare comp) {
  if (first == last) return last;
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  using value_t =
      typename std::remove_const<typename itr_traits::value_type>::type;
  using res_t = std::pair<ForwardIt, value_t>;
  auto args = std::make_tuple(first, last, comp);
  std::vector<res_t> res(localities.size());
  size_t i = 0;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++i) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<ForwardIt, ForwardIt, Compare>& args,
           res_t* result) {
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto lmin = std::min_element(begin, end, std::get<2>(args));
          ForwardIt gres = itr_traits::iterator_from_local(gbegin, gend, lmin);
          if (gres != gend)
            *result = std::make_pair(gres, *lmin);
          else
            *result = std::make_pair(gres, std::numeric_limits<value_t>::max());
        },
        args, &res[i]);
  }
  auto res_it = std::min_element(
      res.begin(), res.end(),
      [](const res_t& x, const res_t& y) { return x.second < y.second; });
  return res_it->first;
}

template <class ForwardIt, class Compare>
ForwardIt min_element(distributed_parallel_tag&& policy, ForwardIt first,
                      ForwardIt last, Compare comp) {
  if (first == last) return last;
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  using value_t =
      typename std::remove_const<typename itr_traits::value_type>::type;
  using res_t = std::pair<ForwardIt, value_t>;
  size_t i = 0;
  rt::Handle h;
  auto args = std::make_tuple(first, last, comp);
  std::vector<res_t> res(localities.size());
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&, const std::tuple<ForwardIt, ForwardIt, Compare>& args,
           res_t* result) {
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto lmin = std::min_element(begin, end, std::get<2>(args));
          ForwardIt gres = itr_traits::iterator_from_local(gbegin, gend, lmin);
          if (gres != gend)
            *result = std::make_pair(gres, *lmin);
          else
            *result = std::make_pair(gres, std::numeric_limits<value_t>::max());
        },
        args, &res[i]);
  }
  rt::waitForCompletion(h);
  auto res_it = std::min_element(
      res.begin(), res.end(),
      [](const res_t& x, const res_t& y) { return x.second < y.second; });
  return res_it->first;
}

template <class ForwardIt, class Compare>
std::pair<ForwardIt, ForwardIt> minmax_element(
    distributed_sequential_tag&& policy, ForwardIt first, ForwardIt last,
    Compare comp) {
  if (first == last) return std::make_pair(last, last);
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  using value_t =
      typename std::remove_const<typename itr_traits::value_type>::type;
  using res_t =
      std::pair<std::pair<value_t, value_t>, std::pair<ForwardIt, ForwardIt>>;
  auto args = std::make_tuple(first, last, comp);
  std::vector<res_t> res(localities.size());
  size_t i = 0;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++i) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<ForwardIt, ForwardIt, Compare>& args,
           res_t* result) {
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto lminmax = std::minmax_element(begin, end, std::get<2>(args));
          ForwardIt minit =
              itr_traits::iterator_from_local(gbegin, gend, lminmax.first);
          ForwardIt maxit =
              itr_traits::iterator_from_local(gbegin, gend, lminmax.second);

          if (minit != gend) {
            *result = std::make_pair(
                std::make_pair(*(lminmax.first), *(lminmax.second)),
                std::make_pair(minit, maxit));
          } else {
            *result = std::make_pair(
                std::make_pair(std::numeric_limits<value_t>::max(),
                               std::numeric_limits<value_t>::min()),
                std::make_pair(minit, maxit));
          }
        },
        args, &res[i]);
  }
  auto res_min = std::min_element(res.begin(), res.end(),
                                  [](const res_t& x, const res_t& y) {
                                    return x.first.first < y.first.first;
                                  });
  auto res_max = std::max_element(res.begin(), res.end(),
                                  [](const res_t& x, const res_t& y) {
                                    return x.first.second < y.first.second;
                                  });
  return std::make_pair(res_min->second.first, res_max->second.second);
}

template <class ForwardIt, class Compare>
std::pair<ForwardIt, ForwardIt> minmax_element(
    distributed_parallel_tag&& policy, ForwardIt first, ForwardIt last,
    Compare comp) {
  if (first == last) return std::make_pair(last, last);
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  using value_t =
      typename std::remove_const<typename itr_traits::value_type>::type;
  using res_t =
      std::pair<std::pair<value_t, value_t>, std::pair<ForwardIt, ForwardIt>>;
  std::vector<res_t> res(localities.size());
  size_t i = 0;
  rt::Handle h;
  auto args = std::make_tuple(first, last, comp);
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&, const std::tuple<ForwardIt, ForwardIt, Compare>& args,
           res_t* result) {
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto lminmax = std::minmax_element(begin, end, std::get<2>(args));
          ForwardIt minit =
              itr_traits::iterator_from_local(gbegin, gend, lminmax.first);
          ForwardIt maxit =
              itr_traits::iterator_from_local(gbegin, gend, lminmax.second);
          if (minit != gend) {
            *result = std::make_pair(
                std::make_pair(*(lminmax.first), *(lminmax.second)),
                std::make_pair(minit, maxit));
          } else {
            *result = std::make_pair(
                std::make_pair(std::numeric_limits<value_t>::max(),
                               std::numeric_limits<value_t>::min()),
                std::make_pair(minit, maxit));
          }
        },
        args, &res[i]);
  }
  rt::waitForCompletion(h);
  auto res_min = std::min_element(res.begin(), res.end(),
                                  [](const res_t& x, const res_t& y) {
                                    return x.first.first < y.first.first;
                                  });
  auto res_max = std::max_element(res.begin(), res.end(),
                                  [](const res_t& x, const res_t& y) {
                                    return x.first.second < y.first.second;
                                  });
  return std::make_pair(res_min->second.first, res_max->second.second);
}

}  // namespace impl
}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_IMPL_MINIMUM_MAXIMUM_OPS_H */
