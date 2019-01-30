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

#include "impl_patterns.h"

namespace shad {
namespace impl {

// todo drop DefaultConstructible requirement
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
            *result = std::make_pair(gres, value_t{});
        },
        args, &res[i]);
  }

  auto res_it = std::max_element(
      res.begin(), res.end(), [&](const res_t& x, const res_t& y) {
        return y.first != last && comp(x.second, y.second);
      });
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
          using local_iterator_t = typename itr_traits::local_iterator_type;
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto op = std::get<2>(args);

          // map
          auto nil_val = itr_traits::local_range(gbegin, gend).end();
          auto map_res = local_map(
              gbegin, gend,
              // map kernel
              [&](local_iterator_t begin, local_iterator_t end) {
                return std::max_element(begin, end, op);
              },
              nil_val);

          // reduce
          auto lmax = *std::max_element(
              map_res.begin(), map_res.end(),
              [&](const local_iterator_t& x, const local_iterator_t& y) {
                return y != nil_val && op(*x, *y);
              });
          ForwardIt gres = itr_traits::iterator_from_local(gbegin, gend, lmax);

          // local solution
          *result = std::make_pair(gres, lmax != nil_val ? *lmax : value_t{});
        },
        args, &res[i]);
  }
  rt::waitForCompletion(h);
  auto res_it = std::max_element(
      res.begin(), res.end(), [&](const res_t& x, const res_t& y) {
        return y.first != last && comp(x.second, y.second);
      });
  return res_it->first;
}

// todo drop DefaultConstructible requirement
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
            *result = std::make_pair(gres, value_t{});
        },
        args, &res[i]);
  }
  auto res_it = std::min_element(
      res.begin(), res.end(), [&](const res_t& x, const res_t& y) {
        return x.first != last && comp(x.second, y.second);
      });
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
          using local_iterator_t = typename itr_traits::local_iterator_type;
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto op = std::get<2>(args);

          // map
          auto nil_val = itr_traits::local_range(gbegin, gend).end();
          auto map_res = local_map(
              gbegin, gend,
              // map kernel
              [&](local_iterator_t begin, local_iterator_t end) {
                return std::min_element(begin, end, op);
              },
              nil_val);

          // reduce
          auto lmin = *std::min_element(
              map_res.begin(), map_res.end(),
              [&](const local_iterator_t& x, const local_iterator_t& y) {
                return x != nil_val && op(*x, *y);
              });
          ForwardIt gres = itr_traits::iterator_from_local(gbegin, gend, lmin);

          // local solution
          *result = std::make_pair(gres, lmin != nil_val ? *lmin : value_t{});
        },
        args, &res[i]);
  }
  rt::waitForCompletion(h);
  auto res_it = std::min_element(
      res.begin(), res.end(), [&](const res_t& x, const res_t& y) {
        return x.first != last && comp(x.second, y.second);
      });
  return res_it->first;
}

// todo drop DefaultConstructible requirement
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
            *result = std::make_pair(std::make_pair(value_t{}, value_t{}),
                                     std::make_pair(minit, maxit));
          }
        },
        args, &res[i]);
  }
  auto res_min = std::min_element(
      res.begin(), res.end(), [&](const res_t& x, const res_t& y) {
        return x.second.first != last && comp(x.first.first, y.first.first);
      });
  auto res_max = std::max_element(
      res.begin(), res.end(), [&](const res_t& x, const res_t& y) {
        return y.second.first != last && comp(x.first.second, y.first.second);
      });
  return std::make_pair(res_min->second.first, res_max->second.second);
}

// todo drop DefaultConstructible requirement
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
          using local_iterator_t = typename itr_traits::local_iterator_type;
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto op = std::get<2>(args);

          // map
          auto nil_val = itr_traits::local_range(gbegin, gend).end();
          auto map_res = local_map(
              gbegin, gend,
              // map kernel
              [&](local_iterator_t begin, local_iterator_t end) {
                return std::minmax_element(begin, end, op);
              },
              std::make_pair(nil_val, nil_val));

          // reduce
          auto map_res_min =
              std::min_element(
                  map_res.begin(), map_res.end(),
                  [&](const std::pair<local_iterator_t, local_iterator_t>& x,
                      const std::pair<local_iterator_t, local_iterator_t>& y) {
                    return x.first != nil_val && op(*x.first, *y.first);
                  })
                  ->first;
          auto map_res_max =
              std::max_element(
                  map_res.begin(), map_res.end(),
                  [&](const std::pair<local_iterator_t, local_iterator_t>& x,
                      const std::pair<local_iterator_t, local_iterator_t>& y) {
                    return y.second != nil_val && op(*x.second, *y.second);
                  })
                  ->second;

          // local solution
          *result = std::make_pair(
              std::make_pair(map_res_min != nil_val ? *map_res_min : value_t{},
                             map_res_max != nil_val ? *map_res_max : value_t{}),
              std::make_pair(
                  itr_traits::iterator_from_local(gbegin, gend, map_res_min),
                  itr_traits::iterator_from_local(gbegin, gend, map_res_max)));
        },
        args, &res[i]);
  }
  rt::waitForCompletion(h);
  auto res_min = std::min_element(
      res.begin(), res.end(), [&](const res_t& x, const res_t& y) {
        return x.second.first != last && comp(x.first.first, y.first.first);
      });
  auto res_max = std::max_element(
      res.begin(), res.end(), [&](const res_t& x, const res_t& y) {
        return y.second.first != last && comp(x.first.second, y.first.second);
      });
  return std::make_pair(res_min->second.first, res_max->second.second);
}

}  // namespace impl
}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_IMPL_MINIMUM_MAXIMUM_OPS_H */
