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

#ifndef INCLUDE_SHAD_CORE_IMPL_NUMERIC_OPS_H
#define INCLUDE_SHAD_CORE_IMPL_NUMERIC_OPS_H

#include <algorithm>
#include <functional>
#include <iterator>
#include "shad/core/execution.h"
#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {
namespace impl {

template <typename ForwardIterator, typename T>
void iota(ForwardIterator first, ForwardIterator last, const T& value) {
  using itr_traits = distributed_iterator_traits<ForwardIterator>;
  auto localities = itr_traits::localities(first, last);
  size_t next_value = value;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
      rt::executeAtWithRet(
        locality,
        [](const std::tuple<ForwardIterator, ForwardIterator, T>& args,
           size_t* result) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto value = std::get<2>(args);

          auto local_range = itr_traits::local_range(begin, end);
          auto lbeg = local_range.begin();
          while(lbeg != local_range.end()) {
            *lbeg++ = value;
            ++value;
          }
          *result = value;
        },
        std::make_tuple(first, last, next_value), &next_value);
  }
}

template <class InputIt, class T, class BinaryOperation>
T accumulate(InputIt first, InputIt last, T init,
             BinaryOperation op) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<InputIt, InputIt, T, BinaryOperation>& args,
           T* result) {
          auto begin = std::get<0>(args);
          auto end = std::get<1>(args);
          auto init = std::get<2>(args);
          auto op = std::get<3>(args);
          auto local_range = itr_traits::local_range(begin, end);
          *result = std::accumulate(local_range.begin(),
                              local_range.end(), init, op);
        },
        std::make_tuple(first, last, init, op), &init);
  }
  return init;
}

template< class InputIt1, class InputIt2, class T >
T inner_product(InputIt1 first1, InputIt1 last1,
                InputIt2 first2, T init) {
  using itr_traits = distributed_iterator_traits<InputIt1>;
  auto localities = itr_traits::localities(first1, last1);
  auto args = std::make_pair(first2, init);
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<InputIt1, InputIt1,
                            std::pair<InputIt2, T>>& args,
           std::pair<InputIt2, T>* result) {
          auto first2 = std::get<2>(args).first;
          auto init = std::get<2>(args).second;
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          while (begin != end) {
            init = std::move(init) + *begin * *first2;
            ++begin;
            ++first2;
          }
          *result = std::make_pair(first2, init);
        },
        std::make_tuple(first1, last1, args), &args);
  }
  return args.second;
}


template< class InputIt1, class InputIt2, class T,
          class BinaryOperation1, class BinaryOperation2>
T inner_product(InputIt1 first1, InputIt1 last1,
                InputIt2 first2, T init,
                BinaryOperation1 op1, BinaryOperation2 op2) {
  using itr_traits = distributed_iterator_traits<InputIt1>;
  auto localities = itr_traits::localities(first1, last1);
  auto args = std::make_pair(first2, init);
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<InputIt1, InputIt1,
                            std::pair<InputIt2, T>,
                            BinaryOperation1, BinaryOperation2>& args,
           std::pair<InputIt2, T>* result) {
          auto first2 = std::get<2>(args).first;
          auto init = std::get<2>(args).second;
          auto op1 = std::get<3>(args);
          auto op2 = std::get<4>(args);
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          while (begin != end) {
            init = op1(std::move(init), op2(*begin, *first2));
            ++begin;
            ++first2;
          }
          *result = std::make_pair(first2, init);
        },
        std::make_tuple(first1, last1, args, op1, op2), &args);
  }
  return args.second;
}

template <class InputIt, class OutputIt, class BinaryOperation>
OutputIt adjacent_difference(distributed_sequential_tag&&,
                             InputIt first, InputIt last,
                             OutputIt d_first, BinaryOperation op) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  using value_t = typename itr_traits::value_type;
  auto startingLoc = localities.begin();
  value_t acc;
  auto res = std::make_pair(d_first, acc);
  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<InputIt, InputIt, OutputIt,
                            rt::Locality, value_t, BinaryOperation>& args,
           std::pair<OutputIt, value_t>* result) {
          auto d_first = std::get<2>(args);
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          if (begin == end) {
            *result = std::make_pair(d_first, std::get<4>(args));
            return;
          }
          BinaryOperation op = std::get<5>(args);
          value_t acc = *begin;
          if (rt::thisLocality() == std::get<3>(args)) {
            *d_first = acc;
          } else {
            *d_first = op(acc, std::get<4>(args));
          } 
          while (++begin != end) {
              value_t val = *begin;
              *++d_first = val - std::move(acc);
              acc = std::move(val);
          }
          *result = std::make_pair(++d_first, acc);
        },
        std::make_tuple(first, last, res.first, startingLoc, res.second, op),
                        &res);
  }
  return d_first;
}

template <class InputIt, class OutputIt, class BinaryOperation>
OutputIt adjacent_difference(distributed_parallel_tag&& policy,
                             InputIt first, InputIt last,
                             OutputIt d_first, BinaryOperation op) {
  if (first == last) return d_first;
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  using value_t = typename itr_traits::value_type;
  auto startingLoc = localities.begin();
  value_t acc;
  uint32_t numLoc = localities.size();
  std::vector<OutputIt>res(numLoc);
  rt::Handle h;
  size_t i = 0;
  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&, const std::tuple<InputIt, InputIt, OutputIt,
                                         rt::Locality, BinaryOperation>& args,
           OutputIt* result) {
          
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto d_first = std::get<2>(args);
          auto begin = local_range.begin();
          auto end = local_range.end();
          if (begin == end) {
            *result = d_first;
            return;
          }
          value_t acc = *begin;
          BinaryOperation op = std::get<4>(args);
          if (rt::thisLocality() == std::get<3>(args)) {
            *d_first = acc;
          } else {
            auto it = itr_traits::iterator_from_local(gbegin, gend, begin);
            --it;
            std::advance(d_first, (std::distance(gbegin, it)));
            value_t val = *d_first;
            *d_first = op(*begin, *it);
          } 
          while (++begin != end) {
              value_t val = *begin;
              *++d_first = op(val, std::move(acc));
              acc = std::move(val);
          }
          *result = ++d_first;
        },
        std::make_tuple(first, last, d_first, startingLoc, op),
                        &res[i]);
  }
  rt::waitForCompletion(h);
  return res[numLoc-1];
}


template <class InputIt, class OutputIt, class BinaryOperation>
OutputIt partial_sum(InputIt first, InputIt last,
                     OutputIt d_first, BinaryOperation op) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  using value_t = typename itr_traits::value_type;
  auto startingLoc = localities.begin();
  value_t acc;
  auto res = std::make_pair(d_first, acc);
  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<InputIt, InputIt, OutputIt,
                            rt::Locality, value_t, BinaryOperation>& args,
           std::pair<OutputIt, value_t>* result) {
          auto d_first = std::get<2>(args);
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          if (begin == end) {
            *result = std::make_pair(d_first, std::get<4>(args));
            return;
          }
          BinaryOperation op = std::get<5>(args);
          value_t acc = *begin;
          if (rt::thisLocality() != std::get<3>(args)) {
            acc = op(std::move(acc), std::get<4>(args));
          }
          *d_first = acc;
          while (++begin != end) {
            acc = op(std::move(acc), *begin);
            *++d_first = acc;
          }
          *result = std::make_pair(++d_first, acc);
        },
        std::make_tuple(first, last, res.first, startingLoc, res.second, op),
                        &res);
  }
  return d_first;
}

template <class InputIt, class T, class BinaryOperation>
T reduce(distributed_sequential_tag&& policy,
         InputIt first, InputIt last, T init,
         BinaryOperation op) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<InputIt, InputIt, T, BinaryOperation>& args,
           T* result) {
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto init = std::get<2>(args);
          auto op = std::get<3>(args);
          for (; begin != end; ++begin) {
            init = op(std::move(init), *begin);
          }          
          *result = init;
        },
        std::make_tuple(first, last, init, op), &init);
  }
  return init;
}

template <class InputIt, class T, class BinaryOperation>
T reduce(distributed_parallel_tag&& policy,
         InputIt first, InputIt last, T init,
         BinaryOperation op) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  rt::Handle h;
  std::vector<T> results(localities.size());
  size_t i = 0;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle &h,
           const std::tuple<InputIt, InputIt, BinaryOperation>& args,
           T* result) {
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto op = std::get<2>(args);
          T acc = *begin;
          while (++begin != end) {
            acc = op(std::move(acc), *begin);
          }
          *result = acc;
        },
        std::make_tuple(first, last, op), &results[i]);
  }
  rt::waitForCompletion(h);
  for (auto lval : results) {
    init = op(std::move(init), lval);
  }
  return init;
}

template <class InputIt, class OutputIt,
          class BinaryOperation, class T>
OutputIt exclusive_scan(distributed_sequential_tag&& policy,
                        InputIt first, InputIt last, OutputIt d_first,
                        BinaryOperation op, T init) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  auto startingLoc = localities.begin();
  auto res = std::make_pair(d_first, init);
  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<InputIt, InputIt, OutputIt,
                            T, BinaryOperation>& args,
           std::pair<OutputIt, T>* result) {
          auto d_first = std::get<2>(args);
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          if (begin == end) {
            *result = std::make_pair(d_first, std::get<3>(args));
            return;
          }
          BinaryOperation op = std::get<4>(args);
          T acc = std::get<3>(args);
          *d_first = acc;
          acc = op(std::move(acc), *begin);
          while (++begin != end) {
            *++d_first = acc;
            acc = op(std::move(acc), *begin);
          }
          *result = std::make_pair(++d_first, acc);
        },
        std::make_tuple(first, last, res.first, res.second, op), &res);
  }
  return d_first;
}

template <class InputIt, class OutputIt,
          class BinaryOperation, class T>
OutputIt exclusive_scan(distributed_parallel_tag&& policy,
                        InputIt first, InputIt last, OutputIt d_first,
                        BinaryOperation op, T init) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  auto startingLoc = localities.begin();
  uint32_t numLoc = localities.size();
  std::vector<std::pair<OutputIt, T>> res(numLoc);
  rt::Handle h;
  size_t i = 0;
  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&, const std::tuple<InputIt, InputIt, OutputIt,
                                         BinaryOperation>& args,
           std::pair<OutputIt, T>* result) {
          auto d_first = std::get<2>(args);
          auto df = d_first;
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto it = itr_traits::iterator_from_local(gbegin, gend, begin);
          auto dist = std::distance(gbegin, it);
          std::advance(d_first, dist);
          if (begin == end) {
            *result = std::make_pair(d_first, T{});
            return;
          }
          BinaryOperation op = std::get<3>(args);
          T acc = *begin;
          *d_first = acc;
          while (++begin != end) {
            *++d_first = acc;
            acc = op(std::move(acc), *begin);
          }
          *result = std::make_pair(++d_first, acc);
        },
        std::make_tuple(first, last, d_first, op), &res[i]);
  }
  rt::waitForCompletion(h);
  auto d_f = d_first;
  auto acc = init;
  OutputIt chunk_end = d_first;
  using outitr_traits = distributed_iterator_traits<OutputIt>;
  for (i=0; i<numLoc; ++i) {
    chunk_end = res[i].first;
    auto d_localities = outitr_traits::localities(d_f, chunk_end);
    auto d_startingLoc = d_localities.begin();
    for (auto locality = d_startingLoc, end = d_localities.end();
         locality != end; ++locality) {
      rt::asyncExecuteAt(
          h, locality,
          [](rt::Handle&,
             const std::tuple<OutputIt, OutputIt,
                              BinaryOperation, T>& args) {
            auto gbegin = std::get<0>(args);
            auto gend = std::get<1>(args);
            auto local_range = outitr_traits::local_range(std::get<0>(args),
                                                          std::get<1>(args));
            auto begin = local_range.begin();
            auto end = local_range.end();
            if (begin == end) return;
            BinaryOperation op = std::get<2>(args);
            auto acc = std::get<3>(args);
            *begin = acc;
            for (++begin; begin!= end; ++begin) {
              *begin = op(std::move(acc), *begin);
            }
          },
          std::make_tuple(d_f, chunk_end, op, acc));
    }
    d_f = chunk_end;
    acc = op(std::move(acc), res[i].second);
  }
  rt::waitForCompletion(h);
  return chunk_end;
}

template <class InputIt, class OutputIt, class BinaryOperation>
OutputIt inclusive_scan(distributed_sequential_tag&& policy,
                        InputIt first, InputIt last, OutputIt d_first,
                        BinaryOperation op) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  auto startingLoc = localities.begin();
  using value_t = typename itr_traits::value_type;
  value_t acc;
  auto res = std::make_pair(d_first, acc);
  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<InputIt, InputIt, OutputIt,
                            rt::Locality, value_t, BinaryOperation>& args,
           std::pair<OutputIt, value_t>* result) {
          auto d_first = std::get<2>(args);
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          if (begin == end) {
            *result = std::make_pair(d_first, std::get<4>(args));
            return;
          }
          BinaryOperation op = std::get<5>(args);
          value_t acc = *begin;
          if (rt::thisLocality() == std::get<3>(args)) {
            *d_first = acc;
          } else {
            acc = op(std::move(acc), std::get<4>(args));
            *d_first = acc;
          }
          while (++begin != end) {
            acc = op(std::move(acc), *begin);
            *++d_first = acc;
          }
          *result = std::make_pair(++d_first, acc);
        },
        std::make_tuple(first, last, res.first, startingLoc, res.second, op),
                        &res);
  }
  return d_first;
}

template <class InputIt, class OutputIt,
          class BinaryOperation>
OutputIt inclusive_scan(distributed_parallel_tag&& policy,
                        InputIt first, InputIt last, OutputIt d_first,
                        BinaryOperation op) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  using value_t = typename itr_traits::value_type;
  auto localities = itr_traits::localities(first, last);
  auto startingLoc = localities.begin();
  uint32_t numLoc = localities.size();
  if (numLoc == 0) {
    return d_first;
  }
  std::vector<std::pair<OutputIt, value_t>> res(numLoc);
  rt::Handle h;
  size_t i = 0;
  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&, const std::tuple<InputIt, InputIt, OutputIt,
                                         BinaryOperation>& args,
           std::pair<OutputIt, value_t>* result) {
          auto d_first = std::get<2>(args);
          auto df = d_first;
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto it = itr_traits::iterator_from_local(gbegin, gend, begin);
          auto dist = std::distance(gbegin, it);
          std::advance(d_first, dist);
          if (begin == end) {
            *result = std::make_pair(d_first, value_t{});
            return;
          }
          BinaryOperation op = std::get<3>(args);
          value_t acc = *begin;
          *d_first = acc;
          while (++begin != end) {
            acc = op(std::move(acc), *begin);
            *++d_first = acc;
          }
          *result = std::make_pair(++d_first, acc);
        },
        std::make_tuple(first, last, d_first, op), &res[i]);
  }
  rt::waitForCompletion(h);
  auto d_f = res[0].first;
  value_t acc = res[0].second;
  OutputIt chunk_end = d_first;
  for (i=1; i<numLoc; ++i) {
    chunk_end = res[i].first;
    auto d_localities = itr_traits::localities(d_f, chunk_end);
    auto d_startingLoc = d_localities.begin();
    for (auto locality = d_startingLoc, end = d_localities.end();
         locality != end; ++locality) {
      rt::asyncExecuteAt(
          h, locality,
          [](rt::Handle&, const std::tuple<OutputIt, OutputIt,
                                           BinaryOperation, value_t>& args) {
            auto gbegin = std::get<0>(args);
            auto gend = std::get<1>(args);
            auto local_range = itr_traits::local_range(std::get<0>(args),
                                                       std::get<1>(args));
            auto begin = local_range.begin();
            auto end = local_range.end();
            BinaryOperation op = std::get<2>(args);
            auto acc = std::get<3>(args);
            for (auto it = begin; it!= end; ++it) {
              *it = op(*it, std::move(acc));
            }
          },
          std::make_tuple(d_f, chunk_end, op, acc));
    }
    d_f = chunk_end;
    acc = op(std::move(acc), res[i].second);
  }
  rt::waitForCompletion(h);
  return chunk_end;
}

template <class InputIt, class OutputIt,
          class BinaryOperation, class T>
OutputIt inclusive_scan(distributed_sequential_tag&& policy,
                        InputIt first, InputIt last, OutputIt d_first,
                        BinaryOperation op, T init) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  auto startingLoc = localities.begin();
  auto res = std::make_pair(d_first, init);
  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<InputIt, InputIt, OutputIt,
                            T, BinaryOperation>& args,
           std::pair<OutputIt, T>* result) {
          auto d_first = std::get<2>(args);
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          if (begin == end) {
            *result = std::make_pair(d_first, std::get<3>(args));
            return;
          }
          BinaryOperation op = std::get<4>(args);
          T acc = op(std::get<3>(args), *begin);
          *d_first = acc; 
          while (++begin != end) {
            acc = op(std::move(acc), *begin);
            *++d_first = acc;
          }
          *result = std::make_pair(++d_first, acc);
        },
        std::make_tuple(first, last, res.first, res.second, op),
                        &res);
  }
  return d_first;
}

template <class InputIt, class OutputIt,
          class BinaryOperation, class T>
OutputIt inclusive_scan(distributed_parallel_tag&& policy,
                        InputIt first, InputIt last, OutputIt d_first,
                        BinaryOperation op, T init) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  auto startingLoc = localities.begin();
  uint32_t numLoc = localities.size();
  std::vector<std::pair<OutputIt, T>> res(numLoc);
  rt::Handle h;
  size_t i = 0;
  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&, const std::tuple<InputIt, InputIt, OutputIt,
                                         BinaryOperation>& args,
           std::pair<OutputIt, T>* result) {
          auto d_first = std::get<2>(args);
          auto df = d_first;
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto it = itr_traits::iterator_from_local(gbegin, gend, begin);
          auto dist = std::distance(gbegin, it);
          std::advance(d_first, dist);
          if (begin == end) {
            *result = std::make_pair(d_first, T{});
            return;
          }
          BinaryOperation op = std::get<3>(args);
          T acc = *begin;
          *d_first = acc;
          while (++begin != end) {
            acc = op(std::move(acc), *begin);
            *++d_first = acc;
          }
          *result = std::make_pair(++d_first, acc);
        },
        std::make_tuple(first, last, d_first, op), &res[i]);
  }
  rt::waitForCompletion(h);
  auto d_f = d_first;
  auto acc = init;
  OutputIt chunk_end = d_first;
  using outitr_traits = distributed_iterator_traits<OutputIt>;
  for (i=0; i<numLoc; ++i) {
    chunk_end = res[i].first;
    auto d_localities = outitr_traits::localities(d_f, chunk_end);
    auto d_startingLoc = d_localities.begin();
    for (auto locality = d_startingLoc, end = d_localities.end();
         locality != end; ++locality) {
      rt::asyncExecuteAt(
          h, locality,
          [](rt::Handle&, const std::tuple<OutputIt, OutputIt,
                                           BinaryOperation, T>& args) {
            auto gbegin = std::get<0>(args);
            auto gend = std::get<1>(args);
            auto local_range = itr_traits::local_range(std::get<0>(args),
                                                       std::get<1>(args));
            auto begin = local_range.begin();
            auto end = local_range.end();
            BinaryOperation op = std::get<2>(args);
            auto acc = std::get<3>(args);
            for (auto it = begin; it!= end; ++it) {
              *it = op(std::move(acc), *it);
            }
          },
          std::make_tuple(d_f, chunk_end, op, acc));
    }
    d_f = chunk_end;
    acc = op(std::move(acc), res[i].second);
  }
  rt::waitForCompletion(h);
  return chunk_end;
}

template <class ForwardIt, class T, class BinaryOp, class UnaryOp>
T transform_reduce(distributed_sequential_tag&& policy,
                   ForwardIt first, ForwardIt last, T init,
                   BinaryOp op, UnaryOp uop) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<ForwardIt, ForwardIt, T,
                            BinaryOp, UnaryOp>& args,
           T* result) {
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto init = std::get<2>(args);
          auto op = std::get<3>(args);
          auto uop = std::get<4>(args);
          for (; begin != end; ++begin) {
            init = op(std::move(init), uop(*begin));
          }          
          *result = init;
        },
        std::make_tuple(first, last, init, op, uop), &init);
  }
  return init;
}

template <class ForwardIt, class T, class BinaryOp, class UnaryOp>
T transform_reduce(distributed_parallel_tag&& policy,
                   ForwardIt first, ForwardIt last, T init,
                   BinaryOp op, UnaryOp uop) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto localities = itr_traits::localities(first, last);
  rt::Handle h;
  std::vector<T> results(localities.size());
  size_t i = 0;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle &h,
           const std::tuple<ForwardIt, ForwardIt, BinaryOp, UnaryOp>& args,
           T* result) {
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto op = std::get<2>(args);
          auto uop = std::get<3>(args);
          T acc = uop(*begin);
          while (++begin != end) {
            acc = op(std::move(acc), uop(*begin));
          }
          *result = acc;
        },
        std::make_tuple(first, last, op, uop), &results[i]);
  }
  rt::waitForCompletion(h);
  for (auto lval : results) {
    init = op(std::move(init), lval);
  }
  return init;
}

template <class ForwardIt1, class ForwardIt2,
          class T, class BinaryOp1, class BinaryOp2>
T transform_reduce(distributed_sequential_tag&& policy,
                   ForwardIt1 first1, ForwardIt1 last1, ForwardIt2 first2,
                   T init, BinaryOp1 op1, BinaryOp2 op2) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  auto localities = itr_traits::localities(first1, last1);
  std::pair<ForwardIt2, T> res = std::make_pair(first2, init);
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<ForwardIt1, ForwardIt1, ForwardIt2,
                            T, BinaryOp1, BinaryOp2>& args,
           std::pair<ForwardIt2, T>* result) {
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto first2 = std::get<2>(args);
          auto init = std::get<3>(args);
          auto op = std::get<4>(args);
          auto op2 = std::get<5>(args);
          for (; begin != end; ++begin, ++first2) {
            init = op(std::move(init), op2(*begin, *first2));
          }          
          *result = std::make_pair(first2, init);
        },
        std::make_tuple(first1, last1, res.first, res.second, op1, op2), &res);
  }
  return res.second;
}

template <class ForwardIt1, class ForwardIt2,
          class T, class BinaryOp1, class BinaryOp2>
T transform_reduce(distributed_parallel_tag&&  policy,
                   ForwardIt1 first1, ForwardIt1 last1, ForwardIt2 first2,
                   T init, BinaryOp1 op1, BinaryOp2 op2) {
  using itr_traits = distributed_iterator_traits<ForwardIt1>;
  auto localities = itr_traits::localities(first1, last1);
  rt::Handle h;
  std::vector<T> results(localities.size());
  size_t i = 0;
  for (auto locality = localities.begin(), end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle &h,
           const std::tuple<ForwardIt1, ForwardIt1, ForwardIt2,
                            BinaryOp1, BinaryOp2>& args,
           T* result) {
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto it = itr_traits::iterator_from_local(gbegin, gend, begin);
          auto first2 = std::get<2>(args);
          size_t dist = std::distance(gbegin, it);
          std::advance(first2, dist);
          auto op1 = std::get<3>(args);
          auto op2 = std::get<4>(args);
          T acc = op2(*begin, *first2);
          while (++begin != end) {
            acc = op1(std::move(acc), op2(*begin, *(++first2)));
          }
          *result = acc;
        },
        std::make_tuple(first1, last1, first2, op1, op2), &results[i]);
  }
  rt::waitForCompletion(h);
  for (auto lval : results) {
    init = op1(std::move(init), lval);
  }
  return init;
}

template <class InputIt, class OutputIt, class T,
          class BinaryOperation, class UnaryOperation>
OutputIt transform_exclusive_scan(distributed_sequential_tag&& policy,
                                  InputIt first, InputIt last,
                                  OutputIt d_first, T init,
                                  BinaryOperation op,
                                  UnaryOperation uop) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  auto startingLoc = localities.begin();
  auto res = std::make_pair(d_first, init);
  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<InputIt, InputIt, OutputIt,
                            T, BinaryOperation, UnaryOperation>& args,
           std::pair<OutputIt, T>* result) {
          auto d_first = std::get<2>(args);
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          if (begin == end) {
            *result = std::make_pair(d_first, std::get<3>(args));
            return;
          }
          T acc = std::get<3>(args);
          auto op = std::get<4>(args);
          auto uop = std::get<5>(args);
          *d_first = acc;
          acc = op(std::move(acc), uop(*begin));
          while (++begin != end) {
            *++d_first = acc;
            acc = op(std::move(acc), uop(*begin));
          }
          *result = std::make_pair(++d_first, acc);
        },
        std::make_tuple(first, last, res.first, res.second, op, uop), &res);
  }
  return res.first;
}

template <class InputIt, class OutputIt, class T,
          class BinaryOperation, class UnaryOperation>
OutputIt transform_exclusive_scan(distributed_parallel_tag&& policy,
                                  InputIt first, InputIt last,
                                  OutputIt d_first, T init,
                                  BinaryOperation op,
                                  UnaryOperation uop) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  auto startingLoc = localities.begin();
  uint32_t numLoc = localities.size();
  std::vector<std::pair<OutputIt, T>> res(numLoc);
  rt::Handle h;
  size_t i = 0;
  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&,
           const std::tuple<InputIt, InputIt, OutputIt,
                            BinaryOperation, UnaryOperation>& args,
           std::pair<OutputIt, T>* result) {
          auto d_first = std::get<2>(args);
          auto df = d_first;
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto it = itr_traits::iterator_from_local(gbegin, gend, begin);
          auto dist = std::distance(gbegin, it);
          std::advance(d_first, dist);
          if (begin == end) {
            *result = std::make_pair(d_first, T{});
            return;
          }
          auto op = std::get<3>(args);
          auto uop = std::get<4>(args);
          T acc = *begin;
          *d_first = acc;
          acc = uop(acc);
          while (++begin != end) {
            *++d_first = acc;
            acc = op(std::move(acc), uop(*begin));
          }
          *result = std::make_pair(++d_first, acc);
        },
        std::make_tuple(first, last, d_first, op, uop), &res[i]);
  }
  rt::waitForCompletion(h);
  auto d_f = d_first;
  auto acc = init;
  OutputIt chunk_end = d_first;
  using outitr_traits = distributed_iterator_traits<OutputIt>;
  for (i=0; i<numLoc; ++i) {
    chunk_end = res[i].first;
    auto d_localities = outitr_traits::localities(d_f, chunk_end);
    auto d_startingLoc = d_localities.begin();
    for (auto locality = d_startingLoc, end = d_localities.end();
         locality != end; ++locality) {
      rt::asyncExecuteAt(
          h, locality,
          [](rt::Handle&,
             const std::tuple<OutputIt, OutputIt,
                              BinaryOperation, T>& args) {
            auto gbegin = std::get<0>(args);
            auto gend = std::get<1>(args);
            auto local_range = itr_traits::local_range(std::get<0>(args),
                                                       std::get<1>(args));
            auto begin = local_range.begin();
            auto end = local_range.end();
            if (begin == end) return;
            BinaryOperation op = std::get<2>(args);
            auto acc = std::get<3>(args);
            *begin = acc;
            for (++begin; begin!= end; ++begin) {
              *begin = op(std::move(acc), *begin);
            }
          },
          std::make_tuple(d_f, chunk_end, op, acc));
    }
    d_f = chunk_end;
    acc = op(std::move(acc), res[i].second);
  }
  rt::waitForCompletion(h);
  return chunk_end;
}

template <class InputIt, class OutputIt,
          class BinaryOperation, class UnaryOperation>
OutputIt transform_inclusive_scan(distributed_sequential_tag&& policy,
                                  InputIt first, InputIt last,
                                  OutputIt d_first,
                                  BinaryOperation op, UnaryOperation uop) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  auto startingLoc = localities.begin();
  using value_t = typename itr_traits::value_type;
  value_t acc;
  auto res = std::make_pair(d_first, acc);
  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<InputIt, InputIt, OutputIt,
                            rt::Locality, value_t,
                            BinaryOperation, UnaryOperation>& args,
           std::pair<OutputIt, value_t>* result) {
          auto d_first = std::get<2>(args);
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          if (begin == end) {
            *result = std::make_pair(d_first, std::get<4>(args));
            return;
          }
          BinaryOperation op = std::get<5>(args);
          UnaryOperation uop = std::get<6>(args);
          value_t acc = uop(*begin);
          if (rt::thisLocality() == std::get<3>(args)) {
            *d_first = acc;
          } else {
            acc = op(std::move(acc), std::get<4>(args));
            *d_first = acc;
          }
          while (++begin != end) {
            acc = op(std::move(acc), uop(*begin));
            *++d_first = acc;
          }
          *result = std::make_pair(++d_first, acc);
        },
        std::make_tuple(first, last, res.first, startingLoc,
                        res.second, op, uop),
                        &res);
  }
  return res.first;
}

template <class InputIt, class OutputIt,
          class BinaryOperation, class UnaryOperation>
OutputIt transform_inclusive_scan(distributed_parallel_tag&& policy,
                                  InputIt first, InputIt last,
                                  OutputIt d_first,
                                  BinaryOperation op, UnaryOperation uop) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  using value_t = typename itr_traits::value_type;
  auto localities = itr_traits::localities(first, last);
  auto startingLoc = localities.begin();
  uint32_t numLoc = localities.size();
  if (numLoc == 0) {
    return d_first;
  }
  std::vector<std::pair<OutputIt, value_t>> res(numLoc);
  rt::Handle h;
  size_t i = 0;
  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&,
           const std::tuple<InputIt, InputIt, OutputIt,
                            BinaryOperation, UnaryOperation>& args,
           std::pair<OutputIt, value_t>* result) {
          auto d_first = std::get<2>(args);
          auto df = d_first;
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto it = itr_traits::iterator_from_local(gbegin, gend, begin);
          auto dist = std::distance(gbegin, it);
          std::advance(d_first, dist);
          if (begin == end) {
            *result = std::make_pair(d_first, value_t{});
            return;
          }
          BinaryOperation op = std::get<3>(args);
          UnaryOperation uop = std::get<4>(args);
          value_t acc = uop(*begin);
          *d_first = acc;
          while (++begin != end) {
            acc = op(std::move(acc), uop(*begin));
            *++d_first = acc;
          }
          *result = std::make_pair(++d_first, acc);
        },
        std::make_tuple(first, last, d_first, op, uop), &res[i]);
  }
  rt::waitForCompletion(h);
  auto d_f = res[0].first;
  value_t acc = res[0].second;
  OutputIt chunk_end = d_first;
  using outitr_traits = distributed_iterator_traits<OutputIt>;
  for (i=1; i<numLoc; ++i) {
    chunk_end = res[i].first;
    auto d_localities = outitr_traits::localities(d_f, chunk_end);
    auto d_startingLoc = d_localities.begin();
    for (auto locality = d_startingLoc, end = d_localities.end();
         locality != end; ++locality) {
      rt::asyncExecuteAt(
          h, locality,
          [](rt::Handle&, const std::tuple<OutputIt, OutputIt,
                                           BinaryOperation, value_t>& args) {
            auto gbegin = std::get<0>(args);
            auto gend = std::get<1>(args);
            auto local_range = itr_traits::local_range(std::get<0>(args),
                                                       std::get<1>(args));
            auto begin = local_range.begin();
            auto end = local_range.end();
            BinaryOperation op = std::get<2>(args);
            auto acc = std::get<3>(args);
            for (auto it = begin; it!= end; ++it) {
              *it = op(std::move(acc), *it);
            }
          },
          std::make_tuple(d_f, chunk_end, op, acc));
    }
    d_f = chunk_end;
    acc = op(std::move(acc), res[i].second);
  }
  rt::waitForCompletion(h);
  return chunk_end;
}

template <class InputIt, class OutputIt,
          class BinaryOperation, class UnaryOperation, class T>
OutputIt transform_inclusive_scan(distributed_sequential_tag&& policy,
                                  InputIt first, InputIt last,
                                  OutputIt d_first,
                                  BinaryOperation op,
                                  UnaryOperation uop, T init) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  auto startingLoc = localities.begin();
  auto res = std::make_pair(d_first, init);
  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality) {
    rt::executeAtWithRet(
        locality,
        [](const std::tuple<InputIt, InputIt, OutputIt,
                            T, BinaryOperation, UnaryOperation>& args,
           std::pair<OutputIt, T>* result) {
          auto d_first = std::get<2>(args);
          auto local_range = itr_traits::local_range(std::get<0>(args),
                                                     std::get<1>(args));
          auto begin = local_range.begin();
          auto end = local_range.end();
          if (begin == end) {
            *result = std::make_pair(d_first, std::get<3>(args));
            return;
          }
          BinaryOperation op = std::get<4>(args);
          UnaryOperation uop = std::get<5>(args);
          T acc = op(std::get<3>(args), uop(*begin));
          *d_first = acc; 
          while (++begin != end) {
            acc = op(std::move(acc), uop(*begin));
            *++d_first = acc;
          }
          *result = std::make_pair(++d_first, acc);
        },
        std::make_tuple(first, last, res.first, res.second, op, uop),
                        &res);
  }
  return res.first;
}

template <class InputIt, class OutputIt,
          class BinaryOperation, class UnaryOperation, class T>
OutputIt transform_inclusive_scan(distributed_parallel_tag&& policy,
                                  InputIt first, InputIt last,
                                  OutputIt d_first,
                                  BinaryOperation op,
                                  UnaryOperation uop, T init) {
  using itr_traits = distributed_iterator_traits<InputIt>;
  auto localities = itr_traits::localities(first, last);
  auto startingLoc = localities.begin();
  uint32_t numLoc = localities.size();
  if (numLoc == 0) {
    return d_first;
  }
  std::vector<std::pair<OutputIt, T>> res(numLoc);
  rt::Handle h;
  size_t i = 0;
  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality, ++i) {
    rt::asyncExecuteAtWithRet(
        h, locality,
        [](rt::Handle&,
           const std::tuple<InputIt, InputIt, OutputIt,
                            BinaryOperation, UnaryOperation>& args,
           std::pair<OutputIt, T>* result) {
          auto d_first = std::get<2>(args);
          auto df = d_first;
          auto gbegin = std::get<0>(args);
          auto gend = std::get<1>(args);
          auto local_range = itr_traits::local_range(gbegin, gend);
          auto begin = local_range.begin();
          auto end = local_range.end();
          auto it = itr_traits::iterator_from_local(gbegin, gend, begin);
          auto dist = std::distance(gbegin, it);
          std::advance(d_first, dist);
          if (begin == end) {
            *result = std::make_pair(d_first, T{});
            return;
          }
          BinaryOperation op = std::get<3>(args);
          UnaryOperation uop = std::get<4>(args);
          T acc = uop(*begin);
          *d_first = acc;
          while (++begin != end) {
            acc = op(std::move(acc), uop(*begin));
            *++d_first = acc;
          }
          *result = std::make_pair(++d_first, acc);
        },
        std::make_tuple(first, last, d_first, op, uop), &res[i]);
  }
  rt::waitForCompletion(h);
  auto d_f = res[0].first;
  auto acc = res[0].second;
  OutputIt chunk_end = d_first;
  using outitr_traits = distributed_iterator_traits<OutputIt>;
  for (i=1; i<numLoc; ++i) {
    chunk_end = res[i].first;
    auto d_localities = outitr_traits::localities(d_f, chunk_end);
    auto d_startingLoc = d_localities.begin();
    for (auto locality = d_startingLoc, end = d_localities.end();
         locality != end; ++locality) {
      rt::asyncExecuteAt(
          h, locality,
          [](rt::Handle&, const std::tuple<OutputIt, OutputIt,
                                           BinaryOperation, T>& args) {
            auto gbegin = std::get<0>(args);
            auto gend = std::get<1>(args);
            auto local_range = itr_traits::local_range(std::get<0>(args),
                                                       std::get<1>(args));
            auto begin = local_range.begin();
            auto end = local_range.end();
            BinaryOperation op = std::get<2>(args);
            auto acc = std::get<3>(args);
            for (auto it = begin; it!= end; ++it) {
              *it = op(std::move(acc), *it);
            }
          },
          std::make_tuple(d_f, chunk_end, op, acc));
    }
    d_f = chunk_end;
    acc = op(std::move(acc), res[i].second);
  }
  rt::waitForCompletion(h);
  return chunk_end;
}

}  // namespace impl
}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_IMPL_NUMERIC_OPS_H */
