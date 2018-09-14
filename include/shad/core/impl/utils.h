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

#ifndef INCLUDE_SHAD_CORE_IMPL_SORTING_OPS_H
#define INCLUDE_SHAD_CORE_IMPL_SORTING_OPS_H

#include <algorithm>
#include <iterator>
#include <tuple>
#include <vector>

#include "shad/core/execution.h"
#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {
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
}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_IMPL_SORTING_OPS_H */
