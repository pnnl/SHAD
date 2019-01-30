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

#include <iterator>

#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {
namespace impl {

template <typename ForwardIt, typename MapF>
struct mapped_t {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  using local_iterator_t = typename itr_traits::local_iterator_type;
  using type = typename std::result_of<MapF&(const local_iterator_t&,
                                             const local_iterator_t&)>::type;
};

template <typename ForwardIt, typename MapF>
auto local_map(ForwardIt gbegin, ForwardIt gend, MapF&& map_kernel,
               const typename mapped_t<ForwardIt, MapF>::type& init) {
  using itr_traits = distributed_iterator_traits<ForwardIt>;
  auto local_range = itr_traits::local_range(gbegin, gend);
  auto begin = local_range.begin();
  auto end = local_range.end();

  // allocate partial results
  using mapped_t = typename mapped_t<ForwardIt, MapF>::type;
  auto range_len = std::distance(begin, end);
  auto max_n_blocks = rt::impl::getConcurrency();
  auto block_size = (range_len + max_n_blocks - 1) / max_n_blocks;
  std::vector<mapped_t> map_res(max_n_blocks, init);

  rt::Handle map_h;
  for (size_t block_id = 0, first = 0;
       block_id < max_n_blocks && first < range_len;
       ++block_id, first += block_size) {
    auto map_args = std::make_tuple(block_id, block_size, begin, end,
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

  // reduce
  return map_res;
}

}  // namespace impl
}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_IMPL_IMPL_PATTERNS_H */
