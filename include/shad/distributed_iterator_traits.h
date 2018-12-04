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

#ifndef INCLUDE_SHAD_DISTRIBUTED_ITERATOR_TRAIT_H
#define INCLUDE_SHAD_DISTRIBUTED_ITERATOR_TRAIT_H

#include <iterator>

#include "shad/runtime/locality.h"

namespace shad {

template <typename Iterator>
struct distributed_iterator_traits : public std::iterator_traits<Iterator> {
  using difference_type =
      typename std::iterator_traits<Iterator>::difference_type;
  using value_type = typename std::iterator_traits<Iterator>::value_type;
  using pointer = typename std::iterator_traits<Iterator>::pointer;
  using reference = typename std::iterator_traits<Iterator>::reference;
  using iterator_category =
      typename std::iterator_traits<Iterator>::iterator_category;

  using local_iterator_range = typename Iterator::local_iterator_range;
  using local_iterator_type = typename Iterator::local_iterator_type;

  static rt::localities_range localities(Iterator begin, Iterator end) {
    return Iterator::localities(begin, end);
  }

  static local_iterator_range local_range(Iterator begin, Iterator end) {
    return Iterator::local_range(begin, end);
  }

  static Iterator iterator_from_local(Iterator begin, Iterator end,
                                      local_iterator_type itr) {
    return Iterator::iterator_from_local(begin, end, itr);
  }
};

template <typename T, typename = void>
struct is_distributed_iterator {
   static constexpr bool value = false;
};

template <typename T>
struct is_distributed_iterator<T, typename std::enable_if<!std::is_same<
                                  typename distributed_iterator_traits<T>
                                  ::value_type, void>::value>::type> {
   static constexpr bool value = true;
};

template <typename Iterator>
struct distributed_random_access_iterator_trait :
      public distributed_iterator_traits<Iterator> {
  using value_type = typename distributed_iterator_traits<Iterator>::value_type;
  using pointer = typename distributed_iterator_traits<Iterator>::pointer;
  using reference = typename distributed_iterator_traits<Iterator>::reference;
  using iterator_category =
      typename distributed_iterator_traits<Iterator>::iterator_category;
  using distribution_range = typename Iterator::distribution_range;

  static distribution_range
  distribution(Iterator begin, Iterator end) {
    return Iterator::distribution(begin, end);
  }
};

}  // namespace shad

#endif /* INCLUDE_SHAD_DISTRIBUTED_ITERATOR_TRAIT_H */
