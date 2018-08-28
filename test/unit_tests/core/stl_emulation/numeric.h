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

#ifndef TEST_UNIT_TESTS_STL_STL_EMULATION_NUMERIC_H_
#define TEST_UNIT_TESTS_STL_STL_EMULATION_NUMERIC_H_

namespace shad_test_stl {

template <class InputIt, class T>
T accumulate_(InputIt first, InputIt last, T init) {
  for (; first != last; ++first) {
    init = std::move(init) + *first;  // std::move since C++20
  }
  return init;
}

template <class InputIt, class T, class BinaryOperation>
T accumulate_(InputIt first, InputIt last, T init, BinaryOperation op) {
  for (; first != last; ++first) {
    init = op(std::move(init), *first);  // std::move since C++20
  }
  return init;
}

template <class InputIt>
typename std::iterator_traits<InputIt>::value_type reduce_(InputIt first,
                                                           InputIt last) {
  assert(first != last);
  auto init = *first++;
  return accumulate_(first, last, init);
}

}  // namespace shad_test_stl

#endif
