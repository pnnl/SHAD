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
//===----------------------------------------------------------------------===/

#include <stdlib.h>
#include <iostream>
#include <random>

#include "shad/core/algorithm.h"
#include "shad/core/array.h"
#include "shad/util/measure.h"

constexpr static size_t kArraySize = 1024;
using iterator = shad::impl::array<int, kArraySize>::array_iterator<int>;

void shad_generate_algorithm(shad::array<int, kArraySize> &in) {
  shad::generate(shad::distributed_parallel_tag{}, in.begin(), in.end(), [=]() {
    std::random_device rd;
    std::default_random_engine G(rd());
    std::uniform_int_distribution<int> dist(1, 10);
    return dist(G);
  });
}

void shad_fill_algorithm(shad::array<int, kArraySize> &in) {
  shad::fill(shad::distributed_parallel_tag{}, in.begin(), in.end(), 42);
}

size_t shad_count_algorithm(shad::array<int, kArraySize> &in) {
  auto counter =
      shad::count(shad::distributed_parallel_tag{}, in.begin(), in.end(), 1);
  return counter;
}

iterator shad_find_if_algorithm(shad::array<int, kArraySize> &in) {
  auto iter = shad::find_if(shad::distributed_parallel_tag{}, in.begin(),
                            in.end(), [](int i) { return i % 2 == 0; });
  return iter;
}

void shad_for_each_algorithm(shad::array<int, kArraySize> &in) {
  shad::for_each(shad::distributed_parallel_tag{}, in.begin(), in.end(),
                 [](int &i) { i++; });
}

std::pair<iterator, iterator> shad_minmax_algorithm(
    shad::array<int, kArraySize> &in) {
  auto [min, max] = shad::minmax_element(shad::distributed_parallel_tag{},
                                         in.begin(), in.end());
  return {min, max};
}

void shad_transform_algorithm(shad::array<int, kArraySize> &in) {
  shad::transform(shad::distributed_parallel_tag{}, in.begin(), in.end(),
                  in.begin(), [](int i) { return i + 2; });
}

namespace shad {

int main(int argc, char *argv[]) {
  // array
  shad::array<int, kArraySize> in;

  // shad fill algorithm
  auto execute_time = shad::measure<std::chrono::seconds>::duration(
      [&]() { shad_fill_algorithm(in); });
  std::cout << "Array, using " << shad::rt::numLocalities()
            << " localities, shad::fill took " << execute_time.count()
            << " seconds" << std::endl;

  // shad generate algorithm
  execute_time = shad::measure<std::chrono::seconds>::duration(
      [&]() { shad_generate_algorithm(in); });
  std::cout << "Array, using " << shad::rt::numLocalities()
            << " localities, shad::generate took " << execute_time.count()
            << " seconds" << std::endl;

  // shad count algorithm
  size_t counter;
  execute_time = shad::measure<std::chrono::seconds>::duration(
      [&]() { counter = shad_count_algorithm(in); });
  std::cout << "Array, using " << shad::rt::numLocalities()
            << " localities, shad::count took " << execute_time.count()
            << " seconds (numbers of 0 = " << counter << " )" << std::endl;

  // shad find_if algorithm
  iterator iter;
  execute_time = shad::measure<std::chrono::seconds>::duration(
      [&]() { iter = shad_find_if_algorithm(in); });
  std::cout << "Array, using " << shad::rt::numLocalities()
            << " localities, shad::find_if took " << execute_time.count()
            << " seconds, ";
  (iter != in.end())
      ? std::cout << "array contains an even number" << std::endl
      : std::cout << "array does not contain even numbers" << std::endl;

  // shad for_each algorithm
  execute_time = shad::measure<std::chrono::seconds>::duration(
      [&]() { shad_for_each_algorithm(in); });
  std::cout << "Array, using " << shad::rt::numLocalities()
            << " localities, shad::for_each took " << execute_time.count()
            << " seconds" << std::endl;

  // shad minmax algorithm
  std::pair<iterator, iterator> min_max;
  execute_time = shad::measure<std::chrono::seconds>::duration(
      [&]() { min_max = shad_minmax_algorithm(in); });
  std::cout << "Array, using " << shad::rt::numLocalities()
            << " localities, shad::minmax took " << execute_time.count()
            << " seconds" << std::endl;

  // shad transform algorithm
  execute_time = shad::measure<std::chrono::seconds>::duration(
      [&]() { shad_transform_algorithm(in); });
  std::cout << "Array, using " << shad::rt::numLocalities()
            << " localities, shad::transform took " << execute_time.count()
            << " seconds" << std::endl;

  return 0;
}

}  // namespace shad