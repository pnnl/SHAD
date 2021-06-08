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

#include <iostream>

#include "shad/core/algorithm.h"
#include "shad/util/measure.h"
#include "shad/core/unordered_set.h"

constexpr static size_t kSize = 1024;
using set_t = shad::Set<int>;
using iterator = set_t::iterator;
using value_type = set_t::value_type;
using shad_inserter_t = shad::insert_iterator<shad::unordered_set<int>>;
using shad_buffered_inserter_t = shad::buffered_insert_iterator<shad::unordered_set<int>>;

std::pair<iterator, iterator> shad_minmax_algorithm(shad::unordered_set<int> &in){
  auto [min, max] = shad::minmax_element(
                    shad::distributed_parallel_tag{},
                    in.begin(), in.end());
  return {min, max};
}

iterator shad_find_if_algorithm(shad::unordered_set<int> &in){
  auto iter = shad::find_if(
                    shad::distributed_parallel_tag{},
                    in.begin(), in.end(),
                    [](const value_type &i){ return i%2 == 0;});
  return iter;
}

bool shad_any_of_algorithm(shad::unordered_set<int> &in){
  auto res = shad::any_of(
                    shad::distributed_parallel_tag{},
                    in.begin(), in.end(),
                    [](const value_type &i){ return i%7 == 0;});
  return res;
}

size_t shad_count_if_algorithm(shad::unordered_set<int> &in){
  auto counter = shad::count_if(
                    shad::distributed_parallel_tag{},
                    in.begin(), in.end(), 
                    [](const value_type &i){return i % 3 == 0;});
  return counter;
}

template<typename shad_inserter>
void shad_transform_algorithm(shad::unordered_set<int> &in){
  shad::unordered_set<int> out;
  shad::transform(
                  shad::distributed_parallel_tag{},
                  in.begin(), in.end(), shad_inserter(out, out.begin()),
                  [](const value_type &i){ return i;});
}

namespace shad {

int main(int argc, char *argv[]) {

  // unordered_set
  shad::unordered_set<int> set_;

  // create set
  shad_buffered_inserter_t ins(set_, set_.begin());
  for (size_t i = 0; i < kSize; ++i){
    ins = 2 * (i + 1);
  }
  ins.wait();
  ins.flush();

  // shad minmax algorithm
  std::pair<iterator, iterator> min_max;
  auto execute_time = shad::measure<std::chrono::seconds>::duration(
    [&](){ min_max = shad_minmax_algorithm(set_); });
  std::cout << "Unordered set, using " << shad::rt::numLocalities() 
            << " localities, shad::count took " 
            << execute_time.count() << " seconds (min = "
            << *min_max.first<< ", max = " << *min_max.second << " )" << std::endl;

  // shad find_if algorithm
  iterator iter;
  execute_time = shad::measure<std::chrono::seconds>::duration(
    [&](){iter = shad_find_if_algorithm(set_);});
  std::cout << "Unordered set, using " << shad::rt::numLocalities() 
            << " localities, shad::find_if took " 
            << execute_time.count() << " seconds, ";
  (iter != set_.end()) 
    ? std::cout << "and this unordered set contains an even number" << std::endl
    : std::cout << "and this unordered set does not contain even numbers" << std::endl;


  // shad any_of algorithm
  bool res;
  execute_time = shad::measure<std::chrono::seconds>::duration(
    [&](){res = shad_any_of_algorithm(set_);});
  std::cout << "Unordered set, using " << shad::rt::numLocalities() 
            << " localities, shad::any_of took " 
            << execute_time.count() << " seconds, ";
  (res == true) 
    ? std::cout << "and this unordered set contains at least one number that is divisible by 7" << std::endl
    : std::cout << "and this unordered set does not contain any number that is divisible by 7" << std::endl;


  // shad count_if algorithm
  size_t counter;
  execute_time = shad::measure<std::chrono::seconds>::duration(
    [&](){counter = shad_count_if_algorithm(set_);});
  std::cout << "Unordered set, using " << shad::rt::numLocalities() 
            << " localities, shad::count_if took " 
            << execute_time.count() << " seconds, "
            << "and number divisible by 3: " << counter << std::endl;


  // shad transform algorithm
  execute_time = shad::measure<std::chrono::seconds>::duration(
    [&](){shad_transform_algorithm<shad_inserter_t>(set_);});
  std::cout << "Unordered set, using " << shad::rt::numLocalities() 
            << " localities, shad::transform took " 
            << execute_time.count() << " seconds" << std::endl;

  return 0;
}

} //namespace shad
