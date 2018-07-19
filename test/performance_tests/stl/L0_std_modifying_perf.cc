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

#include <algorithm>
#include <array>  //todo
#include <vector>

#include "benchmark/benchmark.h"

#include "shad/data_structures/array.h"

#include "common.h"

///////////////////////////////////////
//
// std::vector
//
///////////////////////////////////////
// transform
BENCHMARK_TEMPLATE_DEFINE_F(VectorPerf, std_vector_transform, std::vector<int>)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  using unary_t = shad_test_stl::add_two<int>;
  auto f = std::transform<it_t, it_t, unary_t>;
  this->run_io(st, f, unary_t{});
}
BENCHMARK_REGISTER_F_(VectorPerf, std_vector_transform);

// todo std::array

///////////////////////////////////////
//
// shad::array - min size
//
///////////////////////////////////////
using shad_array_t_s0 = shad::array<int, shad_test_stl::BENCHMARK_MIN_SIZE>;

// transform
BENCHMARK_TEMPLATE_DEFINE_F(ArrayPerf, shad_array_transform_s0, shad_array_t_s0)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  using unary_t = shad_test_stl::add_two<int>;
  auto f = std::transform<it_t, it_t, unary_t>;
  this->run_io(st, f, unary_t{});
}
BENCHMARK_REGISTER_F(ArrayPerf, shad_array_transform_s0)
    ->Arg(shad_test_stl::BENCHMARK_MIN_SIZE);

///////////////////////////////////////
//
// shad::array - max size
//
///////////////////////////////////////
using shad_array_t_s1 = shad::array<int, shad_test_stl::BENCHMARK_MAX_SIZE>;

// transform
BENCHMARK_TEMPLATE_DEFINE_F(ArrayPerf, shad_array_transform_s1, shad_array_t_s1)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  using unary_t = shad_test_stl::add_two<int>;
  auto f = std::transform<it_t, it_t, unary_t>;
  this->run_io(st, f, unary_t{});
}
BENCHMARK_REGISTER_F(ArrayPerf, shad_array_transform_s1)
    ->Arg(shad_test_stl::BENCHMARK_MAX_SIZE);

///////////////////////////////////////
//
// std::unordered_set
//
///////////////////////////////////////

// transform - not supported

///////////////////////////////////////
//
// std::unordered_map
//
///////////////////////////////////////

// transform - not supported
