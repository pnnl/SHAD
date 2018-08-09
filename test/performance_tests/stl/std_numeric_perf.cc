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

#include <array>  //todo
#include <numeric>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "benchmark/benchmark.h"

#include "shad/data_structures/array.h"
#include "shad/data_structures/hashmap.h"
#include "shad/data_structures/set.h"

#include "common.h"

///////////////////////////////////////
//
// vector
//
///////////////////////////////////////
// accumulate
BENCHMARK_TEMPLATE_DEFINE_F(VectorPerf, std_vector_accumulate, std::vector<int>)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  using value_t = typename std::vector<int>::iterator::value_type;
  auto f = std::accumulate<it_t, value_t>;
  this->run(st, f, 0);
}
BENCHMARK_REGISTER_F_(VectorPerf, std_vector_accumulate);

#ifdef STD_REDUCE_TEST
// reduce
BENCHMARK_TEMPLATE_DEFINE_F(VectorPerf, std_vector_reduce, std::vector<int>)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  auto f = std::reduce<it_t>;
  this->run(st, f);
}
BENCHMARK_REGISTER_F_(VectorPerf, std_vector_reduce);
#endif

// todo std::array

///////////////////////////////////////
//
// shad::array - min size
//
///////////////////////////////////////
using shad_array_t_s0 = shad::array<int, shad_test_stl::BENCHMARK_MIN_SIZE>;

BENCHMARK_TEMPLATE_DEFINE_F(ArrayPerf, shad_array_accumulate_s0,
                            shad_array_t_s0)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  using value_t = typename std::vector<int>::iterator::value_type;
  auto f = std::accumulate<it_t, value_t>;
  this->run(st, f, 0);
}
BENCHMARK_REGISTER_F(ArrayPerf, shad_array_accumulate_s0)
    ->Arg(shad_test_stl::BENCHMARK_MIN_SIZE);

#ifdef STD_REDUCE_TEST
BENCHMARK_TEMPLATE_DEFINE_F(ArrayPerf, shad_array_reduce_s0, shad_array_t_s0)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  auto f = std::reduce<it_t>;
  this->run(st, f);
}
BENCHMARK_REGISTER_F(ArrayPerf, shad_array_reduce_s0)
    ->Arg(shad_test_stl::BENCHMARK_MIN_SIZE);
#endif

///////////////////////////////////////
//
// shad::array - max size
//
///////////////////////////////////////
using shad_array_t_s1 = shad::array<int, shad_test_stl::BENCHMARK_MAX_SIZE>;

BENCHMARK_TEMPLATE_DEFINE_F(ArrayPerf, shad_array_accumulate_s1,
                            shad_array_t_s1)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  using value_t = typename std::vector<int>::iterator::value_type;
  auto f = std::accumulate<it_t, value_t>;
  this->run(st, f, 0);
}
BENCHMARK_REGISTER_F(ArrayPerf, shad_array_accumulate_s1)
    ->Arg(shad_test_stl::BENCHMARK_MAX_SIZE);

#ifdef STD_REDUCE_TEST
BENCHMARK_TEMPLATE_DEFINE_F(ArrayPerf, shad_array_reduce_s1, shad_array_t_s1)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  auto f = std::reduce<it_t>;
  this->run(st, f);
}
BENCHMARK_REGISTER_F(ArrayPerf, shad_array_reduce_s1)
    ->Arg(shad_test_stl::BENCHMARK_MAX_SIZE);
#endif

///////////////////////////////////////
//
// unordered_set
//
///////////////////////////////////////
// accumulate
BENCHMARK_TEMPLATE_DEFINE_F(SetPerf, std_set_accumulate,
                            std::unordered_set<int>)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  auto f = std::accumulate<it_t, int>;
  this->run(st, f, 0);
}
BENCHMARK_REGISTER_F_(SetPerf, std_set_accumulate);

#ifdef STD_REDUCE_TEST
// reduce
BENCHMARK_TEMPLATE_DEFINE_F(SetPerf, std_set_reduce, std::unordered_set<int>)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  auto f = std::reduce<it_t>;
  this->run(st, f);
}
BENCHMARK_REGISTER_F_(SetPerf, std_set_reduce);
#endif

///////////////////////////////////////
//
// shad::Set
//
///////////////////////////////////////
// accumulate
BENCHMARK_TEMPLATE_DEFINE_F(SetPerf, shad_set_accumulate,
                            shad::Set<int>)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  auto f = std::accumulate<it_t, int>;
  this->run(st, f, 0);
}
BENCHMARK_REGISTER_F_(SetPerf, shad_set_accumulate);

#ifdef STD_REDUCE_TEST
// reduce
BENCHMARK_TEMPLATE_DEFINE_F(SetPerf, shad_set_reduce, shad::Set<int>)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  auto f = std::reduce<it_t>;
  this->run(st, f);
}
BENCHMARK_REGISTER_F_(SetPerf, shad_set_reduce);
#endif

///////////////////////////////////////
//
// unordered_map
//
///////////////////////////////////////
using std_map_t = std::unordered_map<int, int>;

// accumulate
BENCHMARK_TEMPLATE_DEFINE_F(MapPerf, std_map_accumulate, std_map_t)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  using val_t = typename std_map_t::value_type;
  using op_t = shad_test_stl::pair_acc<int, val_t>;
  auto f = std::accumulate<it_t, int, op_t>;
  this->run(st, f, 0, op_t{});
}
BENCHMARK_REGISTER_F_(MapPerf, std_map_accumulate);

#ifdef STD_REDUCE_TEST
// reduce
BENCHMARK_TEMPLATE_DEFINE_F(MapPerf, std_map_reduce, std_map_t)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  auto f = std::reduce<it_t>;
  this->run(st, f);
}
BENCHMARK_REGISTER_F_(MapPerf, std_map_reduce);
#endif

///////////////////////////////////////
//
// shad::Hasmap
//
///////////////////////////////////////
using shad_map_t = shad::Hashmap<int, int>;

// accumulate
BENCHMARK_TEMPLATE_DEFINE_F(MapPerf, shad_map_accumulate, shad_map_t)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  using val_t = typename shad_map_t::value_type;
  using op_t = shad_test_stl::pair_acc<int, val_t>;
  auto f = std::accumulate<it_t, int, op_t>;
  this->run(st, f, 0, op_t{});
}
BENCHMARK_REGISTER_F_(MapPerf, shad_map_accumulate);

#ifdef STD_REDUCE_TEST
// reduce
BENCHMARK_TEMPLATE_DEFINE_F(MapPerf, shad_map_reduce, shad_map_t)
(benchmark::State& st) {
  using it_t = typeof(this->in->begin());
  auto f = std::reduce<it_t>;
  this->run(st, f);
}
BENCHMARK_REGISTER_F_(MapPerf, shad_map_reduce);
#endif
