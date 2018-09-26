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

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <random>
#include <thread>

#include <benchmark/benchmark.h>

#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

std::atomic<int> globalCounter;

struct exData {
  char c[4000];
};

/**
 * Create a Google Benchmark test fixture for data initialization.
 */
class TestFixture : public ::benchmark::Fixture {
 public:
  /**
   * Executes before each test function.
   */
  void SetUp(benchmark::State& state) override {}

  /**
   * Executes after each test function.
   */
  void TearDown(benchmark::State& state) override {}
};

void testFunctionForEachAt(const exData& data, size_t i) {
  globalCounter += data.c[0] + data.c[1];
}

BENCHMARK_F(TestFixture, test_forEachAt)(benchmark::State& state) {
  exData data{"hello"};
  int i = 0;

  for (auto _ : state) {
    shad::rt::forEachAt(shad::rt::Locality(i++ % shad::rt::numLocalities()),
                        testFunctionForEachAt, data, state.iterations());
  }
}

void testFunctionForEachAtInputBuffer(const uint8_t* data, const uint32_t size,
                                      size_t i) {
  globalCounter += data[0] + data[1];
}

BENCHMARK_F(TestFixture, test_forEachAtInputBuffer)(benchmark::State& state) {
  exData value{1, 2};
  std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)],
                                std::default_delete<uint8_t[]>());
  std::memcpy(data.get(), &value, sizeof(exData));
  int i = 0;

  for (auto _ : state) {
    shad::rt::forEachAt(shad::rt::Locality(i++ % shad::rt::numLocalities()),
                        testFunctionForEachAtInputBuffer, data, sizeof(data),
                        state.iterations());
  }
}

BENCHMARK_F(TestFixture, test_forEachOnAll)(benchmark::State& state) {
  exData data{"hello"};

  for (auto _ : state) {
    shad::rt::forEachOnAll(testFunctionForEachAt, data, state.iterations());
  }
}

BENCHMARK_F(TestFixture, test_forEachOnAllInputBuffer)
(benchmark::State& state) {
  exData value{1, 2};
  std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)],
                                std::default_delete<uint8_t[]>());
  std::memcpy(data.get(), &value, sizeof(exData));

  for (auto _ : state) {
    shad::rt::forEachOnAll(testFunctionForEachAtInputBuffer, data, sizeof(data),
                           state.iterations());
  }
}

namespace shad {
int main(int argc, char** argv) {
  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  ::benchmark::RunSpecifiedBenchmarks();

  return 0;
}
}  // namespace shad
