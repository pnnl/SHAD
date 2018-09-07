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
#include <fstream>
#include <iostream>
#include <random>
#include <thread>

#include <benchmark/benchmark.h>

#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

std::atomic<int> globalCounter;

struct exData {
  char c[4040];
};

struct retData {
  char c[2048];
};

/**
 * Create a Google Benchmark test fixture for data initialization.
 */
class TestFixture : public ::benchmark::Fixture {
 public:
  /**
   * Executes before each test function.
   */
  void SetUp(benchmark::State &state) override {}

  /**
   * Executes after each test function.
   */
  void TearDown(benchmark::State &state) override {}
};

void testFunctionExecuteAt(const exData &data) {
  globalCounter += data.c[0] + data.c[1];
}

BENCHMARK_F(TestFixture, test_executeAt)(benchmark::State &state) {
  exData data{"hello"};
  int i = 0;

  for (auto _ : state) {
    shad::rt::executeAt(shad::rt::Locality(i++ % shad::rt::numLocalities()),
                        testFunctionExecuteAt, data);
  }
}

void testFunctionExecuteAtInputBuffer(const uint8_t *data,
                                      const uint32_t size) {
  globalCounter += data[0] + data[1];
}

BENCHMARK_F(TestFixture, test_executeAtInputBuffer)(benchmark::State &state) {
  std::shared_ptr<uint8_t> data(new uint8_t[4040],
                                std::default_delete<uint8_t[]>());

  int i = 0;

  for (auto _ : state) {
    shad::rt::executeAt(shad::rt::Locality(i++ % shad::rt::numLocalities()),
                        testFunctionExecuteAtInputBuffer, data, sizeof(data));
  }
}

void testFunctionExecuteAtWithRetBuff(const exData &data, uint8_t *,
                                      uint32_t *size) {
  globalCounter += data.c[0] + data.c[1];
  *size = 2048;
}

BENCHMARK_F(TestFixture, test_executeAtWithRetBuff)(benchmark::State &state) {
  exData data{"hello"};

  uint8_t buffer[2048];
  uint32_t size;
  int i = 0;

  for (auto _ : state) {
    shad::rt::executeAtWithRetBuff(
        shad::rt::Locality(i++ % shad::rt::numLocalities()),
        testFunctionExecuteAtWithRetBuff, data, buffer, &size);
  }
}

void testFunctionExecuteAtWithRetBuffInputBuffer(const uint8_t *data,
                                                 const uint32_t, uint8_t *ret,
                                                 uint32_t *size) {
  globalCounter += data[0] + data[1];
  memcpy(ret, &data, sizeof(retData));
  *size = 2048;
}

BENCHMARK_F(TestFixture, test_executeAtWithRetBuffInputBuffer)
(benchmark::State &state) {
  std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)]{1, 2},
                                std::default_delete<uint8_t[]>());
  uint8_t buffer[2048];
  uint32_t size;
  int i = 0;

  for (auto _ : state) {
    shad::rt::executeAtWithRetBuff(
        shad::rt::Locality(i++ % shad::rt::numLocalities()),
        testFunctionExecuteAtWithRetBuffInputBuffer, data, sizeof(data), buffer,
        &size);
  }
}

void testFunctionExecuteAtWithRet(const exData &data, retData *ret) {
  globalCounter += data.c[0] + data.c[1];
  memcpy(ret, &data, sizeof(retData));
}

BENCHMARK_F(TestFixture, test_executeAtWithRet)(benchmark::State &state) {
  exData data{"hello"};
  int i = 0;

  for (auto _ : state) {
    retData ret;
    shad::rt::executeAtWithRet(
        shad::rt::Locality(i++ % shad::rt::numLocalities()),
        testFunctionExecuteAtWithRet, data, &ret);
  }
}

void testFunctionExecuteAtWithRetInputBuffer(const uint8_t *data,
                                             const uint32_t, retData *ret) {
  globalCounter += data[0] + data[1];
  memcpy(ret, data, sizeof(retData));
}

BENCHMARK_F(TestFixture, test_executeAtWithRetInputBuffer)
(benchmark::State &state) {
  std::shared_ptr<uint8_t> data(new uint8_t[4040],
                                std::default_delete<uint8_t[]>());
  int i = 0;

  for (auto _ : state) {
    retData ret;
    shad::rt::executeAtWithRet(
        shad::rt::Locality(i++ % shad::rt::numLocalities()),
        testFunctionExecuteAtWithRetInputBuffer, data, 4040, &ret);
  }
}

BENCHMARK_F(TestFixture, test_executeOnAll)(benchmark::State &state) {
  exData data{"hello"};

  for (auto _ : state) {
    shad::rt::executeOnAll(testFunctionExecuteAt, data);
  }
}

BENCHMARK_F(TestFixture, test_executeOnAllInputBuffer)
(benchmark::State &state) {
  std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)],
                                std::default_delete<uint8_t[]>());
  new (data.get()) exData{1, 2};

  for (auto _ : state) {
    shad::rt::executeOnAll(testFunctionExecuteAtInputBuffer, data,
                           sizeof(data));
  }
}

namespace shad {
int main(int argc, char **argv) {
  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  ::benchmark::RunSpecifiedBenchmarks();

  return 0;
}
}  // namespace shad
