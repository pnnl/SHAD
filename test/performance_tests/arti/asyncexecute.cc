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

void testFunctionAsyncExecuteAt(shad::rt::Handle &, const exData &data) {
  globalCounter += data.c[0] + data.c[1];
}

BENCHMARK_F(TestFixture, test_asyncExecuteAt)(benchmark::State &state) {
  exData data{"hello"};
  int i = 0;

  for (auto _ : state) {
    shad::rt::Handle handle;
    shad::rt::asyncExecuteAt(
        handle, shad::rt::Locality(i++ % shad::rt::numLocalities()),
        testFunctionAsyncExecuteAt, data);

    shad::rt::waitForCompletion(handle);
  }
}

void testFunctionAsyncExecuteAtInputBuffer(shad::rt::Handle &,
                                           const uint8_t *data,
                                           const uint32_t size) {
  globalCounter += data[0] + data[1];
}

BENCHMARK_F(TestFixture, test_asyncExecuteAtInputBuffer)
(benchmark::State &state) {
  exData value{1, 2};
  std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)],
                                std::default_delete<uint8_t[]>());
  std::memcpy(data.get(), &value, sizeof(exData));
  int i = 0;

  for (auto _ : state) {
    shad::rt::Handle handle;
    shad::rt::asyncExecuteAt(
        handle, shad::rt::Locality(i++ % shad::rt::numLocalities()),
        testFunctionAsyncExecuteAtInputBuffer, data, sizeof(exData));

    shad::rt::waitForCompletion(handle);
  }
}

void testFunctionAsyncExecuteAtWithRetBuff(shad::rt::Handle &,
                                           const exData &data, uint8_t *,
                                           uint32_t *size) {
  globalCounter += data.c[0] + data.c[1];
  *size = 2048;
}

BENCHMARK_F(TestFixture, test_asyncExecuteAtWithRetBuff)
(benchmark::State &state) {
  exData data{"hello"};

  uint8_t buffer[2048];
  uint32_t size;
  int i = 0;

  for (auto _ : state) {
    shad::rt::Handle handle;
    shad::rt::asyncExecuteAtWithRetBuff(
        handle, shad::rt::Locality(i++ % shad::rt::numLocalities()),
        testFunctionAsyncExecuteAtWithRetBuff, data, buffer, &size);

    shad::rt::waitForCompletion(handle);
  }
}

void testFunctionAsyncExecuteAtWithRetBuffInputBuffer(shad::rt::Handle &,
                                                      const uint8_t *data,
                                                      const uint32_t, uint8_t *,
                                                      uint32_t *size) {
  globalCounter += data[0] + data[1];
  *size = 2048;
}

BENCHMARK_F(TestFixture, test_asyncExecuteAtWithRetBuffInputBuffer)
(benchmark::State &state) {
  std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)]{1, 2},
                                std::default_delete<uint8_t[]>());

  uint8_t buffer[2048];
  uint32_t size;
  int i = 0;

  for (auto _ : state) {
    shad::rt::Handle handle;
    shad::rt::asyncExecuteAtWithRetBuff(
        handle, shad::rt::Locality(i++ % shad::rt::numLocalities()),
        testFunctionAsyncExecuteAtWithRetBuffInputBuffer, data, sizeof(data),
        buffer, &size);

    shad::rt::waitForCompletion(handle);
  }
}

void testFunctionAsyncExecuteAtWithRet(shad::rt::Handle &, const exData &data,
                                       retData *ret) {
  globalCounter += data.c[0] + data.c[1];
  memcpy(ret, &data, sizeof(retData));
}

BENCHMARK_F(TestFixture, test_asyncExecuteAtWithRet)(benchmark::State &state) {
  exData data{"hello"};
  int i = 0;

  for (auto _ : state) {
    retData ret;
    shad::rt::Handle handle;
    shad::rt::asyncExecuteAtWithRet(
        handle, shad::rt::Locality(i++ % shad::rt::numLocalities()),
        testFunctionAsyncExecuteAtWithRet, data, &ret);

    shad::rt::waitForCompletion(handle);
  }
}

void testFunctionAsyncExecuteAtWithRetInputBuffer(shad::rt::Handle &,
                                                  const uint8_t *data,
                                                  const uint32_t,
                                                  retData *ret) {
  globalCounter += data[0] + data[1];
  memcpy(ret, data, sizeof(retData));
}

BENCHMARK_F(TestFixture, test_asyncExecuteAtWithRetInputBuffer)
(benchmark::State &state) {
  exData value{1, 2};
  std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)],
                                std::default_delete<uint8_t[]>());
  int i = 0;

  for (auto _ : state) {
    retData ret;
    shad::rt::Handle handle;
    shad::rt::asyncExecuteAtWithRet(
        handle, shad::rt::Locality(i++ % shad::rt::numLocalities()),
        testFunctionAsyncExecuteAtWithRetInputBuffer, data, sizeof(data), &ret);

    shad::rt::waitForCompletion(handle);
  }
}

BENCHMARK_F(TestFixture, test_asyncExecuteOnAll)(benchmark::State &state) {
  exData data{"hello"};

  for (auto _ : state) {
    shad::rt::Handle handle;
    shad::rt::asyncExecuteOnAll(handle, testFunctionAsyncExecuteAt, data);

    shad::rt::waitForCompletion(handle);
  }
}

BENCHMARK_F(TestFixture, test_asyncExecuteOnAllInputBuffer)
(benchmark::State &state) {
  std::shared_ptr<uint8_t> data(new uint8_t[2]{1, 2},
                                std::default_delete<uint8_t[]>());

  for (auto _ : state) {
    shad::rt::Handle handle;
    shad::rt::asyncExecuteOnAll(handle, testFunctionAsyncExecuteAtInputBuffer,
                                data, 2);

    shad::rt::waitForCompletion(handle);
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
