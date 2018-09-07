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
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <random>
#include <thread>

#include <benchmark/benchmark.h>

#include "shad/data_structures/array.h"
#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

static size_t SMALL_ARRAY_SIZE = 1000;
static size_t ARRAY_SIZE = 100000;
static size_t NUM_ITER = 20;
static std::string FILE_NAME = "results_array_perf.txt";
static int *rawptr;

using ArrayT = shad::Array<int>;
using unit = std::chrono::microseconds;
static double secUnit = 1000000;
static ArrayT::ShadArrayPtr arrayPtr_;

/**
 * Create a Google Benchmark test fixture for data initialization.
 */
class TestFixture : public ::benchmark::Fixture {
 public:
  /**
   * Executes before each test function.
   */
  void SetUp(benchmark::State &state) override {
    std::vector<int> array(ARRAY_SIZE, 0);
    if (shad::rt::numLocalities() == 1) {
      rawptr = array.data();
    }

    auto ptr = ArrayT::Create(ARRAY_SIZE, 0);
    struct Args {
      ArrayT::ObjectID oid1;
      size_t as;
    };
    auto propagateLambda = [](const Args &args) {
      ARRAY_SIZE = args.as;
      arrayPtr_ = ArrayT::GetPtr(args.oid1);
    };
    Args args = {ptr->GetGlobalID(), ARRAY_SIZE};
    shad::rt::executeOnAll(propagateLambda, args);
  }

  /**
   * Executes after each test function.
   */
  void TearDown(benchmark::State &state) override {
    ArrayT::Destroy(arrayPtr_->GetGlobalID());
  }
};

bool fake;

BENCHMARK_F(TestFixture, test_RawArray)(benchmark::State &state) {
  for (auto _ : state) {
    for (size_t i = 0; i < ARRAY_SIZE; i++) {
      rawptr[i] = i;
    }
  }
}

BENCHMARK_F(TestFixture, test_ParallelAsyncRawArray)(benchmark::State &state) {
  auto feLambda = [](shad::rt::Handle &, const bool &, size_t i) {
    rawptr[i] = i;
  };

  for (auto _ : state) {
    shad::rt::Handle handle;
    shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(), feLambda, fake,
                             ARRAY_SIZE);
    shad::rt::waitForCompletion(handle);
  }
}

BENCHMARK_F(TestFixture, test_SerialUpdate)(benchmark::State &state) {
  for (auto _ : state) {
    for (size_t i = 0; i < ARRAY_SIZE; i++) {
      arrayPtr_->InsertAt(i, i);
    }
  }
}

void applyFun(size_t i, int &elem) { elem = i; }

BENCHMARK_F(TestFixture, test_AsyncUpdate)(benchmark::State &state) {
  for (auto _ : state) {
    shad::rt::Handle handle;
    for (size_t i = 0; i < ARRAY_SIZE; i++) {
      arrayPtr_->AsyncInsertAt(handle, i, i);
    }
    shad::rt::waitForCompletion(handle);
  }
}

BENCHMARK_F(TestFixture, test_ParallelAsyncUpdate)(benchmark::State &state) {
  auto feLambda = [](shad::rt::Handle &handle, const bool &, size_t i) {
    arrayPtr_->AsyncInsertAt(handle, i, i);
  };

  for (auto _ : state) {
    shad::rt::Handle handle;
    shad::rt::asyncForEachOnAll(handle, feLambda, fake, ARRAY_SIZE);
    shad::rt::waitForCompletion(handle);
  }
}

BENCHMARK_F(TestFixture, test_ParallelAsyncBufferedUpdate)
(benchmark::State &state) {
  auto feLambda = [](shad::rt::Handle &handle, const bool &, size_t i) {
    arrayPtr_->BufferedAsyncInsertAt(handle, i, i);
  };

  for (auto _ : state) {
    shad::rt::Handle handle;
    shad::rt::asyncForEachOnAll(handle, feLambda, fake, ARRAY_SIZE);
    shad::rt::waitForCompletion(handle);
    arrayPtr_->WaitForBufferedInsert();
  }
}

BENCHMARK_F(TestFixture, test_AsyncBufferedUpdate)(benchmark::State &state) {
  for (auto _ : state) {
    shad::rt::Handle handle;
    for (size_t i = 0; i < ARRAY_SIZE; i++) {
      arrayPtr_->BufferedAsyncInsertAt(handle, i, i);
    }
    shad::rt::waitForCompletion(handle);
    arrayPtr_->WaitForBufferedInsert();
  }
}

void asyncApplyFun(shad::rt::Handle &, size_t, int &elem, bool &) { elem++; }
void asyncApplyFun2(shad::rt::Handle &, size_t, int &elem) { elem++; }

BENCHMARK_F(TestFixture, test_AsyncUpdateWithApply)(benchmark::State &state) {
  for (auto _ : state) {
    shad::rt::Handle handle;
    for (size_t i = 0; i < ARRAY_SIZE; i++) {
      arrayPtr_->AsyncApply(handle, i, asyncApplyFun, fake);
    }
    shad::rt::waitForCompletion(handle);
  }
}

BENCHMARK_F(TestFixture, test_AsyncUpdateWithFE)(benchmark::State &state) {
  for (auto _ : state) {
    shad::rt::Handle handle;
    arrayPtr_->AsyncForEach(handle, asyncApplyFun2);
    shad::rt::waitForCompletion(handle);
  }
}

/**
 * Custom main() instead of calling BENCHMARK_MAIN()
 */
namespace shad {
int main(int argc, char **argv) {
  // Parse command line args
  for (size_t argIndex = 1; argIndex < argc - 1; argIndex++) {
    std::string arg(argv[argIndex]);
    if (arg == "--Size") {
      ++argIndex;
      ARRAY_SIZE = strtoul(argv[argIndex], nullptr, 0);
    } else if (arg == "--NumIter") {
      ++argIndex;
      NUM_ITER = strtoul(argv[argIndex], nullptr, 0);
    } else if (arg == "--OutFileName") {
      ++argIndex;
      FILE_NAME = std::string(argv[argIndex]);
    }
  }
  std::cout << "\n ARRAY_SIZE: " << ARRAY_SIZE << std::endl;
  std::cout << "\n NUM_ITER: " << NUM_ITER << std::endl;
  std::cout << std::endl;

  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();

  return 0;
}
}  // namespace shad
