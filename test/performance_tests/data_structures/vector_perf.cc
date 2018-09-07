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

#include "shad/data_structures/vector.h"
#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

using VectorT = shad::Vector<int>;

static size_t VECTOR_SIZE = 100000;
static size_t NUM_ITER = 20;
static std::string FILE_NAME = "results_vector_perf.txt";

static shad::AbstractDataStructure<VectorT>::SharedPtr vectorPtr_;
static std::vector<int> stdvector_;

/**
 * Create a Google Benchmark test fixture for data initialization.
 */
class TestFixture : public ::benchmark::Fixture {
 public:
  /**
   * Executes before each test function.
   */
  void SetUp(benchmark::State& state) override {
    stdvector_ = std::vector<int>(VECTOR_SIZE, 0);

    auto ptr = VectorT::Create(VECTOR_SIZE);
    struct Args {
      VectorT::ObjectID oid1;
    };
    auto propagateLambda = [](const Args& args) {
      vectorPtr_ = VectorT::GetPtr(args.oid1);
    };
    Args args = {ptr->GetGlobalID()};
    shad::rt::executeOnAll(propagateLambda, args);
  }

  /**
   * Executes after each test function.
   */
  void TearDown(benchmark::State& state) override {
    VectorT::Destroy(vectorPtr_->GetGlobalID());
  }
};

BENCHMARK_F(TestFixture, test_RawVector)(benchmark::State& state) {
  for (auto _ : state) {
    for (std::size_t i = 0; i < VECTOR_SIZE; i++) {
      stdvector_[i] = i;
    }
  }
}

BENCHMARK_F(TestFixture, test_ParallelAsyncRawVector)(benchmark::State& state) {
  auto feLambda = [](shad::rt::Handle&, const bool&, std::size_t i) {
    stdvector_[i] = i;
  };
  shad::rt::Handle handle;
  bool fake = 0;

  for (auto _ : state) {
    shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(), feLambda, fake,
                             VECTOR_SIZE);
    shad::rt::waitForCompletion(handle);
  }
}

BENCHMARK_F(TestFixture, test_SerialUpdate)(benchmark::State& state) {
  for (auto _ : state) {
    for (std::size_t i = 0; i < VECTOR_SIZE; i++) {
      vectorPtr_->InsertAt(i, i);
    }
  }
}

BENCHMARK_F(TestFixture, test_AsyncUpdate)(benchmark::State& state) {
  shad::rt::Handle handle;

  for (auto _ : state) {
    for (std::size_t i = 0; i < VECTOR_SIZE; i++) {
      vectorPtr_->AsyncInsertAt(handle, i, i);
    }
    shad::rt::waitForCompletion(handle);
  }
}

BENCHMARK_F(TestFixture, test_ParallelAsyncUpdate)(benchmark::State& state) {
  auto feLambda = [](shad::rt::Handle& handle, const bool&, std::size_t i) {
    vectorPtr_->AsyncInsertAt(handle, i, i);
  };
  shad::rt::Handle handle;
  bool fake = 0;

  for (auto _ : state) {
    shad::rt::asyncForEachOnAll(handle, feLambda, fake, VECTOR_SIZE);
    shad::rt::waitForCompletion(handle);
  }
}

BENCHMARK_F(TestFixture, test_ParallelAsyncBufferedUpdate)
(benchmark::State& state) {
  auto feLambda = [](shad::rt::Handle& handle, const bool&, std::size_t i) {
    vectorPtr_->BufferedAsyncInsertAt(handle, i, i);
  };
  shad::rt::Handle handle;
  bool fake = 0;

  for (auto _ : state) {
    shad::rt::asyncForEachOnAll(handle, feLambda, fake, VECTOR_SIZE);
    shad::rt::waitForCompletion(handle);
    vectorPtr_->WaitForBufferedInsert();
  }
}

BENCHMARK_F(TestFixture, test_AsyncBufferedUpdate)(benchmark::State& state) {
  shad::rt::Handle handle;

  for (auto _ : state) {
    for (std::size_t i = 0; i < VECTOR_SIZE; i++) {
      vectorPtr_->BufferedAsyncInsertAt(handle, i, i);
    }
    shad::rt::waitForCompletion(handle);
    vectorPtr_->WaitForBufferedInsert();
  }
}

static void asyncApplyFun(shad::rt::Handle&, shad::Vector<int>::size_type i,
                          int& elem) {
  elem = i;
}

BENCHMARK_F(TestFixture, test_AsyncUpdateWithApply)(benchmark::State& state) {
  shad::rt::Handle handle;

  for (auto _ : state) {
    for (std::size_t i = 0; i < VECTOR_SIZE; i++) {
      vectorPtr_->AsyncApply(handle, i, asyncApplyFun);
    }
    shad::rt::waitForCompletion(handle);
  }
}

BENCHMARK_F(TestFixture, test_AsyncUpdateWithFE)(benchmark::State& state) {
  shad::rt::Handle handle;

  for (auto _ : state) {
    vectorPtr_->AsyncForEachInRange(handle, 0, VECTOR_SIZE, asyncApplyFun);
    shad::rt::waitForCompletion(handle);
  }
}

/**
 * Custom main() instead of calling BENCHMARK_MAIN()
 */
namespace shad {
int main(int argc, char** argv) {
  // Parse command line args
  for (size_t argIndex = 1; argIndex < argc - 1; argIndex++) {
    std::string arg(argv[argIndex]);
    if (arg == "--Size") {
      ++argIndex;
      VECTOR_SIZE = strtoul(argv[argIndex], nullptr, 0);
    } else if (arg == "--NumIter") {
      ++argIndex;
      NUM_ITER = strtoul(argv[argIndex], nullptr, 0);
    } else if (arg == "--OutFileName") {
      ++argIndex;
      FILE_NAME = std::string(argv[argIndex]);
    }
  }
  std::cout << "\n VECTOR_SIZE: " << VECTOR_SIZE << std::endl;
  std::cout << "\n NUM_ITER: " << NUM_ITER << std::endl;
  std::cout << std::endl;

  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();

  return 0;
}
}  // namespace shad
