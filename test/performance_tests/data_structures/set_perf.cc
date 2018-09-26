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
#include <unordered_set>

#include <benchmark/benchmark.h>

#include "shad/data_structures/set.h"
#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

static size_t SET_SIZE = 100000;
static size_t NUM_ITER = 20;
static std::string FILE_NAME = "results_set_perf.txt";

using SetT = shad::Set<int>;
using unit = std::chrono::microseconds;
static double secUnit = 1000000;
static SetT::SharedPtr setPtr_;
static std::unordered_set<int> stdset_;

/**
 * Create a Google Benchmark test fixture for data initialization.
 */
class TestFixture : public ::benchmark::Fixture {
 public:
  /**
   * Executes before each test function.
   */
  void SetUp(benchmark::State& state) override {
    auto ptr = SetT::Create(SET_SIZE);
    struct Args {
      SetT::ObjectID oid1;
      size_t as;
    };
    auto propagateLambda = [](const Args& args) {
      SET_SIZE = args.as;
      setPtr_ = SetT::GetPtr(args.oid1);
    };
    Args args = {ptr->GetGlobalID(), SET_SIZE};
    shad::rt::executeOnAll(propagateLambda, args);
  }

  /**
   * Executes after each test function.
   */
  void TearDown(benchmark::State& state) override {
    SetT::Destroy(setPtr_->GetGlobalID());
  }

  std::size_t vectorSize_;
  std::size_t numIter_;
};

bool fake;

BENCHMARK_F(TestFixture, test_RawSet)(benchmark::State& state) {
  for (auto _ : state) {
    for (size_t i = 0; i < SET_SIZE; i++) {
      stdset_.insert(i);
    }
  }
}

// BENCHMARK_F(TestFixture, test_ParallelAsyncRawSet() {
//   auto feLambda = [] (shad::rt::Handle&,
//                       const bool&, size_t i) {
//     stdset_[i] = i;
//   };
//   shad::rt::Handle handle;
//   shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
//                            feLambda, fake, SET_SIZE);
//   shad::rt::waitForCompletion(handle);
// }

BENCHMARK_F(TestFixture, test_SerialInsert)(benchmark::State& state) {
  for (auto _ : state) {
    for (size_t i = 0; i < SET_SIZE; i++) {
      setPtr_->Insert(i);
    }
  }
}

// void applyFun(const int &key, int& elem) {
//   elem = key;
// }

BENCHMARK_F(TestFixture, test_AsyncInsert)(benchmark::State& state) {
  for (auto _ : state) {
    shad::rt::Handle handle;
    for (size_t i = 0; i < SET_SIZE; i++) {
      setPtr_->AsyncInsert(handle, i);
    }
    shad::rt::waitForCompletion(handle);
  }
}

BENCHMARK_F(TestFixture, test_ParallelAsyncInsert)(benchmark::State& state) {
  auto feLambda = [](shad::rt::Handle& handle, const bool&, size_t i) {
    setPtr_->AsyncInsert(handle, i);
  };

  for (auto _ : state) {
    shad::rt::Handle handle;
    shad::rt::asyncForEachOnAll(handle, feLambda, fake, SET_SIZE);
    shad::rt::waitForCompletion(handle);
  }
}

BENCHMARK_F(TestFixture, test_ParallelAsyncBufferedInsert)
(benchmark::State& state) {
  auto feLambda = [](shad::rt::Handle& handle, const bool&, size_t i) {
    setPtr_->BufferedAsyncInsert(handle, i);
  };

  for (auto _ : state) {
    shad::rt::Handle handle;
    shad::rt::asyncForEachOnAll(handle, feLambda, fake, SET_SIZE);
    shad::rt::waitForCompletion(handle);
    setPtr_->WaitForBufferedInsert();
  }
}

BENCHMARK_F(TestFixture, test_AsyncBufferedInsert)(benchmark::State& state) {
  for (auto _ : state) {
    shad::rt::Handle handle;
    for (size_t i = 0; i < SET_SIZE; i++) {
      setPtr_->BufferedAsyncInsert(handle, i);
    }
    shad::rt::waitForCompletion(handle);
    setPtr_->WaitForBufferedInsert();
  }
}

static void asyncApplyFun(shad::rt::Handle&, const int& key) {
  // do nothing
}

BENCHMARK_F(TestFixture, test_AsyncVisitWithFE)(benchmark::State& state) {
  for (auto _ : state) {
    shad::rt::Handle handle;
    setPtr_->AsyncForEachElement(handle, asyncApplyFun);
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
      SET_SIZE = strtoul(argv[argIndex], nullptr, 0);
    } else if (arg == "--NumIter") {
      ++argIndex;
      NUM_ITER = strtoul(argv[argIndex], nullptr, 0);
    } else if (arg == "--OutFileName") {
      ++argIndex;
      FILE_NAME = std::string(argv[argIndex]);
    }
  }
  std::cout << "\n SET_SIZE: " << SET_SIZE << std::endl;
  std::cout << "\n NUM_ITER: " << NUM_ITER << std::endl;
  std::cout << std::endl;

  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();

  return 0;
}
}  // namespace shad
