//===------------------------------------------------------------*- C++ -*-===//
//
//                                     SHAD
//
//      The Scalable High-performance Algorithms and Data Structure Library
//
//===----------------------------------------------------------------------===//
//
// Copyright 2017 Pacific Northwest National Laboratory
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

void testInit(int argc, char *argv[]) {
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
  auto ptr = SetT::Create(SET_SIZE);
  struct Args {
    SetT::ObjectID oid1;
    size_t as;
  };
  auto propagateLambda = [](const Args &args) {
    SET_SIZE = args.as;
    setPtr_ = SetT::GetPtr(args.oid1);
  };
  Args args = {ptr->GetGlobalID(), SET_SIZE};
  shad::rt::executeOnAll(propagateLambda, args);
}

void testFinalize() { SetT::Destroy(setPtr_->GetGlobalID()); }

bool fake;

void test_RawSet() {
  for (size_t i = 0; i < SET_SIZE; i++) {
    stdset_.insert(i);
  }
}

// void test_ParallelAsyncRawSet() {
//   auto feLambda = [] (shad::rt::Handle&,
//                       const bool&, size_t i) {
//     stdset_[i] = i;
//   };
//   shad::rt::Handle handle;
//   shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(),
//                            feLambda, fake, SET_SIZE);
//   shad::rt::waitForCompletion(handle);
// }

void test_SerialInsert() {
  for (size_t i = 0; i < SET_SIZE; i++) {
    setPtr_->Insert(i);
  }
}

// void applyFun(const int &key, int& elem) {
//   elem = key;
// }

void test_AsyncInsert() {
  shad::rt::Handle handle;
  for (size_t i = 0; i < SET_SIZE; i++) {
    setPtr_->AsyncInsert(handle, i);
  }
  shad::rt::waitForCompletion(handle);
}

void test_ParallelAsyncInsert() {
  auto feLambda = [](shad::rt::Handle &handle, const bool &, size_t i) {
    setPtr_->AsyncInsert(handle, i);
  };
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(handle, feLambda, fake, SET_SIZE);
  shad::rt::waitForCompletion(handle);
}

void test_ParallelAsyncBufferedInsert() {
  auto feLambda = [](shad::rt::Handle &handle, const bool &, size_t i) {
    setPtr_->BufferedAsyncInsert(handle, i);
  };
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(handle, feLambda, fake, SET_SIZE);
  shad::rt::waitForCompletion(handle);
  setPtr_->WaitForBufferedInsert();
}

void test_AsyncBufferedInsert() {
  shad::rt::Handle handle;
  for (size_t i = 0; i < SET_SIZE; i++) {
    setPtr_->BufferedAsyncInsert(handle, i);
  }
  shad::rt::waitForCompletion(handle);
  setPtr_->WaitForBufferedInsert();
}

static void asyncApplyFun(shad::rt::Handle &, const int &key) {
  // do nothing
}

void test_AsyncVisitWithFE() {
  shad::rt::Handle handle;
  setPtr_->AsyncForEachElement(handle, asyncApplyFun);
  shad::rt::waitForCompletion(handle);
}

void printResults(std::string funName, double time, size_t size) {
  std::cout << "\n\n*** " << funName << " ***\n---Time: " << time
            << " secs\n---Throughput: " << (double)size / time << " ops/sec"
            << std::endl;
}

namespace shad {

int main(int argc, char *argv[]) {
  testInit(argc, argv);

  std::ofstream resFile;
  resFile.open(FILE_NAME);

  size_t RawSetMeasureTot = 0;
  size_t ParallelAsyncRawSetMeasureTot = 0;
  size_t SerialInsertMeasureTot = 0;
  size_t AsyncInsertSecTot = 0;
  size_t ParallelAsyncInsertSecTot = 0;
  size_t AsyncBufferedInsertMeasureTot = 0;
  size_t AsyncVisitWithFEMeasureTot = 0;

  size_t RawSetMeasure = 0;
  size_t ParallelAsyncRawSetMeasure = 0;
  size_t SerialInsertMeasure = 0;
  size_t AsyncInsertSec = 0;
  size_t ParallelAsyncInsertSec = 0;
  size_t AsyncBufferedInsertMeasure = 0;
  size_t AsyncVisitWithFEMeasure = 0;

  size_t i = 0;
  stdset_ = std::unordered_set<int>();
  for (; i < NUM_ITER; i++) {
    if (shad::rt::numLocalities() == 1) {
      RawSetMeasure = measure<unit>::duration(test_RawSet).count();

      SerialInsertMeasure = measure<unit>::duration(test_SerialInsert).count();
      setPtr_->Reset(SET_SIZE);
    }

    AsyncInsertSec = measure<unit>::duration(test_AsyncInsert).count();
    setPtr_->Reset(SET_SIZE);

    ParallelAsyncInsertSec =
        measure<unit>::duration(test_ParallelAsyncInsert).count();
    setPtr_->Reset(SET_SIZE);

    AsyncBufferedInsertMeasure =
        measure<unit>::duration(test_AsyncBufferedInsert).count();

    test_AsyncVisitWithFE();
    std::cout << "Size: " << setPtr_->Size() << std::endl;
    AsyncVisitWithFEMeasure =
        measure<unit>::duration(test_AsyncVisitWithFE).count();

    RawSetMeasureTot += RawSetMeasure;
    ParallelAsyncRawSetMeasureTot += ParallelAsyncRawSetMeasure;
    SerialInsertMeasureTot += SerialInsertMeasure;
    AsyncInsertSecTot += AsyncInsertSec;
    ParallelAsyncInsertSecTot += ParallelAsyncInsertSec;
    AsyncBufferedInsertMeasureTot += AsyncBufferedInsertMeasure;
    AsyncVisitWithFEMeasureTot += AsyncVisitWithFEMeasure;

    resFile << i << " " << RawSetMeasure << " " << ParallelAsyncRawSetMeasure
            << " " << SerialInsertMeasure << " " << AsyncInsertSec << " "
            << ParallelAsyncInsertSec << " " << AsyncBufferedInsertMeasure
            << " " << AsyncVisitWithFEMeasure << " " << std::endl;
  }
  resFile << i << " " << RawSetMeasureTot << " "
          << ParallelAsyncRawSetMeasureTot << " " << SerialInsertMeasureTot
          << " " << AsyncInsertSecTot << " " << ParallelAsyncInsertSecTot << " "
          << AsyncBufferedInsertMeasureTot << " " << AsyncVisitWithFEMeasureTot
          << " " << std::endl;

  std::cout << "\n\n----AVERAGE RESULTS----\n";
  size_t numElements = SET_SIZE * NUM_ITER;
  printResults("STL-Set Serial Insert", RawSetMeasureTot / secUnit,
               numElements);
  printResults("STL-Set Parallel Async Update",
               ParallelAsyncRawSetMeasureTot / secUnit, numElements);
  printResults("Serial Update", SerialInsertMeasureTot / secUnit, numElements);
  printResults("Async Update", AsyncInsertSecTot / secUnit, numElements);
  printResults("Parallel Async Update", ParallelAsyncInsertSecTot / secUnit,
               numElements);
  printResults("Async Buffered Update", AsyncBufferedInsertMeasureTot / secUnit,
               numElements);
  printResults("Async Visit", AsyncVisitWithFEMeasureTot / secUnit,
               numElements);

  resFile.close();
  testFinalize();

  return 0;
}
}  // namespace shad
