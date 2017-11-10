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

#include "shad/data_structures/vector.h"
#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

static size_t VECTOR_SIZE = 100000;
static size_t NUM_ITER = 20;
static std::string FILE_NAME = "results_vector_perf.txt";

using VectorT = shad::Vector<int>;
using unit = std::chrono::microseconds;
static double secUnit = 1000000;
static shad::AbstractDataStructure<VectorT>::SharedPtr vectorPtr_;
static std::vector<int> stdvector_;

void testInit(int argc, char *argv[]) {
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
  auto ptr = VectorT::Create(VECTOR_SIZE);
  struct Args {
    VectorT::ObjectID oid1;
    size_t as;
  };
  auto propagateLambda = [](const Args &args) {
    VECTOR_SIZE = args.as;
    vectorPtr_ = VectorT::GetPtr(args.oid1);
  };
  Args args = {ptr->GetGlobalID(), VECTOR_SIZE};
  shad::rt::executeOnAll(propagateLambda, args);
}

void testFinalize() { VectorT::Destroy(vectorPtr_->GetGlobalID()); }

void test_RawVector() {
  for (size_t i = 0; i < VECTOR_SIZE; i++) {
    stdvector_[i] = i;
  }
}

void test_ParallelAsyncRawVector() {
  auto feLambda = [](shad::rt::Handle &, const bool &, size_t i) {
    stdvector_[i] = i;
  };
  shad::rt::Handle handle;
  bool fake = 0;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(), feLambda, fake,
                           VECTOR_SIZE);
  shad::rt::waitForCompletion(handle);
}

void test_SerialUpdate() {
  for (size_t i = 0; i < VECTOR_SIZE; i++) {
    vectorPtr_->InsertAt(i, i);
  }
}

void applyFun(size_t i, int &elem) { elem = i; }

void test_AsyncUpdate() {
  shad::rt::Handle handle;
  for (size_t i = 0; i < VECTOR_SIZE; i++) {
    vectorPtr_->AsyncInsertAt(handle, i, i);
  }
  shad::rt::waitForCompletion(handle);
}

void test_ParallelAsyncUpdate() {
  auto feLambda = [](shad::rt::Handle &handle, const bool &, size_t i) {
    vectorPtr_->AsyncInsertAt(handle, i, i);
  };
  shad::rt::Handle handle;
  bool fake = 0;
  shad::rt::asyncForEachOnAll(handle, feLambda, fake, VECTOR_SIZE);
  shad::rt::waitForCompletion(handle);
}

void test_ParallelAsyncBufferedUpdate() {
  auto feLambda = [](shad::rt::Handle &handle, const bool &, size_t i) {
    vectorPtr_->BufferedAsyncInsertAt(handle, i, i);
  };
  shad::rt::Handle handle;
  bool fake = 0;
  shad::rt::asyncForEachOnAll(handle, feLambda, fake, VECTOR_SIZE);
  shad::rt::waitForCompletion(handle);
  vectorPtr_->WaitForBufferedInsert();
}

void test_AsyncBufferedUpdate() {
  shad::rt::Handle handle;
  for (size_t i = 0; i < VECTOR_SIZE; i++) {
    vectorPtr_->BufferedAsyncInsertAt(handle, i, i);
  }
  shad::rt::waitForCompletion(handle);
  vectorPtr_->WaitForBufferedInsert();
}

static void asyncApplyFun(shad::rt::Handle &, shad::Vector<int>::size_type i,
                          int &elem) {
  elem = i;
}
void test_AsyncUpdateWithApply() {
  shad::rt::Handle handle;
  for (size_t i = 0; i < VECTOR_SIZE; i++) {
    vectorPtr_->AsyncApply(handle, i, asyncApplyFun);
  }
  shad::rt::waitForCompletion(handle);
}

void test_AsyncUpdateWithFE() {
  shad::rt::Handle handle;
  vectorPtr_->AsyncForEachInRange(handle, 0, VECTOR_SIZE, asyncApplyFun);
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

  size_t RawVectorMeasureTot = 0;
  size_t ParallelAsyncRawVectorMeasureTot = 0;
  size_t SerialUpdateMeasureTot = 0;
  size_t AsyncUpdateSecTot = 0;
  size_t ParallelAsyncUpdateSecTot = 0;
  size_t AsyncBufferedUpdateMeasureTot = 0;
  size_t AsyncUpdateWithFEMeasureTot = 0;

  size_t RawVectorMeasure = 0;
  size_t ParallelAsyncRawVectorMeasure = 0;
  size_t SerialUpdateMeasure = 0;
  size_t AsyncUpdateSec = 0;
  size_t ParallelAsyncUpdateSec = 0;
  size_t AsyncBufferedUpdateMeasure = 0;
  size_t AsyncUpdateWithFEMeasure = 0;

  size_t i = 0;
  stdvector_ = std::vector<int>(VECTOR_SIZE, 0);
  for (; i < NUM_ITER; i++) {
    if (shad::rt::numLocalities() == 1) {
      RawVectorMeasure = measure<unit>::duration(test_RawVector).count();

      ParallelAsyncRawVectorMeasure =
          measure<unit>::duration(test_ParallelAsyncRawVector).count();

      SerialUpdateMeasure = measure<unit>::duration(test_SerialUpdate).count();
    }

    AsyncUpdateSec = measure<unit>::duration(test_AsyncUpdate).count();

    ParallelAsyncUpdateSec =
        measure<unit>::duration(test_ParallelAsyncUpdate).count();

    AsyncBufferedUpdateMeasure =
        measure<unit>::duration(test_AsyncBufferedUpdate).count();

    AsyncUpdateWithFEMeasure =
        measure<unit>::duration(test_AsyncUpdateWithFE).count();

    RawVectorMeasureTot += RawVectorMeasure;
    ParallelAsyncRawVectorMeasureTot += ParallelAsyncRawVectorMeasure;
    SerialUpdateMeasureTot += SerialUpdateMeasure;
    AsyncUpdateSecTot += AsyncUpdateSec;
    ParallelAsyncUpdateSecTot += ParallelAsyncUpdateSec;
    AsyncBufferedUpdateMeasureTot += AsyncBufferedUpdateMeasure;
    AsyncUpdateWithFEMeasureTot += AsyncUpdateWithFEMeasure;

    resFile << i << " " << RawVectorMeasure << " "
            << ParallelAsyncRawVectorMeasure << " " << SerialUpdateMeasure
            << " " << AsyncUpdateSec << " " << ParallelAsyncUpdateSec << " "
            << AsyncBufferedUpdateMeasure << " " << AsyncUpdateWithFEMeasure
            << " " << std::endl;
  }
  resFile << i << " " << RawVectorMeasureTot << " "
          << ParallelAsyncRawVectorMeasureTot << " " << SerialUpdateMeasureTot
          << " " << AsyncUpdateSecTot << " " << ParallelAsyncUpdateSecTot << " "
          << AsyncBufferedUpdateMeasureTot << " " << AsyncUpdateWithFEMeasureTot
          << " " << std::endl;

  std::cout << "\n\n----AVERAGE RESULTS----\n";
  size_t numElements = VECTOR_SIZE * NUM_ITER;
  printResults("C-Vector Serial Update", RawVectorMeasureTot / secUnit,
               numElements);
  printResults("C-Vector Parallel Async Update",
               ParallelAsyncRawVectorMeasureTot / secUnit, numElements);
  printResults("Serial Update", SerialUpdateMeasureTot / secUnit, numElements);
  printResults("Async Update", AsyncUpdateSecTot / secUnit, numElements);
  printResults("Parallel Async Update", ParallelAsyncUpdateSecTot / secUnit,
               numElements);
  printResults("Async Buffered Update", AsyncBufferedUpdateMeasureTot / secUnit,
               numElements);
  printResults("Async For Each Update", AsyncUpdateWithFEMeasureTot / secUnit,
               numElements);

  resFile.close();
  testFinalize();

  return 0;
}
}  // namespace shad
