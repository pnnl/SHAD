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

void testInit(int argc, char *argv[]) {
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

void testFinalize() { ArrayT::Destroy(arrayPtr_->GetGlobalID()); }

bool fake;

void test_RawArray(int *ptr) {
  for (size_t i = 0; i < ARRAY_SIZE; i++) {
    ptr[i] = i;
  }
}

void test_ParallelAsyncRawArray() {
  auto feLambda = [](shad::rt::Handle &, const bool &, size_t i) {
    rawptr[i] = i;
  };
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(), feLambda, fake,
                           ARRAY_SIZE);
  shad::rt::waitForCompletion(handle);
}

void test_SerialUpdate() {
  for (size_t i = 0; i < ARRAY_SIZE; i++) {
    arrayPtr_->InsertAt(i, i);
  }
}

void applyFun(size_t i, int &elem) { elem = i; }

void test_AsyncUpdate() {
  shad::rt::Handle handle;
  for (size_t i = 0; i < ARRAY_SIZE; i++) {
    arrayPtr_->AsyncInsertAt(handle, i, i);
  }
  shad::rt::waitForCompletion(handle);
}

void test_ParallelAsyncUpdate() {
  auto feLambda = [](shad::rt::Handle &handle, const bool &, size_t i) {
    arrayPtr_->AsyncInsertAt(handle, i, i);
  };
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(handle, feLambda, fake, ARRAY_SIZE);
  shad::rt::waitForCompletion(handle);
}

void test_ParallelAsyncBufferedUpdate() {
  auto feLambda = [](shad::rt::Handle &handle, const bool &, size_t i) {
    arrayPtr_->BufferedAsyncInsertAt(handle, i, i);
  };
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(handle, feLambda, fake, ARRAY_SIZE);
  shad::rt::waitForCompletion(handle);
  arrayPtr_->WaitForBufferedInsert();
}

void test_AsyncBufferedUpdate() {
  shad::rt::Handle handle;
  for (size_t i = 0; i < ARRAY_SIZE; i++) {
    arrayPtr_->BufferedAsyncInsertAt(handle, i, i);
  }
  shad::rt::waitForCompletion(handle);
  arrayPtr_->WaitForBufferedInsert();
}

void asyncApplyFun(shad::rt::Handle &, size_t, int &elem, bool &) { elem++; }
void asyncApplyFun2(shad::rt::Handle &, size_t, int &elem) { elem++; }
void test_AsyncUpdateWithApply() {
  shad::rt::Handle handle;
  for (size_t i = 0; i < ARRAY_SIZE; i++) {
    arrayPtr_->AsyncApply(handle, i, asyncApplyFun, fake);
  }
  shad::rt::waitForCompletion(handle);
}

void test_AsyncUpdateWithFE() {
  shad::rt::Handle handle;
  arrayPtr_->AsyncForEach(handle, asyncApplyFun2);
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

  size_t RawArrayMeasureTot = 0;
  size_t ParallelAsyncRawArrayMeasureTot = 0;
  size_t SerialUpdateMeasureTot = 0;
  size_t AsyncUpdateSecTot = 0;
  size_t ParallelAsyncUpdateSecTot = 0;
  size_t AsyncBufferedUpdateMeasureTot = 0;
  size_t AsyncUpdateWithFEMeasureTot = 0;

  size_t RawArrayMeasure = 0;
  size_t ParallelAsyncRawArrayMeasure = 0;
  size_t SerialUpdateMeasure = 0;
  size_t AsyncUpdateSec = 0;
  size_t ParallelAsyncUpdateSec = 0;
  size_t AsyncBufferedUpdateMeasure = 0;
  size_t AsyncUpdateWithFEMeasure = 0;

  size_t i = 0;
  std::vector<int> array(ARRAY_SIZE, 0);
  if (shad::rt::numLocalities() == 1) {
    rawptr = array.data();
  }

  for (; i < NUM_ITER; i++) {
    if (shad::rt::numLocalities() == 1) {
      RawArrayMeasure = measure<unit>::duration(test_RawArray, rawptr).count();

      ParallelAsyncRawArrayMeasure =
          measure<unit>::duration(test_ParallelAsyncRawArray).count();

      SerialUpdateMeasure = measure<unit>::duration(test_SerialUpdate).count();
    }

    AsyncUpdateSec = measure<unit>::duration(test_AsyncUpdate).count();

    ParallelAsyncUpdateSec =
        measure<unit>::duration(test_ParallelAsyncUpdate).count();

    AsyncBufferedUpdateMeasure =
        measure<unit>::duration(test_AsyncBufferedUpdate).count();

    AsyncUpdateWithFEMeasure =
        measure<unit>::duration(test_AsyncUpdateWithFE).count();

    RawArrayMeasureTot += RawArrayMeasure;
    ParallelAsyncRawArrayMeasureTot += ParallelAsyncRawArrayMeasure;
    SerialUpdateMeasureTot += SerialUpdateMeasure;
    AsyncUpdateSecTot += AsyncUpdateSec;
    ParallelAsyncUpdateSecTot += ParallelAsyncUpdateSec;
    AsyncBufferedUpdateMeasureTot += AsyncBufferedUpdateMeasure;
    AsyncUpdateWithFEMeasureTot += AsyncUpdateWithFEMeasure;

    resFile << i << " " << RawArrayMeasure << " "
            << ParallelAsyncRawArrayMeasure << " " << SerialUpdateMeasure << " "
            << AsyncUpdateSec << " " << ParallelAsyncUpdateSec << " "
            << AsyncBufferedUpdateMeasure << " " << AsyncUpdateWithFEMeasure
            << " " << std::endl;
  }
  resFile << i << " " << RawArrayMeasureTot << " "
          << ParallelAsyncRawArrayMeasureTot << " " << SerialUpdateMeasureTot
          << " " << AsyncUpdateSecTot << " " << ParallelAsyncUpdateSecTot << " "
          << AsyncBufferedUpdateMeasureTot << " " << AsyncUpdateWithFEMeasureTot
          << " " << std::endl;

  std::cout << "\n\n----AVERAGE RESULTS----\n";
  size_t numElements = ARRAY_SIZE * NUM_ITER;
  printResults("C-Array Serial Update", RawArrayMeasureTot / secUnit,
               numElements);
  printResults("C-Array Parallel Async Update",
               ParallelAsyncRawArrayMeasureTot / secUnit, numElements);
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
