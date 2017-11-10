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
#include <unordered_map>

#include "shad/data_structures/hashmap.h"
#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

static size_t MAP_SIZE = 100000;
static size_t NUM_ITER = 20;
static std::string FILE_NAME = "results_map_perf.txt";

using MapT = shad::Hashmap<int, int>;
using unit = std::chrono::microseconds;
static double secUnit = 1000000;
static MapT::SharedPtr mapPtr_;
static std::unordered_map<int, int> stdmap_;

void testInit(int argc, char *argv[]) {
  for (size_t argIndex = 1; argIndex < argc - 1; argIndex++) {
    std::string arg(argv[argIndex]);
    if (arg == "--Size") {
      ++argIndex;
      MAP_SIZE = strtoul(argv[argIndex], nullptr, 0);
    } else if (arg == "--NumIter") {
      ++argIndex;
      NUM_ITER = strtoul(argv[argIndex], nullptr, 0);
    } else if (arg == "--OutFileName") {
      ++argIndex;
      FILE_NAME = std::string(argv[argIndex]);
    }
  }
  std::cout << "\n MAP_SIZE: " << MAP_SIZE << std::endl;
  std::cout << "\n NUM_ITER: " << NUM_ITER << std::endl;
  std::cout << std::endl;
  auto ptr = MapT::Create(MAP_SIZE);
  struct Args {
    MapT::ObjectID oid1;
    size_t as;
  };
  auto propagateLambda = [](const Args &args) {
    MAP_SIZE = args.as;
    mapPtr_ = MapT::GetPtr(args.oid1);
  };
  Args args = {ptr->GetGlobalID(), MAP_SIZE};
  shad::rt::executeOnAll(propagateLambda, args);
}

void testFinalize() { MapT::Destroy(mapPtr_->GetGlobalID()); }

bool fake;

void test_RawMap() {
  for (size_t i = 0; i < MAP_SIZE; i++) {
    stdmap_[i] = i;
  }
}

void test_ParallelAsyncRawMap() {
  auto feLambda = [](shad::rt::Handle &, const bool &, size_t i) {
    stdmap_[i] = i;
  };
  shad::rt::Handle handle;
  shad::rt::asyncForEachAt(handle, shad::rt::thisLocality(), feLambda, fake,
                           MAP_SIZE);
  shad::rt::waitForCompletion(handle);
}

void test_SerialInsert() {
  for (size_t i = 0; i < MAP_SIZE; i++) {
    mapPtr_->Insert(i, i);
  }
}

void applyFun(const int &key, int &elem) { elem = key; }

void test_AsyncInsert() {
  shad::rt::Handle handle;
  for (size_t i = 0; i < MAP_SIZE; i++) {
    mapPtr_->AsyncInsert(handle, i, i);
  }
  shad::rt::waitForCompletion(handle);
}

void test_ParallelAsyncInsert() {
  auto feLambda = [](shad::rt::Handle &handle, const bool &, size_t i) {
    mapPtr_->AsyncInsert(handle, i, i);
  };
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(handle, feLambda, fake, MAP_SIZE);
  shad::rt::waitForCompletion(handle);
}

void test_ParallelAsyncBufferedInsert() {
  auto feLambda = [](shad::rt::Handle &handle, const bool &, size_t i) {
    mapPtr_->BufferedAsyncInsert(handle, i, i);
  };
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(handle, feLambda, fake, MAP_SIZE);
  shad::rt::waitForCompletion(handle);
  mapPtr_->WaitForBufferedInsert();
}

void test_AsyncBufferedInsert() {
  shad::rt::Handle handle;
  for (size_t i = 0; i < MAP_SIZE; i++) {
    mapPtr_->BufferedAsyncInsert(handle, i, i);
  }
  shad::rt::waitForCompletion(handle);
  mapPtr_->WaitForBufferedInsert();
}

static void asyncApplyFun(shad::rt::Handle &, const int &key, int &elem) {
  elem = key;
}

static void asyncFEfun(shad::rt::Handle &, const int &key) {
  // do nothing
}

void test_AsyncUpdateWithApply() {
  shad::rt::Handle handle;
  for (size_t i = 0; i < MAP_SIZE; i++) {
    mapPtr_->AsyncApply(handle, i, asyncApplyFun);
  }
  shad::rt::waitForCompletion(handle);
}

void test_AsyncUpdateWithFE() {
  shad::rt::Handle handle;
  mapPtr_->AsyncForEachEntry(handle, asyncApplyFun);
  shad::rt::waitForCompletion(handle);
}

void test_AsyncFEKey() {
  shad::rt::Handle handle;
  mapPtr_->AsyncForEachKey(handle, asyncFEfun);
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

  size_t RawMapMeasureTot = 0;
  size_t ParallelAsyncRawMapMeasureTot = 0;
  size_t SerialInsertMeasureTot = 0;
  size_t AsyncInsertSecTot = 0;
  size_t ParallelAsyncInsertSecTot = 0;
  size_t AsyncBufferedInsertMeasureTot = 0;
  size_t AsyncUpdateWithFEMeasureTot = 0;
  size_t AsyncFEkeyMeasureTot = 0;

  size_t RawMapMeasure = 0;
  size_t ParallelAsyncRawMapMeasure = 0;
  size_t SerialInsertMeasure = 0;
  size_t AsyncInsertSec = 0;
  size_t ParallelAsyncInsertSec = 0;
  size_t AsyncBufferedInsertMeasure = 0;
  size_t AsyncUpdateWithFEMeasure = 0;
  size_t AsyncFEkeyMeasure = 0;

  size_t i = 0;
  stdmap_ = std::unordered_map<int, int>();
  for (; i < NUM_ITER; i++) {
    if (shad::rt::numLocalities() == 1) {
      RawMapMeasure = measure<unit>::duration(test_RawMap).count();

      ParallelAsyncRawMapMeasure =
          measure<unit>::duration(test_ParallelAsyncRawMap).count();
      stdmap_.clear();

      SerialInsertMeasure = measure<unit>::duration(test_SerialInsert).count();
      stdmap_.clear();
    }

    AsyncInsertSec = measure<unit>::duration(test_AsyncInsert).count();
    mapPtr_->Clear();

    ParallelAsyncInsertSec =
        measure<unit>::duration(test_ParallelAsyncInsert).count();
    mapPtr_->Clear();

    AsyncBufferedInsertMeasure =
        measure<unit>::duration(test_ParallelAsyncBufferedInsert).count();

    std::cout << "Size: " << mapPtr_->Size() << std::endl;
    AsyncUpdateWithFEMeasure =
        measure<unit>::duration(test_AsyncUpdateWithFE).count();

    AsyncFEkeyMeasure = measure<unit>::duration(test_AsyncFEKey).count();

    RawMapMeasureTot += RawMapMeasure;
    ParallelAsyncRawMapMeasureTot += ParallelAsyncRawMapMeasure;
    SerialInsertMeasureTot += SerialInsertMeasure;
    AsyncInsertSecTot += AsyncInsertSec;
    ParallelAsyncInsertSecTot += ParallelAsyncInsertSec;
    AsyncBufferedInsertMeasureTot += AsyncBufferedInsertMeasure;
    AsyncUpdateWithFEMeasureTot += AsyncUpdateWithFEMeasure;
    AsyncFEkeyMeasureTot += AsyncFEkeyMeasure;

    resFile << i << " " << RawMapMeasure << " " << ParallelAsyncRawMapMeasure
            << " " << SerialInsertMeasure << " " << AsyncInsertSec << " "
            << ParallelAsyncInsertSec << " " << AsyncBufferedInsertMeasure
            << " " << AsyncUpdateWithFEMeasure << " " << AsyncFEkeyMeasure
            << " " << std::endl;
  }
  resFile << i << " " << RawMapMeasureTot << " "
          << ParallelAsyncRawMapMeasureTot << " " << SerialInsertMeasureTot
          << " " << AsyncInsertSecTot << " " << ParallelAsyncInsertSecTot << " "
          << AsyncBufferedInsertMeasureTot << " " << AsyncUpdateWithFEMeasureTot
          << " " << AsyncFEkeyMeasure << " " << std::endl;

  std::cout << "\n\n----AVERAGE RESULTS----\n";
  size_t numElements = MAP_SIZE * NUM_ITER;
  printResults("STL-Map Serial Insert", RawMapMeasureTot / secUnit,
               numElements);
  printResults("STL-Map Parallel Async Update",
               ParallelAsyncRawMapMeasureTot / secUnit, numElements);
  printResults("Serial Update", SerialInsertMeasureTot / secUnit, numElements);
  printResults("Async Update", AsyncInsertSecTot / secUnit, numElements);
  printResults("Parallel Async Update", ParallelAsyncInsertSecTot / secUnit,
               numElements);
  printResults("Async Buffered Update", AsyncBufferedInsertMeasureTot / secUnit,
               numElements);
  printResults("Async For Each Update", AsyncUpdateWithFEMeasureTot / secUnit,
               numElements);
  printResults("Async For Each Key", AsyncFEkeyMeasureTot / secUnit,
               numElements);

  resFile.close();
  testFinalize();

  return 0;
}
}  // namespace shad
