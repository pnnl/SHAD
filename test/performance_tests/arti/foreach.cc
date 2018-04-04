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

#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

std::atomic<int> globalCounter;

struct exData {
  char c[4000];
};

void testFunctionForEachAt(const exData &data, size_t i) {
  globalCounter += data.c[0] + data.c[1];
}

void test_forEachAt(size_t numTasks, size_t numIteration) {
  exData data{"hello"};

  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::forEachAt(shad::rt::Locality(i % shad::rt::numLocalities()),
                        testFunctionForEachAt, data, numIteration);
}

void testFunctionForEachAtInputBuffer(const uint8_t *data, const uint32_t size,
                                      size_t i) {
  globalCounter += data[0] + data[1];
}

void test_forEachAtInputBuffer(size_t numTasks, size_t numIteration) {
  exData value{1, 2};
  std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)],
                                std::default_delete<uint8_t[]>());
  std::memcpy(data.get(), &value, sizeof(exData));

  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::forEachAt(shad::rt::Locality(i % shad::rt::numLocalities()),
                        testFunctionForEachAtInputBuffer, data, sizeof(data),
                        numIteration);
}

void test_forEachOnAll(size_t numTasks, size_t numIteration) {
  exData data{"hello"};

  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::forEachOnAll(testFunctionForEachAt, data, numIteration);
}

void test_forEachOnAllInputBuffer(size_t numTasks, size_t numIteration) {
  exData value{1, 2};
  std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)],
                                std::default_delete<uint8_t[]>());
  std::memcpy(data.get(), &value, sizeof(exData));

  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::forEachOnAll(testFunctionForEachAtInputBuffer, data, sizeof(data),
                           numIteration);
}

namespace shad {

int main(int argc, char *argv[]) {
  const size_t numTasks = 1000;
  const size_t loopIterations = 1000;

  for (int i = 0; i < 15; i++) {
    auto ForEachAtTime =
        measure<>::duration(test_forEachAt, numTasks, loopIterations);
    auto ForEachAtInputBufferTime = measure<>::duration(
        test_forEachAtInputBuffer, numTasks, loopIterations);
    auto ForEachOnAllTime =
        measure<>::duration(test_forEachOnAll, numTasks, loopIterations);
    auto ForEachOnAllInputBufferTime = measure<>::duration(
        test_forEachOnAllInputBuffer, numTasks, loopIterations);
  }

  for (int i = 0; i < 100; i++) {
    auto ForEachAtTime =
        measure<>::duration(test_forEachAt, numTasks, loopIterations);
    auto ForEachAtInputBufferTime = measure<>::duration(
        test_forEachAtInputBuffer, numTasks, loopIterations);
    auto ForEachOnAllTime =
        measure<>::duration(test_forEachOnAll, numTasks, loopIterations);
    auto ForEachOnAllInputBufferTime = measure<>::duration(
        test_forEachOnAllInputBuffer, numTasks, loopIterations);

    std::cout << i << " " << ForEachAtTime.count() << " "
              << ForEachAtInputBufferTime.count() << " "
              << ForEachOnAllTime.count() << " "
              << ForEachOnAllInputBufferTime.count() << std::endl;
  }

  return 0;
}
}  // namespace shad
