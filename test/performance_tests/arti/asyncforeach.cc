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
#include <iostream>
#include <random>
#include <thread>

#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

std::atomic<int> globalCounter;

struct exData {
  char c[4000];
};

void testFunctionAsyncForEachAt(shad::rt::Handle &, const exData &data,
                                size_t i) {
  globalCounter += data.c[0] + data.c[1];
}

void test_asyncForEachAt(size_t numTasks, size_t numIteration) {
  exData data{"hello"};

  shad::rt::Handle handle;
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::asyncForEachAt(handle,
                             shad::rt::Locality(i % shad::rt::numLocalities()),
                             testFunctionAsyncForEachAt, data, numIteration);

  shad::rt::waitForCompletion(handle);
}

void testFunctionAsyncForEachAtInputBuffer(shad::rt::Handle &,
                                           const uint8_t *data,
                                           const uint32_t size, size_t i) {
  globalCounter += data[0] + data[1];
}

void test_asyncForEachAtInputBuffer(size_t numTasks, size_t numIteration) {
  std::shared_ptr<uint8_t> data(new uint8_t[2]{1, 2},
                                std::default_delete<uint8_t[]>());

  shad::rt::Handle handle;
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::asyncForEachAt(
        handle, shad::rt::Locality(i % shad::rt::numLocalities()),
        testFunctionAsyncForEachAtInputBuffer, data, 2, numIteration);

  shad::rt::waitForCompletion(handle);
}

void test_asyncForEachOnAll(size_t numTasks, size_t numIteration) {
  exData data{"hello"};

  shad::rt::Handle handle;
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::asyncForEachOnAll(handle, testFunctionAsyncForEachAt, data,
                                numIteration);

  shad::rt::waitForCompletion(handle);
}

void test_asyncForEachOnAllInputBuffer(size_t numTasks, size_t numIteration) {
  std::shared_ptr<uint8_t> data(new uint8_t[2]{1, 2},
                                std::default_delete<uint8_t[]>());
  shad::rt::Handle handle;
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::asyncForEachOnAll(handle, testFunctionAsyncForEachAtInputBuffer,
                                data, 2, numIteration);

  shad::rt::waitForCompletion(handle);
}

namespace shad {

int main(int argc, char *argv[]) {
  const size_t numTasks = 100000;
  const size_t loopIterations = 1000;

  for (int i = 0; i < 15; i++) {
    auto AsyncForEachAtTime =
        measure<>::duration(test_asyncForEachAt, numTasks, loopIterations);
    auto AsyncForEachAtInputBufferTime = measure<>::duration(
        test_asyncForEachAtInputBuffer, numTasks, loopIterations);
    auto AsyncForEachOnAllTime =
        measure<>::duration(test_asyncForEachOnAll, numTasks, loopIterations);
    auto AsyncForEachOnAllInputBufferTime = measure<>::duration(
        test_asyncForEachOnAllInputBuffer, numTasks, loopIterations);
  }

  for (int i = 0; i < 100; i++) {
    auto AsyncForEachAtTime =
        measure<>::duration(test_asyncForEachAt, numTasks, loopIterations);
    auto AsyncForEachAtInputBufferTime = measure<>::duration(
        test_asyncForEachAtInputBuffer, numTasks, loopIterations);
    auto AsyncForEachOnAllTime =
        measure<>::duration(test_asyncForEachOnAll, numTasks, loopIterations);
    auto AsyncForEachOnAllInputBufferTime = measure<>::duration(
        test_asyncForEachOnAllInputBuffer, numTasks, loopIterations);

    std::cout << i << " " << AsyncForEachAtTime.count() << " "
              << AsyncForEachAtInputBufferTime.count() << " "
              << AsyncForEachOnAllTime.count() << " "
              << AsyncForEachOnAllInputBufferTime.count() << std::endl;
  }

  return 0;
}
}  // namespace shad
