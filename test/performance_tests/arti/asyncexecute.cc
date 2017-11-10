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
#include <cstring>
#include <iostream>
#include <random>
#include <thread>

#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

std::atomic<int> globalCounter;

struct exData {
  char c[4040];
};

struct retData {
  char c[2048];
};

void testFunctionAsyncExecuteAt(shad::rt::Handle &, const exData &data) {
  globalCounter += data.c[0] + data.c[1];
}

void test_asyncExecuteAt(size_t numTasks) {
  exData data{"hello"};

  shad::rt::Handle handle;
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::asyncExecuteAt(handle,
                             shad::rt::Locality(i % shad::rt::numLocalities()),
                             testFunctionAsyncExecuteAt, data);

  shad::rt::waitForCompletion(handle);
}

void testFunctionAsyncExecuteAtInputBuffer(shad::rt::Handle &,
                                           const uint8_t *data,
                                           const uint32_t size) {
  globalCounter += data[0] + data[1];
}

void test_asyncExecuteAtInputBuffer(size_t numTasks) {
  exData value{1, 2};
  std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)],
                                std::default_delete<uint8_t[]>());
  std::memcpy(data.get(), &value, sizeof(exData));

  shad::rt::Handle handle;
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::asyncExecuteAt(
        handle, shad::rt::Locality(i % shad::rt::numLocalities()),
        testFunctionAsyncExecuteAtInputBuffer, data, sizeof(exData));

  shad::rt::waitForCompletion(handle);
}

void testFunctionAsyncExecuteAtWithRetBuff(shad::rt::Handle &,
                                           const exData &data, uint8_t *,
                                           uint32_t *size) {
  globalCounter += data.c[0] + data.c[1];
  *size = 2048;
}

void test_asyncExecuteAtWithRetBuff(size_t numTasks) {
  exData data{"hello"};

  uint8_t buffer[2048];
  uint32_t size;

  shad::rt::Handle handle;
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::asyncExecuteAtWithRetBuff(
        handle, shad::rt::Locality(i % shad::rt::numLocalities()),
        testFunctionAsyncExecuteAtWithRetBuff, data, buffer, &size);

  shad::rt::waitForCompletion(handle);
}

void testFunctionAsyncExecuteAtWithRetBuffInputBuffer(shad::rt::Handle &,
                                                      const uint8_t *data,
                                                      const uint32_t, uint8_t *,
                                                      uint32_t *size) {
  globalCounter += data[0] + data[1];
  *size = 2048;
}

void test_asyncExecuteAtWithRetBuffInputBuffer(size_t numTasks) {
  std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)]{1, 2},
                                std::default_delete<uint8_t[]>());

  uint8_t buffer[2048];
  uint32_t size;

  shad::rt::Handle handle;
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::asyncExecuteAtWithRetBuff(
        handle, shad::rt::Locality(i % shad::rt::numLocalities()),
        testFunctionAsyncExecuteAtWithRetBuffInputBuffer, data, sizeof(data),
        buffer, &size);

  shad::rt::waitForCompletion(handle);
}

void testFunctionAsyncExecuteAtWithRet(shad::rt::Handle &, const exData &data,
                                       retData *ret) {
  globalCounter += data.c[0] + data.c[1];
  memcpy(ret, &data, sizeof(retData));
}

void test_asyncExecuteAtWithRet(size_t numTasks) {
  exData data{"hello"};

  retData ret;
  shad::rt::Handle handle;
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::asyncExecuteAtWithRet(
        handle, shad::rt::Locality(i % shad::rt::numLocalities()),
        testFunctionAsyncExecuteAtWithRet, data, &ret);

  shad::rt::waitForCompletion(handle);
}

void testFunctionAsyncExecuteAtWithRetInputBuffer(shad::rt::Handle &,
                                                  const uint8_t *data,
                                                  const uint32_t,
                                                  retData *ret) {
  globalCounter += data[0] + data[1];
  memcpy(ret, data, sizeof(retData));
}

void test_asyncExecuteAtWithRetInputBuffer(size_t numTasks) {
  exData value{1, 2};
  std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)],
                                std::default_delete<uint8_t[]>());

  retData ret;
  shad::rt::Handle handle;
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::asyncExecuteAtWithRet(
        handle, shad::rt::Locality(i % shad::rt::numLocalities()),
        testFunctionAsyncExecuteAtWithRetInputBuffer, data, sizeof(data), &ret);

  shad::rt::waitForCompletion(handle);
}

void test_asyncExecuteOnAll(size_t numTasks) {
  exData data{"hello"};
  shad::rt::Handle handle;
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::asyncExecuteOnAll(handle, testFunctionAsyncExecuteAt, data);

  shad::rt::waitForCompletion(handle);
}

void test_asyncExecuteOnAllInputBuffer(size_t numTasks) {
  std::shared_ptr<uint8_t> data(new uint8_t[2]{1, 2},
                                std::default_delete<uint8_t[]>());

  shad::rt::Handle handle;
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::asyncExecuteOnAll(handle, testFunctionAsyncExecuteAtInputBuffer,
                                data, 2);

  shad::rt::waitForCompletion(handle);
}

namespace shad {

int main(int argc, char *argv[]) {
  const size_t numTasks = 100000;

  for (int i = 0; i < 15; i++) {
    auto asyncExecuteAtTime =
        measure<>::duration(test_asyncExecuteAt, numTasks);
    auto asyncExecuteAtWithRetTime =
        measure<>::duration(test_asyncExecuteAtWithRet, numTasks);
    auto asyncExecuteAtWithRetBuffTime =
        measure<>::duration(test_asyncExecuteAtWithRetBuff, numTasks);
    auto asyncExecuteAtInputBufferTime =
        measure<>::duration(test_asyncExecuteAtInputBuffer, numTasks);
    auto asyncExecuteAtWithRetInputBufferTime =
        measure<>::duration(test_asyncExecuteAtWithRetInputBuffer, numTasks);
    auto asyncExecuteAtWithRetBuffInputBufferTime = measure<>::duration(
        test_asyncExecuteAtWithRetBuffInputBuffer, numTasks);
    auto asyncExecuteOnAllTime =
        measure<>::duration(test_asyncExecuteOnAll, numTasks);
    auto asyncExecuteOnAllInputBufferTime =
        measure<>::duration(test_asyncExecuteOnAllInputBuffer, numTasks);
  }

  for (int i = 0; i < 100; i++) {
    auto asyncExecuteAtTime =
        measure<>::duration(test_asyncExecuteAt, numTasks);
    std::cout << "#### asyncExecuteAt" << std::endl;

    auto asyncExecuteAtWithRetTime =
        measure<>::duration(test_asyncExecuteAtWithRet, numTasks);
    std::cout << "#### asyncExecuteAtWithRet" << std::endl;

    auto asyncExecuteAtWithRetBuffTime =
        measure<>::duration(test_asyncExecuteAtWithRetBuff, numTasks);
    std::cout << "#### asyncExecuteAtWithRetBuff" << std::endl;

    auto asyncExecuteAtInputBufferTime =
        measure<>::duration(test_asyncExecuteAtInputBuffer, numTasks);
    std::cout << "#### asyncExecuteAtInputBuffer" << std::endl;

    auto asyncExecuteAtWithRetInputBufferTime =
        measure<>::duration(test_asyncExecuteAtWithRetInputBuffer, numTasks);
    std::cout << "#### asyncExecuteAtWithRetInputBuffer" << std::endl;

    auto asyncExecuteAtWithRetBuffInputBufferTime = measure<>::duration(
        test_asyncExecuteAtWithRetBuffInputBuffer, numTasks);
    std::cout << "#### asyncExecuteAtWithretBuffInputBuffer" << std::endl;

    auto asyncExecuteOnAllTime =
        measure<>::duration(test_asyncExecuteOnAll, numTasks);
    std::cout << "#### asyncExecuteOnAll" << std::endl;

    auto asyncExecuteOnAllInputBufferTime =
        measure<>::duration(test_asyncExecuteOnAllInputBuffer, numTasks);
    std::cout << "#### asyncExecuteOnAllInputBuffer" << std::endl;

    std::cout << i << " " << asyncExecuteAtTime.count() << " "
              << asyncExecuteAtWithRetTime.count() << " "
              << asyncExecuteAtWithRetBuffTime.count() << " "
              << asyncExecuteAtInputBufferTime.count() << " "
              << asyncExecuteAtWithRetInputBufferTime.count() << " "
              << asyncExecuteAtWithRetBuffInputBufferTime.count() << " "
              << asyncExecuteOnAllTime.count() << " "
              << asyncExecuteOnAllInputBufferTime.count() << std::endl;
  }

  return 0;
}
}  // namespace shad
