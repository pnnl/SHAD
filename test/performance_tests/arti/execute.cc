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
#include <fstream>
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

void testFunctionExecuteAt(const exData &data) {
  globalCounter += data.c[0] + data.c[1];
}

void test_executeAt(size_t numTasks) {
  exData data{"hello"};

  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::executeAt(shad::rt::Locality(i % shad::rt::numLocalities()),
                        testFunctionExecuteAt, data);
}

void testFunctionExecuteAtInputBuffer(const uint8_t *data,
                                      const uint32_t size) {
  globalCounter += data[0] + data[1];
}

void test_executeAtInputBuffer(size_t numTasks) {
  std::shared_ptr<uint8_t> data(new uint8_t[4040],
                                std::default_delete<uint8_t[]>());
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::executeAt(shad::rt::Locality(i % shad::rt::numLocalities()),
                        testFunctionExecuteAtInputBuffer, data, sizeof(data));
}

void testFunctionExecuteAtWithRetBuff(const exData &data, uint8_t *,
                                      uint32_t *size) {
  globalCounter += data.c[0] + data.c[1];
  *size = 2048;
}

void test_executeAtWithRetBuff(size_t numTasks) {
  exData data{"hello"};

  uint8_t buffer[2048];
  uint32_t size;

  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::executeAtWithRetBuff(
        shad::rt::Locality(i % shad::rt::numLocalities()),
        testFunctionExecuteAtWithRetBuff, data, buffer, &size);
}

void testFunctionExecuteAtWithRetBuffInputBuffer(const uint8_t *data,
                                                 const uint32_t, uint8_t *ret,
                                                 uint32_t *size) {
  globalCounter += data[0] + data[1];
  memcpy(ret, &data, sizeof(retData));
  *size = 2048;
}

void test_executeAtWithRetBuffInputBuffer(size_t numTasks) {
  std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)]{1, 2},
                                std::default_delete<uint8_t[]>());
  uint8_t buffer[2048];
  uint32_t size;

  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::executeAtWithRetBuff(
        shad::rt::Locality(i % shad::rt::numLocalities()),
        testFunctionExecuteAtWithRetBuffInputBuffer, data, sizeof(data), buffer,
        &size);
}

void testFunctionExecuteAtWithRet(const exData &data, retData *ret) {
  globalCounter += data.c[0] + data.c[1];
  memcpy(ret, &data, sizeof(retData));
}

void test_executeAtWithRet(size_t numTasks) {
  exData data{"hello"};

  retData ret;
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::executeAtWithRet(
        shad::rt::Locality(i % shad::rt::numLocalities()),
        testFunctionExecuteAtWithRet, data, &ret);
}

void testFunctionExecuteAtWithRetInputBuffer(const uint8_t *data,
                                             const uint32_t, retData *ret) {
  globalCounter += data[0] + data[1];
  memcpy(ret, data, sizeof(retData));
}

void test_executeAtWithRetInputBuffer(size_t numTasks) {
  std::shared_ptr<uint8_t> data(new uint8_t[4040],
                                std::default_delete<uint8_t[]>());

  retData ret;
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::executeAtWithRet(
        shad::rt::Locality(i % shad::rt::numLocalities()),
        testFunctionExecuteAtWithRetInputBuffer, data, 4040, &ret);
}

void test_executeOnAll(size_t numTasks) {
  exData data{"hello"};
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::executeOnAll(testFunctionExecuteAt, data);
}

void test_executeOnAllInputBuffer(size_t numTasks) {
  std::shared_ptr<uint8_t> data(new uint8_t[sizeof(exData)],
                                std::default_delete<uint8_t[]>());
  new (data.get()) exData{1, 2};
  for (size_t i = 0; i < numTasks; ++i)
    shad::rt::executeOnAll(testFunctionExecuteAtInputBuffer, data,
                           sizeof(data));
}

namespace shad {

int main(int argc, char *argv[]) {
  const size_t numTasks = 10000;

  // Avoid cold start...
  for (int i = 0; i < 15; i++) {
    auto executeAtTime = measure<>::duration(test_executeAt, numTasks);
    auto executeAtWithRetTime =
        measure<>::duration(test_executeAtWithRet, numTasks);
    auto executeAtWithRetBuffTime =
        measure<>::duration(test_executeAtWithRetBuff, numTasks);
    auto executeAtInputBufferTime =
        measure<>::duration(test_executeAtInputBuffer, numTasks);
    auto executeAtWithRetInputBufferTime =
        measure<>::duration(test_executeAtWithRetInputBuffer, numTasks);
    auto executeAtWithRetBuffInputBufferTime =
        measure<>::duration(test_executeAtWithRetBuffInputBuffer, numTasks);
    auto executeOnAllTime = measure<>::duration(test_executeOnAll, numTasks);
    auto executeOnAllInputBufferTime =
        measure<>::duration(test_executeOnAllInputBuffer, numTasks);
  }

  for (int i = 0; i < 100; i++) {
    auto executeAtTime = measure<>::duration(test_executeAt, numTasks);
    auto executeAtWithRetTime =
        measure<>::duration(test_executeAtWithRet, numTasks);
    auto executeAtWithRetBuffTime =
        measure<>::duration(test_executeAtWithRetBuff, numTasks);
    auto executeAtInputBufferTime =
        measure<>::duration(test_executeAtInputBuffer, numTasks);
    auto executeAtWithRetInputBufferTime =
        measure<>::duration(test_executeAtWithRetInputBuffer, numTasks);
    auto executeAtWithRetBuffInputBufferTime =
        measure<>::duration(test_executeAtWithRetBuffInputBuffer, numTasks);
    auto executeOnAllTime = measure<>::duration(test_executeOnAll, numTasks);
    auto executeOnAllInputBufferTime =
        measure<>::duration(test_executeOnAllInputBuffer, numTasks);

    std::cout << i << " " << executeAtTime.count() << " "
              << executeAtWithRetTime.count() << " "
              << executeAtWithRetBuffTime.count() << " "
              << executeAtInputBufferTime.count() << " "
              << executeAtWithRetInputBufferTime.count() << " "
              << executeAtWithRetBuffInputBufferTime.count() << " "
              << executeOnAllTime.count() << " "
              << executeOnAllInputBufferTime.count() << std::endl;
  }

  return 0;
}
}  // namespace shad
