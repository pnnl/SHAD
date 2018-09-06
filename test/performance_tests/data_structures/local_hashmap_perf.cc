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
#include <iostream>
#include <random>
#include <thread>

#include "shad/data_structures/local_hashmap.h"
#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

namespace shad {

namespace localhmap_perf_test {
static size_t kNumKeys = 1000000;
static size_t kKeySize = 1;
static size_t kValueSize = 1;
static size_t kNumBuckets = 1024;
}  // namespace localhmap_perf_test

static void PrintParameters() {
  std::cout << " Running Local Hashmap Performance test with"
            << "\n   NumKeys: " << localhmap_perf_test::kNumKeys
            << "\n   KeySize: " << localhmap_perf_test::kKeySize
            << "\n   ValueSize: " << localhmap_perf_test::kValueSize
            << "\n   NumBuckets: " << localhmap_perf_test::kNumBuckets
            << std::endl;
}

template <typename duration_t>
void print_time(const std::string &label, duration_t duration) {
  auto dc = duration.count();
  std::cout << "Time to execute " << label << ": " << dc
            << " ms, throughput:: ";
  if (dc)
    std::cout << (double)localhmap_perf_test::kNumKeys / dc * 1000;
  else
    std::cout << "N/A";
  std::cout << " ops/s\n\n" << std::endl;
}

int main(int argc, char *argv[]) {
  if (shad::rt::numLocalities() != 1) {
    std::cout
        << "ERROR: This performance test should execute on a single locality\n";
    return 0;
  }
  bool default_num_buckets = true;

  for (size_t argIndex = 1; argIndex < argc - 1; argIndex++) {
    std::string arg(argv[argIndex]);
    if (arg == "--NumKeys") {
      ++argIndex;
      localhmap_perf_test::kNumKeys = atoi(argv[argIndex]);
      if (localhmap_perf_test::kNumKeys == 0) {
        std::cout << "Invalid Number of keys: " << argv[argIndex] << std::endl;
        return 0;
      }
    } else if (arg == "--KeySize") {
      ++argIndex;
      localhmap_perf_test::kKeySize = atoi(argv[argIndex]);
      if (localhmap_perf_test::kKeySize == 0) {
        std::cout << "Invalid key size: " << argv[argIndex] << std::endl;
        return 0;
      }
    } else if (arg == "--ValueSize") {
      ++argIndex;
      localhmap_perf_test::kValueSize = atoi(argv[argIndex]);
      if (localhmap_perf_test::kValueSize == 0) {
        std::cout << "Invalid value size: " << argv[argIndex] << std::endl;
        return 0;
      }
    } else if (arg == "--NumBuckets") {
      default_num_buckets = false;
      ++argIndex;
      localhmap_perf_test::kNumBuckets = atoi(argv[argIndex]);
      if (localhmap_perf_test::kNumBuckets == 0) {
        std::cout << "Invalid number of buckets: " << argv[argIndex]
                  << std::endl;
        return 0;
      }
    }
  }

  if (default_num_buckets)
    localhmap_perf_test::kNumBuckets =
        std::max<size_t>(1024, localhmap_perf_test::kNumKeys /
                                   constants::kDefaultNumEntriesPerBucket);
  PrintParameters();

  // Fill the input vector
  static std::vector<std::pair<std::vector<uint64_t>, std::vector<uint64_t>>>
      input(localhmap_perf_test::kNumKeys);
  std::vector<uint64_t> keys(localhmap_perf_test::kKeySize);
  std::vector<uint64_t> values(localhmap_perf_test::kValueSize);
  for (uint64_t i = 0; i < localhmap_perf_test::kNumKeys; ++i) {
    std::iota(keys.begin(), keys.end(), i);
    std::iota(values.begin(), values.end(), i);
    input[i] = std::make_pair(keys, values);
  }

  using MapT = shad::LocalHashmap<std::vector<uint64_t>, std::vector<uint64_t>>;

  MapT hmap(localhmap_perf_test::kNumBuckets);

  std::cout << "Local hashmap instance created" << std::endl;

  shad::rt::Handle handle;
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      shad::measure<>::duration([&]() {
        shad::rt::asyncForEachAt(
            handle, shad::rt::thisLocality(),
            [](shad::rt::Handle &, const std::tuple<MapT *> &t,
               const size_t iter) {
              std::get<0>(t)->Insert(input[iter].first, input[iter].second);
            },
            std::make_tuple(&hmap), localhmap_perf_test::kNumKeys);
        shad::rt::waitForCompletion(handle);
      }));

  print_time("Populate", duration);

  std::vector<std::vector<uint64_t> *> results(localhmap_perf_test::kNumKeys);
  auto ParallelLookup =
      [](shad::rt::Handle &handle,
         const std::tuple<MapT *, std::vector<std::vector<uint64_t> *> *> &t,
         const size_t iter) {
        auto mapPtr = std::get<0>(t);
        auto resPtr = std::get<1>(t);
        mapPtr->AsyncLookup(handle, input[iter].first, &(resPtr->at(iter)));
      };

  duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      shad::measure<>::duration([&]() {
        shad::rt::asyncForEachAt(
            handle, shad::rt::thisLocality(), ParallelLookup,
            std::make_tuple(&hmap, &results), localhmap_perf_test::kNumKeys);
        shad::rt::waitForCompletion(handle);
      }));

  print_time("Lookup", duration);

  duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      shad::measure<>::duration([&]() {
        for (size_t i = 0; i < localhmap_perf_test::kNumKeys; i++) {
          hmap.AsyncLookup(handle, input[i].first, &results[i]);
        }
        shad::rt::waitForCompletion(handle);
      }));

  print_time("Async-Lookup", duration);

  auto AsyncForEachKeyLambda = [](shad::rt::Handle &,
                                  const std::vector<uint64_t> &) {};

  duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      shad::measure<>::duration([&]() {
        hmap.AsyncForEachKey(handle, AsyncForEachKeyLambda);
        shad::rt::waitForCompletion(handle);
      }));

  print_time("ForEachKey", duration);

  auto AsyncVisitLambda = [](shad::rt::Handle &, const std::vector<uint64_t> &,
                             std::vector<uint64_t> &) {};

  duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      shad::measure<>::duration([&]() {
        hmap.AsyncForEachEntry(handle, AsyncVisitLambda);
        shad::rt::waitForCompletion(handle);
      }));

  return 0;
}
}  // namespace shad
