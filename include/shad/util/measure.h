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

#ifndef INCLUDE_SHAD_UTIL_MEASURE_H_
#define INCLUDE_SHAD_UTIL_MEASURE_H_

#include <chrono>
#include <utility>

namespace shad {

/// @brief Utility for time measurements.
template <typename TimeT = std::chrono::milliseconds>
struct measure {
  /// @brief Compute the time spend to execute a function.
  ///
  /// Typical Usage:
  /// @code
  /// int aParameter = 1;
  /// auto time = shad::measure<std::chrono::seconds>::duration(
  ///   [](int aParameter) {
  ///     // ... do what needs to be measured ...
  ///   }, aParameter);
  ///
  /// std::cout << "Time spent : " << time.count() << std::endl;
  /// @endcode
  ///
  /// @tparam F The type of the function to be computed
  /// @tparam Args The types of the arguments to be forwarded to the function.
  ///
  /// @param[in] function A collable object that will be measured
  /// @param[in] args The list of args forwarded to the function.
  /// @return the time spent to compute the input function.
  template <typename F, typename... Args>
  static auto duration(F&& function, Args... args) {
    auto start = std::chrono::steady_clock::now();
    std::forward<decltype(function)>(function)(std::forward<Args>(args)...);
    std::chrono::duration<double, typename TimeT::period> duration =
                   std::chrono::steady_clock::now() - start;

    return duration;
  }
};


}  // namespace shad

#endif  // INCLUDE_SHAD_UTIL_MEASURE_H_
