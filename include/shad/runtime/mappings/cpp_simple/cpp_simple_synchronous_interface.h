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

#ifndef INCLUDE_SHAD_RUNTIME_MAPPINGS_CPP_SIMPLE_CPP_SIMPLE_SYNCHRONOUS_INTERFACE_H_
#define INCLUDE_SHAD_RUNTIME_MAPPINGS_CPP_SIMPLE_CPP_SIMPLE_SYNCHRONOUS_INTERFACE_H_

#include <cstring>
#include <memory>
#include <utility>

#include "shad/runtime/locality.h"
#include "shad/runtime/mappings/cpp_simple/cpp_simple_traits_mapping.h"
#include "shad/runtime/mappings/cpp_simple/cpp_simple_utility.h"
#include "shad/runtime/synchronous_interface.h"

namespace shad {
namespace rt {

namespace impl {

template <>
struct SynchronousInterface<cpp_tag> {
  template <typename FunT, typename InArgsT>
  static void executeAt(const Locality &loc, FunT &&function,
                        const InArgsT &args) {
    using FunctionTy = void (*)(const InArgsT &);
    checkLocality(loc);
    FunctionTy fn = std::forward<decltype(function)>(function);
    fn(args);
  }

  template <typename FunT>
  static void executeAt(const Locality &loc, FunT &&function,
                        const std::shared_ptr<uint8_t> &argsBuffer,
                        const uint32_t bufferSize) {
    using FunctionTy = void (*)(const uint8_t *, const uint32_t);
    FunctionTy fn = std::forward<decltype(function)>(function);
    checkLocality(loc);
    fn(argsBuffer.get(), bufferSize);
  }

  template <typename FunT, typename InArgsT>
  static void executeAtWithRetBuff(const Locality &loc, FunT &&function,
                                   const InArgsT &args, uint8_t *resultBuffer,
                                   uint32_t *resultSize) {
    using FunctionTy = void (*)(const InArgsT &, uint8_t *, uint32_t *);
    FunctionTy fn = std::forward<decltype(function)>(function);
    checkLocality(loc);
    fn(args, resultBuffer, resultSize);
  }

  template <typename FunT>
  static void executeAtWithRetBuff(const Locality &loc, FunT &&function,
                                   const std::shared_ptr<uint8_t> &argsBuffer,
                                   const uint32_t bufferSize,
                                   uint8_t *resultBuffer,
                                   uint32_t *resultSize) {
    using FunctionTy =
        void (*)(const uint8_t *, const uint32_t, uint8_t *, uint32_t *);
    FunctionTy fn = std::forward<decltype(function)>(function);
    checkLocality(loc);
    fn(argsBuffer.get(), bufferSize, resultBuffer, resultSize);
  }

  template <typename FunT, typename InArgsT, typename ResT>
  static void executeAtWithRet(const Locality &loc, FunT &&function,
                               const InArgsT &args, ResT *result) {
    using FunctionTy = void (*)(const InArgsT &, ResT *);
    FunctionTy fn = std::forward<decltype(function)>(function);
    checkLocality(loc);
    fn(args, result);
  }

  template <typename FunT, typename ResT>
  static void executeAtWithRet(const Locality &loc, FunT &&function,
                               const std::shared_ptr<uint8_t> &argsBuffer,
                               const uint32_t bufferSize, ResT *result) {
    using FunctionTy = void (*)(const uint8_t *, const uint32_t, ResT *);
    FunctionTy fn = std::forward<decltype(function)>(function);
    checkLocality(loc);
    fn(argsBuffer.get(), bufferSize, result);
  }

  template <typename FunT, typename InArgsT>
  static void executeOnAll(FunT &&function, const InArgsT &args) {
    using FunctionTy = void (*)(const InArgsT &);
    FunctionTy fn = std::forward<decltype(function)>(function);
    fn(args);
  }

  template <typename FunT>
  static void executeOnAll(FunT &&function,
                           const std::shared_ptr<uint8_t> &argsBuffer,
                           const uint32_t bufferSize) {
    using FunctionTy = void (*)(const uint8_t *, const uint32_t);
    FunctionTy fn = std::forward<decltype(function)>(function);
    fn(argsBuffer.get(), bufferSize);
  }

  template <typename FunT, typename InArgsT>
  static void forEachAt(const Locality &loc, FunT &&function,
                        const InArgsT &args, const size_t numIters) {
    using FunctionTy = void (*)(const InArgsT &, size_t);
    FunctionTy fn = std::forward<decltype(function)>(function);
    checkLocality(loc);
    for (auto i = 0; i < numIters; ++i) fn(args, i);
  }

  template <typename FunT>
  static void forEachAt(const Locality &loc, FunT &&function,
                        const std::shared_ptr<uint8_t> &argsBuffer,
                        const uint32_t bufferSize, const size_t numIters) {
    using FunctionTy = void (*)(const uint8_t *, const uint32_t, size_t);
    FunctionTy fn = std::forward<decltype(function)>(function);
    checkLocality(loc);
    for (auto i = 0; i < numIters; ++i) fn(argsBuffer.get(), bufferSize, i);
  }

  template <typename FunT, typename InArgsT>
  static void forEachOnAll(FunT &&function, const InArgsT &args,
                           const size_t numIters) {
    using FunctionTy = void (*)(const InArgsT &, size_t);
    FunctionTy fn = std::forward<decltype(function)>(function);
    for (auto i = 0; i < numIters; ++i) fn(args, i);
  }

  template <typename FunT>
  static void forEachOnAll(FunT &&function,
                           const std::shared_ptr<uint8_t> &argsBuffer,
                           const uint32_t bufferSize, const size_t numIters) {
    using FunctionTy = void (*)(const uint8_t *, const uint32_t, size_t);
    FunctionTy fn = std::forward<decltype(function)>(function);
    for (auto i = 0; i < numIters; ++i) fn(argsBuffer.get(), bufferSize, i);
  }

  template <typename T>
  static void dma(const Locality &, const T* remoteAddress,
                  const T* localData, const size_t numElements) {
    memcpy((uint8_t*)remoteAddress, (uint8_t*)localData,
           numElements*sizeof(T));
  }

  template <typename T>
  static void dma(const T* localAddress, const Locality &,
                  const T* remoteData, const size_t numElements) {
    memcpy((uint8_t*)localAddress, (uint8_t*)remoteData,
           numElements*sizeof(T));
  }
};

}  // namespace impl

}  // namespace rt
}  // namespace shad

#endif  // INCLUDE_SHAD_RUNTIME_MAPPINGS_CPP_SIMPLE_CPP_SIMPLE_SYNCHRONOUS_INTERFACE_H_
