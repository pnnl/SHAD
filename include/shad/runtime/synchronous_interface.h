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

#ifndef INCLUDE_SHAD_RUNTIME_SYNCHRONOUS_INTERFACE_H_
#define INCLUDE_SHAD_RUNTIME_SYNCHRONOUS_INTERFACE_H_

#include <cstddef>
#include <cstdint>
#include <memory>

#include "shad/runtime/locality.h"

namespace shad {
namespace rt {

namespace impl {

template <typename TargetSystemTag>
struct SynchronousInterface {
  template <typename FunT, typename InArgsT>
  static void executeAt(const Locality &loc, FunT &&func, const InArgsT &args);

  template <typename FunT>
  static void executeAt(const Locality &loc, FunT &&func,
                        const std::shared_ptr<uint8_t> &argsBuffer,
                        const uint32_t bufferSize);

  template <typename FunT, typename InArgsT>
  static void executeAtWithRetBuff(const Locality &loc, FunT &&func,
                                   const InArgsT &args,
                                   const uint8_t *resultBuffer,
                                   const uint32_t *resultSize);

  template <typename FunT>
  static void executeAtWithRetBuff(const Locality &loc, FunT &&func,
                                   const std::shared_ptr<uint8_t> &argsBuffer,
                                   const uint32_t bufferSize,
                                   const uint8_t *resultBuffer,
                                   const uint32_t *resultSize);

  template <typename FunT, typename InArgsT, typename ResT>
  static void executeAtWithRet(const Locality &loc, FunT &&func,
                               const InArgsT &args, ResT *result);

  template <typename FunT, typename ResT>
  static void executeAtWithRet(const Locality &loc, FunT &&func,
                               const uint8_t *argsBuffer,
                               const uint32_t bufferSize, ResT *result);

  template <typename FunT, typename InArgsT>
  static void executeOnAll(FunT &&func, const InArgsT &args);

  template <typename FunT>
  static void executeOnAll(FunT &&func,
                           const std::shared_ptr<uint8_t> &argsBuffer,
                           const uint32_t bufferSize);

  template <typename FunT, typename InArgsT>
  static void forEachAt(const Locality &loc, FunT &&func, const InArgsT &args,
                        const size_t numIters);

  template <typename FunT>
  static void forEachAt(const Locality &loc, FunT &&func,
                        const std::shared_ptr<uint8_t> &argsBuffer,
                        const uint32_t bufferSize, const size_t numIters);

  template <typename FunT, typename InArgsT>
  static void forEachOnAll(FunT &&func, const InArgsT &args,
                           const size_t numIters);

  template <typename FunT>
  static void forEachOnAll(FunT &&func,
                           const std::shared_ptr<uint8_t> &argsBuffer,
                           const uint32_t bufferSize, const size_t numIters);

  template <typename T>
  static void dma(const Locality &destLoc, const T* remoteAddress,
                  const T* localData, const size_t numElements);
 
  template <typename T>
  static void dma(const T* localAddress, const Locality &srcLoc,
                  const T* remoteData, const size_t numElements);
  
  };

}  // namespace impl

}  // namespace rt
}  // namespace shad

#endif  // INCLUDE_SHAD_RUNTIME_SYNCHRONOUS_INTERFACE_H_
