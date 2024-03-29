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

#ifndef INCLUDE_SHAD_RUNTIME_MAPPINGS_GMT_GMT_SYNCHRONOUS_INTERFACE_H_
#define INCLUDE_SHAD_RUNTIME_MAPPINGS_GMT_GMT_SYNCHRONOUS_INTERFACE_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <system_error>
#include <utility>

#include "shad/runtime/locality.h"
#include "shad/runtime/mappings/gmt/gmt_traits_mapping.h"
#include "shad/runtime/mappings/gmt/gmt_utility.h"
#include "shad/runtime/synchronous_interface.h"

namespace shad {
namespace rt {

namespace impl {

template <>
struct SynchronousInterface<gmt_tag> {
  template <typename FunT, typename InArgsT>
  static void executeAt(const Locality &loc, FunT &&function,
                        const InArgsT &args) {
    using FunctionTy = void (*)(const InArgsT &);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);
    checkInputSize(sizeof(InArgsT));

    ExecFunWrapperArgs<FunctionTy, InArgsT> funArgs{fn, args};

    gmt_execute_on_node(getNodeId(loc), execFunWrapper<FunctionTy, InArgsT>,
                        reinterpret_cast<const uint8_t *>(&funArgs),
                        sizeof(funArgs), nullptr, nullptr, GMT_PREEMPTABLE);
  }

  template <typename FunT>
  static void executeAt(const Locality &loc, FunT &&function,
                        const std::shared_ptr<uint8_t> &argsBuffer,
                        const uint32_t bufferSize) {
    using FunctionTy = void (*)(const uint8_t *, const uint32_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    impl::checkLocality(loc);
    impl::checkInputSize(bufferSize);

    uint32_t newBufferSize = bufferSize + sizeof(fn);
    std::unique_ptr<uint8_t[]> buffer(new uint8_t[newBufferSize]);

    *reinterpret_cast<FunctionTy *>(const_cast<uint8_t *>(buffer.get())) = fn;

    if (argsBuffer != nullptr && bufferSize)
      memcpy(buffer.get() + sizeof(fn), argsBuffer.get(), bufferSize);

    gmt_execute_on_node(impl::getNodeId(loc), execFunWrapper, buffer.get(),
                        newBufferSize, nullptr, nullptr, GMT_PREEMPTABLE);
  }

  template <typename FunT, typename InArgsT>
  static void executeAtWithRetBuff(const Locality &loc, FunT &&function,
                                   const InArgsT &args, uint8_t *resultBuffer,
                                   uint32_t *resultSize) {
    using FunctionTy = void (*)(const InArgsT &, uint8_t *, uint32_t *);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);
    checkInputSize(sizeof(InArgsT));

    ExecFunWrapperArgs<FunctionTy, InArgsT> funArgs{fn, args};

    gmt_execute_on_node(
        getNodeId(loc), execFunWithRetBuffWrapper<FunctionTy, InArgsT>,
        reinterpret_cast<const uint8_t *>(&funArgs), sizeof(funArgs),
        resultBuffer, resultSize, GMT_PREEMPTABLE);
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
    checkInputSize(bufferSize);

    uint32_t newBufferSize = bufferSize + sizeof(fn);
    std::unique_ptr<uint8_t[]> buffer(new uint8_t[newBufferSize]);

    *reinterpret_cast<FunctionTy *>(const_cast<uint8_t *>(buffer.get())) = fn;

    if (argsBuffer != nullptr && bufferSize)
      memcpy(buffer.get() + sizeof(fn), argsBuffer.get(), bufferSize);

    gmt_execute_on_node(getNodeId(loc), execFunWithRetBuffWrapper, buffer.get(),
                        newBufferSize, resultBuffer, resultSize,
                        GMT_PREEMPTABLE);
  }

  template <typename FunT, typename InArgsT, typename ResT>
  static void executeAtWithRet(const Locality &loc, FunT &&function,
                               const InArgsT &args, ResT *result) {
    using FunctionTy = void (*)(const InArgsT &, ResT *);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);
    checkInputSize(sizeof(InArgsT));

    ExecFunWrapperArgs<FunctionTy, InArgsT> funArgs{fn, args};

    uint32_t resultSize = 0;
    gmt_execute_on_node(getNodeId(loc),
                        execFunWithRetWrapper<FunctionTy, InArgsT, ResT>,
                        reinterpret_cast<const uint8_t *>(&funArgs),
                        sizeof(funArgs), result, &resultSize, GMT_PREEMPTABLE);
  }

  template <typename FunT, typename ResT>
  static void executeAtWithRet(const Locality &loc, FunT &&function,
                               const std::shared_ptr<uint8_t> &argsBuffer,
                               const uint32_t bufferSize, ResT *result) {
    using FunctionTy = void (*)(const uint8_t *, const uint32_t, ResT *);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);
    checkInputSize(bufferSize);

    uint32_t newBufferSize = bufferSize + sizeof(fn);
    std::unique_ptr<uint8_t[]> buffer(new uint8_t[newBufferSize]);

    *reinterpret_cast<FunctionTy *>(const_cast<uint8_t *>(buffer.get())) = fn;

    if (argsBuffer != nullptr && bufferSize)
      memcpy(buffer.get() + sizeof(fn), argsBuffer.get(), bufferSize);

    uint32_t retSize;
    gmt_execute_on_node(getNodeId(loc), execFunWithRetWrapper<ResT>,
                        buffer.get(), newBufferSize, result, &retSize,
                        GMT_PREEMPTABLE);
  }

  template <typename FunT, typename InArgsT>
  static void executeOnAll(FunT &&function, const InArgsT &args) {
    using FunctionTy = void (*)(const InArgsT &);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkInputSize(sizeof(InArgsT));

    ExecFunWrapperArgs<FunctionTy, InArgsT> funArgs{fn, args};

    gmt_execute_on_all(execFunWrapper<FunctionTy, InArgsT>,
                       reinterpret_cast<const uint8_t *>(&funArgs),
                       sizeof(funArgs), GMT_PREEMPTABLE);
  }

  template <typename FunT>
  static void executeOnAll(FunT &&function,
                           const std::shared_ptr<uint8_t> &argsBuffer,
                           const uint32_t bufferSize) {
    using FunctionTy = void (*)(const uint8_t *, const uint32_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    impl::checkInputSize(bufferSize);

    uint32_t newBufferSize = bufferSize + sizeof(fn);
    std::unique_ptr<uint8_t[]> buffer(new uint8_t[newBufferSize]);

    *reinterpret_cast<FunctionTy *>(const_cast<uint8_t *>(buffer.get())) = fn;

    if (argsBuffer != nullptr && bufferSize)
      memcpy(buffer.get() + sizeof(fn), argsBuffer.get(), bufferSize);

    gmt_execute_on_all(impl::execFunWrapper, buffer.get(), newBufferSize,
                       GMT_PREEMPTABLE);
  }

  template <typename FunT, typename InArgsT>
  static void forEachAt(const Locality &loc, FunT &&function,
                        const InArgsT &args, const size_t numIters) {
    using FunctionTy = void (*)(const InArgsT &, size_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);
    checkInputSize(sizeof(InArgsT));

    // No need to do anything.
    if (!numIters) return;

    uint32_t workload =
        numIters / (gmt_num_workers() * kOverSubscriptionFactor);
    workload = std::max(workload, uint32_t(1));

    impl::ExecFunWrapperArgs<FunctionTy, InArgsT> funArgs{fn, args};

    gmt_for_loop_on_node(impl::getNodeId(loc), numIters, workload,
                         impl::forEachWrapper<FunctionTy, InArgsT>,
                         reinterpret_cast<const uint8_t *>(&funArgs),
                         sizeof(funArgs));
  }

  template <typename FunT>
  static void forEachAt(const Locality &loc, FunT &&function,
                        const std::shared_ptr<uint8_t> &argsBuffer,
                        const uint32_t bufferSize, const size_t numIters) {
    using FunctionTy = void (*)(const uint8_t *, const uint32_t, size_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);
    checkInputSize(bufferSize);

    // No need to do anything.
    if (!numIters) return;

    uint32_t newBufferSize = bufferSize + sizeof(fn) + sizeof(bufferSize);
    std::unique_ptr<uint8_t[]> buffer(new uint8_t[newBufferSize]);

    uint8_t *basePtr = buffer.get();
    *reinterpret_cast<FunctionTy *>(basePtr) = fn;
    basePtr += sizeof(fn);

    if (argsBuffer != nullptr && bufferSize) {
      memcpy(basePtr, &bufferSize, sizeof(bufferSize));
      basePtr += sizeof(bufferSize);
      memcpy(basePtr, argsBuffer.get(), bufferSize);
    }
    uint32_t workload =
        numIters / (gmt_num_workers() * kOverSubscriptionFactor);
    workload = std::max(workload, uint32_t(1));

    gmt_for_loop_on_node(getNodeId(loc), numIters, workload, forEachWrapper,
                         buffer.get(), newBufferSize);
  }

  template <typename FunT, typename InArgsT>
  static void forEachOnAll(FunT &&function, const InArgsT &args,
                           const size_t numIters) {
    using FunctionTy = void (*)(const InArgsT &, size_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    impl::checkInputSize(sizeof(InArgsT));

    // No need to do anything.
    if (!numIters) return;

    impl::ExecFunWrapperArgs<FunctionTy, InArgsT> funArgs{fn, args};

    uint32_t workload = (numIters / gmt_num_nodes()) / gmt_num_workers();
    workload = std::max(workload, uint32_t(1));

    gmt_for_loop(numIters, workload, impl::forEachWrapper<FunctionTy, InArgsT>,
                 reinterpret_cast<const uint8_t *>(&funArgs), sizeof(funArgs),
                 GMT_SPAWN_SPREAD);
  }

  template <typename FunT>
  static void forEachOnAll(FunT &&function,
                           const std::shared_ptr<uint8_t> &argsBuffer,
                           const uint32_t bufferSize, const size_t numIters) {
    using FunctionTy = void (*)(const uint8_t *, const uint32_t, size_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkInputSize(bufferSize);

    // No need to do anything.
    if (!numIters) return;

    uint32_t newBufferSize = bufferSize + sizeof(fn) + sizeof(bufferSize);
    std::unique_ptr<uint8_t[]> buffer(new uint8_t[newBufferSize]);

    *reinterpret_cast<FunctionTy *>(const_cast<uint8_t *>(buffer.get())) = fn;

    if (argsBuffer != nullptr && bufferSize) {
      uint8_t *basePtr = buffer.get() + sizeof(fn);
      memcpy(basePtr, &bufferSize, sizeof(bufferSize));
      basePtr += sizeof(bufferSize);
      memcpy(basePtr, argsBuffer.get(), bufferSize);
    }

    uint32_t workload = (numIters / gmt_num_nodes()) / gmt_num_workers();
    workload = std::max(workload, uint32_t(1));

    gmt_for_loop(numIters, workload, forEachWrapper, buffer.get(),
                 newBufferSize, GMT_SPAWN_SPREAD);
  }

  template <typename T>
  static void dma(const Locality &destLoc, const T* remoteAddress,
                  const T* localData, const size_t numElements) {
    gmt_mem_put(getNodeId(destLoc), (uint8_t*)remoteAddress,
                (uint8_t*)(localData), numElements*sizeof(T));
  }

  template <typename T>
  static void dma(const T* localAddress, const Locality &srcLoc,
                  const T* remoteData, const size_t numElements) {
    gmt_mem_get(getNodeId(srcLoc), (uint8_t*)localAddress,
                (uint8_t*)(remoteData), numElements*sizeof(T));
  }
};

}  // namespace impl

}  // namespace rt
}  // namespace shad

#endif  // INCLUDE_SHAD_RUNTIME_GMT_MAPPINGS_GMT_SYNCHRONOUS_INTERFACE_H_
