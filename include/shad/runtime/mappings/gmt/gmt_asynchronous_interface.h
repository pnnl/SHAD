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

/*
 Developer      Date            Description
 ================================================================================
 Methun K       08/01/2018      Added logging
 --------------------------------------------------------------------------------
 */

#ifndef INCLUDE_SHAD_RUNTIME_MAPPINGS_GMT_GMT_ASYNCHRONOUS_INTERFACE_H_
#define INCLUDE_SHAD_RUNTIME_MAPPINGS_GMT_GMT_ASYNCHRONOUS_INTERFACE_H_

#include <algorithm>
#include <memory>
#include <mutex>
#include <set>
#include <utility>

#include "gmt/gmt.h"

#include "shad/runtime/asynchronous_interface.h"
#include "shad/runtime/handle.h"
#include "shad/runtime/locality.h"
#include "shad/runtime/mapping_traits.h"
//#include "shad/runtime/mappings/gmt/gmt_traits_mapping.h"
#include "shad/runtime/mappings/gmt/gmt_utility.h"
#include "shad/util/slog.h"

namespace shad {
namespace rt {

namespace impl {

template <>
struct AsynchronousInterface<gmt_tag> {
  template <typename FunT, typename InArgsT>
  static void asyncExecuteAt(Handle &handle, const Locality &loc,
                             FunT &&function, const InArgsT &args) {
      // Start logging time
      auto t1 = shad_clock::now();
      
      using FunctionTy = void (*)(Handle &, const InArgsT &);
      
      FunctionTy fn = std::forward<decltype(function)>(function);
      
      checkLocality(loc);
      checkInputSize(sizeof(InArgsT));
      
      ExecFunWrapperArgs<FunctionTy, InArgsT> funArgs{fn, args};
      
      handle = (handle.IsNull()) ? Handle(HandleTrait<gmt_tag>::CreateNewHandle())
      : handle;
      gmt_execute_on_node_with_handle(
                                      getNodeId(loc), execAsyncFunWrapper<FunT, InArgsT>,
                                      reinterpret_cast<const uint8_t *>(&funArgs), sizeof(funArgs), nullptr,
                                      nullptr, GMT_PREEMPTABLE, getGmtHandle(handle));
      
      // End logging time
      auto t2 = shad_clock::now();
      std::chrono::duration<double> diff = t2-t1;
      auto log_handler = shad::slog::ShadLog::Instance();
      log_handler->printlf("asyncExecuteAt", diff.count(), &handle, RuntimeInternalsTrait<gmt_tag>::ThisLocality(), loc, sizeof(InArgsT),0);
  }

  template <typename FunT>
  static void asyncExecuteAt(Handle &handle, const Locality &loc,
                             FunT &&function,
                             const std::shared_ptr<uint8_t> &argsBuffer,
                             const uint32_t bufferSize) {
      // Start logging time
      auto t1 = shad_clock::now();
      
      using FunctionTy = void (*)(Handle &, const uint8_t *, const uint32_t);
      
      FunctionTy fn = std::forward<decltype(function)>(function);
      
      checkLocality(loc);
      checkInputSize(bufferSize);
      
      uint32_t newBufferSize = bufferSize + sizeof(fn);
      std::unique_ptr<uint8_t[]> buffer(new uint8_t[newBufferSize]);
      
      *reinterpret_cast<FunctionTy *>(const_cast<uint8_t *>(buffer.get())) = fn;
      
      if (argsBuffer != nullptr && bufferSize)
          memcpy(buffer.get() + sizeof(fn), argsBuffer.get(), bufferSize);
      
      handle = (handle.IsNull()) ? Handle(HandleTrait<gmt_tag>::CreateNewHandle())
      : handle;
      
      gmt_execute_on_node_with_handle(
                                      getNodeId(loc), execAsyncFunWrapper, buffer.get(), newBufferSize,
                                      nullptr, nullptr, GMT_PREEMPTABLE, getGmtHandle(handle));
      
      // End logging time
      auto t2 = shad_clock::now();
      std::chrono::duration<double> diff = t2-t1;
      auto log_handler = shad::slog::ShadLog::Instance();
      log_handler->printlf("asyncExecuteAt-argsBuffer", diff.count(), &handle, RuntimeInternalsTrait<gmt_tag>::ThisLocality(), loc, sizeof(std::shared_ptr<uint8_t>),0);
  }

  template <typename FunT, typename InArgsT>
  static void asyncExecuteAtWithRetBuff(Handle &handle, const Locality &loc,
                                        FunT &&function, const InArgsT &args,
                                        uint8_t *resultBuffer,
                                        uint32_t *resultSize) {
      // Start logging time
      auto t1 = shad_clock::now();
      
      using FunctionTy =
      void (*)(Handle &, const InArgsT &, uint8_t *, uint32_t *);
      
      FunctionTy fn = std::forward<decltype(function)>(function);
      
      checkLocality(loc);
      checkInputSize(sizeof(InArgsT));
      
      ExecFunWrapperArgs<FunctionTy, InArgsT> funArgs{fn, args};
      
      handle = (handle.IsNull()) ? Handle(HandleTrait<gmt_tag>::CreateNewHandle())
      : handle;
      
      gmt_execute_on_node_with_handle(
                                      getNodeId(loc), asyncExecFunWithRetBuffWrapper<FunctionTy, InArgsT>,
                                      reinterpret_cast<const uint8_t *>(&funArgs), sizeof(funArgs),
                                      resultBuffer, resultSize, GMT_PREEMPTABLE, getGmtHandle(handle));
      
      // End logging time
      auto t2 = shad_clock::now();
      std::chrono::duration<double> diff = t2-t1;
      auto log_handler = shad::slog::ShadLog::Instance();
      log_handler->printlf("asyncExecuteAtWithRetBuff", diff.count(), &handle, RuntimeInternalsTrait<gmt_tag>::ThisLocality(), loc, sizeof(InArgsT),0);
  }

  template <typename FunT>
  static void asyncExecuteAtWithRetBuff(
      Handle &handle, const Locality &loc, FunT &&function,
      const std::shared_ptr<uint8_t> &argsBuffer, const uint32_t bufferSize,
      uint8_t *resultBuffer, uint32_t *resultSize) {
      // Start logging time
      auto t1 = shad_clock::now();
      
      using FunctionTy = void (*)(Handle &, const uint8_t *, const uint32_t,
                                  uint8_t *, uint32_t *);
      
      FunctionTy fn = std::forward<decltype(function)>(function);
      
      checkLocality(loc);
      checkInputSize(bufferSize);
      
      uint32_t newBufferSize = bufferSize + sizeof(fn);
      std::unique_ptr<uint8_t[]> buffer(new uint8_t[newBufferSize]);
      
      *reinterpret_cast<FunctionTy *>(const_cast<uint8_t *>(buffer.get())) = fn;
      
      if (argsBuffer != nullptr && bufferSize)
          memcpy(buffer.get() + sizeof(fn), argsBuffer.get(), bufferSize);
      
      handle = (handle.IsNull()) ? Handle(HandleTrait<gmt_tag>::CreateNewHandle())
      : handle;
      
      gmt_execute_on_node_with_handle(
                                      getNodeId(loc), asyncExecFunWithRetBuffWrapper, buffer.get(),
                                      newBufferSize, resultBuffer, resultSize, GMT_PREEMPTABLE,
                                      getGmtHandle(handle));
      
      // End logging time
      auto t2 = shad_clock::now();
      std::chrono::duration<double> diff = t2-t1;
      auto log_handler = shad::slog::ShadLog::Instance();
      log_handler->printlf("asyncExecuteAtWithRetBuff-argsBuffer", diff.count(), &handle, RuntimeInternalsTrait<gmt_tag>::ThisLocality(), loc, sizeof(std::shared_ptr<uint8_t>),0);
  }

  template <typename FunT, typename ResT>
  static void asyncExecuteAtWithRet(Handle &handle, const Locality &loc,
                                    FunT &&function,
                                    const std::shared_ptr<uint8_t> &argsBuffer,
                                    const uint32_t bufferSize, ResT *result) {
      // Start logging time
      auto t1 = shad_clock::now();
      
      using FunctionTy =
      void (*)(Handle &, const uint8_t *, const uint32_t, ResT *);
      
      FunctionTy fn = std::forward<decltype(function)>(function);
      
      checkLocality(loc);
      checkInputSize(bufferSize);
      
      uint32_t newBufferSize = bufferSize + sizeof(fn);
      std::unique_ptr<uint8_t[]> buffer(new uint8_t[newBufferSize]);
      
      *reinterpret_cast<FunctionTy *>(const_cast<uint8_t *>(buffer.get())) = fn;
      
      if (argsBuffer != nullptr && bufferSize)
          memcpy(buffer.get() + sizeof(fn), argsBuffer.get(), bufferSize);
      
      handle = (handle.IsNull()) ? Handle(HandleTrait<gmt_tag>::CreateNewHandle())
      : handle;
      
      gmt_execute_on_node_with_handle(
                                      getNodeId(loc), asyncExecFunWithRetWrapper<ResT>, buffer.get(),
                                      newBufferSize, result, &garbageSize, GMT_PREEMPTABLE,
                                      getGmtHandle(handle));
      
      // End logging time
      auto t2 = shad_clock::now();
      std::chrono::duration<double> diff = t2-t1;
      auto log_handler = shad::slog::ShadLog::Instance();
      log_handler->printlf("asyncExecuteAtWithRet-argsBuffer", diff.count(), &handle, RuntimeInternalsTrait<gmt_tag>::ThisLocality(), loc, sizeof(std::shared_ptr<uint8_t>),0);
  }

  template <typename FunT, typename InArgsT, typename ResT>
  static void asyncExecuteAtWithRet(Handle &handle, const Locality &loc,
                                    FunT &&function, const InArgsT &args,
                                    ResT *result) {
      // Start logging time
      auto t1 = shad_clock::now();
      
      using FunctionTy = void (*)(Handle &, const InArgsT &, ResT *);
      
      FunctionTy fn = std::forward<decltype(function)>(function);
      
      checkLocality(loc);
      checkInputSize(sizeof(InArgsT));
      
      ExecFunWrapperArgs<FunctionTy, InArgsT> funArgs{fn, args};
      
      uint32_t resultSize = 0;
      
      handle = (handle.IsNull()) ? Handle(HandleTrait<gmt_tag>::CreateNewHandle())
      : handle;
      
      gmt_execute_on_node_with_handle(
                                      getNodeId(loc), asyncExecFunWithRetWrapper<FunctionTy, InArgsT, ResT>,
                                      reinterpret_cast<const uint8_t *>(&funArgs), sizeof(funArgs), result,
                                      &garbageSize, GMT_PREEMPTABLE, getGmtHandle(handle));
      
      // End logging time
      auto t2 = shad_clock::now();
      std::chrono::duration<double> diff = t2-t1;
      auto log_handler = shad::slog::ShadLog::Instance();
      log_handler->printlf("asyncExecuteAtWithRet", diff.count(), &handle, RuntimeInternalsTrait<gmt_tag>::ThisLocality(), loc, sizeof(InArgsT),0);
  }

  template <typename FunT, typename InArgsT>
  static void asyncExecuteOnAll(Handle &handle, FunT &&function,
                                const InArgsT &args) {
      // Start logging time
      auto t1 = shad_clock::now();
      
      using FunctionTy = void (*)(Handle &, const InArgsT &);
      
      FunctionTy fn = std::forward<decltype(function)>(function);
      
      checkInputSize(sizeof(InArgsT));
      
      ExecFunWrapperArgs<FunctionTy, InArgsT> funArgs{fn, args};
      
      handle = (handle.IsNull()) ? Handle(HandleTrait<gmt_tag>::CreateNewHandle())
      : handle;
      
      gmt_execute_on_all_with_handle(execAsyncFunWrapper<FunctionTy, InArgsT>,
                                     reinterpret_cast<const uint8_t *>(&funArgs),
                                     sizeof(funArgs), GMT_PREEMPTABLE,
                                     getGmtHandle(handle));
      
      // End logging time
      auto t2 = shad_clock::now();
      std::chrono::duration<double> diff = t2-t1;
      auto log_handler = shad::slog::ShadLog::Instance();
      log_handler->printlf("asyncExecuteOnAll", diff.count(), &handle, RuntimeInternalsTrait<gmt_tag>::ThisLocality(), RuntimeInternalsTrait<gmt_tag>::ThisLocality(), sizeof(InArgsT),0);
  }

  template <typename FunT>
  static void asyncExecuteOnAll(Handle &handle, FunT &&function,
                                const std::shared_ptr<uint8_t> &argsBuffer,
                                const uint32_t bufferSize) {
      // Start logging time
      auto t1 = shad_clock::now();
      
      using FunctionTy = void (*)(Handle &, const uint8_t *, const uint32_t);
      
      FunctionTy fn = std::forward<decltype(function)>(function);
      
      checkInputSize(bufferSize);
      
      uint32_t newBufferSize = bufferSize + sizeof(fn);
      std::unique_ptr<uint8_t[]> buffer(new uint8_t[newBufferSize]);
      
      *reinterpret_cast<FunctionTy *>(const_cast<uint8_t *>(buffer.get())) = fn;
      
      if (argsBuffer != nullptr && bufferSize)
          memcpy(buffer.get() + sizeof(fn), argsBuffer.get(), bufferSize);
      
      handle = (handle.IsNull()) ? Handle(HandleTrait<gmt_tag>::CreateNewHandle())
      : handle;
      
      gmt_execute_on_all_with_handle(execAsyncFunWrapper, buffer.get(),
                                     newBufferSize, GMT_PREEMPTABLE,
                                     getGmtHandle(handle));
      
      // End logging time
      auto t2 = shad_clock::now();
      std::chrono::duration<double> diff = t2-t1;
      auto log_handler = shad::slog::ShadLog::Instance();
      log_handler->printlf("asyncExecuteOnAll-argsBuffer", diff.count(), &handle, RuntimeInternalsTrait<gmt_tag>::ThisLocality(), RuntimeInternalsTrait<gmt_tag>::ThisLocality(), sizeof(std::shared_ptr<uint8_t>),0);
  }

  template <typename FunT, typename InArgsT>
  static void asyncForEachAt(Handle &handle, const Locality &loc,
                             FunT &&function, const InArgsT &args,
                             const size_t numIters) {
      // Start logging time
      auto t1 = shad_clock::now();
      
      using FunctionTy = void (*)(Handle &, const InArgsT &, size_t);
      
      FunctionTy fn = std::forward<decltype(function)>(function);
      
      checkLocality(loc);
      checkInputSize(sizeof(InArgsT));
      
      // No need to do anything.
      if (!numIters) return;
      
      handle = (handle.IsNull()) ? Handle(HandleTrait<gmt_tag>::CreateNewHandle())
      : handle;
      
      uint32_t workload =
      numIters / (gmt_num_workers() * kOverSubscriptionFactor);
      workload = std::max(workload, uint32_t(1));
      
      ExecFunWrapperArgs<FunctionTy, InArgsT> funArgs{fn, args};
      
      gmt_for_loop_on_node_with_handle(
                                       getNodeId(loc), numIters, workload,
                                       asyncForEachWrapper<FunctionTy, InArgsT>,
                                       reinterpret_cast<const uint8_t *>(&funArgs), sizeof(funArgs),
                                       getGmtHandle(handle));
      
      // End logging time
      auto t2 = shad_clock::now();
      std::chrono::duration<double> diff = t2-t1;
      auto log_handler = shad::slog::ShadLog::Instance();
      log_handler->printlf("asyncForEachAt", diff.count(), &handle, RuntimeInternalsTrait<gmt_tag>::ThisLocality(), loc, sizeof(InArgsT),0, numIters);
  }

  template <typename FunT>
  static void asyncForEachAt(Handle &handle, const Locality &loc,
                             FunT &&function,
                             const std::shared_ptr<uint8_t> &argsBuffer,
                             const uint32_t bufferSize, const size_t numIters) {
      // Start logging time
      auto t1 = shad_clock::now();
      
      using FunctionTy =
      void (*)(Handle &, const uint8_t *, const uint32_t, size_t);
      
      FunctionTy fn = std::forward<decltype(function)>(function);
      
      checkLocality(loc);
      checkInputSize(bufferSize);
      
      // No need to do anything.
      if (!numIters) return;
      
      handle = (handle.IsNull()) ? Handle(HandleTrait<gmt_tag>::CreateNewHandle())
      : handle;
      
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
      
      gmt_for_loop_on_node_with_handle(getNodeId(loc), numIters, workload,
                                       asyncForEachWrapper, buffer.get(),
                                       newBufferSize, getGmtHandle(handle));
      
      // End logging time
      auto t2 = shad_clock::now();
      std::chrono::duration<double> diff = t2-t1;
      auto log_handler = shad::slog::ShadLog::Instance();
      log_handler->printlf("asyncForEachAt-argsBuffer", diff.count(), &handle, RuntimeInternalsTrait<gmt_tag>::ThisLocality(), loc, sizeof(std::shared_ptr<uint8_t>),0, numIters);
  }

  template <typename FunT, typename InArgsT>
  static void asyncForEachOnAll(Handle &handle, FunT &&function,
                                const InArgsT &args, const size_t numIters) {
      // Start logging time
      auto t1 = shad_clock::now();
      
      using FunctionTy = void (*)(Handle &, const InArgsT &, size_t);
      
      FunctionTy fn = std::forward<decltype(function)>(function);
      
      checkInputSize(sizeof(InArgsT));
      
      // No need to do anything.
      if (!numIters) return;
      
      ExecFunWrapperArgs<FunctionTy, InArgsT> funArgs{fn, args};
      
      uint32_t workload = (numIters / gmt_num_nodes()) /
      (gmt_num_workers() * kOverSubscriptionFactor);
      workload = std::max(workload, uint32_t(1));
      
      handle = (handle.IsNull()) ? Handle(HandleTrait<gmt_tag>::CreateNewHandle())
      : handle;
      
      gmt_for_loop_with_handle(
                               numIters, workload, asyncForEachWrapper<FunctionTy, InArgsT>,
                               reinterpret_cast<const uint8_t *>(&funArgs), sizeof(funArgs),
                               GMT_SPAWN_SPREAD, getGmtHandle(handle));
      
      // End logging time
      auto t2 = shad_clock::now();
      std::chrono::duration<double> diff = t2-t1;
      auto log_handler = shad::slog::ShadLog::Instance();
      log_handler->printlf("asyncForEachOnAll", diff.count(), &handle, RuntimeInternalsTrait<gmt_tag>::ThisLocality(), RuntimeInternalsTrait<gmt_tag>::ThisLocality(), sizeof(InArgsT),0, numIters);
  }

  template <typename FunT>
  static void asyncForEachOnAll(Handle &handle, FunT &&function,
                                const std::shared_ptr<uint8_t> &argsBuffer,
                                const uint32_t bufferSize,
                                const size_t numIters) {
      // Start logging time
      auto t1 = shad_clock::now();
      
      using FunctionTy =
      void (*)(Handle &, const uint8_t *, const uint32_t, size_t);
      
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
      
      handle = (handle.IsNull()) ? Handle(HandleTrait<gmt_tag>::CreateNewHandle())
      : handle;
      
      gmt_for_loop_with_handle(numIters, workload, asyncForEachWrapper,
                               buffer.get(), newBufferSize, GMT_SPAWN_SPREAD,
                               getGmtHandle(handle));
      
      // End logging time
      auto t2 = shad_clock::now();
      std::chrono::duration<double> diff = t2-t1;
      auto log_handler = shad::slog::ShadLog::Instance();
      log_handler->printlf("asyncForEachOnAll-argsBuffer", diff.count(), &handle, RuntimeInternalsTrait<gmt_tag>::ThisLocality(), RuntimeInternalsTrait<gmt_tag>::ThisLocality(), sizeof(std::shared_ptr<uint8_t>),0, numIters);
  }
};

}  // namespace impl

}  // namespace rt
}  // namespace shad

#endif  // INCLUDE_SHAD_RUNTIME_MAPPINGS_GMT_GMT_ASYNCHRONOUS_INTERFACE_H_
