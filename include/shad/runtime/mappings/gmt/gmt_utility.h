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

#ifndef INCLUDE_SHAD_RUNTIME_MAPPINGS_GMT_GMT_UTILITY_H_
#define INCLUDE_SHAD_RUNTIME_MAPPINGS_GMT_GMT_UTILITY_H_

#include <cstddef>
#include <cstdint>
#include <sstream>
#include <system_error>

#include "shad/runtime/locality.h"
#include "shad/runtime/mappings/gmt/gmt_traits_mapping.h"
#include "shad/util/slog.h"

namespace shad {
namespace rt {

namespace impl {

static uint32_t kOverSubscriptionFactor = 300;

inline uint32_t getNodeId(const Locality &loc) {
  return static_cast<uint32_t>(loc);
}

inline void checkLocality(const Locality &loc) {
  uint32_t nodeID = getNodeId(loc);
  if (nodeID >= gmt_num_nodes()) {
    std::stringstream ss;
    ss << "The system does not include " << loc;
    throw std::system_error(0xdeadc0de, std::generic_category(), ss.str());
  }
}

inline void checkInputSize(size_t size) {
  if (size > gmt_max_args_per_task()) {
    std::stringstream ss;
    ss << "The input size exeeds the hard limit of " << gmt_max_args_per_task()
       << "B imposed by GMT.  A more general solution is under development.";
    throw std::system_error(0xdeadc0de, std::generic_category(), ss.str());
  }
}

inline void checkOutputSize(size_t size) {
  if (size > gmt_max_return_size()) {
    std::stringstream ss;
    ss << "The output size exeeds the hard limit of " << gmt_max_return_size()
       << "B imposed by GMT.  A more general solution is under development.";
    throw std::system_error(0xdeadc0de, std::generic_category(), ss.str());
  }
}

/// @brief Structure to build the function closure to be sent.
template <typename FunT, typename InArgsT>
struct ExecFunWrapperArgs {
  FunT fun;
  InArgsT args;
};

template <typename FunT, typename InArgsT>
void execFunWrapper(const void *args, uint32_t args_size, void *, uint32_t *,
                    gmt_handle_t) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    const ExecFunWrapperArgs<FunT, InArgsT> &funArgs = *reinterpret_cast<const ExecFunWrapperArgs<FunT, InArgsT> *>(args);
    funArgs.fun(funArgs.args);
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("execFunWrapper", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(InArgsT), 0);
}

inline void execFunWrapper(const void *args, uint32_t args_size, void *,
                           uint32_t *, gmt_handle_t) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    using FunctionTy = void (*)(const uint8_t *, const uint32_t);
    
    FunctionTy functionPtr = *reinterpret_cast<FunctionTy *>(const_cast<void *>(args));
    functionPtr(reinterpret_cast<const uint8_t *>(args) + sizeof(functionPtr),
                args_size - sizeof(functionPtr));
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("execFunWrapper-inline", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), args_size - sizeof(functionPtr), 0);
}

template <typename FunT, typename InArgsT>
void execFunWithRetBuffWrapper(const void *args, uint32_t, void *result,
                               uint32_t *resultSize, gmt_handle_t) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    const impl::ExecFunWrapperArgs<FunT, InArgsT> *funArgs = reinterpret_cast<const impl::ExecFunWrapperArgs<FunT, InArgsT> *>(args);
    const InArgsT &fnargs = funArgs->args;
    funArgs->fun(fnargs, reinterpret_cast<uint8_t *>(result), resultSize);
    
    checkOutputSize(*resultSize);
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("execFunWithRetBuffWrapper", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(InArgsT), sizeof(uint32_t));
}

inline void execFunWithRetBuffWrapper(const void *args, uint32_t argsSize,
                                      void *result, uint32_t *resultSize,
                                      gmt_handle_t) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    using FunctionTy =
    void (*)(const uint8_t *, const uint32_t, uint8_t *, uint32_t *);
    
    FunctionTy functionPtr =
    *reinterpret_cast<FunctionTy *>(const_cast<void *>(args));
    functionPtr(reinterpret_cast<const uint8_t *>(args) + sizeof(functionPtr),
                argsSize - sizeof(functionPtr),
                reinterpret_cast<uint8_t *>(result), resultSize);
    
    checkOutputSize(*resultSize);
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("execFunWithRetBuffWrapper-inline", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), argsSize - sizeof(functionPtr), sizeof(*resultSize));
}

template <typename FunT, typename InArgsT, typename ResT>
void execFunWithRetWrapper(const void *args, uint32_t args_size, void *result,
                           uint32_t *resSize, gmt_handle_t) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    using FunctionTy = void (*)(const InArgsT &, ResT *);
    
    const impl::ExecFunWrapperArgs<FunT, InArgsT> *funArgs =
    reinterpret_cast<const impl::ExecFunWrapperArgs<FunT, InArgsT> *>(args);
    const InArgsT &fnargs = funArgs->args;
    
    funArgs->fun(fnargs, reinterpret_cast<ResT *>(result));
    *resSize = sizeof(ResT);
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("execFunWithRetWrapper", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(InArgsT), sizeof(ResT));
}

template <typename ResT>
void execFunWithRetWrapper(const void *args, uint32_t args_size, void *result,
                           uint32_t *resSize, gmt_handle_t) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    using FunctionTy = void (*)(const uint8_t *, const uint32_t, ResT *);
    
    FunctionTy functionPtr =
    *reinterpret_cast<FunctionTy *>(const_cast<void *>(args));
    
    ResT *resultPtr = reinterpret_cast<ResT *>(result);
    
    functionPtr(reinterpret_cast<const uint8_t *>(args) + sizeof(functionPtr),
                args_size - sizeof(functionPtr), resultPtr);
    *resSize = sizeof(ResT);
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("execFunWithRetWrapper-ResT", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), args_size - sizeof(functionPtr), sizeof(ResT));
}

inline void forEachWrapper(uint64_t startIt, uint64_t numIters,
                           const void *args, gmt_handle_t) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    using FunctionTy = void (*)(const uint8_t *, const uint32_t, size_t);
    
    FunctionTy functionPtr =
    *reinterpret_cast<FunctionTy *>(const_cast<void *>(args));
    const uint32_t argSize = *reinterpret_cast<const uint32_t *>(
                                                                 reinterpret_cast<const uint8_t *>(args) + sizeof(functionPtr));
    
    const uint8_t *buffer = reinterpret_cast<const uint8_t *>(args) +
    sizeof(functionPtr) + sizeof(argSize);
    
    for (size_t i = 0; i < numIters; ++i)
        functionPtr(buffer, argSize, startIt + i);
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("forEachWrapper-inline", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(argSize), 0, numIters);
}

template <typename FunT, typename InArgsT>
void forEachWrapper(uint64_t startIt, uint64_t numIters, const void *args,
                    gmt_handle_t) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    const impl::ExecFunWrapperArgs<FunT, InArgsT> *funArgs =
    reinterpret_cast<const impl::ExecFunWrapperArgs<FunT, InArgsT> *>(args);
    const InArgsT &fnargs = funArgs->args;
    
    for (size_t i = 0; i < numIters; ++i) funArgs->fun(fnargs, startIt + i);
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("forEachWrapper", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(InArgsT), 0, numIters);
}

static uint32_t garbageSize;

inline gmt_handle_t getGmtHandle(Handle &handle) {
  return static_cast<uint64_t>(handle);
}

template <typename FunT, typename InArgsT>
void execAsyncFunWrapper(const void *args, uint32_t args_size, void *,
                         uint32_t *, gmt_handle_t handle) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    const ExecFunWrapperArgs<FunT, InArgsT> &funArgs = *reinterpret_cast<const ExecFunWrapperArgs<FunT, InArgsT> *>(args);
    
    Handle H(handle);
    funArgs.fun(H, funArgs.args);
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("execAsyncFunWrapper", diff.count(), &H, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(InArgsT), 0, numIters);
}

inline void execAsyncFunWrapper(const void *args, uint32_t args_size, void *,
                                uint32_t *, gmt_handle_t handle) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    using FunctionTy = void (*)(Handle &, const uint8_t *, const uint32_t);
    
    FunctionTy functionPtr = *reinterpret_cast<FunctionTy *>(const_cast<void *>(args));
    
    Handle H(handle);
    functionPtr(H, reinterpret_cast<const uint8_t *>(args) + sizeof(functionPtr),
                args_size - sizeof(functionPtr));
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("execAsyncFunWrapper-inline", diff.count(), &H, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), args_size - sizeof(functionPtr), 0);
}

template <typename FunT, typename InArgsT, typename ResT>
void asyncExecFunWithRetWrapper(const void *args, uint32_t args_size,
                                void *result, uint32_t *resSize,
                                gmt_handle_t handle) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    using FunctionTy = void (*)(Handle &, const InArgsT &, ResT *);
    
    const ExecFunWrapperArgs<FunT, InArgsT> *funArgs = reinterpret_cast<const ExecFunWrapperArgs<FunT, InArgsT> *>(args);
    const InArgsT &fnargs = funArgs->args;
    
    Handle H(handle);
    funArgs->fun(H, fnargs, reinterpret_cast<ResT *>(result));
    *resSize = sizeof(ResT);
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("asyncExecFunWithRetWrapper", diff.count(), &H, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(InArgsT), sizeof(ResT));
}

template <typename ResT>
void asyncExecFunWithRetWrapper(const void *args, uint32_t args_size,
                                void *result, uint32_t *resSize,
                                gmt_handle_t handle) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    using FunctionTy =
    void (*)(Handle &, const uint8_t *, const uint32_t, ResT *);
    
    FunctionTy functionPtr = *reinterpret_cast<FunctionTy *>(const_cast<void *>(args));
    
    ResT *resultPtr = reinterpret_cast<ResT *>(result);
    
    Handle H(handle);
    functionPtr(H, reinterpret_cast<const uint8_t *>(args) + sizeof(functionPtr),
                args_size - sizeof(functionPtr), resultPtr);
    *resSize = sizeof(ResT);
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("asyncExecFunWithRetWrapper-ResT", diff.count(), &H, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), args_size - sizeof(functionPtr), sizeof(ResT));
}

template <typename FunT, typename InArgsT>
void asyncExecFunWithRetBuffWrapper(const void *args, uint32_t, void *result,
                                    uint32_t *resultSize, gmt_handle_t handle) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    const ExecFunWrapperArgs<FunT, InArgsT> *funArgs = reinterpret_cast<const ExecFunWrapperArgs<FunT, InArgsT> *>(args);
    const InArgsT &fnargs = funArgs->args;
    
    Handle H(handle);
    funArgs->fun(H, fnargs, reinterpret_cast<uint8_t *>(result), resultSize);
    
    checkOutputSize(*resultSize);
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("asyncExecFunWithRetBuffWrapper", diff.count(), &H, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(InArgsT), sizeof(*resultSize));
}

inline void asyncExecFunWithRetBuffWrapper(const void *args, uint32_t argsSize,
                                           void *result, uint32_t *resultSize,
                                           gmt_handle_t handle) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    using FunctionTy = void (*)(Handle &, const uint8_t *, const uint32_t,
                                uint8_t *, uint32_t *);
    
    FunctionTy functionPtr = *reinterpret_cast<FunctionTy *>(const_cast<void *>(args));
    
    Handle H(handle);
    functionPtr(H, reinterpret_cast<const uint8_t *>(args) + sizeof(functionPtr),
                argsSize - sizeof(functionPtr),
                reinterpret_cast<uint8_t *>(result), resultSize);
    
    checkOutputSize(*resultSize);
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("asyncExecFunWithRetBuffWrapper-inline", diff.count(), &H, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), argsSize - sizeof(functionPtr), sizeof(*resultSize));
}

inline void asyncForEachWrapper(uint64_t startIt, uint64_t numIters,
                                const void *args, gmt_handle_t handle) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    using FunctionTy =
    void (*)(Handle &, const uint8_t *, const uint32_t, size_t);
    
    FunctionTy functionPtr = *reinterpret_cast<FunctionTy *>(const_cast<void *>(args));
    const uint32_t argSize = *reinterpret_cast<const uint32_t *>(reinterpret_cast<const uint8_t *>(args) + sizeof(functionPtr));
    
    const uint8_t *buffer = reinterpret_cast<const uint8_t *>(args) + sizeof(functionPtr) + sizeof(argSize);
    Handle H(handle);
    for (size_t i = 0; i < numIters; ++i)
        functionPtr(H, buffer, argSize, startIt + i);
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("asyncForEachWrapper-inline", diff.count(), &H, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(argSize), 0, numIters);
}

template <typename FunT, typename InArgsT>
void asyncForEachWrapper(uint64_t startIt, uint64_t numIters, const void *args,
                         gmt_handle_t handle) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    const ExecFunWrapperArgs<FunT, InArgsT> *funArgs = reinterpret_cast<const ExecFunWrapperArgs<FunT, InArgsT> *>(args);
    const InArgsT &fnargs = funArgs->args;
    
    Handle H(handle);
    for (size_t i = 0; i < numIters; ++i) funArgs->fun(H, fnargs, startIt + i);
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("asyncForEachWrapper", diff.count(), &H, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(InArgsT), 0, numIters);
}

template <typename FunT, typename InArgsT>
void execAsyncFunWithRetBuffWrapper(const void *args, uint32_t args_size,
                                    void *result, uint32_t *resSize,
                                    gmt_handle_t handle) {
    // Start logging time
    auto t1 = shad_clock::now();
    
    const ExecFunWrapperArgs<FunT, InArgsT> *funArgs =
    reinterpret_cast<const ExecFunWrapperArgs<FunT, InArgsT> *>(args);
    const InArgsT &fnargs = funArgs->args;
    funArgs->fun(fnargs, reinterpret_cast<uint8_t *>(result), resSize,
                 Handle(handle));
    
    checkOutputSize(*resSize);
    
    // End logging time
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("execAsyncFunWithRetBuffWrapper", diff.count(), &H, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(InArgsT), sizeof(*resSize));
}

}  // namespace impl

}  // namespace rt
}  // namespace shad

#endif  // INCLUDE_SHAD_RUNTIME_MAPPINGS_GMT_GMT_UTILITY_H_
