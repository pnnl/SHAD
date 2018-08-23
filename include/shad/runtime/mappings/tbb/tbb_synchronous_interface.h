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
 Methun K       08/01/2018      Adding logging
 --------------------------------------------------------------------------------
 */

#ifndef INCLUDE_SHAD_RUNTIME_MAPPINGS_TBB_TBB_SYNCHRONOUS_INTERFACE_H_
#define INCLUDE_SHAD_RUNTIME_MAPPINGS_TBB_TBB_SYNCHRONOUS_INTERFACE_H_

#include <memory>
#include <utility>

#include "shad/runtime/locality.h"
#include "shad/runtime/mappings/tbb/tbb_traits_mapping.h"
#include "shad/runtime/mappings/tbb/tbb_utility.h"
#include "shad/runtime/synchronous_interface.h"
#include "shad/util/slog.h"

namespace shad {
    namespace rt {
        
        namespace impl {
            
            template <>
            struct SynchronousInterface<tbb_tag> {
                template <typename FunT, typename InArgsT>
                static void executeAt(const Locality &loc, FunT &&function,
                                      const InArgsT &args) {
#if defined HAVE_LOGGING
                    auto t1 = shad_clock::now();
#endif
                    
                    using FunctionTy = void (*)(const InArgsT &);
                    
                    checkLocality(loc);
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    fn(args);
                    
#if defined HAVE_LOGGING
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("executeAt", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(InArgsT), 0);
#endif
                }
                
                template <typename FunT>
                static void executeAt(const Locality &loc, FunT &&function,
                                      const std::shared_ptr<uint8_t> &argsBuffer,
                                      const uint32_t bufferSize) {
#if defined HAVE_LOGGING
                    auto t1 = shad_clock::now();
#endif
                    
                    using FunctionTy = void (*)(const uint8_t *, const uint32_t);
                    
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    checkLocality(loc);
                    fn(argsBuffer.get(), bufferSize);
                    
#if defined HAVE_LOGGING
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("executeAt", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(std::shared_ptr<uint8_t>), 0);
#endif
                }
                
                template <typename FunT, typename InArgsT>
                static void executeAtWithRetBuff(const Locality &loc, FunT &&function,
                                                 const InArgsT &args, uint8_t *resultBuffer,
                                                 uint32_t *resultSize) {
#if defined HAVE_LOGGING
                    auto t1 = shad_clock::now();
#endif
                    
                    using FunctionTy = void (*)(const InArgsT &, uint8_t *, uint32_t *);
                    
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    checkLocality(loc);
                    fn(args, resultBuffer, resultSize);
                    
#if defined HAVE_LOGGING
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("executeAtWithRetBuff", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(InArgsT), 0);
#endif
                }
                
                template <typename FunT>
                static void executeAtWithRetBuff(const Locality &loc, FunT &&function,
                                                 const std::shared_ptr<uint8_t> &argsBuffer,
                                                 const uint32_t bufferSize,
                                                 uint8_t *resultBuffer,
                                                 uint32_t *resultSize) {
#if defined HAVE_LOGGING
                    auto t1 = shad_clock::now();
#endif
                    
                    using FunctionTy =
                    void (*)(const uint8_t *, const uint32_t, uint8_t *, uint32_t *);
                    
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    checkLocality(loc);
                    fn(argsBuffer.get(), bufferSize, resultBuffer, resultSize);
                    
#if defined HAVE_LOGGING
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("executeAtWithRetBuff", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(std::shared_ptr<uint8_t>), 0);
#endif
                }
                
                template <typename FunT, typename InArgsT, typename ResT>
                static void executeAtWithRet(const Locality &loc, FunT &&function,
                                             const InArgsT &args, ResT *result) {
#if defined HAVE_LOGGING
                    auto t1 = shad_clock::now();
#endif
                    
                    using FunctionTy = void (*)(const InArgsT &, ResT *);
                    
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    checkLocality(loc);
                    fn(args, result);
                    
#if defined HAVE_LOGGING
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("executeAtWithRet", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(InArgsT), 0);
#endif
                }
                
                template <typename FunT, typename ResT>
                static void executeAtWithRet(const Locality &loc, FunT &&function,
                                             const std::shared_ptr<uint8_t> &argsBuffer,
                                             const uint32_t bufferSize, ResT *result) {
#if defined HAVE_LOGGING
                    auto t1 = shad_clock::now();
#endif
                    
                    using FunctionTy = void (*)(const uint8_t *, const uint32_t, ResT *);
                    
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    checkLocality(loc);
                    fn(argsBuffer.get(), bufferSize, result);
                    
#if defined HAVE_LOGGING
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("executeAtWithRet", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(std::shared_ptr<uint8_t>), 0);
#endif
                }
                
                template <typename FunT, typename InArgsT>
                static void executeOnAll(FunT &&function, const InArgsT &args) {
#if defined HAVE_LOGGING
                    auto t1 = shad_clock::now();
#endif
                    
                    using FunctionTy = void (*)(const InArgsT &);
                    
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    fn(args);
                    
#if defined HAVE_LOGGING
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("executeOnAll", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(InArgsT), 0);
#endif
                }
                
                template <typename FunT>
                static void executeOnAll(FunT &&function,
                                         const std::shared_ptr<uint8_t> &argsBuffer,
                                         const uint32_t bufferSize) {
#if defined HAVE_LOGGING
                    auto t1 = shad_clock::now();
#endif
                    
                    using FunctionTy = void (*)(const uint8_t *, const uint32_t);
                    
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    fn(argsBuffer.get(), bufferSize);
                    
#if defined HAVE_LOGGING
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("executeOnAll", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(std::shared_ptr<uint8_t>), 0);
#endif
                }
                
                template <typename FunT, typename InArgsT>
                static void forEachAt(const Locality &loc, FunT &&function,
                                      const InArgsT &args, const size_t numIters) {
#if defined HAVE_LOGGING
                    auto t1 = shad_clock::now();
#endif
                    
                    using FunctionTy = void (*)(const InArgsT &, size_t);
                    
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    
                    checkLocality(loc);
                    tbb::parallel_for(tbb::blocked_range<size_t>(0, numIters),
                                      [&](const tbb::blocked_range<size_t> &range) {
                                          for (auto i = range.begin(); i < range.end(); ++i)
                                              fn(args, i);
                                      });
                    
#if defined HAVE_LOGGING
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("forEachAt", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(InArgsT), 0);
#endif
                }
                
                template <typename FunT>
                static void forEachAt(const Locality &loc, FunT &&function,
                                      const std::shared_ptr<uint8_t> &argsBuffer,
                                      const uint32_t bufferSize, const size_t numIters) {
#if defined HAVE_LOGGING
                    auto t1 = shad_clock::now();
#endif
                    
                    using FunctionTy = void (*)(const uint8_t *, const uint32_t, size_t);
                    
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    
                    checkLocality(loc);
                    tbb::parallel_for(tbb::blocked_range<size_t>(0, numIters),
                                      [&](const tbb::blocked_range<size_t> &range) {
                                          for (auto i = range.begin(); i < range.end(); ++i)
                                              fn(argsBuffer.get(), bufferSize, i);
                                      });
                    
#if defined HAVE_LOGGING
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("forEachAt", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(std::shared_ptr<uint8_t>), 0);
#endif
                }
                
                template <typename FunT, typename InArgsT>
                static void forEachOnAll(FunT &&function, const InArgsT &args,
                                         const size_t numIters) {
#if defined HAVE_LOGGING
                    auto t1 = shad_clock::now();
#endif
                    
                    using FunctionTy = void (*)(const InArgsT &, size_t);
                    
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    
                    tbb::parallel_for(tbb::blocked_range<size_t>(0, numIters),
                                      [&](const tbb::blocked_range<size_t> &range) {
                                          for (auto i = range.begin(); i < range.end(); ++i)
                                              fn(args, i);
                                      });
                    
#if defined HAVE_LOGGING
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("forEachOnAll", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(InArgsT), 0, numIters);
#endif
                }
                
                template <typename FunT>
                static void forEachOnAll(FunT &&function,
                                         const std::shared_ptr<uint8_t> &argsBuffer,
                                         const uint32_t bufferSize, const size_t numIters) {
#if defined HAVE_LOGGING
                    auto t1 = shad_clock::now();
#endif
                    
                    using FunctionTy = void (*)(const uint8_t *, const uint32_t, size_t);
                    
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    
                    tbb::parallel_for(tbb::blocked_range<size_t>(0, numIters),
                                      [&](const tbb::blocked_range<size_t> &range) {
                                          for (auto i = range.begin(); i < range.end(); ++i)
                                              fn(argsBuffer.get(), bufferSize, i);
                                      });
                    
#if defined HAVE_LOGGING
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("forEachOnAll", diff.count(), nullptr, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(std::shared_ptr<uint8_t>), 0, numIters);
#endif
                }
            };
            
        }  // namespace impl
        
    }  // namespace rt
}  // namespace shad

#endif  // INCLUDE_SHAD_RUNTIME_MAPPINGS_TBB_TBB_SYNCHRONOUS_INTERFACE_H_

