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
 Methun K       07/27/2018      Adding logging functionality in asyncExecuteAt method
 Methun K       07/31/2018      Adding logging functionality in other methods
 Methun K       08/01/2018      Fixing the thisLocality issue
 --------------------------------------------------------------------------------
 */

#ifndef INCLUDE_SHAD_RUNTIME_MAPPINGS_TBB_TBB_ASYNCHRONOUS_INTERFACE_H_
#define INCLUDE_SHAD_RUNTIME_MAPPINGS_TBB_TBB_ASYNCHRONOUS_INTERFACE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "shad/runtime/asynchronous_interface.h"
#include "shad/runtime/handle.h"
#include "shad/runtime/locality.h"
#include "shad/runtime/mapping_traits.h"
#include "shad/runtime/mappings/tbb/tbb_traits_mapping.h"
#include "shad/runtime/mappings/tbb/tbb_utility.h"
#include "shad/util/slog.h"

namespace shad {
    namespace rt {
        
        namespace impl {
            
            template <>
            struct AsynchronousInterface<tbb_tag> {
                template <typename FunT, typename InArgsT>
                static void asyncExecuteAt(Handle &handle, const Locality &loc,
                                           FunT &&function, const InArgsT &args) {
                    // Start logging time
                    auto t1 = shad_clock::now();
                    
                    using FunctionTy = void (*)(Handle &, const InArgsT &);
                    
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    
                    checkLocality(loc);
                    
                    handle.id_ =
                    handle.IsNull() ? HandleTrait<tbb_tag>::CreateNewHandle() : handle.id_;
                    
                    handle.id_->run([=, &handle] { fn(handle, args); });
                    
                    // End logging time
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("asyncExecuteAt", diff.count(), &handle, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(InArgsT),0);
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
                    
                    handle.id_ =
                    handle.IsNull() ? HandleTrait<tbb_tag>::CreateNewHandle() : handle.id_;
                    
                    handle.id_->run([=, &handle] { fn(handle, argsBuffer.get(), bufferSize); });
                    
                    // End logging time
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("asyncExecuteAt-argBuffer", diff.count(), &handle, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(std::shared_ptr<uint8_t>), 0);
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
                    
                    handle.id_ =
                    handle.IsNull() ? HandleTrait<tbb_tag>::CreateNewHandle() : handle.id_;
                    
                    handle.id_->run(
                                    [=, &handle] { fn(handle, args, resultBuffer, resultSize); });
                    
                    // End logging time
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("asyncExecuteAtWithRetBuff", diff.count(), &handle, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(InArgsT), 0);
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
                    
                    handle.id_ =
                    handle.IsNull() ? HandleTrait<tbb_tag>::CreateNewHandle() : handle.id_;
                    
                    handle.id_->run([=, &handle] {
                        fn(handle, argsBuffer.get(), bufferSize, resultBuffer, resultSize);
                    });
                    
                    // End logging time
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("asyncExecuteAtWithRetBuff-argBuffer", diff.count(), &handle, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(std::shared_ptr<uint8_t>), 0);
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
                    
                    handle.id_ =
                    handle.IsNull() ? HandleTrait<tbb_tag>::CreateNewHandle() : handle.id_;
                    
                    handle.id_->run([=, &handle] { fn(handle, args, result); });
                    
                    // End logging time
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("asyncExecuteAtWithRet", diff.count(), &handle, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(InArgsT), 0);
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
                    
                    handle.id_ =
                    handle.IsNull() ? HandleTrait<tbb_tag>::CreateNewHandle() : handle.id_;
                    
                    handle.id_->run([&, fn, argsBuffer, bufferSize, result] {
                        fn(handle, argsBuffer.get(), bufferSize, result);
                    });
                    
                    // End logging time
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("asyncExecuteAtWithRet-argBuffer", diff.count(), &handle, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(std::shared_ptr<uint8_t>), 0);
                }
                
                template <typename FunT, typename InArgsT>
                static void asyncExecuteOnAll(Handle &handle, FunT &&function,
                                              const InArgsT &args) {
                    // Start logging time
                    auto t1 = shad_clock::now();
                    
                    using FunctionTy = void (*)(Handle &, const InArgsT &);
                    
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    
                    handle.id_ =
                    handle.IsNull() ? HandleTrait<tbb_tag>::CreateNewHandle() : handle.id_;
                    
                    handle.id_->run([=, &handle] { fn(handle, args); });
                    
                    // End logging time
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("asyncExecuteOnAll", diff.count(), &handle, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(InArgsT), 0);
                }
                
                template <typename FunT>
                static void asyncExecuteOnAll(Handle &handle, FunT &&function,
                                              const std::shared_ptr<uint8_t> &argsBuffer,
                                              const uint32_t bufferSize) {
                    // Start logging time
                    auto t1 = shad_clock::now();
                    
                    using FunctionTy = void (*)(Handle &, const uint8_t *, const uint32_t);
                    
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    
                    handle.id_ =
                    handle.IsNull() ? HandleTrait<tbb_tag>::CreateNewHandle() : handle.id_;
                    
                    handle.id_->run([=, &handle] { fn(handle, argsBuffer.get(), bufferSize); });
                    
                    // End logging time
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("asyncExecuteOnAll-argBuffer", diff.count(), &handle, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(std::shared_ptr<uint8_t>), 0);
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
                    
                    handle.id_ =
                    handle.IsNull() ? HandleTrait<tbb_tag>::CreateNewHandle() : handle.id_;
                    
                    handle.id_->run([=, &handle] {
                        tbb::parallel_for(tbb::blocked_range<size_t>(0, numIters),
                                          [=, &handle](const tbb::blocked_range<size_t> &range) {
                                              for (auto i = range.begin(); i < range.end(); ++i)
                                                  fn(handle, args, i);
                                          });
                    });
                    
                    // End logging time
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("asyncForEachAt", diff.count(), &handle, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(InArgsT), 0, numIters);
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
                    
                    handle.id_ =
                    handle.IsNull() ? HandleTrait<tbb_tag>::CreateNewHandle() : handle.id_;
                    
                    handle.id_->run([=, &handle] {
                        tbb::parallel_for(tbb::blocked_range<size_t>(0, numIters),
                                          [=, &handle](const tbb::blocked_range<size_t> &range) {
                                              for (auto i = range.begin(); i < range.end(); ++i)
                                                  fn(handle, argsBuffer.get(), bufferSize, i);
                                          });
                    });
                    
                    // End logging time
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("asyncForEachAt-argsBuffer", diff.count(), &handle, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), loc, sizeof(std::shared_ptr<uint8_t>), 0, numIters);
                }
                
                template <typename FunT, typename InArgsT>
                static void asyncForEachOnAll(Handle &handle, FunT &&function,
                                              const InArgsT &args, const size_t numIters) {
                    // Start logging time
                    auto t1 = shad_clock::now();
                    
                    using FunctionTy = void (*)(Handle &, const InArgsT &, size_t);
                    
                    FunctionTy fn = std::forward<decltype(function)>(function);
                    
                    handle.id_ =
                    handle.IsNull() ? HandleTrait<tbb_tag>::CreateNewHandle() : handle.id_;
                    
                    handle.id_->run([=, &handle] {
                        tbb::parallel_for(tbb::blocked_range<size_t>(0, numIters),
                                          [&](const tbb::blocked_range<size_t> &range) {
                                              for (auto i = range.begin(); i < range.end(); ++i)
                                                  fn(handle, args, i);
                                          });
                    });
                    
                    // End logging time
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("asyncForEachOnAll", diff.count(), &handle, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(InArgsT), 0, numIters);
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
                    
                    handle.id_ =
                    handle.IsNull() ? HandleTrait<tbb_tag>::CreateNewHandle() : handle.id_;
                    
                    handle.id_->run([=, &handle] {
                        tbb::parallel_for(tbb::blocked_range<size_t>(0, numIters),
                                          [=, &handle](const tbb::blocked_range<size_t> &range) {
                                              for (auto i = range.begin(); i < range.end(); ++i)
                                                  fn(handle, argsBuffer.get(), bufferSize, i);
                                          });
                    });
                    
                    // End logging time
                    auto t2 = shad_clock::now();
                    std::chrono::duration<double> diff = t2-t1;
                    auto log_handler = shad::slog::ShadLog::Instance();
                    log_handler->printlf("asyncForEachOnAll-argsBuffer", diff.count(), &handle, RuntimeInternalsTrait<tbb_tag>::ThisLocality(), RuntimeInternalsTrait<tbb_tag>::ThisLocality(), sizeof(std::shared_ptr<uint8_t>), 0, numIters);
                }
            };
            
        }  // namespace impl
        
    }  // namespace rt
}  // namespace shad

#endif  // INCLUDE_SHAD_RUNTIME_MAPPINGS_TBB_TBB_ASYNCHRONOUS_INTERFACE_H_
