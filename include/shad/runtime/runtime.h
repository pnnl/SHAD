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

#ifndef INCLUDE_SHAD_RUNTIME_RUNTIME_H_
#define INCLUDE_SHAD_RUNTIME_RUNTIME_H_

#include <cstddef>
#include <cstdint>

#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <unordered_set>
#include <utility>
#include <vector>

#include "shad/config/config.h"
#include "shad/runtime/handle.h"
#include "shad/runtime/locality.h"
#include "shad/runtime/mapping_traits.h"
#include "shad/runtime/mappings/available_mappings.h"
#include "shad/runtime/synchronous_interface.h"

/// @namespace shad
namespace shad {

namespace rt {

/// @brief Lock.
///
/// The Lock can be used to lock a non-threadsafe objects.
/// Locks are local, and cannot be transfered remote localities.
class Lock {
 public:
  /// @brief Acquire the lock.
  void lock() { impl::LockTrait<TargetSystemTag>::lock(lock_); }
  /// @brief Release the lock.
  void unlock() { impl::LockTrait<TargetSystemTag>::unlock(lock_); }

 private:
  typename impl::LockTrait<TargetSystemTag>::LockTy lock_;
};

namespace impl {

/// @brief yield the runtime
inline void yield() { RuntimeInternalsTrait<TargetSystemTag>::Yield(); }

/// @brief returns number of threads/cores
inline size_t getConcurrency() {
  return RuntimeInternalsTrait<TargetSystemTag>::Concurrency();
}

/// @brief Initialize the runtime environment.
/// @param argc pointer to argument count
/// @param argv pointer to array of char *
inline void initialize(int argc, char *argv[]) {
  RuntimeInternalsTrait<TargetSystemTag>::Initialize(argc, argv);
}

/// @brief Finailize the runtime environment prior to program termination.
inline void finalize() { RuntimeInternalsTrait<TargetSystemTag>::Finalize(); }

/// @brief Creates a new Handle.
inline Handle createHandle() {
  // auto handle = HandleTrait<TargetSystemTag>::CreateNewHandle();
  // Handle newHandle(handle);
  return Handle(HandleTrait<TargetSystemTag>::CreateNewHandle());
}

}  // namespace impl

/// @brief Number of localities on which the runtime is executing.
inline uint32_t numLocalities() {
  return impl::RuntimeInternalsTrait<TargetSystemTag>::NumLocalities();
}

/// @brief Identity of locality on which the call is made.
inline Locality thisLocality() {
  return Locality(impl::RuntimeInternalsTrait<TargetSystemTag>::ThisLocality());
}

inline std::set<Locality> allLocalities() {
  std::set<Locality> result;
  for (uint32_t L = 0; L < numLocalities(); ++L) {
    result.insert(Locality(L));
  }
  return result;
}

/// @brief Execute a function on a selected locality synchronously.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(const Args & args) {  /* do something */ }
///
/// Args args { 2, 'a' };
/// for (auto & locality : allLocalities)
///   if (static_cast<uint32_t>(locality) % 2)
///     executeAt(locality, task, args);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(const InArgsT &);
/// @endcode
///
/// @tparam InArgsT The type of the argument accepted by the function.  The type
/// can be a structure or a class but with the restriction that must be
/// memcopy-able.
///
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param args The arguments to be passed to the function.
template <typename FunT, typename InArgsT>
void executeAt(const Locality &loc, FunT &&func, const InArgsT &args) {
  impl::SynchronousInterface<TargetSystemTag>::executeAt(loc, func, args);
}

/// @brief Execute a function on a selected locality synchronously.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(const uint8_t *, const uint32_t) {  /* do something */ }
///
/// Args args { 2, 'a' };
/// /* Args doesn't need a dynamic allocated buffer but
///  * more complicated data structure might need it */
/// std::shared_ptr<uint8_t> ptr(new uint8_t[sizeof(Args)],
///                              std::default_delete<uint8_t[]>());
/// memcpy(ptr.get(), &args, sizeof(Args));
///
/// for (auto & locality : allLocalities)
///   if (static_cast<uint32_t>(locality) % 2)
///     executeAt(locality, task, ptr, sizeof(Args));
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(const uint8_t *, const uint32_t);
/// @endcode
///
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param argsBuffer A buffer of bytes to be passed to the function.
/// @param bufferSize The size of the buffer argsBuffer passed.
template <typename FunT>
void executeAt(const Locality &loc, FunT &&func,
               const std::shared_ptr<uint8_t> &argsBuffer,
               const uint32_t bufferSize) {
  impl::SynchronousInterface<TargetSystemTag>::executeAt(loc, func, argsBuffer,
                                                         bufferSize);
}

/// @brief Execute a function on a selected locality synchronously and return a
/// buffer.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(const Args & args,
///           const uint8_t *dst, uint32_t * outsize) {
///   memcpy(&args, dst, sizeof(Args));
///   *outsize = sizeof(Args);
/// }
///
/// Args args { 2, 'a' };
/// std::unique_ptr<uint8_t[]> outbuff(new uint8_t[sizeof(Args)]);
/// uint32_t size(0);
/// for (auto & locality : allLocalities)
///   if (static_cast<uint32_t>(locality) % 2)
///     // every call will overwrite outbuff
///     executeAtWithRetBuffer(locality, task, args, outbuff.get(), &size);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(const InArgsT &, const uint8_t *, uint32_t *);
/// @endcode
///
/// @tparam InArgsT The type of the argument accepted by the function.  The type
/// can be a structure or a class but with the restriction that must be
/// memcopy-able.
///
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param args The arguments to be passed to the function.
/// @param resultBuffer The buffer where to store the results.
/// @param resultSize The location where the runtime will store the number of
/// bytes written in the result buffer
template <typename FunT, typename InArgsT>
void executeAtWithRetBuff(const Locality &loc, FunT &&func, const InArgsT &args,
                          uint8_t *resultBuffer, uint32_t *resultSize) {
  impl::SynchronousInterface<TargetSystemTag>::executeAtWithRetBuff(
      loc, func, args, resultBuffer, resultSize);
}

/// @brief Execute a function on a selected locality synchronously and
/// return a result buffer.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(const uint8_t *src, const uint32_t size,
///           const uint8_t *dst, uint32_t * outsize) {
///   memcpy(src, dst, size);
///   *outsize = size;
/// }
///
/// Args args { 2, 'a' };
/// /* Args doesn't need a dynamic allocated buffer but
///  * more complicated data structure might need it */
/// std::shared_ptr<uint8_t> ptr(new uint8_t[sizeof(Args)],
///                              std::default_delete<uint8_t[]>());
/// memcpy(ptr.get(), &args, sizeof(Args));
///
/// std::unique_ptr<uint8_t[]> outbuff(new uint8_t[sizeof(Args)]);
/// uint32_t size(0);
/// for (auto & locality : allLocalities)
///   if (static_cast<uint32_t>(locality) % 2)
///     // every call will overwrite outbuff
///     executeAtWithRetBuff(locality, task, ptr, sizeof(Args),
///                          outbuff.get(), &size);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(const uint8_t *, const uint32_t, const uint8_t *, uint32_t *);
/// @endcode
///
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param argsBuffer A buffer of bytes to be passed to the function.
/// @param bufferSize The size of the buffer argsBuffer passed.
/// @param resultBuffer The buffer where to store the results.
/// @param resultSize The location where the runtime will store the number of
/// bytes written in the result buffer
template <typename FunT>
void executeAtWithRetBuff(const Locality &loc, FunT &&func,
                          const std::shared_ptr<uint8_t> &argsBuffer,
                          const uint32_t bufferSize, uint8_t *resultBuffer,
                          uint32_t *resultSize) {
  impl::SynchronousInterface<TargetSystemTag>::executeAtWithRetBuff(
      loc, func, argsBuffer, bufferSize, resultBuffer, resultSize);
}

/// @brief Execute a function on a selected locality synchronously and return a
/// result.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(const Args & args, Args * outArgs) {
///   *outArgs = args;
/// }
///
/// Args args { 2, 'a' };
/// Args out;
/// for (auto & locality : allLocalities)
///   if (static_cast<uint32_t>(locality) % 2)
///     // every call will overwrite outbuff
///     executeAtWithRet(locality, task, args, out);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(const InArgsT &, ResT *);
/// @endcode
///
/// @tparam InArgsT The type of the argument accepted by the function.  The type
/// can be a structure or a class but with the restriction that must be
/// memcopy-able.
///
/// @tparam ResT The type of the result value.  The type can be a structure or a
/// class but with the restriction that must be memcopy-able.
///
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param args The arguments to be passed to the function.
/// @param result The location where to store the result.
template <typename FunT, typename InArgsT, typename ResT>
void executeAtWithRet(const Locality &loc, FunT &&func, const InArgsT &args,
                      ResT *result) {
  impl::SynchronousInterface<TargetSystemTag>::executeAtWithRet(loc, func, args,
                                                                result);
}

/// @brief Execute a function on a selected locality synchronously and
/// return a result.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(const uint8_t *src, const uint32_t size,
///           Args * res) {
///   memcpy(src, res, sizeof(Args));
/// }
///
/// Args args { 2, 'a' };
/// /* Args doesn't need a dynamic allocated buffer but
///  * more complicated data structure might need it */
/// std::shared_ptr<uint8_t> ptr(new uint8_t[sizeof(Args)],
///                              std::default_delete<uint8_t[]>());
/// memcpy(ptr.get(), &args, sizeof(Args));
///
/// Args out;
/// for (auto & locality : allLocalities)
///   if (static_cast<uint32_t>(locality) % 2)
///     // every call will overwrite out
///     executeAtWithRet(locality, task, ptr, sizeof(Args), &out);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(const uint8_t *, const uint32_t, ResT *);
/// @endcode
///
/// @tparam ResT The type of the result value.  The type can be a structure or a
/// class but with the restriction that must be memcopy-able.
///
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param argsBuffer A buffer of bytes to be passed to the function.
/// @param bufferSize The size of the buffer argsBuffer passed.
/// @param result The location where to store the result.
template <typename FunT, typename ResT>
void executeAtWithRet(const Locality &loc, FunT &&func,
                      const std::shared_ptr<uint8_t> &argsBuffer,
                      const uint32_t bufferSize, ResT *result) {
  impl::SynchronousInterface<TargetSystemTag>::executeAtWithRet(
      loc, func, argsBuffer, bufferSize, result);
}

/// @brief Execute a function on all localities synchronously.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(const Args & args) {  /* do something */ }
///
/// Args args { 2, 'a' };
/// executeOnAll(task, args);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(const InArgsT &);
/// @endcode
///
/// @tparam InArgsT The type of the argument accepted by the function.  The type
/// can be a structure or a class but with the restriction that must be
/// memcopy-able.
///
/// @param func The function to execute.
/// @param args The arguments to be passed to the function.
template <typename FunT, typename InArgsT>
void executeOnAll(FunT &&func, const InArgsT &args) {
  impl::SynchronousInterface<TargetSystemTag>::executeOnAll(func, args);
}

/// @brief Execute a function on all localities synchronously.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(const uint8_t *, const uint32_t) {  /* do something */ }
///
/// Args args { 2, 'a' };
/// /* Args doesn't need a dynamic allocated buffer but
///  * more complicated data structure might need it */
/// std::shared_ptr<uint8_t> ptr(new uint8_t[sizeof(Args)],
///                              std::default_delete<uint8_t[]>());
/// memcpy(ptr.get(), &args, sizeof(Args));
///
/// executeOnAll(task, ptr.get(), sizeof(Args));
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(const uint8_t *, const uint32_t);
/// @endcode
///
/// @param func The function to execute.
/// @param argsBuffer A buffer of bytes to be passed to the function.
/// @param bufferSize The size of the buffer argsBuffer passed.
template <typename FunT>
void executeOnAll(FunT &&func, const std::shared_ptr<uint8_t> &argsBuffer,
                  const uint32_t bufferSize) {
  impl::SynchronousInterface<TargetSystemTag>::executeOnAll(func, argsBuffer,
                                                            bufferSize);
}

/// @brief Execute a parallel loop at a specific locality.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// Args args { 2, 'a' };
///
/// shad::rt::forEachAt(locality,
///     [](const Args & input, size_t itrNum) {
///         // Do something.
///     },
///     args, iterations);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(const ArgsT &, size_t itrNum);
/// @endcode
/// where the itrNum is the n-th iteration of the loop.
///
/// @tparam InArgsT The type of the argument accepted by the function.  The type
/// can be a structure or a class but with the restriction that must be
/// memcopy-able.
///
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param args The arguments to be passed to the function.
/// @param numIters  The total number of iteration of the loop.
template <typename FunT, typename InArgsT>
void forEachAt(const Locality &loc, FunT &&func, const InArgsT &args,
               const size_t numIters) {
  impl::SynchronousInterface<TargetSystemTag>::forEachAt(loc, func, args,
                                                         numIters);
}

/// @brief Execute a parallel loop at a specific locality.
///
/// Typical Usage:
/// @code
/// std::shared_ptr<uint8_t> ptr(new uint8_t[2]{ 5, 5 },
///                              std::default_delete<uint8_t[]>());
///
/// shad::rt::forEachAt(locality,
///     [](const uint8_t * input, const uint32_t size, size_t itrNum) {
///         // Do something.
///     },
///     buffer, sizeof(buffer),
///     iterations);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(const uint8_t *, const uint32_t, size_t itrNum);
/// @endcode
/// where the itrNum is the n-th iteration of the loop.
///
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param argsBuffer A buffer of bytes to be passed to the function.
/// @param bufferSize The size of the buffer argsBuffer passed.
/// @param numIters  The total number of iteration of the loop.
template <typename FunT>
void forEachAt(const Locality &loc, FunT &&func,
               const std::shared_ptr<uint8_t> &argsBuffer,
               const uint32_t bufferSize, const size_t numIters) {
  impl::SynchronousInterface<TargetSystemTag>::forEachAt(loc, func, argsBuffer,
                                                         bufferSize, numIters);
}

/// @brief Execute a parallel loop on the whole system.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// Args args { 2, 'a' };
///
/// shad::rt::forEachOnAll(
///     [](const Args & args, size_t itrNum) {
///       // Do something
///     },
///     args,
///     iterations);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(const ArgsT &, size_t itrNum);
/// @endcode
/// where the itrNum is the n-th iteration of the loop.
///
/// @tparam InArgsT The type of the argument accepted by the function.  The type
/// can be a structure or a class but with the restriction that must be
/// memcopy-able.
///
/// @param func The function to execute.
/// @param args The arguments to be passed to the function.
/// @param numIters  The total number of iteration of the loop.
template <typename FunT, typename InArgsT>
void forEachOnAll(FunT &&func, const InArgsT &args, const size_t numIters) {
  impl::SynchronousInterface<TargetSystemTag>::forEachOnAll(func, args,
                                                            numIters);
}

/// @brief Execute a parallel loop on the whole system.
///
/// Typical Usage:
/// @code
/// std::shared_ptr<uint8_t> ptr(new uint8_t[2]{ 5, 5 },
///                              std::default_delete<uint8_t[]>());
///
///   shad::rt::forEachOnAll(
///       [](const uint8_t * input, const uint32_t size, size_t itrNum) {
///         // Do something
///       },
///       buffer, sizeof(buffer),
///       iterations);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(const uint8_t *, const uint32_t, size_t itrNum);
/// @endcode
/// where the itrNum is the n-th iteration of the loop.
///
/// @param func The function to execute.
/// @param argsBuffer A buffer of bytes to be passed to the function.
/// @param bufferSize The size of the buffer argsBuffer passed.
/// @param numIters  The total number of iteration of the loop.
template <typename FunT>
void forEachOnAll(FunT &&func, const std::shared_ptr<uint8_t> &argsBuffer,
                  const uint32_t bufferSize, const size_t numIters) {
  impl::SynchronousInterface<TargetSystemTag>::forEachOnAll(
      func, argsBuffer, bufferSize, numIters);
}

/// @brief Execute a function on a selected locality asynchronously.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(Handle & handle, const Args & args) {  /* do something */ }
///
/// Args args { 2, 'a' };
/// Handle handle;
/// for (auto & locality : allLocalities)
///   if (static_cast<uint32_t>(locality) % 2)
///     asyncExecuteAt(handle, locality, task, args);
///
/// waitForCompletion(handle);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(Handle &, const InArgsT &);
/// @endcode
///
/// @tparam InArgsT The type of the argument accepted by the function.  The type
/// can be a structure or a class but with the restriction that must be
/// memcopy-able.
///
/// @param handle An Handle for the associated task-group.
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param args The arguments to be passed to the function.
template <typename FunT, typename InArgsT>
void asyncExecuteAt(Handle &handle, const Locality &loc, FunT &&func,
                    const InArgsT &args) {
  impl::AsynchronousInterface<TargetSystemTag>::asyncExecuteAt(handle, loc,
                                                               func, args);
}

/// @brief Execute a function on a selected locality asynchronously.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(Handle & handle,
///           const uint8_t * buff,
///           const uint32_t size) {  /* do something */ }
///
/// Args args { 2, 'a' };
/// /* Args doesn't need a dynamic allocated buffer but
///  * more complicated data structure might need it */
/// std::shared_ptr<uint8_t> ptr(new uint8_t[sizeof(Args)],
///                              std::default_delete<uint8_t[]>());
/// memcpy(ptr.get(), &args, sizeof(Args));
///
/// Handle handle;
/// for (auto & locality : allLocalities)
///   if (static_cast<uint32_t>(locality) % 2)
///     executeAt(handle, locality, task, ptr.get(), sizeof(Args));
///
/// waitForCompletion(handle)
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(Handle &, const uint8_t *, const uint32_t);
/// @endcode
///
/// @param handle An Handle for the associated task-group.
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param argsBuffer A buffer of bytes to be passed to the function.
/// @param bufferSize The size of the buffer argsBuffer passed.
template <typename FunT>
void asyncExecuteAt(Handle &handle, const Locality &loc, FunT &&func,
                    const std::shared_ptr<uint8_t> &argsBuffer,
                    const uint32_t bufferSize) {
  impl::AsynchronousInterface<TargetSystemTag>::asyncExecuteAt(
      handle, loc, func, argsBuffer, bufferSize);
}

/// @brief Execute a function on a selected locality synchronously and return a
/// buffer.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(Handle & handle, const Args & args,
///           const uint8_t *dst, uint32_t * outsize) {
///   memcpy(&args, dst, sizeof(Args));
///   *outsize = sizeof(Args);
/// }
///
/// Args args { 2, 'a' };
/// std::vector<std::unique_ptr<uint8_t[]>> outBuffers(numLocalities());
/// std::vector<uint32_t> outSizes(numLocalities(), 0);
/// for (auto & vecPtr : outBuffers) {
///   std::unique_ptr<uint8_t[]> outbuff(new uint8_t[sizeof(Args)]);
///   vecPtr = std::move(outbuff);
/// }
///
/// Handle handle;
/// for (auto & locality : allLocalities()) {
///   uint32_t localityID = static_cast<uint32_t>(locality);
///   if (localityID % 2)
///     // every call will overwrite outbuff
///     asyncExecuteAtWithRetBuff(
///       handle, locality, task, args, outBuffers[localityID].get(),
///       &outSizes[localityID]);
/// }
///
/// waitForCompletion(handle);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(Handle &, const InArgsT &, const uint8_t *, uint32_t *);
/// @endcode
///
/// @tparam InArgsT The type of the argument accepted by the function.  The type
/// can be a structure or a class but with the restriction that must be
/// memcopy-able.
///
/// @param handle An Handle for the associated task-group.
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param args The arguments to be passed to the function.
/// @param resultBuffer The buffer where to store the results.
/// @param resultSize The location where the runtime will store the number of
/// bytes written in the result buffer
template <typename FunT, typename InArgsT>
void asyncExecuteAtWithRetBuff(Handle &handle, const Locality &loc, FunT &&func,
                               const InArgsT &args, uint8_t *resultBuffer,
                               uint32_t *resultSize) {
  impl::AsynchronousInterface<TargetSystemTag>::asyncExecuteAtWithRetBuff(
      handle, loc, func, args, resultBuffer, resultSize);
}

/// @brief Execute a function on a selected locality asynchronously and return a
/// result buffer.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(Handle & handle, const uint8_t *src, const uint32_t size,
///           const uint8_t *dst, uint32_t * outsize) {
///   memcpy(src, dst, size);
///   *outsize = size;
/// }
///
/// Args args { 2, 'a' };
/// /* Args doesn't need a dynamic allocated buffer but
///  * more complicated data structure might need it */
/// std::shared_ptr<uint8_t> ptr(new uint8_t[sizeof(Args)],
///                              std::default_delete<uint8_t[]>());
/// memcpy(ptr.get(), &args, sizeof(Args));
///
/// std::vector<std::unique_ptr<uint8_t[]>> outBuffers(numLocalities());
/// std::vector<uint32_t> outSizes(numLocalities(), 0);
/// for (auto & vecPtr : outBuffers) {
///   std::unique_ptr<uint8_t[]> outbuff(new uint8_t[sizeof(Args)]);
///   vecPtr = std::move(outbuff);
/// }
///
/// Handle handle;
/// for (auto & locality : allLocalities) {
///   if (static_cast<uint32_t>(locality) % 2)
///     // every call will overwrite outbuff
///     asyncExecuteAtWithRetBuff(
///       handle, locality, task, ptr, sizeof(Args),
///       outBuffers[localityID].get(), &outSizes[localityID]);
/// }
///
/// waitForCompletion(handle);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(Handle &, const uint8_t *, const uint32_t, const uint8_t *, uint32_t *)
/// @endcode
///
/// @param handle An Handle for the associated task-group.
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param argsBuffer A buffer of bytes to be passed to the function.
/// @param bufferSize The size of the buffer argsBuffer passed.
/// @param resultBuffer The buffer where to store the results.
/// @param resultSize The location where the runtime will store the number of
/// bytes written in the result buffer
template <typename FunT>
void asyncExecuteAtWithRetBuff(Handle &handle, const Locality &loc, FunT &&func,
                               const std::shared_ptr<uint8_t> &argsBuffer,
                               const uint32_t bufferSize, uint8_t *resultBuffer,
                               uint32_t *resultSize) {
  impl::AsynchronousInterface<TargetSystemTag>::asyncExecuteAtWithRetBuff(
      handle, loc, func, argsBuffer, bufferSize, resultBuffer, resultSize);
}

/// @brief Execute a function on a selected locality asynchronously and return a
/// result.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(Handle & handle, const Args & args, Args * res) {
///   // Do something.
/// }
///
/// Args args { 2, 'a' };
/// /* Args doesn't need a dynamic allocated buffer but
///  * more complicated data structure might need it */
/// std::vector<Args> outBuffers(numLocalities());
///
/// Handle handle;
/// for (auto & locality : allLocalities()) {
///   uint32_t localityID = static_cast<uint32_t>(locality);
///   asyncExecuteAtWithRet(
///     handle, locality, task, args, &outBuffers[localityID]);
/// }
/// waitForCompletion(handle);
/// @endcode
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(Handle &, const InArgsT &, ResT *);
/// @endcode
///
/// @tparam InArgsT The type of the argument accepted by the function.  The type
/// can be a structure or a class but with the restriction that must be
/// memcopy-able.
///
/// @tparam ResT The type of the result value.  The type can be a structure or a
/// class but with the restriction that must be memcopy-able.
///
/// @param handle An Handle for the associated task-group.
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param args The arguments to be passed to the function.
/// @param result The location where to store the result.
template <typename FunT, typename InArgsT, typename ResT>
void asyncExecuteAtWithRet(Handle &handle, const Locality &loc, FunT &&func,
                           const InArgsT &args, ResT *result) {
  impl::AsynchronousInterface<TargetSystemTag>::asyncExecuteAtWithRet(
      handle, loc, func, args, result);
}

/// @brief Execute a function on a selected locality asynchronously and return a
/// result.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(Handle & handle, const uint8_t *src, const uint32_t size,
///           Args * res) {
///   // Do something
/// }
///
/// Args args { 2, 'a' };
/// /* Args doesn't need a dynamic allocated buffer but
///  * more complicated data structure might need it */
/// std::vector<std::unique_ptr<Args> outBuffers(numLocalities());
/// std::unique_ptr<uint8_t[]> ptr(new uint8_t[sizeof(Args)]);
/// memcpy(ptr.get(), &args, sizeof(Args));
///
/// Handle handle;
/// for (auto & locality : allLocalities()) {
///   uint32_t localityID = static_cast<uint32_t>(locality);
///   asyncExecuteAtWithRet(
///     handle, locality, task, ptr.get(), sizeof(Args),
///     &outBuffers[localityID]);
/// }
/// waitForCompletion(handle);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(Handle &, const uint8_t *, const uint32_t, ResT *);
/// @endcode
///
/// @tparam ResT The type of the result value.  The type can be a structure or a
/// class but with the restriction that must be memcopy-able.
///
/// @param handle An Handle for the associated task-group.
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param argsBuffer A buffer of bytes to be passed to the function.
/// @param bufferSize The size of the buffer argsBuffer passed.
/// @param result The location where to store the result.
template <typename FunT, typename ResT>
void asyncExecuteAtWithRet(Handle &handle, const Locality &loc, FunT &&func,
                           const std::shared_ptr<uint8_t> &argsBuffer,
                           const uint32_t bufferSize, ResT *result) {
  impl::AsynchronousInterface<TargetSystemTag>::asyncExecuteAtWithRet(
      handle, loc, func, argsBuffer, bufferSize, result);
}

/// @brief Execute a function on all localities asynchronously.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(Handle & handle, const Args & args) {  /* do something */ }
///
/// Args args { 2, 'a' };
/// Handle handle;
/// asyncExecuteOnAll(handle, task, args);
/// /* do something else */
/// waitForcompletion(handle);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(Handle &, const InArgsT &);
/// @endcode
///
/// @tparam InArgsT The type of the argument accepted by the function.  The type
/// can be a structure or a class but with the restriction that must be
/// memcopy-able.
///
/// @param handle An Handle for the associated task-group.
/// @param func The function to execute.
/// @param args The arguments to be passed to the function.
template <typename FunT, typename InArgsT>
void asyncExecuteOnAll(Handle &handle, FunT &&func, const InArgsT &args) {
  impl::AsynchronousInterface<TargetSystemTag>::asyncExecuteOnAll(handle, func,
                                                                  args);
}

/// @brief Execute a function on all localities synchronously.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// void task(Handle & handle, const uint8_t *,
///                            const uint32_t) {  /* do something */ }
///
/// Args args { 2, 'a' };
/// /* Args doesn't need a dynamic allocated buffer but
///  * more complicated data structure might need it */
/// std::shared_ptr<uint8_t> ptr(new uint8_t[sizeof(Args)],
///                              std::default_delete<uint8_t[]>());
/// memcpy(ptr.get(), &args, sizeof(Args));
///
/// Handle handle;
/// asyncExecuteOnAll(handle, task, ptr.get(), sizeof(Args));
/// /* do something else */
/// waitForCompletion(handle);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(Handle & handle, const uint8_t *, const uint32_t);
/// @endcode
///
/// @param handle An Handle for the associated task-group.
/// @param func The function to execute.
/// @param argsBuffer A buffer of bytes to be passed to the function.
/// @param bufferSize The size of the buffer argsBuffer passed.
template <typename FunT>
void asyncExecuteOnAll(Handle &handle, FunT &&func,
                       const std::shared_ptr<uint8_t> &argsBuffer,
                       const uint32_t bufferSize) {
  impl::AsynchronousInterface<TargetSystemTag>::asyncExecuteOnAll(
      handle, func, argsBuffer, bufferSize);
}

/// @brief Execute a parallel loop at a specific locality asynchronously.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// Args args { 2, 'a' };
/// Handle handle;
/// shad::rt::asyncForEachAt(locality,
///     [](Handle & handle, const Args & input, size_t itrNum) {
///       // Do something.
///     },
///     args, iterations, handle);
/// shad::rt::waitForcompletion(handle);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(Handle &, const ArgsT &, size_t itrNum);
/// @endcode
/// where the itrNum is the n-th iteration of the loop.
///
/// @tparam InArgsT The type of the argument accepted by the function.  The type
/// can be a structure or a class but with the restriction that must be
/// memcopy-able.
///
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param args The arguments to be passed to the function.
/// @param numIters  The total number of iteration of the loop.
/// @param handle An Handle for the associated task-group.
template <typename FunT, typename InArgsT>
void asyncForEachAt(Handle &handle, const Locality &loc, FunT &&func,
                    const InArgsT &args, const size_t numIters) {
  impl::AsynchronousInterface<TargetSystemTag>::asyncForEachAt(
      handle, loc, func, args, numIters);
}

/// @brief Execute a parallel loop at a specific locality asynchronously.
///
/// Typical Usage:
/// @code
/// std::shared_ptr<uint8_t> buffer(new uint8_t[2]{ 5, 5 },
///                                 std::default_delete<uint8_t[]>());
///
/// Handle handle;
/// shad::rt::asyncForEachAt(locality,
///     handle,
///     [](Handle & handle, const uint8_t * input, const uint32_t size,
///        size_t itrNum) {
///       // Do something.
///     },
///     buffer, 2,
///     iterations);
/// /* do something else */
/// shad::rt::waitForCompletion(handle);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(Handle &, const uint8_t *, const uint32_t, size_t itrNum);
/// @endcode
/// where the itrNum is the n-th iteration of the loop.
///
/// @param handle An Handle for the associated task-group.
/// @param loc The Locality where the function must be executed.
/// @param func The function to execute.
/// @param argsBuffer A buffer of bytes to be passed to the function.
/// @param bufferSize The size of the buffer argsBuffer passed.
/// @param numIters  The total number of iteration of the loop.
template <typename FunT>
void asyncForEachAt(Handle &handle, const Locality &loc, FunT &&func,
                    const std::shared_ptr<uint8_t> &argsBuffer,
                    const uint32_t bufferSize, const size_t numIters) {
  impl::AsynchronousInterface<TargetSystemTag>::asyncForEachAt(
      handle, loc, func, argsBuffer, bufferSize, numIters);
}

/// @brief Execute a parallel loop on the whole system asynchronously.
///
/// Typical Usage:
/// @code
/// struct Args {
///   int a;
///   char b;
/// };
///
/// Args args { 2, 'a' };
/// Handle handle;
/// shad::rt::asyncForEachOnAll(
///     handle,
///     [](Handle &, const Args &, size_t itrNum) {
///       // Do something.
///     },
///     args,
///     iterations);
/// /* Do something else */
/// shad::rt::waitForCompletion(handle);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(Handle &, const ArgsT &, size_t itrNum);
/// @endcode
/// where the itrNum is the n-th iteration of the loop.
///
/// @tparam InArgsT The type of the argument accepted by the function.  The type
/// can be a structure or a class but with the restriction that must be
/// memcopy-able.
///
/// @param handle An Handle for the associated task-group.
/// @param func The function to execute.
/// @param args The arguments to be passed to the function.
/// @param numIters  The total number of iteration of the loop.
template <typename FunT, typename InArgsT>
void asyncForEachOnAll(Handle &handle, FunT &&func, const InArgsT &args,
                       const size_t numIters) {
  impl::AsynchronousInterface<TargetSystemTag>::asyncForEachOnAll(
      handle, func, args, numIters);
}

/// @brief Execute a parallel loop on the whole system asynchronously.
///
/// Typical Usage:
/// @code
/// std::shared_ptr<uint8_t> buffer(new uint8_t[2]{ 5, 5 },
///                                 std::default_delete<uint8_t[]>());
///
/// Handle handle;
/// shad::rt::asyncForEachOnAll(handle,
///     [](Handle &, const uint8_t * input, const uint32_t size,
///        size_t itrNum) {
///       // Do something
///     },
///     buffer, 2,
///     iterations);
/// /* do something else */
/// shad::rt::waitForCompletion(handle);
/// @endcode
///
/// @tparam FunT The type of the function to be executed.  The function
/// prototype must be:
/// @code
/// void(Handle &, const uint8_t *, const uint32_t, size_t itrNum);
/// @endcode
/// where the itrNum is the n-th iteration of the loop.
///
/// @param handle An Handle for the associated task-group.
/// @param func The function to execute.
/// @param argsBuffer A buffer of bytes to be passed to the function.
/// @param bufferSize The size of the buffer argsBuffer passed.
/// @param numIters  The total number of iteration of the loop.
template <typename FunT>
void asyncForEachOnAll(Handle &handle, FunT &&func,
                       const std::shared_ptr<uint8_t> &argsBuffer,
                       const uint32_t bufferSize, const size_t numIters) {
  impl::AsynchronousInterface<TargetSystemTag>::asyncForEachOnAll(
      handle, func, argsBuffer, bufferSize, numIters);
}

/// @brief Wait for completion of a set of tasks
inline void waitForCompletion(Handle &handle) {
  impl::HandleTrait<TargetSystemTag>::WaitFor(handle.id_);
}
/// @}

}  // namespace rt
}  // namespace shad

#endif  // INCLUDE_SHAD_RUNTIME_RUNTIME_H_
