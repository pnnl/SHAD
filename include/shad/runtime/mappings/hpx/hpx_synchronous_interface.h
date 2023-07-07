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

#ifndef INCLUDE_SHAD_RUNTIME_MAPPINGS_HPX_HPX_SYNCHRONOUS_INTERFACE_H_
#define INCLUDE_SHAD_RUNTIME_MAPPINGS_HPX_HPX_SYNCHRONOUS_INTERFACE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "hpx/hpx.hpp"
#include "hpx/modules/serialization.hpp"

#include "shad/runtime/locality.h"
#include "shad/runtime/mappings/hpx/hpx_traits_mapping.h"
#include "shad/runtime/mappings/hpx/hpx_utility.h"
#include "shad/runtime/synchronous_interface.h"

namespace shad {
namespace rt {

namespace impl {

template <>
struct SynchronousInterface<hpx_tag> {
  template <typename FunT, typename InArgsT>
  static void executeAt(const Locality &loc, FunT &&function,
                        const InArgsT &args) {
    using FunctionTy = void (*)(const InArgsT &);

    checkLocality(loc);
    FunctionTy fn = std::forward<decltype(function)>(function);

    using action_type = invoke_executeAt_action<decltype(fn)>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);
    hpx::sync<action_type>(id, fn, args);
  }

  template <typename FunT>
  static void executeAt(const Locality &loc, FunT &&function,
                        const std::shared_ptr<uint8_t> &argsBuffer,
                        const uint32_t bufferSize) {
    using FunctionTy = void (*)(const uint8_t *, const uint32_t);

    FunctionTy fn = std::forward<decltype(function)>(function);
    checkLocality(loc);

    using action_type = invoke_executeAt_buffer_action;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    hpx::sync<action_type>(
        id, fn, 
        buffer_type(argsBuffer.get(), bufferSize, buffer_type::reference));

  }

  template <typename FunT, typename InArgsT>
  static void executeAtWithRetBuff(const Locality &loc, FunT &&function,
                                   const InArgsT &args, uint8_t *resultBuffer,
                                   uint32_t *resultSize) {
    using FunctionTy = void (*)(const InArgsT &, uint8_t *, uint32_t *);

    FunctionTy fn = std::forward<decltype(function)>(function);
    checkLocality(loc);

    using action_type = invoke_executeAtWithRetBuff_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    buffer_type result = hpx::sync<action_type>(
        id, fn, args, *resultSize);

    *resultSize = result.size();
    std::memcpy(resultBuffer, result.data(), result.size());
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

    using action_type = invoke_executeAtWithRetBuff_buff_action;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    buffer_type result = hpx::sync<action_type>(
        id, fn, 
        buffer_type(argsBuffer.get(), bufferSize, buffer_type::reference), 
        *resultSize);

    *resultSize = result.size();
    std::memcpy(resultBuffer, result.data(), result.size());
  }

  template <typename FunT, typename InArgsT, typename ResT>
  static void executeAtWithRet(const Locality &loc, FunT &&function,
                               const InArgsT &args, ResT *result) {
    using FunctionTy = void (*)(const InArgsT &, ResT *);

    FunctionTy fn = std::forward<decltype(function)>(function);
    checkLocality(loc);

    using action_type = invoke_executeAtWithRet_action<decltype(fn)>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    *result = hpx::sync<action_type>(id, fn, args, *result);                   
  }

  template <typename FunT, typename ResT>
  static void executeAtWithRet(const Locality &loc, FunT &&function,
                               const std::shared_ptr<uint8_t> &argsBuffer,
                               const uint32_t bufferSize, ResT *result) {
    using FunctionTy = void (*)(const uint8_t *, const uint32_t, ResT *);

    FunctionTy fn = std::forward<decltype(function)>(function);
    checkLocality(loc);

    using action_type = invoke_executeAtWithRet_buff_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    *result = hpx::sync<action_type>(id, fn,
        buffer_type(argsBuffer.get(), bufferSize, buffer_type::reference),
        *result);
  }

  template <typename FunT, typename InArgsT>
  static void executeOnAll(FunT &&function, const InArgsT &args) {
    using FunctionTy = void (*)(const InArgsT &);

    FunctionTy fn = std::forward<decltype(function)>(function);

    using action_type = invoke_executeAt_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::vector<hpx::id_type> localities = hpx::find_all_localities();
    std::vector<hpx::future<void>> futures;
    for (hpx::naming::id_type const &loc : localities) {
      futures.push_back(hpx::async<action_type>(
          loc, fn, args));
    }
    hpx::wait_all(futures);
  }

  template <typename FunT>
  static void executeOnAll(FunT &&function,
                           const std::shared_ptr<uint8_t> &argsBuffer,
                           const uint32_t bufferSize) {
    using FunctionTy = void (*)(const uint8_t *, const uint32_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    using action_type = invoke_executeAt_buffer_action;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::vector<hpx::id_type> localities = hpx::find_all_localities();
    std::vector<hpx::future<void>> futures;
    for (hpx::naming::id_type const &loc : localities) {
      futures.push_back(hpx::async<action_type>(
          loc, fn,
          buffer_type(argsBuffer.get(), bufferSize, buffer_type::reference)));
    }
    hpx::wait_all(futures);

  }

  template <typename FunT, typename InArgsT>
  static void forEachAt(const Locality &loc, FunT &&function,
                        const InArgsT &args, const size_t numIters) {
    using FunctionTy = void (*)(const InArgsT &, size_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);

    using action_type = invoke_forEachAt_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    hpx::sync<action_type>(id, fn, args, numIters);
  }

  template <typename FunT>
  static void forEachAt(const Locality &loc, FunT &&function,
                        const std::shared_ptr<uint8_t> &argsBuffer,
                        const uint32_t bufferSize, const size_t numIters) {
    using FunctionTy = void (*)(const uint8_t *, const uint32_t, size_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);

    using action_type = invoke_forEachAt_buffer_action;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    hpx::sync<action_type>(id, fn,
        buffer_type(argsBuffer.get(), bufferSize, buffer_type::reference),
        numIters);
  }

  template <typename FunT, typename InArgsT>
  static void forEachOnAll(FunT &&function, const InArgsT &args,
                           const size_t numIters) {
    using FunctionTy = void (*)(const InArgsT &, size_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    using action_type = invoke_forEachOnAll_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::vector<hpx::id_type> localities = hpx::find_all_localities();
    std::size_t last_loc_idx = localities.size() - 1;
    std::size_t iters = (numIters + last_loc_idx) / localities.size();
    std::size_t iters_last = numIters - iters * last_loc_idx;
    std::vector<hpx::future<void>> futures;

    for (std::size_t i = 0; i != last_loc_idx; ++i) {
      hpx::id_type const &loc = localities[i];
      futures.push_back(
          hpx::async<action_type>(loc, fn, args, iters * i, iters * (i + 1)));
    }

    hpx::id_type const &loc = localities[last_loc_idx];
    futures.push_back(
        hpx::async<action_type>(loc, fn, args, iters * last_loc_idx, numIters));

    hpx::wait_all(futures);
  }

  template <typename FunT>
  static void forEachOnAll(FunT &&function,
                           const std::shared_ptr<uint8_t> &argsBuffer,
                           const uint32_t bufferSize, const size_t numIters) {
    using FunctionTy = void (*)(const uint8_t *, const uint32_t, size_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    using action_type = invoke_forEachOnAll_buffer_action;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::vector<hpx::id_type> localities = hpx::find_all_localities();
    std::size_t last_loc_idx = localities.size() - 1;
    std::size_t iters = (numIters + last_loc_idx) / localities.size();
    std::size_t iters_last = numIters - iters * last_loc_idx;
    std::vector<hpx::future<void>> futures;

    auto buffer =
        buffer_type(argsBuffer.get(), bufferSize, buffer_type::reference);

    for (std::size_t i = 0; i != last_loc_idx; ++i) {
      hpx::id_type const &loc = localities[i];
      futures.push_back(
          hpx::async<action_type>(loc, fn, buffer, iters * i, iters * (i + 1)));
    }

    hpx::id_type const &loc = localities[last_loc_idx];
    futures.push_back(
      hpx::async<action_type>(loc, fn, buffer, iters * last_loc_idx, numIters));

    hpx::wait_all(futures);
  }

  template <typename T>
  static void dma(const Locality &destLoc, const T *remoteAddress,
                  const T *localData, const size_t numElements) {

    using action_type = invoke_dma_put_action<T>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::uint32_t loc_id = getLocalityId(destLoc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    hpx::sync<action_type>(
        id,
        buffer_type(reinterpret_cast<const uint8_t *>(localData),
                    numElements * sizeof(T), buffer_type::reference),
        reinterpret_cast<std::size_t>(remoteAddress));
  }

  template <typename T>
  static void dma(const T *localAddress, const Locality &srcLoc,
                  const T *remoteData, const size_t numElements) {

    using action_type = invoke_dma_get_action;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::uint32_t loc_id = getLocalityId(srcLoc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    buffer_type res = hpx::sync<action_type>(
        id, reinterpret_cast<std::size_t>(remoteData), numElements * sizeof(T));

    std::memcpy(
        const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(localAddress)),
        res.data(), res.size());
  }
};

}  // namespace impl

}  // namespace rt
}  // namespace shad

#endif  // INCLUDE_SHAD_RUNTIME_MAPPINGS_HPX_HPX_SYNCHRONOUS_INTERFACE_H_
