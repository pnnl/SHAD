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

#ifndef INCLUDE_SHAD_RUNTIME_MAPPINGS_HPX_HPX_ASYNCHRONOUS_INTERFACE_H_
#define INCLUDE_SHAD_RUNTIME_MAPPINGS_HPX_HPX_ASYNCHRONOUS_INTERFACE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "shad/runtime/asynchronous_interface.h"
#include "shad/runtime/handle.h"
#include "shad/runtime/locality.h"
#include "shad/runtime/mapping_traits.h"
#include "shad/runtime/mappings/hpx/hpx_traits_mapping.h"
#include "shad/runtime/mappings/hpx/hpx_utility.h"

namespace shad {
namespace rt {

namespace impl {

template <>
struct AsynchronousInterface<hpx_tag> {
  template <typename FunT, typename InArgsT>
  static void asyncExecuteAt(Handle &handle, const Locality &loc,
                             FunT &&function, const InArgsT &args) {
    using FunctionTy = void (*)(Handle &, const InArgsT &);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);

    handle.id_ =
        handle.IsNull() ? HandleTrait<hpx_tag>::CreateNewHandle() : handle.id_;

    using action_type = invoke_asyncExecuteAt_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    handle.id_->run(
        [=]() { action_type()(id, fn, args); });
  }

  template <typename FunT>
  static void asyncExecuteAt(Handle &handle, const Locality &loc,
                             FunT &&function,
                             const std::shared_ptr<uint8_t> &argsBuffer,
                             const uint32_t bufferSize) {
    using FunctionTy = void (*)(Handle &, const uint8_t *, const uint32_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);

    handle.id_ =
        handle.IsNull() ? HandleTrait<hpx_tag>::CreateNewHandle() : handle.id_;

    using action_type = invoke_asyncExecuteAt_buff_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    handle.id_->run([=]() {
      action_type()(
          id, fn,
          buffer_type(argsBuffer.get(), bufferSize, buffer_type::reference));
    });

  }

  template <typename FunT, typename InArgsT>
  static void asyncExecuteAtWithRetBuff(Handle &handle, const Locality &loc,
                                        FunT &&function, const InArgsT &args,
                                        uint8_t *resultBuffer,
                                        uint32_t *resultSize) {
    using FunctionTy =
        void (*)(Handle &, const InArgsT &, uint8_t *, uint32_t *);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);

    handle.id_ =
        handle.IsNull() ? HandleTrait<hpx_tag>::CreateNewHandle() : handle.id_;

    using action_type = invoke_asyncExecuteAtWithRetBuff_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    handle.id_->run([=]() {
      buffer_type result = action_type()(id, fn, args, *resultSize);

      std::memcpy(resultBuffer, result.data(), result.size());
      *resultSize = result.size();
    });
  }

  template <typename FunT>
  static void asyncExecuteAtWithRetBuff(
      Handle &handle, const Locality &loc, FunT &&function,
      const std::shared_ptr<uint8_t> &argsBuffer, const uint32_t bufferSize,
      uint8_t *resultBuffer, uint32_t *resultSize) {
    using FunctionTy = void (*)(Handle &, const uint8_t *, const uint32_t,
                                uint8_t *, uint32_t *);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);

    handle.id_ =
        handle.IsNull() ? HandleTrait<hpx_tag>::CreateNewHandle() : handle.id_;

    using action_type =
        invoke_asyncExecuteAtWithRetBuff_buff_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    handle.id_->run([=]() {
      buffer_type result = action_type()(
          id, fn,
          buffer_type(argsBuffer.get(), bufferSize, buffer_type::reference),
          *resultSize);

      std::memcpy(resultBuffer, result.data(), result.size());
      *resultSize = result.size();
    });
  }

  template <typename FunT, typename InArgsT, typename ResT>
  static void asyncExecuteAtWithRet(Handle &handle, const Locality &loc,
                                    FunT &&function, const InArgsT &args,
                                    ResT *result) {
    using FunctionTy = void (*)(Handle &, const InArgsT &, ResT *);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);

    handle.id_ =
        handle.IsNull() ? HandleTrait<hpx_tag>::CreateNewHandle() : handle.id_;

    using action_type = invoke_asyncExecuteAtWithRet_action<decltype(fn)>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    handle.id_->run([=]() {
        *result = action_type()(id, fn, args, *result);
    });

  }

  template <typename FunT, typename ResT>
  static void asyncExecuteAtWithRet(Handle &handle, const Locality &loc,
                                    FunT &&function,
                                    const std::shared_ptr<uint8_t> &argsBuffer,
                                    const uint32_t bufferSize, ResT *result) {
    using FunctionTy =
        void (*)(Handle &, const uint8_t *, const uint32_t, ResT *);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);

    handle.id_ =
        handle.IsNull() ? HandleTrait<hpx_tag>::CreateNewHandle() : handle.id_;

    using action_type = invoke_asyncExecuteAtWithRet_buff_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    handle.id_->run([=]() {
      *result = action_type()(
          id, fn,
          buffer_type(argsBuffer.get(), bufferSize, buffer_type::reference),
          *result);
    });
  }

  template <typename FunT, typename InArgsT>
  static void asyncExecuteOnAll(Handle &handle, FunT &&function,
                                const InArgsT &args) {
    using FunctionTy = void (*)(Handle &, const InArgsT &);

    FunctionTy fn = std::forward<decltype(function)>(function);

    handle.id_ =
        handle.IsNull() ? HandleTrait<hpx_tag>::CreateNewHandle() : handle.id_;

    using action_type = invoke_asyncExecuteAt_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::vector<hpx::id_type> localities = hpx::find_all_localities();
    std::vector<hpx::future<void>> futures;
    for (hpx::naming::id_type const &loc : localities) {
      handle.id_->run([=]() {
        action_type()(loc, fn, args);
      });
    }

  }

  template <typename FunT>
  static void asyncExecuteOnAll(Handle &handle, FunT &&function,
                                const std::shared_ptr<uint8_t> &argsBuffer,
                                const uint32_t bufferSize) {
    using FunctionTy = void (*)(Handle &, const uint8_t *, const uint32_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    handle.id_ =
        handle.IsNull() ? HandleTrait<hpx_tag>::CreateNewHandle() : handle.id_;

    using action_type = invoke_asyncExecuteAt_buff_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::vector<hpx::id_type> localities = hpx::find_all_localities();
    std::vector<hpx::future<void>> futures;
    for (hpx::naming::id_type const &loc : localities) {
      handle.id_->run([=]() {
        action_type()(
            loc, fn,
            buffer_type(argsBuffer.get(), bufferSize, buffer_type::reference));
      });
    }

  }

  template <typename FunT, typename InArgsT>
  static void asyncForEachAt(Handle &handle, const Locality &loc,
                             FunT &&function, const InArgsT &args,
                             const size_t numIters) {
    using FunctionTy = void (*)(Handle &, const InArgsT &, size_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);

    handle.id_ =
        handle.IsNull() ? HandleTrait<hpx_tag>::CreateNewHandle() : handle.id_;

    using action_type = invoke_asyncForEachAt_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    handle.id_->run([=](){
        action_type()(id, fn, args, numIters);
    });

  }

  template <typename FunT>
  static void asyncForEachAt(Handle &handle, const Locality &loc,
                             FunT &&function,
                             const std::shared_ptr<uint8_t> &argsBuffer,
                             const uint32_t bufferSize, const size_t numIters) {
    using FunctionTy =
        void (*)(Handle &, const uint8_t *, const uint32_t, size_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    checkLocality(loc);

    handle.id_ =
        handle.IsNull() ? HandleTrait<hpx_tag>::CreateNewHandle() : handle.id_;

    using action_type = invoke_asyncForEachAt_buff_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::uint32_t loc_id = getLocalityId(loc);
    hpx::naming::id_type id = hpx::naming::get_id_from_locality_id(loc_id);

    handle.id_->run([=]() {
      action_type()(
          id, fn,
          buffer_type(argsBuffer.get(), bufferSize, buffer_type::reference),
          numIters);
    });
  }

  template <typename FunT, typename InArgsT>
  static void asyncForEachOnAll(Handle &handle, FunT &&function,
                                const InArgsT &args, const size_t numIters) {
    using FunctionTy = void (*)(Handle &, const InArgsT &, size_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    handle.id_ =
        handle.IsNull() ? HandleTrait<hpx_tag>::CreateNewHandle() : handle.id_;

    using action_type = invoke_asyncForEachOnAll_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::vector<hpx::id_type> localities = hpx::find_all_localities();
    std::size_t last_loc_idx = localities.size() - 1;
    std::size_t iters = (numIters + last_loc_idx) / localities.size();
    std::size_t iters_last = numIters - iters * last_loc_idx;

    for (std::size_t i = 0; i != last_loc_idx; ++i) {
      hpx::id_type const &loc = localities[i];
      handle.id_->run([=]() {
        action_type()(loc, fn, args, iters * i, iters * (i + 1));
      });
    }

    hpx::id_type const &loc_last = localities[last_loc_idx];
    handle.id_->run([=]() {
      action_type()(loc_last, fn, args, iters * last_loc_idx, numIters);
    });
  }

  template <typename FunT>
  static void asyncForEachOnAll(Handle &handle, FunT &&function,
                                const std::shared_ptr<uint8_t> &argsBuffer,
                                const uint32_t bufferSize,
                                const size_t numIters) {
    using FunctionTy =
        void (*)(Handle &, const uint8_t *, const uint32_t, size_t);

    FunctionTy fn = std::forward<decltype(function)>(function);

    handle.id_ =
        handle.IsNull() ? HandleTrait<hpx_tag>::CreateNewHandle() : handle.id_;

    using action_type = invoke_asyncForEachOnAll_buff_action<decltype(fn)>;
    using buffer_type = hpx::serialization::serialize_buffer<std::uint8_t>;

    std::vector<hpx::id_type> localities = hpx::find_all_localities();
    std::size_t last_loc_idx = localities.size() - 1;
    std::size_t iters = (numIters + last_loc_idx) / localities.size();
    std::size_t iters_last = numIters - iters * last_loc_idx;

    auto buffer =
        buffer_type(argsBuffer.get(), bufferSize, buffer_type::reference);

    for (std::size_t i = 0; i != last_loc_idx; ++i) {
      hpx::id_type const &loc = localities[i];
      handle.id_->run([=]() {
        action_type()(loc, fn, buffer, iters * i, iters * (i + 1));
      });
    }

    hpx::id_type const &loc = localities[last_loc_idx];
    handle.id_->run([=]() {
      action_type()(loc, fn, buffer, iters * last_loc_idx, numIters);
    });
  }

};

}  // namespace impl

}  // namespace rt
}  // namespace shad

#endif  // INCLUDE_SHAD_RUNTIME_MAPPINGS_HPX_HPX_ASYNCHRONOUS_INTERFACE_H_
