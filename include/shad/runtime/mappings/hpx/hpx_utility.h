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

#ifndef INCLUDE_SHAD_RUNTIME_MAPPINGS_HPX_HPX_UTILITY_H_
#define INCLUDE_SHAD_RUNTIME_MAPPINGS_HPX_HPX_UTILITY_H_

#include <cstddef>
#include <cstdint>
#include <sstream>
#include <system_error>
#include <utility>

#include "hpx/hpx.hpp"
#include "hpx/hpx_init.hpp"
#include "hpx/serialization/serialize_buffer.hpp"

#include "shad/runtime/locality.h"

namespace shad {
namespace rt {

namespace impl {

inline uint32_t getLocalityId(const Locality &loc) {
  return static_cast<uint32_t>(loc);
}

inline void checkLocality(const Locality& loc) {
  uint32_t localityID = getLocalityId(loc);
  if (localityID >= hpx::get_num_localities(hpx::launch::sync)) {
    std::stringstream ss;
    ss << "The system does not include " << loc;
    throw std::system_error(0xdeadc0de, std::generic_category(), ss.str());
  }
}

namespace detail {
///////////////////////////////////////////////////////////////////////
// simple utility action which invoke an arbitrary global function
// sync functions
template <typename F>
struct invoke_executeAt;
template <typename T>
struct invoke_executeAt<void (*)(T)> {
  static void call(void (*f)(T), T args) { return f(std::move(args)); }
};

struct invoke_executeAt_buffer {
  static void call(void (*f)(const uint8_t *, const uint32_t),
                   hpx::serialization::serialize_buffer<std::uint8_t> args) {
    f(args.data(), args.size());
  }
};

template <typename F>
struct invoke_executeAtWithRetBuff;
template <typename T>
struct invoke_executeAtWithRetBuff<void (*)(T, std::uint8_t *,
                                            std::uint32_t *)> {
  static hpx::serialization::serialize_buffer<std::uint8_t> call(
      void (*f)(T, std::uint8_t *, std::uint32_t *), T args,
      std::uint32_t size) {
    hpx::serialization::serialize_buffer<std::uint8_t> result(2048);

    f(std::move(args), result.data(), &size);

    result.resize_norealloc(size);
    return result;
  }
};

struct invoke_executeAtWithRetBuff_buff {
  static hpx::serialization::serialize_buffer<std::uint8_t> call(
      void (*f)(const uint8_t *, const uint32_t, uint8_t *, uint32_t *),
      hpx::serialization::serialize_buffer<std::uint8_t> args,
      std::uint32_t size) {
    hpx::serialization::serialize_buffer<std::uint8_t> result(2048);
    f(args.data(), args.size(), result.data(), &size);
    result.resize_norealloc(size);
    return result;
  }
};

template <typename F>
struct invoke_executeAtWithRet;
template <typename R, typename T>
struct invoke_executeAtWithRet<void (*)(T, R *)> {
  static R call(void (*f)(T, R *), T args, R result) {
    f(std::move(args), &result);

    return result;
  }
};

template <typename F>
struct invoke_executeAtWithRet_buff;
template <typename R>
struct invoke_executeAtWithRet_buff<void (*)(const uint8_t *, const uint32_t,
                                             R *)> {
  static R call(void (*f)(const uint8_t *, const uint32_t, R *),
                hpx::serialization::serialize_buffer<std::uint8_t> args,
                R result) {
    f(args.data(), args.size(), &result);

    return result;
  }
};

template <typename F>
struct invoke_forEachAt;
template <typename T>
struct invoke_forEachAt<void (*)(T, std::size_t)> {
  static void call(void (*f)(T, std::size_t), T args, std::size_t numIters) {
    hpx::for_loop(hpx::execution::par, 0, numIters, [&](std::size_t i) {
      reinterpret_cast<void (*)(T, std::size_t)>(f)(std::move(args), i);
    });
  }
};

struct invoke_forEachAt_buffer {
  static void call(void (*f)(const uint8_t *, const uint32_t, size_t),
                   hpx::serialization::serialize_buffer<std::uint8_t> args,
                   std::size_t numIters) {
    hpx::for_loop(hpx::execution::par, 0, numIters,
                  [&](std::size_t i) { f(args.data(), args.size(), i); });
  }
};

template <typename F>
struct invoke_forEachOnAll;
template <typename T>
struct invoke_forEachOnAll<void (*)(T, std::size_t)> {
  static void call(void (*f)(T, std::size_t), T args, std::size_t beginIter,
                   std::size_t endIter) {
    hpx::for_loop(hpx::execution::par, beginIter, endIter, [&](std::size_t i) {
      f(std::move(args), i);
    });
  }
};

struct invoke_forEachOnAll_buffer {
  static void call(void (*f)(const uint8_t *, const uint32_t, size_t),
                   hpx::serialization::serialize_buffer<std::uint8_t> args,
                   std::size_t beginIter, std::size_t endIter) {
    hpx::for_loop(hpx::execution::par, beginIter, endIter, [&](std::size_t i) {
      f(args.data(), args.size(), i);
    });
  }
};

template <typename T>
struct invoke_dma_put {
  static void call(hpx::serialization::serialize_buffer<std::uint8_t> args,
                   std::size_t remoteAddress) {
    std::memcpy(reinterpret_cast<T *>(remoteAddress), args.data(), args.size());
  }
};

struct invoke_dma_get {
  static hpx::serialization::serialize_buffer<std::uint8_t> call(
      std::size_t remoteData, std::size_t numBytes) {
    hpx::serialization::serialize_buffer<std::uint8_t> result(
        reinterpret_cast<const uint8_t *>(remoteData), numBytes,
        hpx::serialization::serialize_buffer<std::uint8_t>::reference);
    return result;
  }
};

//////////////////////////////////////////////////////////////////////////////
// async functions
template <typename F>
struct invoke_asyncExecuteAt;
template <typename T, typename H>
struct invoke_asyncExecuteAt<void (*)(H, T)> {
  static void call(void (*f)(H, T), T args) {
    std::remove_reference_t<H> h(HandleTrait<hpx_tag>::CreateNewHandle());

    f(h, std::move(args));

    waitForCompletion(h);
  }
};

template <typename F>
struct invoke_asyncExecuteAt_buff;
template <typename H>
struct invoke_asyncExecuteAt_buff<void (*)(H, const uint8_t *,
                                           const uint32_t)> {
  static void call(void (*f)(H, const uint8_t *, const uint32_t),
                   hpx::serialization::serialize_buffer<std::uint8_t> args) {
    std::remove_reference_t<H> h(HandleTrait<hpx_tag>::CreateNewHandle());

    f(h, args.data(), args.size());

    waitForCompletion(h);
  }
};

template <typename F>
struct invoke_asyncExecuteAtWithRetBuff;
template <typename T, typename H>
struct invoke_asyncExecuteAtWithRetBuff<void (*)(H, T, uint8_t *, uint32_t *)> {
  static hpx::serialization::serialize_buffer<std::uint8_t> call(
      void (*f)(H, T, uint8_t *, uint32_t *), T args, std::uint32_t size) {
    std::remove_reference_t<H> h(HandleTrait<hpx_tag>::CreateNewHandle());

    hpx::serialization::serialize_buffer<std::uint8_t> result(2048);

    f(h, std::move(args), result.data(), &size);

    waitForCompletion(h);
    result.resize_norealloc(size);
    return result;
  }
};

template <typename F>
struct invoke_asyncExecuteAtWithRetBuff_buff;
template <typename H>
struct invoke_asyncExecuteAtWithRetBuff_buff<void (*)(
    H, const uint8_t *, const uint32_t, uint8_t *, uint32_t *)> {
  static hpx::serialization::serialize_buffer<std::uint8_t> call(
      void (*f)(H, const uint8_t *, const uint32_t, uint8_t *, uint32_t *),
      hpx::serialization::serialize_buffer<std::uint8_t> args,
      std::uint32_t size) {
    std::remove_reference_t<H> h(HandleTrait<hpx_tag>::CreateNewHandle());

    hpx::serialization::serialize_buffer<std::uint8_t> result(2048);

    f(h, args.data(), args.size(), result.data(), &size);

    waitForCompletion(h);
    result.resize_norealloc(size);
    return result;
  }
};

template <typename F>
struct invoke_asyncExecuteAtWithRet;
template <typename R, typename T, typename H>
struct invoke_asyncExecuteAtWithRet<void (*)(H, T, R *)> {
  static R call(void (*f)(H, T, R *), T args, R result) {
    std::remove_reference_t<H> h(HandleTrait<hpx_tag>::CreateNewHandle());

    f(h, std::move(args), &result);

    waitForCompletion(h);

    return result;
  }
};

template <typename F>
struct invoke_asyncExecuteAtWithRet_buff;
template <typename R, typename H>
struct invoke_asyncExecuteAtWithRet_buff<void (*)(H, const uint8_t *,
                                                  const uint32_t, R *)> {
  static R call(void (*f)(H, const uint8_t *, const uint32_t, R *),
                hpx::serialization::serialize_buffer<std::uint8_t> args,
                R result) {
    std::remove_reference_t<H> h(HandleTrait<hpx_tag>::CreateNewHandle());

    f(h, args.data(), args.size(), &result);

    waitForCompletion(h);
    return result;
  }
};

template <typename F>
struct invoke_asyncForEachAt;
template <typename T, typename H>
struct invoke_asyncForEachAt<void (*)(H, T, std::size_t)> {
  static void call(void (*f)(H, T, std::size_t), T args, std::size_t numIters) {
    std::remove_reference_t<H> h(HandleTrait<hpx_tag>::CreateNewHandle());
    hpx::for_loop(hpx::execution::par, 0, numIters,
                  [&](std::size_t i) { f(h, std::move(args), i); });
    waitForCompletion(h);
  }
};

template <typename F>
struct invoke_asyncForEachAt_buff;
template <typename H>
struct invoke_asyncForEachAt_buff<void (*)(H, const uint8_t *, const uint32_t,
                                           std::size_t)> {
  static void call(void (*f)(H, const uint8_t *, const uint32_t, std::size_t),
                   hpx::serialization::serialize_buffer<std::uint8_t> args,
                   std::size_t numIters) {
    std::remove_reference_t<H> h(HandleTrait<hpx_tag>::CreateNewHandle());
    hpx::for_loop(hpx::execution::par, 0, numIters,
                  [&](std::size_t i) { f(h, args.data(), args.size(), i); });
    waitForCompletion(h);
  }
};

template <typename F>
struct invoke_asyncForEachOnAll;
template <typename T, typename H>
struct invoke_asyncForEachOnAll<void (*)(H, T, std::size_t)> {
  static void call(void (*f)(H, T, std::size_t), T args, std::size_t beginIter,
                   std::size_t endIter) {
    std::remove_reference_t<H> h(HandleTrait<hpx_tag>::CreateNewHandle());
    hpx::for_loop(hpx::execution::par, beginIter, endIter,
                  [&](std::size_t i) { f(h, std::move(args), i); });
    waitForCompletion(h);
  }
};

template <typename F>
struct invoke_asyncForEachOnAll_buff;
template <typename H>
struct invoke_asyncForEachOnAll_buff<void (*)(H, const uint8_t *,
                                              const uint32_t, std::size_t)> {
  static void call(void (*f)(H, const uint8_t *, const uint32_t, std::size_t),
                   hpx::serialization::serialize_buffer<std::uint8_t> args,
                   std::size_t beginIter, std::size_t endIter) {
    std::remove_reference_t<H> h(HandleTrait<hpx_tag>::CreateNewHandle());
    hpx::for_loop(hpx::execution::par, beginIter, endIter,
                  [&](std::size_t i) { f(h, args.data(), args.size(), i); });
    waitForCompletion(h);
  }
};

template <typename T, typename H>
struct invoke_asyncDma_put {
  static void call(hpx::serialization::serialize_buffer<std::uint8_t> args,
                   std::size_t remoteAddress) {
    std::remove_reference_t<H> h(HandleTrait<hpx_tag>::CreateNewHandle());

    std::memcpy(reinterpret_cast<T *>(remoteAddress), args.data(), args.size());

    waitForCompletion(h);
  }
};

template <typename H>
struct invoke_asyncDma_get {
  static hpx::serialization::serialize_buffer<std::uint8_t> call(
      std::size_t remoteData, std::size_t numBytes) {
    std::remove_reference_t<H> h(HandleTrait<hpx_tag>::CreateNewHandle());

    hpx::serialization::serialize_buffer<std::uint8_t> result(
        reinterpret_cast<const uint8_t *>(remoteData), numBytes,
        hpx::serialization::serialize_buffer<std::uint8_t>::reference);

    waitForCompletion(h);
    return result;
  }
};

}  // namespace detail

// action definition exposing invoke_function_ptr<> that binds a global
// function (Note: this assumes global function addresses are the same on
// all localities. This also assumes that all argument types are bitwise
// copyable

// sync actions
template <typename F>
struct invoke_executeAt_action;
template <typename T>
struct invoke_executeAt_action<void (*)(T)>
    : ::hpx::actions::action<void (*)(void (*)(T), T),
                             &detail::invoke_executeAt<void (*)(T)>::call,
                             invoke_executeAt_action<void (*)(T)>> {};

struct invoke_executeAt_buffer_action
    : ::hpx::actions::action<
          void (*)(void (*)(const uint8_t *, const uint32_t),
                   hpx::serialization::serialize_buffer<std::uint8_t>),
          &detail::invoke_executeAt_buffer::call,
          invoke_executeAt_buffer_action> {};

template <typename F>
struct invoke_executeAtWithRetBuff_action;
template <typename T>
struct invoke_executeAtWithRetBuff_action<void (*)(T, std::uint8_t *,
                                                   std::uint32_t *)>
    : ::hpx::actions::action<
          hpx::serialization::serialize_buffer<std::uint8_t> (*)(
              void (*)(T, std::uint8_t *, std::uint32_t *), T, std::uint32_t),
          &detail::invoke_executeAtWithRetBuff<void (*)(T, std::uint8_t *,
                                                        std::uint32_t *)>::call,
          invoke_executeAtWithRetBuff_action<void (*)(T, std::uint8_t *,
                                                      std::uint32_t *)>> {};

struct invoke_executeAtWithRetBuff_buff_action
    : ::hpx::actions::action<
          hpx::serialization::serialize_buffer<std::uint8_t> (*)(
              void (*)(const uint8_t *, const uint32_t, uint8_t *, uint32_t *),
              hpx::serialization::serialize_buffer<std::uint8_t>,
              std::uint32_t),
          &detail::invoke_executeAtWithRetBuff_buff::call,
          invoke_executeAtWithRetBuff_buff_action> {};

template <typename F>
struct invoke_executeAtWithRet_action;
template <typename R, typename T>
struct invoke_executeAtWithRet_action<void (*)(T, R *)>
    : ::hpx::actions::action<
          R (*)(void (*)(T, R *), T, R),
          &detail::invoke_executeAtWithRet<void (*)(T, R *)>::call,
          invoke_executeAtWithRet_action<void (*)(T, R *)>> {};

template <typename F>
struct invoke_executeAtWithRet_buff_action;
template <typename R>
struct invoke_executeAtWithRet_buff_action<void (*)(const uint8_t *,
                                                    const uint32_t, R *)>
    : ::hpx::actions::action<
          R (*)(void (*)(const uint8_t *, const uint32_t, R *),
                hpx::serialization::serialize_buffer<std::uint8_t>, R),
          &detail::invoke_executeAtWithRet_buff<void (*)(
              const uint8_t *, const uint32_t, R *)>::call,
          invoke_executeAtWithRet_buff_action<void (*)(
              const uint8_t *, const uint32_t, R *)>> {};

template <typename F>
struct invoke_forEachAt_action;
template <typename T>
struct invoke_forEachAt_action<void (*)(T, std::size_t)>
    : ::hpx::actions::action<
          void (*)(void (*)(T, std::size_t), T, std::size_t),
          &detail::invoke_forEachAt<void (*)(T, std::size_t)>::call,
          invoke_forEachAt_action<void (*)(T, std::size_t)>> {};

struct invoke_forEachAt_buffer_action
    : ::hpx::actions::action<
          void (*)(void (*)(const uint8_t *, const uint32_t, size_t),
                   hpx::serialization::serialize_buffer<std::uint8_t>,
                   std::size_t),
          &detail::invoke_forEachAt_buffer::call,
          invoke_forEachAt_buffer_action> {};

template <typename F>
struct invoke_forEachOnAll_action;
template <typename T>
struct invoke_forEachOnAll_action<void (*)(T, std::size_t)>
    : ::hpx::actions::action<
          void (*)(void (*)(T, std::size_t), T, std::size_t, std::size_t),
          &detail::invoke_forEachOnAll<void (*)(T, std::size_t)>::call,
          invoke_forEachOnAll_action<void (*)(T, std::size_t)>> {};

struct invoke_forEachOnAll_buffer_action
    : ::hpx::actions::action<
          void (*)(void (*)(const uint8_t *, const uint32_t, size_t),
                   hpx::serialization::serialize_buffer<std::uint8_t>,
                   std::size_t, std::size_t),
          &detail::invoke_forEachOnAll_buffer::call,
          invoke_forEachOnAll_buffer_action> {};

template <typename T>
struct invoke_dma_put_action
    : ::hpx::actions::action<
          void (*)(hpx::serialization::serialize_buffer<std::uint8_t>,
                   std::size_t),
          &detail::invoke_dma_put<T>::call, invoke_dma_put_action<T>> {};

struct invoke_dma_get_action
    : ::hpx::actions::action<
          hpx::serialization::serialize_buffer<std::uint8_t> (*)(std::size_t,
                                                                 std::size_t),
          &detail::invoke_dma_get::call, invoke_dma_get_action> {};

//////////////////////////////////////////////////////////////////////////////
// async actions
template <typename F>
struct invoke_asyncExecuteAt_action;
template <typename T, typename H>
struct invoke_asyncExecuteAt_action<void (*)(H, T)>
    : ::hpx::actions::action<
          void (*)(void (*)(H, T), T),
          &detail::invoke_asyncExecuteAt<void (*)(H, T)>::call,
          invoke_asyncExecuteAt_action<void (*)(H, T)>> {};

template <typename F>
struct invoke_asyncExecuteAt_buff_action;
template <typename H>
struct invoke_asyncExecuteAt_buff_action<void (*)(H, const uint8_t *,
                                                  const uint32_t)>
    : ::hpx::actions::action<
          void (*)(void (*)(H, const uint8_t *, const uint32_t),
                   hpx::serialization::serialize_buffer<std::uint8_t>),
          &detail::invoke_asyncExecuteAt_buff<void (*)(H, const uint8_t *,
                                                       const uint32_t)>::call,
          invoke_asyncExecuteAt_buff_action<void (*)(H, const uint8_t *,
                                                     const uint32_t)>> {};

template <typename F>
struct invoke_asyncExecuteAtWithRetBuff_action;
template <typename T, typename H>
struct invoke_asyncExecuteAtWithRetBuff_action<void (*)(H, T, uint8_t *,
                                                        uint32_t *)>
    : ::hpx::actions::action<
          hpx::serialization::serialize_buffer<std::uint8_t> (*)(
              void (*)(H, T, uint8_t *, uint32_t *), T, std::uint32_t),
          &detail::invoke_asyncExecuteAtWithRetBuff<void (*)(H, T, uint8_t *,
                                                             uint32_t *)>::call,
          invoke_asyncExecuteAtWithRetBuff_action<void (*)(H, T, uint8_t *,
                                                           uint32_t *)>> {};

template <typename F>
struct invoke_asyncExecuteAtWithRetBuff_buff_action;
template <typename H>
struct invoke_asyncExecuteAtWithRetBuff_buff_action<void (*)(
    H, const uint8_t *, const uint32_t, uint8_t *, uint32_t *)>
    : ::hpx::actions::action<
          hpx::serialization::serialize_buffer<std::uint8_t> (*)(
              void (*)(H, const uint8_t *, const uint32_t, uint8_t *,
                       uint32_t *),
              hpx::serialization::serialize_buffer<std::uint8_t>,
              std::uint32_t),
          &detail::invoke_asyncExecuteAtWithRetBuff_buff<void (*)(
              H, const uint8_t *, const uint32_t, uint8_t *, uint32_t *)>::call,
          invoke_asyncExecuteAtWithRetBuff_buff_action<void (*)(
              H, const uint8_t *, const uint32_t, uint8_t *, uint32_t *)>> {};

template <typename F>
struct invoke_asyncExecuteAtWithRet_action;
template <typename R, typename T, typename H>
struct invoke_asyncExecuteAtWithRet_action<void (*)(H, T, R *)>
    : ::hpx::actions::action<
          R (*)(void (*)(H, T, R *), T, R),
          &detail::invoke_asyncExecuteAtWithRet<void (*)(H, T, R *)>::call,
          invoke_asyncExecuteAtWithRet_action<void (*)(H, T, R *)>> {};

template <typename F>
struct invoke_asyncExecuteAtWithRet_buff_action;
template <typename R, typename H>
struct invoke_asyncExecuteAtWithRet_buff_action<void (*)(H, const uint8_t *,
                                                         const uint32_t, R *)>
    : ::hpx::actions::action<
          R (*)(void (*)(H, const uint8_t *, const uint32_t, R *),
                hpx::serialization::serialize_buffer<std::uint8_t>, R),
          &detail::invoke_asyncExecuteAtWithRet_buff<void (*)(
              H, const uint8_t *, const uint32_t, R *)>::call,
          invoke_asyncExecuteAtWithRet_buff_action<void (*)(
              H, const uint8_t *, const uint32_t, R *)>> {};

template <typename F>
struct invoke_asyncForEachAt_action;
template <typename T, typename H>
struct invoke_asyncForEachAt_action<void (*)(H, T, std::size_t)>
    : ::hpx::actions::action<
          void (*)(void (*)(H, T, std::size_t), T, std::size_t),
          &detail::invoke_asyncForEachAt<void (*)(H, T, std::size_t)>::call,
          invoke_asyncForEachAt_action<void (*)(H, T, std::size_t)>> {};

template <typename F>
struct invoke_asyncForEachAt_buff_action;
template <typename H>
struct invoke_asyncForEachAt_buff_action<void (*)(H, const uint8_t *,
                                                  const uint32_t, std::size_t)>
    : ::hpx::actions::action<
          void (*)(void (*)(H, const uint8_t *, const uint32_t, std::size_t),
                   hpx::serialization::serialize_buffer<std::uint8_t>,
                   std::size_t),
          &detail::invoke_asyncForEachAt_buff<void (*)(
              H, const uint8_t *, const uint32_t, std::size_t)>::call,
          invoke_asyncForEachAt_buff_action<void (*)(
              H, const uint8_t *, const uint32_t, std::size_t)>> {};

template <typename F>
struct invoke_asyncForEachOnAll_action;
template <typename T, typename H>
struct invoke_asyncForEachOnAll_action<void (*)(H, T, std::size_t)>
    : ::hpx::actions::action<
          void (*)(void (*)(H, T, std::size_t), T, std::size_t, std::size_t),
          &detail::invoke_asyncForEachOnAll<void (*)(H, T, std::size_t)>::call,
          invoke_asyncForEachOnAll_action<void (*)(H, T, std::size_t)>> {};

template <typename F>
struct invoke_asyncForEachOnAll_buff_action;
template <typename H>
struct invoke_asyncForEachOnAll_buff_action<void (*)(
    H, const uint8_t *, const uint32_t, std::size_t)>
    : ::hpx::actions::action<
          void (*)(void (*)(H, const uint8_t *, const uint32_t, std::size_t),
                   hpx::serialization::serialize_buffer<std::uint8_t>,
                   std::size_t, std::size_t),
          &detail::invoke_asyncForEachOnAll_buff<void (*)(
              H, const uint8_t *, const uint32_t, std::size_t)>::call,
          invoke_asyncForEachOnAll_buff_action<void (*)(
              H, const uint8_t *, const uint32_t, std::size_t)>> {};

template <typename T, typename H>
struct invoke_asyncDma_put_action
    : ::hpx::actions::action<
          void (*)(hpx::serialization::serialize_buffer<std::uint8_t>,
                   std::size_t),
          &detail::invoke_asyncDma_put<T, H>::call,
          invoke_asyncDma_put_action<T, H>> {};

template <typename H>
struct invoke_asyncDma_get_action
    : ::hpx::actions::action<hpx::serialization::serialize_buffer<
                                 std::uint8_t> (*)(std::size_t, std::size_t),
                             &detail::invoke_asyncDma_get<H>::call,
                             invoke_asyncDma_get_action<H>> {};

}  // namespace impl

}  // namespace rt
}  // namespace shad

#endif  // INCLUDE_SHAD_RUNTIME_MAPPINGS_HPX_HPX_UTILITY_H_
