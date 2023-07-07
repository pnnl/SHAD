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

#ifndef INCLUDE_SHAD_RUNTIME_MAPPINGS_HPX_HPX_TRAITS_MAPPING_H_
#define INCLUDE_SHAD_RUNTIME_MAPPINGS_HPX_HPX_TRAITS_MAPPING_H_

#include <cstdint>
#include <limits>
#include <memory>
#include <string>

#include "hpx/hpx.hpp"

#include "shad/runtime/mapping_traits.h"

namespace shad {

namespace rt {
namespace impl {

struct hpx_tag {};
template <>
struct HandleTrait<hpx_tag> {
  using HandleTy = std::shared_ptr<hpx::execution::experimental::task_group>;
  using ParameterTy = std::shared_ptr<hpx::execution::experimental::task_group> &;
  using ConstParameterTy = const std::shared_ptr<hpx::execution::experimental::task_group> &;

  static void Init(ParameterTy H, ConstParameterTy V) {H=V;}

  static HandleTy NullValue() {
    return std::shared_ptr<hpx::execution::experimental::task_group>(nullptr);
  }

  static bool Equal(ConstParameterTy lhs, ConstParameterTy rhs) {
    return lhs == rhs;
  }

  static std::string toString(ConstParameterTy H) { return ""; }

  static uint64_t toUnsignedInt(ConstParameterTy H) {
    return reinterpret_cast<uint64_t>(H.get());
  }

  static HandleTy CreateNewHandle() {
    return std::shared_ptr<hpx::execution::experimental::task_group>(new hpx::execution::experimental::task_group());
  }

  static void WaitFor(ParameterTy H) {
    if (H == nullptr){
      return;
    } 
    H->wait();
  }
};

template <>
struct LockTrait<hpx_tag> {
  using LockTy = hpx::lcos::local::spinlock;

  static void lock(LockTy &L) { L.lock(); hpx::util::ignore_lock(&L); }
  static void unlock(LockTy &L) { hpx::util::reset_ignored(&L); L.unlock(); }
};

template <>
struct RuntimeInternalsTrait<hpx_tag> {
  static void Initialize(int argc, char *argv[]) {}

  static void Finalize() {}

  static size_t Concurrency() { return hpx::get_os_thread_count(); }
  static void Yield() { hpx::this_thread::yield(); }

  static uint32_t ThisLocality() { return hpx::get_locality_id(); }
  static uint32_t NullLocality() { return hpx::naming::invalid_locality_id; }
  static uint32_t NumLocalities(){
    return hpx::get_num_localities(hpx::launch::sync);
  }
};

}  // namespace impl

using TargetSystemTag = impl::hpx_tag;

}  // namespace rt
}  // namespace shad

#endif  // INCLUDE_SHAD_RUNTIME_MAPPINGS_HPX_HPX_TRAITS_MAPPING_H_
