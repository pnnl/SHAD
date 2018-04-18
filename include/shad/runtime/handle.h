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

#ifndef INCLUDE_SHAD_RUNTIME_HANDLE_H_
#define INCLUDE_SHAD_RUNTIME_HANDLE_H_

#include <type_traits>

#include "shad/config/config.h"
#include "shad/runtime/mapping_traits.h"
#include "shad/runtime/mappings/available_traits_mappings.h"

namespace shad {

namespace rt {

namespace impl {
template <typename TargetSystemTag>
class AsynchronousInterface;
}

/// @brief Handle.
///
/// Handles act as identifiers for a spawning event.
/// Handles are mainly used to wait for termination of asynchronous
/// operations, via the waitForCompletion(Handle &handle) method.
class Handle {
 public:
  /// @brief Constructor.
  /// Initialize the newly created object to a null value.
  Handle() {
    impl::HandleTrait<TargetSystemTag>::Init(
        id_, impl::HandleTrait<TargetSystemTag>::NullValue());
  }

  /// @brief Constructor.
  /// Initialize the newly created object with a specific Handle ID.
  /// @param id The Handle ID to be assigned.
  explicit Handle(typename impl::HandleTrait<TargetSystemTag>::HandleTy id) {
    impl::HandleTrait<TargetSystemTag>::Init(id_, id);
  }

  /// @brief Copy-Constructor.
  /// @param rhs The right-hand side of the operator.
  Handle(const Handle &rhs) = default;

  /// @brief Move-Constructor.
  Handle(Handle &&rhs) = default;

  /// @brief Copy-Assigment.
  Handle &operator=(const Handle &rhs) = default;

  /// @brief Move-Assigment.
  Handle &operator=(Handle &&rhs) = default;

  /// @brief Operator equal.
  friend bool operator==(const Handle &lhs, const Handle &rhs) {
    return impl::HandleTrait<TargetSystemTag>::Equal(lhs.id_, rhs.id_);
  }

  /// @brief Coverson operator to uint64_t.
  explicit operator uint64_t() const {
    return impl::HandleTrait<TargetSystemTag>::toUnsignedInt(id_);
  }

  /// @brief Null Test.
  /// @return true if the Handle is null, false otherwise.
  bool IsNull() const {
    return id_ == impl::HandleTrait<TargetSystemTag>::NullValue();
  }

 private:
  friend void waitForCompletion(Handle &handle);
  friend class impl::AsynchronousInterface<TargetSystemTag>;
  using HandleTy = typename impl::HandleTrait<TargetSystemTag>::HandleTy;
  HandleTy id_;
};

}  // namespace rt
}  // namespace shad

#endif  // INCLUDE_SHAD_RUNTIME_HANDLE_H_
