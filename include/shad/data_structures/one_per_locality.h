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

#ifndef INCLUDE_SHAD_DATA_STRUCTURES_ONE_PER_LOCALITY_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_ONE_PER_LOCALITY_H_

#include "shad/data_structures/abstract_data_structure.h"

namespace shad {

/// @brief Wrapper that instantiate one object per Locality in the system.
///
/// This Wrapper triggers the instantiation of one object per Locality in the
/// system.
///
/// @warning Writes are not propagated across the system.
///
/// @tparam T The typen of the objects that will be instantiated.
template <typename T>
class OnePerLocality : public AbstractDataStructure<OnePerLocality<T>> {
 public:
  using ObjectID = typename AbstractDataStructure<OnePerLocality<T>>::ObjectID;
  using SharedPtr =
      typename AbstractDataStructure<OnePerLocality<T>>::SharedPtr;

  /// @brief Create method.
  ///
  /// Creates instances of a T object on each locality.
  ///
  /// @tparam Args The list of types needed to build an instance of type T
  ///
  /// @param args The parameter pack to be forwarded to T' constructor.
  /// @return A shared pointer to the local OnePerlocality instance.
#ifdef DOXYGEN_IS_RUNNING
  template <typename... Args>
  static SharedPtr Create(Args... args);
#endif

  /// @brief Access the local instance.
  ///
  /// @return A pointer to the local instance.
  T* const operator->() { return &localInstance_; }

  /// @brief Assign the an instance of T to the local object.
  ///
  /// @tparam T the type of the allocated.
  /// @param rhs The right-hand side of the assignment.
  OnePerLocality<T>& operator=(const T& rhs) {
    localInstance_ = rhs;
    return *this;
  }

  /// @brief Retrieve a copy of the local instance.
  explicit operator T() const { return localInstance_; }

 protected:
  /// @brief Constructor.
  template <typename... Args>
  explicit OnePerLocality(ObjectID oid, Args... args)
      : AbstractDataStructure<OnePerLocality<T>>{oid}, localInstance_{args...} {}

 private:
  template <typename>
  friend class AbstractDataStructure;

  T localInstance_;
};

}  // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_ONE_PER_LOCALITY_H_
