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

#ifndef INCLUDE_SHAD_DATA_STRUCTURES_ATOMIC_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_ATOMIC_H_

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
class Atomic : public AbstractDataStructure<Atomic<T>> {
 public:
  using ObjectID = typename AbstractDataStructure<Atomic<T>>::ObjectID;
  using SharedPtr =
      typename AbstractDataStructure<Atomic<T>>::SharedPtr;

  /// @brief Create method.
  ///
  /// Creates instances of a T object on each locality.
  ///
  /// @tparam Args The list of types needed to build an instance of type T
  ///
  /// @param args The parameter pack to be forwarded to T' constructor.
  /// @return A shared pointer to the local Atomic instance.
#ifdef DOXYGEN_IS_RUNNING
  template <typename... Args>
  static SharedPtr Create(Args... args);
#endif

  /// @brief Retieve the Global Identifier.
  ///
  /// @return The global identifier associated with the array instance.
  ObjectID GetGlobalID() const { return oid_; }

  /// @brief Atomic Load.
  ///
  /// @return Atomic's value.
  T Load() {
    if (ownerLoc_ == rt::thisLocality()) {
     return localInstance_.load();
    }
    T retValue;
    auto LoadFun = [](const ObjectID &oid, T *result) {
      auto ptr = Atomic<T>::GetPtr(oid);
      *result = ptr->localInstance_.load();
    };
    rt::executeAtWithRet(ownerLoc_, LoadFun, oid_, &retValue);
    return retValue;
  }

  /// @brief Async Atomic Load.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[out] res Pointer to the region where the atomic's value is
  /// written; res must point to a valid memory allocation.
  void AsyncLoad(rt::Handle& h, T* res) {
    if (ownerLoc_ == rt::thisLocality()) {
     *res = localInstance_.load();
     return;
    }
    auto LoadFun = [](rt::Handle&, const ObjectID &oid, T *result) {
      auto ptr = Atomic<T>::GetPtr(oid);
      *result = ptr->localInstance_.load();
    };
    rt::asyncExecuteAtWithRet(h, ownerLoc_, LoadFun, oid_, res);
    return;
  }

  /// @brief Fetch Add operation.
  ///
  /// @param[in] add Data to be fetch-added.
  /// @return Result of the fect-add operation.
  T FetchAdd(T add) {
    if (ownerLoc_ == rt::thisLocality()) {
     return localInstance_.fetch_add(add);
    }
    T ret;
    auto AddFun = [](const std::pair<ObjectID, T> &args, T *result) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      *result = ptr->localInstance_.fetch_add(args.second);
    };
    auto args = std::make_pair(oid_, add);
    rt::executeAtWithRet(ownerLoc_, AddFun, args, &ret);
    return ret;
  }

  /// @brief Async Fetch Add operation.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[in] add Data to be fetch-added.
  /// @param[out] res Pointer to the region where the result is
  /// written; res must point to a valid memory allocation.
  void AsyncFetchAdd(rt::Handle& h, T add, T* res) {
    if (ownerLoc_ == rt::thisLocality()) {
     *res = localInstance_.fetch_add(add);
     return;
    }
    auto AddFun = [](rt::Handle&,
                     const std::pair<ObjectID, T> &args, T *result) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      *result = ptr->localInstance_.fetch_add(args.second);
    };
    auto args = std::make_pair(oid_, add);
    rt::asyncExecuteAtWithRet(h, ownerLoc_, AddFun, args, res);
    return;
  }

  /// @brief Async Fetch Add operation, with no return value.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[in] add Data to be fetch-added.
  void AsyncFetchAdd(rt::Handle& h, T add) {
    if (ownerLoc_ == rt::thisLocality()) {
     localInstance_.fetch_add(add);
     return;
    }
    auto AddFun = [](rt::Handle&,
                     const std::pair<ObjectID, T> &args) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      ptr->localInstance_.fetch_add(args.second);
    };
    auto args = std::make_pair(oid_, add);
    rt::asyncExecuteAt(h, ownerLoc_, AddFun, args);
    return;
  }
 protected:
  /// @brief Constructor.
  template <typename... Args>
  explicit Atomic(ObjectID oid, T initVal, rt::Locality owner=*(rt::allLocalities().begin()))
      : oid_{oid}, localInstance_{initVal}, ownerLoc_{owner} {}

 private:
  template <typename>
  friend class AbstractDataStructure;

  ObjectID oid_;
  rt::Locality ownerLoc_;
  std::atomic<T> localInstance_;
};

}  // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_ATOMIC_H_
