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

#ifndef INCLUDE_SHAD_DATA_STRUCTURES_ABSTRACT_DATA_STRUCTURE_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_ABSTRACT_DATA_STRUCTURE_H_

#include <deque>
#include <limits>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "shad/data_structures/object_identifier.h"
#include "shad/runtime/runtime.h"

namespace shad {

/// @brief AbstractDataStructure.
///
/// The AbstractDataStructure serves as base class for all the SHAD 'global'
/// data structures. It implements the Create and Destroy methods for the data
/// structures, and allows accessing them on any locality through Global
/// Identifiers, internally managed and maintained.
///
/// @warning Subclasses SUBC of AbstractDataStructure MUST implement the
/// constructor SUBC(ObjectID oid, Args ... args) and the GetGlobalID() method.
///
/// The inheriting class constructor can accept aribitrary memcpyable arguments
/// (args): the create method of AbstractDataStructure can be called from the
/// subclass using the same arguments (args) of the constructor.  It is
/// recommended to implement the constructor as private/protected.
///
/// @tparam DataStructure DataStructure inheriting from AbstractDataStructure.
template <typename DataStructure>
class AbstractDataStructure {
 public:
  /// Global Object Identifier
  using ObjectID = ObjectIdentifier<DataStructure>;
  /// SharedPtr to DataStructure
  using SharedPtr = std::shared_ptr<DataStructure>;

  /// @brief Default constructor
  AbstractDataStructure() = default;

  /// @brief Create method.
  ///
  /// Creates a global instance of DataStructure, and associates to it a unique
  /// global identifier.  The Create method can be called within the inheriting
  /// class using the arguments args as in the constructor.
  ///
  /// @warning The Create method assumes that DataStructure implements a
  /// constructor with signature:
  /// @code
  /// DataStructure(ObjectID oid, Args ... args);
  /// @endcode
  /// with arbitrary memcpyable arguments.
  ///
  /// @param args Arguments to the DataStructure constructor.
  /// @return A shared_ptr to the newly created DataStructure instance.
  template <typename... Args>
  static SharedPtr Create(Args... args) {
    auto catalogRef = Catalog::Instance();
    ObjectID id = catalogRef->GetNextID();
    std::tuple<ObjectID, Args...> tuple(id, args...);
    rt::executeOnAll(CreateFunWrapper<ObjectID, Args...>, tuple);
    return catalogRef->GetPtr(id);
  }

  /// @brief Destroy method.
  ///
  /// Destroys a global instance of DataStructure, and invalidates its unique
  /// global identifier.
  ///
  /// @param oid Global object identifier of the DataStructure instance to
  /// destroy.
  static void Destroy(const ObjectID &oid) {
    auto catalogRef = Catalog::Instance();
    auto destroyLambda = [](const ObjectID &oid) {
      auto catalogRef = Catalog::Instance();
      catalogRef->Erase(oid);
    };

    rt::executeOnAll(destroyLambda, oid);
  }

  /// @brief DataStructure Shared pointer getter.
  ///
  /// Returns the shared_ptr of the DataStructure associated to the global
  /// identifier oid.  Allows accessing a DataStructure instance on any
  /// locality.
  ///
  /// @warning shared_ptrs are valid ONLY in the locality where it is obtained,
  /// via GetPtr or Create methods.  In case of remote execution, use oids to
  /// retrieve a valid shared_ptr via the GetPtr operation.
  ///
  /// @param oid The global identifier of the DataStructure instance.
  /// @return A shared_ptr to the DataStructure instance associared to oid.
  static SharedPtr GetPtr(ObjectID oid) {
    return Catalog::Instance()->GetPtr(oid);
  }

  /// @brief DataStructure identifier getter.
  ///
  /// Returns the global object identifier associated to a DataStructure
  /// instance.
  ///
  /// @warning It must be implemented in the inheriting DataStructure.
  virtual ObjectID GetGlobalID() const = 0;

 protected:
  template <typename... Args>
  static void UpdateCatalogAndConstruct(const ObjectID &oid, Args &&... args) {
    // Get a local instance on the remote node.
    auto catalogRef = Catalog::Instance();
    std::shared_ptr<DataStructure> ptr(
        new DataStructure(oid, std::forward<Args>(args)...));
    catalogRef->Insert(oid, ptr);
  }

  template <typename... Args, std::size_t... is>
  static void CreateFunInnerWrapper(const std::tuple<Args...> &&tuple,
                                    std::index_sequence<is...>) {
    UpdateCatalogAndConstruct(std::get<is>(tuple)...);
  }

  template <typename... Args>
  static void CreateFunWrapper(const std::tuple<Args...> &args) {
    CreateFunInnerWrapper(std::move(args), std::index_sequence_for<Args...>());
  }

  class Catalog {
   public:
    void Insert(const ObjectID &oid, const SharedPtr ce) {
      uint32_t locality = static_cast<uint32_t>(oid.GetOwnerLocality());
      std::lock_guard<rt::Lock> _(registerLock_);
      if (register_[locality].size() <= oid.GetLocalID()) {
        register_[locality].resize(oid.GetLocalID() + 1);
      }
      register_[locality][oid.GetLocalID()] = ce;
    }

    void Erase(const ObjectID &oid) {
      uint32_t locality = static_cast<uint32_t>(oid.GetOwnerLocality());
      if (rt::thisLocality() == oid.GetOwnerLocality()) {
        std::lock_guard<rt::Lock> _(registerLock_);
        oidCache_.push_back(oid);
        register_[locality][oid.GetLocalID()] = nullptr;
      } else {
        std::lock_guard<rt::Lock> _(registerLock_);
        register_[locality][oid.GetLocalID()] = nullptr;
      }
    }

    SharedPtr GetPtr(const ObjectID &oid) {
      uint32_t locality = static_cast<uint32_t>(oid.GetOwnerLocality());
      return register_[locality][oid.GetLocalID()];
    }

    static Catalog *Instance() {
      static Catalog instance;
      return &instance;
    }

    ObjectID GetNextID() {
      {
        std::lock_guard<rt::Lock> _(registerLock_);
        if (!oidCache_.empty()) {
          auto result = oidCache_.back();
          oidCache_.pop_back();
          return result;
        }
      }
      return ObjectIDCounter::Instance()++;
    }

   private:
    Catalog() : register_(rt::numLocalities()), oidCache_(), registerLock_() {}

    /// The Type storing the counter to obtain new DataStructure object IDs.
    using ObjectIDCounter = ObjectIdentifierCounter<DataStructure>;

    std::vector<std::deque<SharedPtr>> register_;
    std::vector<ObjectID> oidCache_;
    rt::Lock registerLock_;
  };
};

}  // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_ABSTRACT_DATA_STRUCTURE_H_
