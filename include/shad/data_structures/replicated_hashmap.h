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

#ifndef INCLUDE_SHAD_DATA_STRUCTURES_REPLICATED_HASHMAP_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_REPLICATED_HASHMAP_H_

#include <algorithm>
#include <functional>
#include <tuple>
#include <utility>
#include <vector>

#include "shad/data_structures/abstract_data_structure.h"
#include "shad/data_structures/buffer.h"
#include "shad/data_structures/compare_and_hash_utils.h"
#include "shad/data_structures/local_hashmap.h"
#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {

/// @brief The Replicated Hashmap data structure.
///
/// SHAD's Replicated Hashmap is a distributed, thread-safe, associative container.
/// Entires in the map are replicated on each locale.
/// @tparam KTYPE type of the hashmap keys.
/// @tparam VTYPE type of the hashmap values.
/// @tparam KEY_COMPARE key comparison function; default is MemCmp<KTYPE>.
/// @warning obects of type KTYPE and VTYPE need to be trivially copiable.
/// @tparam INSERT_POLICY insertion policy; default is overwrite
/// (i.e. insertions overwrite previous values
///  associated to the same key, if any).
template <typename KTYPE, typename VTYPE, typename KEY_COMPARE = MemCmp<KTYPE>,
          typename INSERT_POLICY = Overwriter<VTYPE>>
class Replicated_Hashmap : public shad::AbstractDataStructure<
                      Replicated_Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>> {
 public:
  using value_type = std::pair<KTYPE, VTYPE>;
  using HmapT = Replicated_Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>;
  using LMapT = LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>;
  using ObjectID = typename AbstractDataStructure<HmapT>::ObjectID;
  using SharedPtr = typename AbstractDataStructure<HmapT>::SharedPtr;

  /// @brief Create method.
  ///
  /// Creates a newhashmap instance.
  /// @param numEntries Expected number of entries.
  /// @return A shared pointer to the newly created hashmap instance.
#ifdef DOXYGEN_IS_RUNNING
  template <typename... Args>
  static SharedPtr Create(const size_t numEntries);
#endif

  /// @brief Retrieve the Global Identifier.
  ///
  /// @return The global identifier associated with the hashmap instance.
  ObjectID GetGlobalID() const { return oid_; }

  /// @brief Getter of the local hasmap.
  ///
  /// @return The pointer to the local hashmap instance.
  LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY> * GetLocalHashmap() {
    return & localMap_;
  };

  /// @brief Size of the hashmap (number of unique entries).
  /// @return the size of the hashmap.
  size_t Size() const {
    return localMap_.Size();
  }

  /// @brief Asynchronously Insert a key-value pair in the hashmap.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle
  /// to be used to wait for completion.
  /// @param[in] key the key.
  /// @param[in] value the value to copy into the hashMap.
  void AsyncInsert(rt::Handle &handle, const KTYPE &key, const VTYPE &value);

  /// @brief Asynchronous Buffered Insert method.
  /// Asynchronously inserts a key-value pair, using aggregation buffers.
  /// @warning asynchronous buffered insertions are finalized only after
  /// calling the rt::waitForCompletion(rt::Handle &handle) method AND
  /// the WaitForBufferedInsert() method, in this order.
  /// @param[in,out] handle Reference to the handle.
  /// @param[in] key The key.
  /// @param[in] value The value.
  void BufferedAsyncInsert(rt::Handle &handle, const KTYPE &key,
                           const VTYPE &value);

  /// @brief Finalize method for buffered insertions.
  void WaitForBufferedInsert() {
  }

  /// @brief Async variant of finalize method for buffered insertions.
  /// @param[in,out] handle Reference to the handle.
  void AsyncWaitForBufferedInsert(rt::Handle& h) {
  }

  /// @brief Clear the content of the hashmap.
  void Clear() {
    auto clearLambda = [](const ObjectID &oid) {
      auto mapPtr = HmapT::GetPtr(oid);
      mapPtr->localMap_.Clear();
    };
    rt::executeOnAll(clearLambda, oid_);
  }

  /// @brief Get the value associated to a key.
  /// @param[in] key the key.
  /// @param[out] res a pointer to the value if the the key-value was found
  ///             and NULL if it does not exists.
  /// @return true if the entry is found, false otherwise.
  bool Lookup(const KTYPE& k, VTYPE*v) {
    return localMap_.Lookup(k, v);
  }

 protected:
  /// @brief Constructor.
  template <typename... Args>
  explicit Replicated_Hashmap(ObjectID oid, const size_t numEntries)
      : oid_{oid}, localMap_{std::max(numEntries, 1lu)} {}

 private:
  template <typename>
  friend class shad::AbstractDataStructure;

  ObjectID oid_;
  shad::LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY> localMap_;

  struct InsertArgs {
    ObjectID oid;
    KTYPE key;
    VTYPE value;
  };

  template <typename FUNTYPE>
  struct InserterArgs {
    ObjectID oid;
    KTYPE key;
    VTYPE value;
    FUNTYPE insfun;
  };

  struct LookupArgs {
    ObjectID oid;
    KTYPE key;
  };
};

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
inline void Replicated_Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::
AsyncInsert(rt::Handle &handle, const KTYPE &key, const VTYPE &value) {
  auto insertLambda = [](shad::rt::Handle&, const InsertArgs &args) {
    auto ptr = HmapT::GetPtr(args.oid);
    ptr->localMap_.Insert(args.key, args.value);
  };
  InsertArgs args = {oid_, key, value};
  shad::rt::asyncExecuteOnAll(handle, insertLambda, args);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
inline void Replicated_Hashmap<KTYPE, VTYPE, KEY_COMPARE,
                               INSERT_POLICY>::
BufferedAsyncInsert(rt::Handle &handle, const KTYPE &key,
                    const VTYPE &value) {
  AsyncInsert(handle, key, value);
}

} // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_REPLICATED_HASHMAP_H_
