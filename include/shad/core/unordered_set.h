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

#ifndef INCLUDE_SHAD_CORE_UNORDERED_SET_H_
#define INCLUDE_SHAD_CORE_UNORDERED_SET_H_

#include <algorithm>

#include "shad/core/iterator.h"
#include "shad/data_structures/compare_and_hash_utils.h"
#include "shad/data_structures/set.h"

namespace shad {

/// @brief Distributed unordered set.
///
/// A distributed associative container that contains a set of unique objects of
/// type Key. Search, insertion, and removal have average constant-time
/// complexity. Internally, the elements are not sorted in any particular order,
/// but organized into buckets. Which bucket an element is placed into depends
/// entirely on the hash of its value. This allows fast access to individual
/// elements, since once a hash is computed, it refers to the exact bucket the
/// element is placed into.
///
/// @tparam Key The type of the elements.
/// @tparam Hash The type of the hashing callable.
///
/// @todo KeyEqual template parameter
/// @todo Allocator template parameter
template <class Key, class Hash = shad::hash<Key>>
class unordered_set {
  using set_t = Set<Key>;

  friend class insert_iterator<unordered_set>;
  friend class buffered_insert_iterator<unordered_set>;

 public:
  /// @defgroup Types
  /// @{
  /// The type of the key.
  using key_type = Key;
  /// The type of the stored value.
  using value_type = typename set_t::value_type;
  /// The type used to represent size.
  using size_type = std::size_t;
  /// The type used to represent distances.
  using difference_type = std::ptrdiff_t;
  // todo hasher
  // todo key_equal
  // todo allocator_type
  // todo reference
  // todo const_reference
  /// The type for pointer to ::value_type.
  using pointer = value_type *;
  /// The type for pointer to ::const_value_type
  using const_pointer = const value_type *;
  /// The type of iterators on the set.
  using iterator = typename set_t::iterator;
  /// The type of const iterators on the set.
  using const_iterator = typename set_t::const_iterator;
  /// The type of local iterators on the set.
  using local_iterator = typename set_t::local_iterator;
  /// The type of const local iterators on the set.
  using const_local_iterator = typename set_t::const_local_iterator;
  // todo node_type
  // todo insert_return_type
  /// @}

 public:
  /// @brief Constructor.
  ///
  /// @param bucket_count The minimum number of buckets.
  /// @param hash The hashing callable.
  ///
  /// @todo add equal parameter
  /// @todo add allocator parameter
  explicit unordered_set(size_type bucket_count = 1021,
                         const Hash &hash = Hash()) {
    ptr = set_t::Create(bucket_count);
  }

  /// @brief Destructor.
  ~unordered_set() { set_t::Destroy(ptr.get()->GetGlobalID()); }

  // todo assignment
  // todo get_allocator

  /// @defgroup Iterators
  /// @{
  /// @brief The iterator to the beginning of the sequence.
  /// @return an ::iterator to the beginning of the sequence.
  iterator begin() noexcept { return impl()->begin(); }
  /// @brief The iterator to the beginning of the sequence.
  /// @return a ::const_iterator to the beginning of the sequence.
  const_iterator begin() const noexcept { return impl()->begin(); }
  /// @brief The iterator to the beginning of the sequence.
  /// @return a ::const_iterator to the beginning of the sequence.
  const_iterator cbegin() const noexcept { return impl()->cbegin(); }
  /// @brief The iterator to the end of the sequence.
  /// @return an ::iterator to the end of the sequence.
  iterator end() noexcept { return impl()->end(); }
  /// @brief The iterator to the end of the sequence.
  /// @return a ::const_iterator to the end of the sequence.
  const_iterator end() const noexcept { return impl()->end(); }
  /// @brief The iterator to the end of the sequence.
  /// @return a ::const_iterator to the end of the sequence.
  const_iterator cend() const noexcept { return impl()->cend(); }
  /// @}

  /// @defgroup Capacity
  /// @{
  /// @brief Empty test.
  /// @return true if empty, and false otherwise.
  bool empty() const noexcept { return size() == 0; }
  /// @brief The size of the container.
  /// @return the size of the container.
  size_type size() const noexcept { return impl()->Size(); }
  // todo max_size
  /// @}

  /// @defgroup Modifiers - todo
  /// @{
  /// @brief Inserts an element into the container, if the container does not
  /// already contain an element with an equivalent key.
  ///
  /// @param value The value to be inserted.
  /// @return a pair consisting of an iterator to the inserted element (or to
  /// the element that prevented the insertion) and a bool denoting whether the
  /// insertion took place.
  std::pair<iterator, bool> insert(const value_type &value) {
    return impl()->insert(value);
  }

  /// @brief Inserts an element into the container, if the container does not
  /// already contain an element with an equivalent key.
  ///
  /// @param value The value to be inserted.
  /// @return an iterator to the inserted element, or to the element that
  /// prevented the insertion.
  iterator insert(const_iterator hint, const value_type &value) {
    return impl()->insert(hint, value).first;
  }
  /// @}

  /// @defgroup Lookup - todo
  /// @{

  /// @}

  /// @defgroup Bucket Interface - todo
  /// @{

  /// @}

  /// @defgroup Hash Policy - todo
  /// @{

  /// @}

  /// @defgroup Observers - todo
  /// @{

  /// @}

 private:
  using internal_container_t = set_t;
  using oid_t = typename internal_container_t::ObjectID;
  oid_t global_id() { return impl()->GetGlobalID(); }
  static internal_container_t *from_global_id(oid_t oid) {
    return internal_container_t::GetPtr(oid).get();
  }

  std::shared_ptr<set_t> ptr = nullptr;
  const set_t *impl() const { return ptr.get(); }
  set_t *impl() { return ptr.get(); }
};

// todo operator==
// todo operator!=
// todo std::swap

}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_UNORDERED_SET_H_ */
