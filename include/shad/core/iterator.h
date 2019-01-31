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

#ifndef INCLUDE_SHAD_CORE_ITERATOR_H_
#define INCLUDE_SHAD_CORE_ITERATOR_H_

#include <iterator>
#include <memory>

#include "shad/runtime/runtime.h"

namespace shad {

/// @brief Insert iterator (over a shad container).
///
/// shad::insert_iterator is an OutputIterator that inserts elements
/// into a distributed container for which it was constructed, at the position
/// pointed to by the supplied iterator. The buffered insertion is performed
/// whenever the iterator (whether dereferenced or not) is assigned to.
/// Incrementing the shad::buffered_insert_iterator is a no-op.
///
/// @tparam Container The type of the distributed container.
template <typename Container>
class insert_iterator
    : public std::iterator<std::output_iterator_tag, void, void, void, void> {
 protected:
  using Iterator = typename Container::iterator;
  using internal_container_t = typename Container::internal_container_t;

 public:
  using value_type = typename Container::value_type;
  using container_type = Container;

  /// @brief Constructor.
  ///
  /// @param container The container into which the iterator inserts.
  /// @param iterator The position at which the iterator starts to insert.
  insert_iterator(Container& container, Iterator iterator)
      : global_id_(container.global_id()), iterator_(iterator) {}

  /// @brief The assignment operator.
  ///
  /// The assignment operator inserts a value (through buffering) and advance
  /// iterator.
  ///
  /// @param value A const reference to the value to be inserted.
  ///
  /// @return A self reference.
  insert_iterator& operator=(const value_type& value) {
    if (!local_container_ptr_ || locality_ != rt::thisLocality()) {
      locality_ = rt::thisLocality();
      local_container_ptr_ = Container::from_global_id(global_id_);
    }
    local_container_ptr_->insert(iterator_, value);
    return *this;
  }

  insert_iterator& operator*() { return *this; }
  insert_iterator& operator++() { return *this; }
  insert_iterator& operator++(int) { return *this; }

 protected:
  typename internal_container_t::ObjectID global_id_;
  Iterator iterator_;
  internal_container_t* local_container_ptr_ = nullptr;
  rt::Locality locality_;
};

/// @brief Buffered insert iterator.
///
/// shad::buffered_insert_iterator is an OutputIterator that inserts elements
/// into a distributed container for which it was constructed, at the position
/// pointed to by the supplied iterator. The buffered insertion is performed
/// whenever the iterator (whether dereferenced or not) is assigned to. The
/// buffer to be flushed into the container when the flush() function is called.
/// Incrementing the shad::buffered_insert_iterator is a no-op.
///
/// @tparam Container The type of the distributed container.
template <typename Container>
class buffered_insert_iterator : public insert_iterator<Container> {
  using base_t = insert_iterator<Container>;
  using Iterator = typename base_t::Iterator;
  using internal_container_t = typename base_t::internal_container_t;

 public:
  using value_type = typename base_t::value_type;
  using container_type = typename base_t::container_type;

  /// @brief Constructor.
  ///
  /// @param container The container into which the iterator inserts.
  /// @param iterator The position at which the iterator starts to insert.
  buffered_insert_iterator(Container& container, Iterator iterator)
      : base_t(container, iterator) {}

  /// @brief The assignment operator.
  ///
  /// The assignment operator inserts a value (through buffering) and advance
  /// iterator.
  ///
  /// @param value A const reference to the value to be inserted.
  ///
  /// @return A self reference.
  buffered_insert_iterator& operator=(const value_type& value) {
    if (!this->local_container_ptr_ || this->locality_ != rt::thisLocality()) {
      this->locality_ = rt::thisLocality();
      this->local_container_ptr_ = Container::from_global_id(this->global_id_);
      rt::Handle h;
      handle_ = h;
    }
    this->local_container_ptr_->buffered_async_insert(handle_, value);
    return *this;
  }

  /// @brief Flushes pending insertions to the container.
  void flush() {
    if (this->local_container_ptr_ != nullptr &&
        this->locality_ == rt::thisLocality()) {
      // if(!handle_.IsNull()) FIXME
      this->local_container_ptr_->buffered_async_flush(handle_);
    }
  }

  buffered_insert_iterator& operator*() { return *this; }
  buffered_insert_iterator& operator++() { return *this; }
  buffered_insert_iterator& operator++(int) { return *this; }

 private:
  rt::Handle handle_;
};

// compile-time test for block-contiguous property
template <typename It>
struct is_block_contiguous {
  static constexpr std::true_type value{};
};

template <typename Container>
struct is_block_contiguous<shad::insert_iterator<Container>> {
  static constexpr std::false_type value{};
};

template <typename Container>
struct is_block_contiguous<shad::buffered_insert_iterator<Container>> {
  static constexpr std::false_type value{};
};

}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_ITERATOR_H_ */
