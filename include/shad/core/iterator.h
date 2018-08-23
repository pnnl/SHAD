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

namespace shad {

/// @brief Buffered insert iterator.
///
/// shad::buffered_insert_iterator is an OutputIterator that inserts elements
/// into a distributed container for which it was constructed, at the position
/// pointed to by the supplied iterator. The buffered insertion is performed
/// whenever the iterator (whether dereferenced or not) is assigned to. The
/// buffer to be flushed into the container when the flush() function is called
/// and also upon destruction. Incrementing the shad::buffered_insert_iterator
/// is a no-op.
///
/// @tparam Container The type of the distributed container.
template <typename Container>
class buffered_insert_iterator
    : public std::iterator<std::output_iterator_tag, void, void, void, void> {
  using Iterator = typename Container::iterator;
  using value_type = typename Container::value_type;

 protected:
  Container* container_;
  Iterator iterator_;

 public:
  using container_type = Container;

  /// @brief Constructor.
  ///
  /// @param container The container into which the iterator inserts.
  /// @param iterator The position at which the iterator starts to insert.
  buffered_insert_iterator(Container& container, Iterator iterator)
      : container_(std::addressof(container)), iterator_(iterator) {}

  /// @brief The assignment operator.
  ///
  /// The assignment operator inserts a value (through buffering) and advance
  /// iterator.
  ///
  /// @param value A const reference to the value to be inserted.
  ///
  /// @return A self reference.
  buffered_insert_iterator& operator=(const value_type& value) {
    container_->buffered_insert(iterator_, value);
    return *this;
  }

  /// @brief Flushes pending insertions to the container.
  void flush() { container_->buffered_flush(); }

  /// @brief Destructor.
  ///
  /// The destruction flushes all pending insertions into the container.
  ~buffered_insert_iterator() { container_->buffered_flush(); }

  inline buffered_insert_iterator& operator*() { return *this; }
  inline buffered_insert_iterator& operator++() { return *this; }
  inline buffered_insert_iterator& operator++(int) { return *this; }
};

}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_ITERATOR_H_ */
