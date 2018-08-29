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

#ifndef INCLUDE_SHAD_CORE_ARRAY_H_
#define INCLUDE_SHAD_CORE_ARRAY_H_

#include <cstdlib>

#include "shad/data_structures/array.h"

namespace shad {

/// @brief Fixed size distributed array.
///
/// Section 21.3.7.1 of the C++ standard defines the ::array as a fixed-size
/// sequence of objects.  An ::array should be a contiguous container (as
/// defined in section 21.2.1).  According to that definition, contiguous
/// containers requires contiguous iterators.  The definition of contiguous
/// iterators implies contiguous memory allocation for the sequence, and it
/// cannot be guaranteed in many distributed settings.  Therefore, ::array
/// relaxes this requirement.
///
/// @tparam T The type of the elements in the distributed array.
/// @tparam N The number of element in the distributed array.
template <class T, std::size_t N>
class array {
  using array_t = impl::array<T, N>;

 public:
  /// @defgroup Types
  /// @{
  /// The type of the stored value.
  using value_type = typename array_t::value_type;
  /// The type used to represent size.
  using size_type = typename array_t::size_type;
  /// The type used to represent distances.
  using difference_type = typename array_t::difference_type;
  /// The type of references to the element in the array.
  using reference = typename array_t::reference;
  /// The type for const references to element in the array.
  using const_reference = typename array_t::const_reference;
  /// The type for pointer to ::value_type.
  using pointer = typename array_t::pointer;
  /// The type for pointer to ::const_value_type
  using const_pointer = typename array_t::const_pointer;
  /// The type of iterators on the array.
  using iterator = typename array_t::iterator;
  /// The type of const iterators on the array.
  using const_iterator = typename array_t::const_iterator;
  // todo reverse_iterator
  // todo const_reverse_iterator
  /// @}

 public:
  /// @brief Constructor.
  explicit array() { ptr = array_t::Create(); }

  /// @brief Destructor.
  ~array() { array_t::Destroy(ptr.get()->GetGlobalID()); }

  /// @brief The copy assignment operator.
  ///
  /// @param O The right-hand side of the operator.
  /// @return A reference to the left-hand side.
  array<T, N> &operator=(const array<T, N> &O) { return ptr->operator=(O); }

  /// @defgroup Element Access
  /// @{

  /// @brief Unchecked element access operator.
  /// @return a ::reference to the n-th element in the array.
  constexpr reference operator[](size_type n) { return ptr->operator[](n); }

  /// @brief Unchecked element access operator.
  /// @return a ::const_reference to the n-th element in the array.
  constexpr const_reference operator[](size_type n) const {
    return ptr->operator[](n);
  }

  /// @brief Checked element access operator.
  /// @return a ::reference to the n-th element in the array.
  constexpr reference at(size_type n) { return ptr->at(n); }

  /// @brief Checked element access operator.
  /// @return a ::const_reference to the n-th element in the array.
  constexpr const_reference at(size_type n) const { return ptr->at(n); }

  /// @brief the first element in the array.
  /// @return a ::reference to the element in position 0.
  constexpr reference front() { return ptr->front(); }

  /// @brief the first element in the array.
  /// @return a ::const_reference to the element in position 0.
  constexpr const_reference front() const { return ptr->front(); }

  /// @brief the last element in the array.
  /// @return a ::reference to the element in position N - 1.
  constexpr reference back() { return ptr->back(); }

  /// @brief the last element in the array.
  /// @return a ::const_reference to the element in position N - 1.
  constexpr const_reference back() const { return ptr->back(); }
  /// @}

  /// @defgroup Iterators
  /// @{
  /// @brief The iterator to the beginning of the sequence.
  /// @return an ::iterator to the beginning of the sequence.
  constexpr iterator begin() noexcept { return ptr->begin(); }

  /// @brief The iterator to the beginning of the sequence.
  /// @return a ::const_iterator to the beginning of the sequence.
  constexpr const_iterator begin() const noexcept { return ptr->begin(); }

  /// @brief The iterator to the end of the sequence.
  /// @return an ::iterator to the end of the sequence.
  constexpr iterator end() noexcept { return ptr->end(); }

  /// @brief The iterator to the end of the sequence.
  /// @return a ::const_iterator to the end of the sequence.
  constexpr const_iterator end() const noexcept { return ptr->end(); }

  /// @brief The iterator to the beginning of the sequence.
  /// @return a ::const_iterator to the beginning of the sequence.
  constexpr const_iterator cbegin() const noexcept { return ptr->cbegin(); }

  /// @brief The iterator to the end of the sequence.
  /// @return a ::const_iterator to the end of the sequence.
  constexpr const_iterator cend() const noexcept { return ptr->cend(); }

  // todo rbegin
  // todo crbegin
  // todo rend
  // todo crend
  /// @}

  /// @defgroup Capacity
  /// @{
  /// @brief Empty test.
  /// @return true if empty (N=0), and false otherwise.
  constexpr bool empty() const noexcept { return ptr->empty(); }

  /// @brief The size of the container.
  /// @return the size of the container (N).
  constexpr size_type size() const noexcept { return ptr->size(); }

  /// @brief The maximum size of the container.
  /// @return the maximum size of the container (N).
  constexpr size_type max_size() const noexcept { return ptr->max_size(); }
  /// @}

  /// @defgroup Operations
  /// @{
  /// @brief Fill the array with an input value.
  ///
  /// @param v The input value used to fill the array.
  void fill(const value_type &v) { ptr->fill(v); }

  /// @brief Swap the content of two array.
  ///
  /// @param O The array to swap the content with.
  void swap(array<T, N> &O) noexcept /* (std::is_nothrow_swappable_v<T>) */ {
    ptr->swap(*O->ptr);
  }
  /// @}

 private:
  std::shared_ptr<array_t> ptr = nullptr;

  friend bool operator==(const array &LHS, const array &RHS) {
    return *LHS.ptr == *RHS.ptr;
  }

  friend bool operator<(const array &LHS, const array &RHS) {
    return operator<(*LHS.ptr, *RHS.ptr);
  }

  friend bool operator>(const array &LHS, const array &RHS) {
    return operator>(*LHS.ptr, *RHS.ptr);
  }
};

}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_ARRAY_H_ */
