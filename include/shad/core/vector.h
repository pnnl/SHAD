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

#ifndef INCLUDE_SHAD_CORE_VECTOR_H_
#define INCLUDE_SHAD_CORE_VECTOR_H_

#include <cstdlib>

#include "shad/data_structures/abstract_data_structure.h"
#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {

namespace impl {
/// @brief Fixed size distributed array.
/// TODO change the descriptions
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
template <typename T>
class vector : public AbstractDataStructure<vector<T>> {
template <typename U>
  class BaseVectorRef;
  template <typename U>
  class VectorRef;

  template <typename U>
  class vector_iterator;

  friend class AbstractDataStructure<vector<T>>;

 public:
  /// @defgroup Types
  /// @{

  /// The type for the global identifier.
  using ObjectID = typename AbstractDataStructure<vector<T>>::ObjectID;

  /// The type of the stored value.
  using value_type = T;
  /// The type used to represent size.
  using size_type = std::size_t;
  /// The type used to represent distances.
  using difference_type = std::ptrdiff_t;
  /// The type of references to the element in the array.
  using reference = VectorRef<value_type>;
  /// The type for const references to element in the array.
  using const_reference = VectorRef<const value_type>;
  /// The type for pointer to ::value_type.
  using pointer = value_type *;
  /// The type for pointer to ::const_value_type
  using const_pointer = const value_type *;
  /// The type of iterators on the array.
  using iterator = vector_iterator<value_type>;
  /// The type of const iterators on the array.
  using const_iterator = vector_iterator<const value_type>;

  // using reverse_iterator = TBD;
  // using const_reverse_iterator = TBD;
  /// @}

 public:
  /// @brief The copy assignment operator.
  ///
  /// @param O The right-hand side of the operator.
  /// @return A reference to the left-hand side.
  vector<T> &operator=(const vector<T> &O) {
    rt::executeOnAll(
        [](const std::pair<ObjectID, ObjectID> &IDs) {
          auto This = vector<T>::GetPtr(std::get<0>(IDs));
          auto Other = vector<T>::GetPtr(std::get<1>(IDs));

          if (This->chunk_size() != Other->chunk_size())
            This->chunk_ = std::unique_ptr<T[]>{new T[Other->chunk_size()]};
          This->p_ = Other->p_;
          
          std::copy(Other->chunk_, Other->chunk_ + chunk_size(), This->chunk_);
        },
        std::make_pair(this->oid_, O.oid_));
    return *this;
  }

  /// @brief Fill the array with an input value.
  ///
  /// @param v The input value used to fill the array.
  void fill(const value_type &v) {
    rt::executeOnAll(
        [](const std::pair<ObjectID, value_type> &args) {
          auto This = vector<T>::GetPtr(std::get<0>(args));
          auto value = std::get<1>(args);

          std::fill(This->chunk_.get(), This->chunk_.get() + chunk_size(), value);
        },
        std::make_pair(this->oid_, v));
  }

  /// @brief Swap the content of two array.
  ///
  /// @param O The array to swap the content with.
  void swap(vector<T> &O) noexcept /* (std::is_nothrow_swappable_v<T>) */ {
    rt::executeOnAll(
        [](const std::pair<ObjectID, ObjectID> &IDs) {
          auto This = vector<T>::GetPtr(std::get<0>(IDs));
          auto Other = vector<T>::GetPtr(std::get<1>(IDs));

          std::swap(This->p_, Other->p_);
          std::swap(This->chunk_, Other->chunk_);
        },
        std::make_pair(this->oid_, O.oid_));
  }

  /// @defgroup Capacity
  /// @{

  /// @brief Empty test.
  /// @return true if empty (N=0), and false otherwise.
  [[nodiscard]] constexpr bool empty() const noexcept { return p_.back() == 0; }

  /// @brief The size of the container.
  /// @return the size of the container (N).
  constexpr size_type size() const noexcept { return p_.back(); }

  /// @brief The maximum size of the container.
  /// @return the maximum size of the container (N).
  constexpr size_type max_size() const noexcept { return p_.back(); }
  /// @}

  /// @defgroup Element Access
  /// @{

  /// @brief Unchecked element access operator.
  /// @return a ::reference to the n-th element in the array.
  constexpr reference operator[](size_type n) {\
    const auto l = locate_index(n);
    return reference{rt::Locality{l}, difference_type{n - p_[l]}, oid_, nullptr};
  }

  /// @brief Unchecked element access operator.
  /// @return a ::const_reference to the n-th element in the array.
  constexpr const_reference operator[](size_type n) const {
    const auto l = locate_index(n);
    return const_reference{rt::Locality{l}, difference_type{n - p_[l]}, oid_, nullptr};
  }

  /// @brief Checked element access operator.
  /// @return a ::reference to the n-th element in the vector.
  constexpr reference at(size_type n) {
    if (n >= size()) throw std::out_of_range("Vector::at()");
    return operator[](n);
  }

  /// @brief Checked element access operator.
  /// @return a ::const_reference to the n-th element in the vector.
  constexpr const_reference at(size_type n) const {
    if (n >= size()) throw std::out_of_range("Vector::at() const");
    return operator[](n);
  }

  /// @brief the first element in the array.
  /// @return a ::reference to the element in position 0.
  constexpr reference front() { return *begin(); }
  /// @brief the first element in the array.
  /// @return a ::const_reference to the element in position 0.
  constexpr const_reference front() const { return *cbegin(); }

  /// @brief the last element in the array.
  /// @return a ::reference to the element in position N - 1.
  constexpr reference back() { return *(end() - 1); }
  /// @brief the last element in the array.
  /// @return a ::const_reference to the element in position N - 1.
  constexpr const_reference back() const { return *(cend() - 1); }

  /// @}

  /// @brief DataStructure identifier getter.
  ///
  /// Returns the global object identifier associated to a DataStructure
  /// instance.
  ///
  /// @warning It must be implemented in the inheriting DataStructure.
  ObjectID GetGlobalID() const { return oid_; }

  friend bool operator!=(const vector<T> &LHS, const vector<T> &RHS) {
    bool result[rt::numLocalities()];

    rt::Handle H;
    for (auto &l : rt::allLocalities()) {
      asyncExecuteAtWithRet(
          H, l,
          [](const rt::Handle &, const std::pair<ObjectID, ObjectID> &IDs,
             bool *result) {
            auto LHS = vector<T>::GetPtr(std::get<0>(IDs));
            auto RHS = vector<T>::GetPtr(std::get<1>(IDs));

            *result = LHS->size() != RHS->size();
            
            if (!*result) {
              if (LHS->p_ == RHS->p_)
                *result = !std::equal(LHS->chunk_, LHS->chunk_ + LHS->chunk_size(),
                                    RHS->chunk_);
              else
                throw std::logic_error("Not yet implemented!");
            }
          },
          std::make_pair(LHS.GetGlobalID(), RHS.GetGlobalID()),
          &result[static_cast<uint32_t>(l)]);
    }

    rt::waitForCompletion(H);

    for (size_t i = 1; i < rt::numLocalities(); ++i) result[0] |= result[i];

    return result[0];
  }

  friend bool operator>=(const vector<T> &LHS, const vector<T> &RHS) {
    bool result[rt::numLocalities()];

    rt::Handle H;
    for (auto &l : rt::allLocalities()) {
      asyncExecuteAtWithRet(
          H, l,
          [](const rt::Handle &, const std::pair<ObjectID, ObjectID> &IDs,
             bool *result) {
            auto LHS = vector<T>::GetPtr(std::get<0>(IDs));
            auto RHS = vector<T>::GetPtr(std::get<1>(IDs));

            if (LHS->p_ != RHS->p_)
              throw std::logic_error("Not yet implemented!");
            
            *result = std::lexicographical_compare(
                LHS->chunk_, LHS->chunk_ + LHS->chunk_size(), RHS->chunk_,
                RHS->chunk_ + RHS->chunk_size(), std::greater_equal<T>());
          },
          std::make_pair(LHS.GetGlobalID(), RHS.GetGlobalID()),
          &result[static_cast<uint32_t>(l)]);
    }

    rt::waitForCompletion(H);

    for (size_t i = 1; i < rt::numLocalities(); ++i) {
      result[0] &= result[i];
    }

    return result[0];
  }

  friend bool operator<=(const vector<T> &LHS, const vector<T> &RHS) {
    bool result[rt::numLocalities()];

    rt::Handle H;
    for (auto &l : rt::allLocalities()) {
      asyncExecuteAtWithRet(
          H, l,
          [](const rt::Handle &, const std::pair<ObjectID, ObjectID> &IDs,
             bool *result) {
            auto LHS = vector<T>::GetPtr(std::get<0>(IDs));
            auto RHS = vector<T>::GetPtr(std::get<1>(IDs));

            if (LHS->p_ != RHS->p_)
              throw std::logic_error("Not yet implemented!");

            *result = std::lexicographical_compare(
                LHS->chunk_, LHS->chunk_ + LHS->chunk_size(), RHS->chunk_,
                RHS->chunk_ + RHS->chunk_size(), std::less_equal<T>());
          },
          std::make_pair(LHS.GetGlobalID(), RHS.GetGlobalID()),
          &result[static_cast<uint32_t>(l)]);
    }

    rt::waitForCompletion(H);

    for (size_t i = 1; i < rt::numLocalities(); ++i) {
      result[0] &= result[i];
    }

    return result[0];
  }

 protected:

  template <typename Iter, typename Int>
  std::size_t lowerbound_index(Iter begin, Iter end, Int v) {
    return std::distance(begin + 1, std::lower_bound(begin, end, v + 1));
  }

  std::size_t locate_index(std::size_t i) {
    return lowerbound_index(p_.cbegin(), p_.cend(), i);
  }

  constexpr std::uint32_t my_id() const {
      return rt::thisLocality().uint32_t();
  }

  constexpr std::size_t chunk_size() const {
      return p_[my_id() + 1] - p[my_id()];
  }

  /// @brief Constructor.
  explicit vector(ObjectID oid, std::size_t N) : oid_{oid} {
    p_.reserve(rt::numLocalities() + 1);
    p_.emplace_back(0);
    for (std::uint32_t i = 1; i <= rt::numLocalities(); i++)
      p_.emplace_back(p_.back() + i * N / rt::numLocalities());
    chunk_ = std::unique_ptr<T[]>{new T[chunk_size()]};
  }

 private:
  std::vector<std::size_t> p_;
  std::unique_ptr<T[]> chunk_;
  ObjectID oid_;
};



}  // namespace impl

}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_VECTOR_H_ */