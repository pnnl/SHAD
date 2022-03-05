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

#include "shad/data_structures/abstract_data_structure.h"
#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {

namespace impl {
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
template <typename T, std::size_t N>
class array : public AbstractDataStructure<array<T, N>> {
  template <typename U>
  class BaseArrayRef;
  template <typename U>
  class ArrayRef;

  template <typename U>
  class array_iterator;

  friend class AbstractDataStructure<array<T, N>>;

 public:
  /// @defgroup Types
  /// @{

  /// The type for the global identifier.
  using ObjectID = typename AbstractDataStructure<array<T, N>>::ObjectID;

  /// The type of the stored value.
  using value_type = T;
  /// The type used to represent size.
  using size_type = std::size_t;
  /// The type used to represent distances.
  using difference_type = std::ptrdiff_t;
  /// The type of references to the element in the array.
  using reference = ArrayRef<value_type>;
  /// The type for const references to element in the array.
  using const_reference = ArrayRef<const value_type>;
  /// The type for pointer to ::value_type.
  using pointer = value_type *;
  /// The type for pointer to ::const_value_type
  using const_pointer = const value_type *;
  /// The type of iterators on the array.
  using iterator = array_iterator<value_type>;
  /// The type of const iterators on the array.
  using const_iterator = array_iterator<const value_type>;

  // using reverse_iterator = TBD;
  // using const_reverse_iterator = TBD;
  /// @}

 public:
  /// @brief The copy assignment operator.
  ///
  /// @param O The right-hand side of the operator.
  /// @return A reference to the left-hand side.
  array<T, N> &operator=(const array<T, N> &O) {
    rt::executeOnAll(
        [](const std::pair<ObjectID, ObjectID> &IDs) {
          auto This = array<T, N>::GetPtr(std::get<0>(IDs));
          auto Other = array<T, N>::GetPtr(std::get<1>(IDs));

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
          auto This = array<T, N>::GetPtr(std::get<0>(args));
          auto value = std::get<1>(args);

          std::fill(This->chunk_.get(), &This->chunk_[chunk_size()], value);
        },
        std::make_pair(this->oid_, v));
  }

  /// @brief Swap the content of two array.
  ///
  /// @param O The array to swap the content with.
  void swap(array<T, N> &O) noexcept /* (std::is_nothrow_swappable_v<T>) */ {
    rt::executeOnAll(
        [](const std::pair<ObjectID, ObjectID> &IDs) {
          auto This = array<T, N>::GetPtr(std::get<0>(IDs));
          auto Other = array<T, N>::GetPtr(std::get<1>(IDs));

          std::swap(This->chunk_, Other->chunk_);
        },
        std::make_pair(this->oid_, O.oid_));
  }

  /// @defgroup Iterators
  /// @{

  /// @brief The iterator to the beginning of the sequence.
  /// @return an ::iterator to the beginning of the sequence.
  constexpr iterator begin() noexcept {
    if (rt::thisLocality() == rt::Locality(0)) {
      return iterator{rt::Locality(0), 0, this->oid_, chunk_.get()};
    }
    return iterator{rt::Locality(0), 0, this->oid_, nullptr};
  }

  /// @brief The iterator to the beginning of the sequence.
  /// @return a ::const_iterator to the beginning of the sequence.
  constexpr const_iterator begin() const noexcept { return cbegin(); }

  /// @brief The iterator to the end of the sequence.
  /// @return an ::iterator to the end of the sequence.
  constexpr iterator end() noexcept {
    if (N < rt::numLocalities()) {
      rt::Locality last(uint32_t(N - 1));
      pointer chunk = last == rt::thisLocality() ? chunk_.get() : nullptr;
      return iterator{std::forward<rt::Locality>(last), 1, this->oid_, chunk};
    }

    difference_type pos = chunk_size();
    if (pivot_locality() != rt::Locality(0)) --pos;

    rt::Locality last(rt::numLocalities() - 1);
    pointer chunk = last == rt::thisLocality() ? chunk_.get() : nullptr;
    return iterator{std::forward<rt::Locality>(last), pos, this->oid_, chunk};
  }

  /// @brief The iterator to the end of the sequence.
  /// @return a ::const_iterator to the end of the sequence.
  constexpr const_iterator end() const noexcept { return cend(); }

  /// @brief The iterator to the beginning of the sequence.
  /// @return a ::const_iterator to the beginning of the sequence.
  constexpr const_iterator cbegin() const noexcept {
    if (rt::thisLocality() == rt::Locality(0)) {
      return const_iterator{rt::Locality(0), 0, this->oid_, chunk_.get()};
    }

    pointer chunk = nullptr;
    rt::executeAtWithRet(rt::Locality(0),
                         [](const ObjectID &ID, pointer *result) {
                           auto This = array<T, N>::GetPtr(ID);
                           *result = This->chunk_.get();
                         },
                         this->GetGlobalID(), &chunk);
    return const_iterator{rt::Locality(0), 0, this->oid_, chunk};
  }

  /// @brief The iterator to the end of the sequence.
  /// @return a ::const_iterator to the end of the sequence.
  constexpr const_iterator cend() const noexcept {
    if (N < rt::numLocalities()) {
      rt::Locality last(uint32_t(N - 1));
      pointer chunk = last == rt::thisLocality() ? chunk_.get() : nullptr;
      return const_iterator{std::forward<rt::Locality>(last), 1, this->oid_, chunk};
    }

    difference_type pos = chunk_size();
    if (pivot_locality() != rt::Locality(0)) --pos;

    rt::Locality last(rt::numLocalities() - 1);
    pointer chunk = last == rt::thisLocality() ? chunk_.get() : nullptr;
    return const_iterator{std::forward<rt::Locality>(last), pos, this->oid_, chunk};
  }

  /// @}

  /// @defgroup Capacity
  /// @{

  /// @brief Empty test.
  /// @return true if empty (N=0), and false otherwise.
  [[nodiscard]] constexpr bool empty() const noexcept { return N == 0; }

  /// @brief The size of the container.
  /// @return the size of the container (N).
  constexpr size_type size() const noexcept { return N; }

  /// @brief The maximum size of the container.
  /// @return the maximum size of the container (N).
  constexpr size_type max_size() const noexcept { return N; }
  /// @}

  /// @defgroup Element Access
  /// @{

  /// @brief Unchecked element access operator.
  /// @return a ::reference to the n-th element in the array.
  constexpr reference operator[](size_type n) {
    size_t chunk = 0;

    for (auto l = rt::Locality(0), end = rt::Locality(rt::numLocalities() - 1);
         l != end; ++l) {
      if (pivot_locality() != rt::Locality(0) && l >= pivot_locality()) {
        chunk = chunk_size() - 1;
      } else {
        chunk = chunk_size();
      }

      if (n < chunk) {
        difference_type position(n);
        return reference{l, position, this->oid_, nullptr};
      }

      n -= chunk;
    }
    difference_type position(n);
    return reference{rt::Locality(rt::numLocalities() - 1), position, this->oid_,
                     nullptr};
  }

  /// @brief Unchecked element access operator.
  /// @return a ::const_reference to the n-th element in the array.
  constexpr const_reference operator[](size_type n) const {
    size_t chunk = 0;

    for (auto l = rt::Locality(0), end = rt::Locality(rt::numLocalities() - 1);
         l != end; ++l) {
      if (pivot_locality() != rt::Locality(0) && l >= pivot_locality()) {
        chunk = chunk_size() - 1;
      } else {
        chunk = chunk_size();
      }

      if (n < chunk) {
        difference_type position(n);
        return const_reference{l, position, this->oid_, nullptr};
      }

      n -= chunk;
    }
    difference_type position(n);
    return const_reference{rt::Locality(rt::numLocalities() - 1), position,
                           this->oid_, nullptr};
  }

  /// @brief Checked element access operator.
  /// @return a ::reference to the n-th element in the array.
  constexpr reference at(size_type n) {
    if (n >= size()) throw std::out_of_range("Array::at()");
    return operator[](n);
  }

  /// @brief Checked element access operator.
  /// @return a ::const_reference to the n-th element in the array.
  constexpr const_reference at(size_type n) const {
    if (n >= size()) throw std::out_of_range("Array::at() const");
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

  friend bool operator!=(const array<T, N> &LHS, const array<T, N> &RHS) {
    bool result[rt::numLocalities()];

    rt::Handle H;
    for (auto &l : rt::allLocalities()) {
      asyncExecuteAtWithRet(
          H, l,
          [](const rt::Handle &, const std::pair<ObjectID, ObjectID> &IDs,
             bool *result) {
            auto LHS = array<T, N>::GetPtr(std::get<0>(IDs));
            auto RHS = array<T, N>::GetPtr(std::get<1>(IDs));

            *result = !std::equal(LHS->chunk_, LHS->chunk_ + LHS->chunk_size(),
                                  RHS->chunk_);
          },
          std::make_pair(LHS.GetGlobalID(), RHS.GetGlobalID()),
          &result[static_cast<uint32_t>(l)]);
    }

    rt::waitForCompletion(H);

    for (size_t i = 1; i < rt::numLocalities(); ++i) result[0] |= result[i];

    return result[0];
  }

  friend bool operator>=(const array<T, N> &LHS, const array<T, N> &RHS) {
    bool result[rt::numLocalities()];

    rt::Handle H;
    for (auto &l : rt::allLocalities()) {
      asyncExecuteAtWithRet(
          H, l,
          [](const rt::Handle &, const std::pair<ObjectID, ObjectID> &IDs,
             bool *result) {
            auto LHS = array<T, N>::GetPtr(std::get<0>(IDs));
            auto RHS = array<T, N>::GetPtr(std::get<1>(IDs));

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

  friend bool operator<=(const array<T, N> &LHS, const array<T, N> &RHS) {
    bool result[rt::numLocalities()];

    rt::Handle H;
    for (auto &l : rt::allLocalities()) {
      asyncExecuteAtWithRet(
          H, l,
          [](const rt::Handle &, const std::pair<ObjectID, ObjectID> &IDs,
             bool *result) {
            auto LHS = array<T, N>::GetPtr(std::get<0>(IDs));
            auto RHS = array<T, N>::GetPtr(std::get<1>(IDs));

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
  static constexpr std::size_t chunk_size() {
    size_t chunkSize = N / rt::numLocalities();
    return N % rt::numLocalities() != 0 ? chunkSize + 1 : chunkSize;
  }

  static constexpr rt::Locality pivot_locality() {
    return rt::Locality(N % rt::numLocalities());
  }

  /// @brief Constructor.
  explicit array(ObjectID oid) : chunk_{new T[chunk_size()]}, AbstractDataStructure<array<T, N>>(oid) {}

 private:
  std::unique_ptr<T[]> chunk_;
};

template <typename T, std::size_t N>
template <typename U>
class array<T, N>::BaseArrayRef {
 public:
  using value_type = U;
  using ObjectID = typename array<T, N>::ObjectID;
  using difference_type = typename array<T, N>::difference_type;
  using pointer = typename array<T, N>::pointer;

  ObjectID oid_;
  mutable pointer chunk_;
  difference_type pos_;
  rt::Locality loc_;

  BaseArrayRef(rt::Locality l, difference_type p, ObjectID oid, pointer chunk)
      : oid_(oid), chunk_(chunk), pos_(p), loc_(l) {}

  BaseArrayRef(const BaseArrayRef &O)
      : oid_(O.oid_), chunk_(O.chunk_), pos_(O.pos_), loc_(O.loc_) {}

  BaseArrayRef(BaseArrayRef &&O)
      : oid_(std::move(O.oid_)),
        chunk_(std::move(O.chunk_)),
        pos_(std::move(O.pos_)),
        loc_(std::move(O.loc_)) {}

  BaseArrayRef &operator=(const BaseArrayRef &O) {
    oid_ = O.oid_;
    chunk_ = O.chunk_;
    pos_ = O.pos_;
    loc_ = O.loc_;
    return *this;
  }

  BaseArrayRef &operator=(BaseArrayRef &&O) {
    oid_ = std::move(O.oid_);
    chunk_ = std::move(O.chunk_);
    pos_ = std::move(O.pos_);
    loc_ = std::move(O.loc_);
    return *this;
  }

  bool operator==(const BaseArrayRef &O) const {
    if (oid_ == O.oid_ && pos_ == O.pos_ && loc_ == O.loc_) return true;
    return this->get() == O.get();
  }

  value_type get() const {
    bool local = this->loc_ == rt::thisLocality();
    if (local) {
      if (chunk_ == nullptr) {
        auto This = array<T, N>::GetPtr(oid_);
        chunk_ = This->chunk_.get();
      }
      return chunk_[pos_];
    }

    if (chunk_ != nullptr) {
      value_type result;
      rt::executeAtWithRet(
          loc_,
          [](const std::pair<pointer, difference_type> &args, T *result) {
            *result = std::get<0>(args)[std::get<1>(args)];
          },
          std::make_pair(chunk_, pos_), &result);
      return result;
    }

    std::pair<value_type, pointer> resultPair;
    rt::executeAtWithRet(loc_,
                         [](const std::pair<ObjectID, difference_type> &args,
                            std::pair<T, pointer> *result) {
                           auto This = array<T, N>::GetPtr(std::get<0>(args));
                           result->first = This->chunk_[std::get<1>(args)];
                           result->second = This->chunk_.get();
                         },
                         std::make_pair(oid_, pos_), &resultPair);
    chunk_ = resultPair.second;
    return resultPair.first;
  }
};

template <typename T, std::size_t N>
template <typename U>
class alignas(64) array<T, N>::ArrayRef
    : public array<T, N>::template BaseArrayRef<U> {
 public:
  using value_type = U;
  using pointer = typename array<T, N>::pointer;
  using difference_type = typename array<T, N>::difference_type;
  using ObjectID = typename array<T, N>::ObjectID;

  ArrayRef(rt::Locality l, difference_type p, ObjectID oid, pointer chunk)
      : array<T, N>::template BaseArrayRef<U>(l, p, oid, chunk) {}

  ArrayRef(const ArrayRef &O) : array<T, N>::template BaseArrayRef<U>(O) {}

  ArrayRef(ArrayRef &&O) : array<T, N>::template BaseArrayRef<U>(O) {}

  ArrayRef &operator=(const ArrayRef &O) {
    array<T, N>::template BaseArrayRef<U>::operator=(O);
    return *this;
  }

  ArrayRef &operator=(ArrayRef &&O) {
    array<T, N>::template BaseArrayRef<U>::operator=(O);
    return *this;
  }

  operator value_type() const {  // NOLINT
    return array<T, N>::template BaseArrayRef<U>::get();
  }

  bool operator==(const ArrayRef &&v) const {
    return array<T, N>::template BaseArrayRef<U>::operator==(v);
  }

  ArrayRef &operator=(const T &v) {
    bool local = this->loc_ == rt::thisLocality();
    if (local) {
      if (this->chunk_ == nullptr) {
        auto This = array<T, N>::GetPtr(this->oid_);
        this->chunk_ = This->chunk_.get();
      }
      this->chunk_[this->pos_] = v;
      return *this;
    }

    if (this->chunk_ == nullptr) {
      rt::executeAtWithRet(
          this->loc_,
          [](const std::tuple<ObjectID, difference_type, T> &args,
             pointer *result) {
            auto This = array<T, N>::GetPtr(std::get<0>(args));
            This->chunk_[std::get<1>(args)] = std::get<2>(args);
            *result = This->chunk_.get();
          },
          std::make_tuple(this->oid_, this->pos_, v), &this->chunk_);
    } else {
      rt::executeAt(this->loc_,
                    [](const std::tuple<pointer, difference_type, T> &args) {
                      std::get<0>(args)[std::get<1>(args)] = std::get<2>(args);
                    },
                    std::make_tuple(this->chunk_, this->pos_, v));
    }
    return *this;
  }

  friend std::ostream &operator<<(std::ostream &stream, const ArrayRef i) {
    stream << i.loc_ << " " << i.pos_ << " " << i.get();
    return stream;
  }
};

template <typename T, std::size_t N>
template <typename U>
class alignas(64) array<T, N>::ArrayRef<const U>
    : public array<T, N>::template BaseArrayRef<U> {
 public:
  using value_type = const U;
  using pointer = typename array<T, N>::pointer;
  using difference_type = typename array<T, N>::difference_type;
  using ObjectID = typename array<T, N>::ObjectID;

  ArrayRef(rt::Locality l, difference_type p, ObjectID oid, pointer chunk)
      : array<T, N>::template BaseArrayRef<U>(l, p, oid, chunk) {}

  ArrayRef(const ArrayRef &O) : array<T, N>::template BaseArrayRef<U>(O) {}

  ArrayRef(ArrayRef &&O) : array<T, N>::template BaseArrayRef<U>(O) {}

  bool operator==(const ArrayRef &&v) const {
    return array<T, N>::template BaseArrayRef<U>::operator==(v);
  }

  ArrayRef &operator=(const ArrayRef &O) {
    array<T, N>::template BaseArrayRef<U>::operator=(O);
    return *this;
  }

  ArrayRef &operator=(ArrayRef &&O) {
    array<T, N>::template BaseArrayRef<U>::operator=(O);
    return *this;
  }

  operator value_type() const {  // NOLINT
    return array<T, N>::template BaseArrayRef<U>::get();
  }

  friend std::ostream &operator<<(std::ostream &stream, const ArrayRef i) {
    stream << i.loc_ << " " << i.pos_;
    return stream;
  }
};

template <typename T, std::size_t N>
bool operator==(const array<T, N> &LHS, const array<T, N> &RHS) {
  return !(LHS != RHS);
}

template <typename T, std::size_t N>
bool operator<(const array<T, N> &LHS, const array<T, N> &RHS) {
  return !(LHS >= RHS);
}

template <typename T, std::size_t N>
bool operator>(const array<T, N> &LHS, const array<T, N> &RHS) {
  return !(LHS <= RHS);
}

template <typename T, std::size_t N>
template <typename U>
class alignas(64) array<T, N>::array_iterator {
 public:
  using reference = typename array<T, N>::template ArrayRef<U>;
  using pointer = typename array<T, N>::pointer;
  using difference_type = std::ptrdiff_t;
  using value_type = typename array<T, N>::value_type;
  using iterator_category = std::random_access_iterator_tag;

  using local_iterator_type = pointer;

  using distribution_range = std::vector<std::pair<rt::Locality, size_t>>;

  /// @brief Constructor.
  array_iterator(rt::Locality &&l, difference_type offset, ObjectID oid,
                 pointer chunk)
      : locality_(l), offset_(offset), oid_(oid), chunk_(chunk) {}

  /// @brief Default constructor.
  array_iterator()
      : array_iterator(rt::Locality(0), -1, ObjectID::kNullID, nullptr) {}

  /// @brief Copy constructor.
  array_iterator(const array_iterator &O)
      : locality_(O.locality_),
        offset_(O.offset_),
        oid_(O.oid_),
        chunk_(O.chunk_) {}

  /// @brief Move constructor.
  array_iterator(const array_iterator &&O) noexcept
      : locality_(std::move(O.locality_)),
        offset_(std::move(O.offset_)),
        oid_(std::move(O.oid_)),
        chunk_(std::move(O.chunk_)) {}

  /// @brief Copy assignment operator.
  array_iterator &operator=(const array_iterator &O) {
    locality_ = O.locality_;
    offset_ = O.offset_;
    oid_ = O.oid_;
    chunk_ = O.chunk_;

    return *this;
  }

  /// @brief Move assignment operator.
  array_iterator &operator=(array_iterator &&O) {
    locality_ = std::move(O.locality_);
    offset_ = std::move(O.offset_);
    oid_ = std::move(O.oid_);
    chunk_ = std::move(O.chunk_);

    return *this;
  }

  bool operator==(const array_iterator &O) const {
    return locality_ == O.locality_ && oid_ == O.oid_ && offset_ == O.offset_;
  }

  bool operator!=(const array_iterator &O) const { return !(*this == O); }

  reference operator*() {
    update_chunk_pointer();
    return reference(locality_, offset_, oid_, chunk_);
  }

  array_iterator &operator++() {
    if (N < rt::numLocalities()) {
      if (static_cast<uint32_t>(locality_) == (N - 1)) {
        ++offset_;
      } else {
        ++locality_;
      }
      return *this;
    }

    size_t chunk = chunk_size();
    if (pivot_locality() != rt::Locality(0) && locality_ >= pivot_locality())
      chunk -= 1;

    ++offset_;
    if (offset_ == chunk && locality_ < rt::Locality(rt::numLocalities() - 1)) {
      ++locality_;
      offset_ = 0;
    }
    return *this;
  }

  array_iterator operator++(int) {
    array_iterator tmp(*this);
    operator++();
    return tmp;
  }

  array_iterator &operator--() {
    if (locality_ > rt::Locality(0) && offset_ == 0) {
      locality_ -= 1;
      if (pivot_locality() != rt::Locality(0) && locality_ >= pivot_locality())
        offset_ = chunk_size() - 1;
      else
        offset_ = chunk_size();
    }

    --offset_;
    return *this;
  }

  array_iterator operator--(int) {
    array_iterator tmp(*this);
    operator--();
    return tmp;
  }

  array_iterator &operator+=(difference_type n) {
    if (n == 0) return *this;

    if (n < 0) return operator-=(-n);

    size_t chunk =
        pivot_locality() != rt::Locality(0) && locality_ >= pivot_locality()
            ? chunk_size() - 1
            : chunk_size();

    rt::Locality last = N < rt::numLocalities()
                            ? rt::Locality(uint32_t(N - 1))
                            : rt::Locality(rt::numLocalities() - 1);

    if (n + offset_ >= chunk && rt::numLocalities() > 1 && locality_ != last) {
      ++locality_;
      n -= chunk - offset_;
      offset_ = 0;

      for (auto end = last; locality_ < end; ++locality_) {
        if (pivot_locality() != rt::Locality(0) &&
            locality_ == pivot_locality())
          chunk = chunk_size() - 1;

        if (n < chunk) break;

        n -= chunk;
      }
    }

    offset_ += n;
    return *this;
  }

  array_iterator &operator-=(difference_type n) {
    if (n == 0) return *this;

    if (n < 0) return operator+=(-n);

    if (n > offset_ && rt::numLocalities() > 1 &&
        locality_ != rt::Locality(0)) {
      n -= offset_ + 1;
      --locality_;
      size_t chunk =
          pivot_locality() != rt::Locality(0) && locality_ >= pivot_locality()
              ? chunk_size() - 1
              : chunk_size();
      offset_ = chunk - 1;

      auto l = locality_ - 1;
      if (l != rt::Locality(0)) {
        for (auto end = rt::Locality(0); locality_ > end && n > offset_;
             --locality_) {
          if (pivot_locality() != rt::Locality(0) && l >= pivot_locality()) {
            chunk = chunk_size() - 1;
          } else {
            chunk = chunk_size();
          }

          n -= offset_ + 1;
          offset_ = chunk - 1;
        }
      }
    }

    offset_ -= n;

    return *this;
  }

  array_iterator operator+(difference_type n) {
    array_iterator tmp(*this);
    return tmp.operator+=(n);
  }

  array_iterator operator-(difference_type n) {
    array_iterator tmp(*this);
    return tmp.operator-=(n);
  }

  difference_type operator-(const array_iterator &O) const {
    if (oid_ != O.oid_) return std::numeric_limits<difference_type>::min();

    if (*this == O) return 0;

    if (*this > O) return -O.operator-(*this);
    difference_type distance = 0;
    size_t chunk = 0;

    for (auto l = locality_, end = O.locality_; l <= end; ++l) {
      if (pivot_locality() != rt::Locality(0) && l >= pivot_locality()) {
        chunk = chunk_size() - 1;
      } else {
        chunk = chunk_size();
      }
      distance += chunk;
    }

    distance -= this->offset_;
    distance -= chunk - O.offset_;

    return -distance;
  }

  bool operator<(const array_iterator &O) const {
    if (oid_ != O.oid_ || locality_ > O.locality_) return false;
    return locality_ < O.locality_ || offset_ < O.offset_;
  }

  bool operator>(const array_iterator &O) const {
    if (oid_ != O.oid_ || locality_ < O.locality_) return false;
    return locality_ > O.locality_ || offset_ > O.offset_;
  }

  bool operator<=(const array_iterator &O) const { return !(*this > O); }

  bool operator>=(const array_iterator &O) const { return !(*this < O); }

  friend std::ostream &operator<<(std::ostream &stream,
                                  const array_iterator i) {
    stream << i.locality_ << " " << i.offset_;
    return stream;
  }

  class local_iterator_range {
   public:
    local_iterator_range(local_iterator_type B, local_iterator_type E)
        : begin_(B), end_(E) {}
    local_iterator_type begin() { return begin_; }
    local_iterator_type end() { return end_; }

   private:
    local_iterator_type begin_;
    local_iterator_type end_;
  };

  static local_iterator_range local_range(array_iterator &B,
                                          array_iterator &E) {
    auto arrayPtr = array<T, N>::GetPtr(B.oid_);
    typename array<T, N>::pointer begin{arrayPtr->chunk_.get()};

    if (rt::thisLocality() < B.locality_ || rt::thisLocality() > E.locality_)
      return local_iterator_range(begin, begin);

    if (B.locality_ == rt::thisLocality()) {
      begin += B.offset_;
    }

    typename array_iterator::difference_type chunk = chunk_size();
    if (pivot_locality() != rt::Locality(0) &&
        rt::thisLocality() >= pivot_locality()) {
      chunk -= 1;
    }

    typename array<T, N>::pointer end{arrayPtr->chunk_.get() + chunk};
    if (E.locality_ == rt::thisLocality()) {
      end = arrayPtr->chunk_.get() + E.offset_;
    }
    return local_iterator_range(begin, end);
  }

  static distribution_range
      distribution(array_iterator &begin, array_iterator &end) {
    distribution_range result;

    // First block:
    typename array_iterator::difference_type start_block_size = chunk_size();
    if (pivot_locality() != rt::Locality(0) &&
        begin.locality_ >= pivot_locality()) {
      start_block_size -= 1;
    }
    if (begin.locality_ == end.locality_)
      start_block_size = end.offset_;
    result.push_back(std::make_pair(begin.locality_,
                                    start_block_size - begin.offset_));

    // Middle blocks:
    for (auto locality = begin.locality_ + 1;
         locality < end.locality_; ++locality) {
      typename array_iterator::difference_type inner_block_size = chunk_size();
      if (pivot_locality() != rt::Locality(0) &&
          locality >= pivot_locality()) {
        inner_block_size -= 1;
      }
      result.push_back(std::make_pair(locality, inner_block_size));
    }

    // Last block:
    if (end.offset_ != 0 && begin.locality_ != end.locality_)
      result.push_back(std::make_pair(end.locality_, end.offset_));

    return result;
  }

  static rt::localities_range localities(array_iterator &B, array_iterator &E) {
    return rt::localities_range(
        B.locality_, rt::Locality(static_cast<uint32_t>(E.locality_) + 1));
  }

  static array_iterator iterator_from_local(array_iterator &B,
                                            array_iterator &E,
                                            local_iterator_type itr) {
    if (rt::thisLocality() < B.locality_ || rt::thisLocality() > E.locality_)
      return E;

    auto arrayPtr = array<T, N>::GetPtr(B.oid_);
    return array_iterator(rt::thisLocality(),
                          std::distance(arrayPtr->chunk_.get(), itr), B.oid_,
                          arrayPtr->chunk_.get());
  }

 private:
  void update_chunk_pointer() const {
    if (locality_ == rt::thisLocality()) {
      auto This = array<T, N>::GetPtr(oid_);
      chunk_ = This->chunk_.get();
      return;
    }

    rt::executeAtWithRet(locality_,
                         [](const ObjectID &ID, pointer *result) {
                           auto This = array<T, N>::GetPtr(ID);
                           *result = This->chunk_.get();
                         },
                         oid_, &chunk_);
  }

  rt::Locality locality_;
  ObjectID oid_;
  difference_type offset_;
  mutable pointer chunk_;
};

}  // namespace impl

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
  ~array() { array_t::Destroy(impl()->GetGlobalID()); }

  /// @brief The copy assignment operator.
  ///
  /// @param O The right-hand side of the operator.
  /// @return A reference to the left-hand side.
  array<T, N> &operator=(const array<T, N> &O) {
    impl()->operator=(*O.ptr);
    return *this;
  }

  /// @defgroup Element Access
  /// @{

  /// @brief Unchecked element access operator.
  /// @return a ::reference to the n-th element in the array.
  constexpr reference operator[](size_type n) { return impl()->operator[](n); }

  /// @brief Unchecked element access operator.
  /// @return a ::const_reference to the n-th element in the array.
  constexpr const_reference operator[](size_type n) const {
    return impl()->operator[](n);
  }

  /// @brief Checked element access operator.
  /// @return a ::reference to the n-th element in the array.
  constexpr reference at(size_type n) { return impl()->at(n); }

  /// @brief Checked element access operator.
  /// @return a ::const_reference to the n-th element in the array.
  constexpr const_reference at(size_type n) const { return impl()->at(n); }

  /// @brief the first element in the array.
  /// @return a ::reference to the element in position 0.
  constexpr reference front() { return impl()->front(); }

  /// @brief the first element in the array.
  /// @return a ::const_reference to the element in position 0.
  constexpr const_reference front() const { return impl()->front(); }

  /// @brief the last element in the array.
  /// @return a ::reference to the element in position N - 1.
  constexpr reference back() { return impl()->back(); }

  /// @brief the last element in the array.
  /// @return a ::const_reference to the element in position N - 1.
  constexpr const_reference back() const { return impl()->back(); }
  /// @}

  /// @defgroup Iterators
  /// @{
  /// @brief The iterator to the beginning of the sequence.
  /// @return an ::iterator to the beginning of the sequence.
  constexpr iterator begin() noexcept { return impl()->begin(); }

  /// @brief The iterator to the beginning of the sequence.
  /// @return a ::const_iterator to the beginning of the sequence.
  constexpr const_iterator begin() const noexcept { return impl()->begin(); }

  /// @brief The iterator to the end of the sequence.
  /// @return an ::iterator to the end of the sequence.
  constexpr iterator end() noexcept { return impl()->end(); }

  /// @brief The iterator to the end of the sequence.
  /// @return a ::const_iterator to the end of the sequence.
  constexpr const_iterator end() const noexcept { return impl()->end(); }

  /// @brief The iterator to the beginning of the sequence.
  /// @return a ::const_iterator to the beginning of the sequence.
  constexpr const_iterator cbegin() const noexcept { return impl()->cbegin(); }

  /// @brief The iterator to the end of the sequence.
  /// @return a ::const_iterator to the end of the sequence.
  constexpr const_iterator cend() const noexcept { return impl()->cend(); }

  // todo rbegin
  // todo crbegin
  // todo rend
  // todo crend
  /// @}

  /// @defgroup Capacity
  /// @{
  /// @brief Empty test.
  /// @return true if empty (N=0), and false otherwise.
  constexpr bool empty() const noexcept { return impl()->empty(); }

  /// @brief The size of the container.
  /// @return the size of the container (N).
  constexpr size_type size() const noexcept { return impl()->size(); }

  /// @brief The maximum size of the container.
  /// @return the maximum size of the container (N).
  constexpr size_type max_size() const noexcept { return impl()->max_size(); }
  /// @}

  /// @defgroup Operations
  /// @{
  /// @brief Fill the array with an input value.
  ///
  /// @param v The input value used to fill the array.
  void fill(const value_type &v) { impl()->fill(v); }

  /// @brief Swap the content of two array.
  ///
  /// @param O The array to swap the content with.
  void swap(array<T, N> &O) noexcept /* (std::is_nothrow_swappable_v<T>) */ {
    impl()->swap(*O->ptr);
  }
  /// @}

 private:
  std::shared_ptr<array_t> ptr = nullptr;

  const array_t *impl() const { return ptr.get(); }

  array_t *impl() { return ptr.get(); }

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
