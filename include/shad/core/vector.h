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
/// @brief Distributed vector without resizing capabilities.
/// TODO change the descriptions
/// The C++ standard defines the ::vector as a dynamically
/// resizable sequence of objects.  A ::vector should be a contiguous container
/// (as defined in the C++ standard).  According to that definition, contiguous
/// containers requires contiguous iterators.  The definition of contiguous
/// iterators implies contiguous memory allocation for the sequence, and it
/// cannot be guaranteed in many distributed settings.  Therefore, ::vector
/// relaxes this requirement.
///
/// @tparam T The type of the elements in the distributed vector.
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
  /// The type of references to the element in the vector.
  using reference = VectorRef<value_type>;
  /// The type for const references to element in the vector.
  using const_reference = VectorRef<const value_type>;
  /// The type for pointer to ::value_type.
  using pointer = value_type *;
  /// The type for pointer to ::const_value_type
  using const_pointer = const value_type *;
  /// The type of iterators on the vector.
  using iterator = vector_iterator<value_type>;
  /// The type of const iterators on the vector.
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

          std::copy(Other->chunk_, Other->chunk_ + This->chunk_size(),
                    This->chunk_);
        },
        std::make_pair(this->oid_, O.oid_));

    fill_ptrs();
    return *this;
  }

  /// @brief Fill the vector with an input value.
  ///
  /// @param v The input value used to fill the vector.
  void fill(const value_type &v) {
    rt::executeOnAll(
        [](const std::pair<ObjectID, value_type> &args) {
          auto This = vector<T>::GetPtr(std::get<0>(args));
          auto value = std::get<1>(args);

          std::fill(This->chunk_.get(), This->chunk_.get() + This->chunk_size(),
                    value);
        },
        std::make_pair(this->oid_, v));
  }

  /// @brief Swap the content of two vector.
  ///
  /// @param O The vector to swap the content with.
  void swap(vector<T> &O) noexcept /* (std::is_nothrow_swappable_v<T>) */ {
    rt::executeOnAll(
        [](const std::pair<ObjectID, ObjectID> &IDs) {
          auto This = vector<T>::GetPtr(std::get<0>(IDs));
          auto Other = vector<T>::GetPtr(std::get<1>(IDs));

          std::swap(This->p_, Other->p_);
          std::swap(This->chunk_, Other->chunk_);
          std::swap(This->ptrs_, Other->ptrs_);
        },
        std::make_pair(this->oid_, O.oid_));
  }
  /// @defgroup Iterators
  /// @{

  /// @brief The iterator to the beginning of the sequence.
  /// @return an ::iterator to the beginning of the sequence.
  constexpr iterator begin() noexcept {
    return iterator{rt::Locality(0), 0, oid_, ptrs_.data(), p_.data()};
  }

  /// @brief The iterator to the beginning of the sequence.
  /// @return a ::const_iterator to the beginning of the sequence.
  constexpr const_iterator begin() const noexcept { return cbegin(); }

  /// @brief The iterator to the end of the sequence.
  /// @return an ::iterator to the end of the sequence.
  constexpr iterator end() noexcept {
    const auto end_l = locate_index(p_.back()) - 1;

    difference_type pos = p_[end_l + 1] - p_[end_l];

    rt::Locality last(end_l);

    return iterator{std::forward<rt::Locality>(last), pos, oid_, ptrs_.data(),
                    p_.data()};
  }

  /// @brief The iterator to the end of the sequence.
  /// @return a ::const_iterator to the end of the sequence.
  constexpr const_iterator end() const noexcept { return cend(); }

  /// @brief The iterator to the beginning of the sequence.
  /// @return a ::const_iterator to the beginning of the sequence.
  constexpr const_iterator cbegin() const noexcept {
    return const_iterator{rt::Locality(0), 0, oid_, ptrs_.data(), p_.data()};
  }

  /// @brief The iterator to the end of the sequence.
  /// @return a ::const_iterator to the end of the sequence.
  constexpr const_iterator cend() const noexcept {
    const auto end_l = locate_index(p_.back()) - 1;

    difference_type pos = p_[end_l + 1] - p_[end_l];

    rt::Locality last(end_l);

    return const_iterator{std::forward<rt::Locality>(last), pos, oid_,
                          ptrs_.data(), p_.data()};
  }

  /// @}

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
  /// @return a ::reference to the n-th element in the vector.
  constexpr reference operator[](size_type n) {
    const std::uint32_t l = locate_index(n);
    return reference{rt::Locality{l}, difference_type{n - p_[l]}, oid_,
                     ptrs_[l]};
  }

  /// @brief Unchecked element access operator.
  /// @return a ::const_reference to the n-th element in the vector.
  constexpr const_reference operator[](size_type n) const {
    const auto l = locate_index(n);
    return const_reference{rt::Locality{l}, difference_type{n - p_[l]}, oid_,
                           ptrs_[l]};
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

  /// @brief the first element in the vector.
  /// @return a ::reference to the element in position 0.
  constexpr reference front() { return *begin(); }
  /// @brief the first element in the vector.
  /// @return a ::const_reference to the element in position 0.
  constexpr const_reference front() const { return *cbegin(); }

  /// @brief the last element in the vector.
  /// @return a ::reference to the element in position N - 1.
  constexpr reference back() { return *(end() - 1); }
  /// @brief the last element in the vector.
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
                *result = !std::equal(
                    LHS->chunk_, LHS->chunk_ + LHS->chunk_size(), RHS->chunk_);
              else
                throw std::logic_error("Not yet implemented!");
            }
          },
          std::make_pair(LHS.GetGlobalID(), RHS.GetGlobalID()),
          &result[static_cast<uint32_t>(l)]);
    }

    rt::waitForCompletion(H);

    for (std::uint32_t i = 1; i < rt::numLocalities(); ++i)
      result[0] |= result[i];

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

    for (std::uint32_t i = 1; i < rt::numLocalities(); ++i) {
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

    for (std::uint32_t i = 1; i < rt::numLocalities(); ++i) {
      result[0] &= result[i];
    }

    return result[0];
  }

  constexpr void fill_ptrs() {
    rt::executeOnAll(
        [](const ObjectID &oid) {
          auto This = vector<T>::GetPtr(oid);
          rt::executeOnAll(
              [](const std::tuple<ObjectID, rt::Locality, pointer> &args) {
                auto This = vector<T>::GetPtr(std::get<0>(args));

                This->ptrs_[to_int(std::get<1>(args))] = std::get<2>(args);
              },
              std::make_tuple(This->GetGlobalID(), rt::thisLocality(),
                              This->chunk_.get()));
        },
        GetGlobalID());
  }

 protected:
  template <typename Iter, typename Int>
  static constexpr difference_type lowerbound_index(Iter begin, Iter end,
                                                    Int v) {
    return std::distance(begin + 1, std::lower_bound(begin, end, v + 1));
  }

  constexpr difference_type locate_index(size_type i) const {
    return lowerbound_index(p_.cbegin(), p_.cend(), i);
  }

  constexpr size_type chunk_size() const {
    return p_[to_int(rt::thisLocality()) + 1] - p_[to_int(rt::thisLocality())];
  }

  /// @brief Constructor.
  explicit vector(ObjectID oid, size_type N) : oid_{oid} {
    p_.reserve(rt::numLocalities() + 1);
    for (std::uint32_t i = 0; i <= rt::numLocalities(); i++)
      p_.emplace_back(i * N / rt::numLocalities());
    chunk_ = std::unique_ptr<T[]>{new T[chunk_size()]};

    ptrs_.resize(rt::numLocalities());
  }

 private:
  // Iterators
  template <typename U>
  class BaseVectorRef {
   public:
    using value_type = U;

    ObjectID oid_;
    pointer chunk_;
    difference_type pos_;
    rt::Locality loc_;

    BaseVectorRef(rt::Locality l, difference_type p, ObjectID oid,
                  pointer chunk)
        : oid_(oid), chunk_(chunk), pos_(p), loc_(l) {}

    BaseVectorRef(const BaseVectorRef &O)
        : oid_(O.oid_), chunk_(O.chunk_), pos_(O.pos_), loc_(O.loc_) {}

    BaseVectorRef(BaseVectorRef &&O)
        : oid_(std::move(O.oid_)),
          chunk_(std::move(O.chunk_)),
          pos_(std::move(O.pos_)),
          loc_(std::move(O.loc_)) {}

    BaseVectorRef &operator=(const BaseVectorRef &O) {
      oid_ = O.oid_;
      chunk_ = O.chunk_;
      pos_ = O.pos_;
      loc_ = O.loc_;
      return *this;
    }

    BaseVectorRef &operator=(BaseVectorRef &&O) {
      oid_ = std::move(O.oid_);
      chunk_ = std::move(O.chunk_);
      pos_ = std::move(O.pos_);
      loc_ = std::move(O.loc_);
      return *this;
    }

    bool operator==(const BaseVectorRef &O) const {
      if (oid_ == O.oid_ && pos_ == O.pos_ && loc_ == O.loc_) return true;
      return this->get() == O.get();
    }

    value_type get() const {
      if (this->loc_ == rt::thisLocality()) return chunk_[pos_];

      value_type result;
      rt::dma(&result, loc_, chunk_ + pos_, 1);
      return result;
    }
  };

  template <typename U>
  class alignas(64) VectorRef : public BaseVectorRef<U> {
   public:
    using value_type = U;

    VectorRef(rt::Locality l, difference_type p, ObjectID oid, pointer chunk)
        : BaseVectorRef<U>(l, p, oid, chunk) {}

    VectorRef(const VectorRef &O) : BaseVectorRef<U>(O) {}

    VectorRef(VectorRef &&O) : BaseVectorRef<U>(O) {}

    VectorRef &operator=(const VectorRef &O) {
      BaseVectorRef<U>::operator=(O);
      return *this;
    }

    VectorRef &operator=(VectorRef &&O) {
      BaseVectorRef<U>::operator=(O);
      return *this;
    }

    operator value_type() const {  // NOLINT
      return BaseVectorRef<U>::get();
    }

    bool operator==(const VectorRef &&v) const {
      return BaseVectorRef<U>::operator==(v);
    }

    VectorRef &operator=(const T &v) {
      bool local = this->loc_ == rt::thisLocality();
      if (local) {
        this->chunk_[this->pos_] = v;
        return *this;
      }

      rt::dma(this->loc_, this->chunk_ + this->pos_, &v, 1);

      return *this;
    }

    friend std::ostream &operator<<(std::ostream &stream, const VectorRef i) {
      stream << i.loc_ << " " << i.pos_ << " " << i.get();
      return stream;
    }
  };

  template <typename U>
  class alignas(64) VectorRef<const U> : public BaseVectorRef<U> {
   public:
    using value_type = const U;

    VectorRef(rt::Locality l, difference_type p, ObjectID oid, pointer chunk)
        : BaseVectorRef<U>(l, p, oid, chunk) {}

    VectorRef(const VectorRef &O) : BaseVectorRef<U>(O) {}

    VectorRef(VectorRef &&O) : BaseVectorRef<U>(O) {}

    bool operator==(const VectorRef &&v) const {
      return BaseVectorRef<U>::operator==(v);
    }

    VectorRef &operator=(const VectorRef &O) {
      BaseVectorRef<U>::operator=(O);
      return *this;
    }

    VectorRef &operator=(VectorRef &&O) {
      BaseVectorRef<U>::operator=(O);
      return *this;
    }

    operator value_type() const {  // NOLINT
      return BaseVectorRef<U>::get();
    }

    friend std::ostream &operator<<(std::ostream &stream, const VectorRef i) {
      stream << i.loc_ << " " << i.pos_;
      return stream;
    }
  };

 private:
  static constexpr std::uint32_t to_int(rt::Locality l) {
    return static_cast<std::uint32_t>(l);
  }

  std::vector<difference_type> p_;
  std::unique_ptr<T[]> chunk_;
  ObjectID oid_;
  std::vector<pointer> ptrs_;
};

template <typename T>
bool operator==(const vector<T> &LHS, const vector<T> &RHS) {
  return !(LHS != RHS);
}

template <typename T>
bool operator<(const vector<T> &LHS, const vector<T> &RHS) {
  return !(LHS >= RHS);
}

template <typename T>
bool operator>(const vector<T> &LHS, const vector<T> &RHS) {
  return !(LHS <= RHS);
}

template <typename T>
template <typename U>
class alignas(64) vector<T>::vector_iterator {
 public:
  using reference = typename vector<T>::template VectorRef<U>;
  using pointer = typename vector<T>::pointer;
  using const_pointer = typename vector<T>::const_pointer;
  using difference_type = std::ptrdiff_t;
  using value_type = typename vector<T>::value_type;
  using iterator_category = std::random_access_iterator_tag;

  using local_iterator_type = pointer;

  using distribution_range = std::vector<std::pair<rt::Locality, size_t>>;

  /// @brief Constructor.
  vector_iterator(rt::Locality &&l, difference_type offset, ObjectID oid,
                  pointer const *ptrs, difference_type const *p_)
      : locality_(l), offset_(offset), oid_(oid), ptrs_(ptrs), p_(p_) {}

  /// @brief Default constructor.
  vector_iterator()
      : vector_iterator(rt::Locality(0), -1, ObjectID::kNullID, nullptr,
                        nullptr) {}

  /// @brief Copy constructor.
  vector_iterator(const vector_iterator &O)
      : locality_(O.locality_),
        offset_(O.offset_),
        oid_(O.oid_),
        ptrs_(O.ptrs_),
        p_(O.p_) {}

  /// @brief Move constructor.
  vector_iterator(const vector_iterator &&O) noexcept
      : locality_(std::move(O.locality_)),
        offset_(std::move(O.offset_)),
        oid_(std::move(O.oid_)),
        ptrs_(std::move(O.ptrs_)),
        p_(std::move(O.p_)) {}

  /// @brief Copy assignment operator.
  vector_iterator &operator=(const vector_iterator &O) {
    locality_ = O.locality_;
    offset_ = O.offset_;
    oid_ = O.oid_;
    ptrs_ = O.ptrs_;
    p_ = O.p_;

    return *this;
  }

  /// @brief Move assignment operator.
  vector_iterator &operator=(vector_iterator &&O) {
    locality_ = std::move(O.locality_);
    offset_ = std::move(O.offset_);
    oid_ = std::move(O.oid_);
    ptrs_ = std::move(O.ptrs_);
    p_ = std::move(O.p_);

    return *this;
  }

  bool operator==(const vector_iterator &O) const {
    return locality_ == O.locality_ && oid_ == O.oid_ && offset_ == O.offset_;
  }

  bool operator!=(const vector_iterator &O) const { return !(*this == O); }

  reference operator*() {
    return reference(locality_, offset_, oid_, get_chunk());
  }

  vector_iterator &operator++() {
    uint32_t l = (uint32_t)locality_;
    const auto g_offset = p_[l] + offset_ + 1;
    if (g_offset < p_[l + 1])
      ++offset_;
    else {
      const auto num_l = rt::numLocalities();
      while (l < num_l && g_offset >= p_[l + 1]) l++;
      if (l == num_l) {
        locality_ = rt::Locality(num_l - 1);
        offset_ = p_[num_l] - p_[num_l - 1];
      } else {
        locality_ = rt::Locality(l);
        offset_ = 0;
      }
    }
    return *this;
  }

  vector_iterator operator++(int) {
    vector_iterator tmp(*this);
    operator++();
    return tmp;
  }

  vector_iterator &operator--() {
    if (offset_ > 0)
      --offset_;
    else {
      uint32_t l = (uint32_t)locality_;
      const difference_type g_offset = p_[l] - 1;
      if (g_offset < 0) {
        locality_ = rt::Locality(0);
        offset_ = -1;
      } else {
        while (g_offset < p_[l - 1]) l--;
        locality_ = rt::Locality(l - 1);
        offset_ = p_[l] - p_[l - 1] - 1;
      }
    }

    return *this;
  }

  vector_iterator operator--(int) {
    vector_iterator tmp(*this);
    operator--();
    return tmp;
  }

  vector_iterator &operator+=(difference_type n) {
    const uint32_t l = (uint32_t)locality_;
    const auto g_offset = p_[l] + offset_ + n;
    if (p_[l] <= g_offset && g_offset < p_[l + 1])
      offset_ += n;
    else {
      const auto num_l = rt::numLocalities();
      const auto l = vector<T>::lowerbound_index(p_, p_ + num_l + 1, g_offset);
      if (l < 0) {
        locality_ = rt::Locality(0);
        offset_ = -1;
      } else if (l >= num_l) {
        locality_ = rt::Locality(num_l - 1);
        offset_ = p_[num_l] - p_[num_l - 1];
      } else {
        locality_ = rt::Locality(l);
        offset_ = g_offset - p_[l];
      }
    }

    return *this;
  }

  vector_iterator &operator-=(difference_type n) { return operator+=(-n); }

  vector_iterator operator+(difference_type n) {
    vector_iterator tmp(*this);
    return tmp.operator+=(n);
  }

  vector_iterator operator-(difference_type n) {
    vector_iterator tmp(*this);
    return tmp.operator-=(n);
  }

  difference_type operator-(const vector_iterator &O) const {
    if (oid_ != O.oid_) return std::numeric_limits<difference_type>::min();

    return get_global_id() - O.get_global_id();
  }

  bool operator<(const vector_iterator &O) const {
    if (oid_ != O.oid_ || locality_ > O.locality_) return false;
    return locality_ < O.locality_ || offset_ < O.offset_;
  }

  bool operator>(const vector_iterator &O) const {
    if (oid_ != O.oid_ || locality_ < O.locality_) return false;
    return locality_ > O.locality_ || offset_ > O.offset_;
  }

  bool operator<=(const vector_iterator &O) const { return !(*this > O); }

  bool operator>=(const vector_iterator &O) const { return !(*this < O); }

  friend std::ostream &operator<<(std::ostream &stream,
                                  const vector_iterator i) {
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

  static local_iterator_range local_range(vector_iterator &B,
                                          vector_iterator &E) {
    auto begin{B.get_local_chunk()};

    if (B.oid_ != E.oid_ || rt::thisLocality() < B.locality_ ||
        rt::thisLocality() > E.locality_)
      return local_iterator_range(begin, begin);

    if (B.locality_ == rt::thisLocality()) begin += B.offset_;

    const std::uint32_t l = rt::thisLocality();
    const auto chunk = B.p_[l + 1] - B.p_[l];

    auto end{B.get_local_chunk() + chunk};
    if (E.locality_ == rt::thisLocality())
      end = B.get_local_chunk() + E.offset_;
    return local_iterator_range(begin, end);
  }

  static distribution_range distribution(vector_iterator &begin,
                                         vector_iterator &end) {
    distribution_range result;

    // First block:
    const std::uint32_t begin_l = begin.locality_;
    auto start_block_size = begin.p_[begin_l + 1] - begin.p_[begin_l];
    if (begin.locality_ == end.locality_) start_block_size = end.offset_;
    result.push_back(
        std::make_pair(begin.locality_, start_block_size - begin.offset_));

    // Middle blocks:
    for (auto locality = begin.locality_ + 1; locality < end.locality_;
         ++locality) {
      const std::uint32_t mid_l = locality;
      const auto inner_block_size = begin.p_[mid_l + 1] - begin.p_[mid_l];
      result.push_back(std::make_pair(locality, inner_block_size));
    }

    // Last block:
    if (end.offset_ != 0 && begin.locality_ != end.locality_)
      result.push_back(std::make_pair(end.locality_, end.offset_));

    return result;
  }

  static rt::localities_range localities(vector_iterator &B,
                                         vector_iterator &E) {
    return rt::localities_range(
        B.locality_, rt::Locality(static_cast<uint32_t>(E.locality_) +
                                  (E.offset_ != 0 ? 1 : 0)));
  }

  static vector_iterator iterator_from_local(vector_iterator &B,
                                             vector_iterator &E,
                                             local_iterator_type itr) {
    if (rt::thisLocality() < B.locality_ || rt::thisLocality() > E.locality_)
      return E;

    return vector_iterator(rt::thisLocality(),
                           std::distance(B.get_local_chunk(), itr), B.oid_,
                           B.ptrs_, B.p_);
  }

 protected:
  constexpr difference_type get_global_id() const {
    return p_[to_int(locality_)] + offset_;
  }

  constexpr pointer get_chunk() const { return ptrs_[to_int(locality_)]; }

  constexpr pointer get_local_chunk() const {
    return ptrs_[to_int(rt::thisLocality())];
  }

 private:
  rt::Locality locality_;
  ObjectID oid_;
  difference_type offset_;
  pointer const *ptrs_;
  difference_type const *p_;
};

}  // namespace impl

/// @brief Fixed size distributed vector.
///
/// Section XX.X.X.X of the C++ standard defines the ::vector as a dynamically
/// resizable sequence of objects.  A ::vector should be a contiguous container
/// (as defined in section XX.X.X).  According to that definition, contiguous
/// containers requires contiguous iterators.  The definition of contiguous
/// iterators implies contiguous memory allocation for the sequence, and it
/// cannot be guaranteed in many distributed settings.  Therefore, ::vector
/// relaxes this requirement.
///
/// @tparam T The type of the elements in the distributed vector.
template <class T>
class vector {
  using vector_t = impl::vector<T>;

 public:
  /// @defgroup Types
  /// @{
  /// The type of the stored value.
  using value_type = typename vector_t::value_type;
  /// The type used to represent size.
  using size_type = typename vector_t::size_type;
  /// The type used to represent distances.
  using difference_type = typename vector_t::difference_type;
  /// The type of references to the element in the vector.
  using reference = typename vector_t::reference;
  /// The type for const references to element in the vector.
  using const_reference = typename vector_t::const_reference;
  /// The type for pointer to ::value_type.
  using pointer = typename vector_t::pointer;
  /// The type for pointer to ::const_value_type
  using const_pointer = typename vector_t::const_pointer;
  /// The type of iterators on the vector.
  using iterator = typename vector_t::iterator;
  /// The type of const iterators on the vector.
  using const_iterator = typename vector_t::const_iterator;
  // todo reverse_iterator
  // todo const_reverse_iterator
  /// @}

 public:
  /// @brief Constructor.
  explicit vector(size_type N = 0) {
    ptr = vector_t::Create(N);
    ptr->fill_ptrs();
  }

  /// @brief Destructor.
  ~vector() { vector_t::Destroy(impl()->GetGlobalID()); }

  /// @brief The copy assignment operator.
  ///
  /// @param O The right-hand side of the operator.
  /// @return A reference to the left-hand side.
  vector<T> &operator=(const vector<T> &O) {
    impl()->operator=(*O.ptr);
    return *this;
  }

  /// @defgroup Element Access
  /// @{

  /// @brief Unchecked element access operator.
  /// @return a ::reference to the n-th element in the vector.
  constexpr reference operator[](size_type n) { return impl()->operator[](n); }

  /// @brief Unchecked element access operator.
  /// @return a ::const_reference to the n-th element in the vector.
  constexpr const_reference operator[](size_type n) const {
    return impl()->operator[](n);
  }

  /// @brief Checked element access operator.
  /// @return a ::reference to the n-th element in the vector.
  constexpr reference at(size_type n) { return impl()->at(n); }

  /// @brief Checked element access operator.
  /// @return a ::const_reference to the n-th element in the vector.
  constexpr const_reference at(size_type n) const { return impl()->at(n); }

  /// @brief the first element in the vector.
  /// @return a ::reference to the element in position 0.
  constexpr reference front() { return impl()->front(); }

  /// @brief the first element in the vector.
  /// @return a ::const_reference to the element in position 0.
  constexpr const_reference front() const { return impl()->front(); }

  /// @brief the last element in the vector.
  /// @return a ::reference to the element in position N - 1.
  constexpr reference back() { return impl()->back(); }

  /// @brief the last element in the vector.
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
  /// @brief Fill the vector with an input value.
  ///
  /// @param v The input value used to fill the vector.
  void fill(const value_type &v) { impl()->fill(v); }

  /// @brief Swap the content of two vector.
  ///
  /// @param O The vector to swap the content with.
  void swap(vector<T> &O) noexcept /* (std::is_nothrow_swappable_v<T>) */ {
    impl()->swap(*O->ptr);
  }
  /// @}

 private:
  std::shared_ptr<vector_t> ptr = nullptr;

  const vector_t *impl() const { return ptr.get(); }

  vector_t *impl() { return ptr.get(); }

  friend bool operator==(const vector &LHS, const vector &RHS) {
    return *LHS.ptr == *RHS.ptr;
  }

  friend bool operator<(const vector &LHS, const vector &RHS) {
    return operator<(*LHS.ptr, *RHS.ptr);
  }

  friend bool operator>(const vector &LHS, const vector &RHS) {
    return operator>(*LHS.ptr, *RHS.ptr);
  }
};

}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_VECTOR_H_ */
