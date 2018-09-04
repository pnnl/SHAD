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

#ifndef INCLUDE_SHAD_RUNTIME_LOCALITY_H_
#define INCLUDE_SHAD_RUNTIME_LOCALITY_H_

#include <cstdint>

#include "shad/config/config.h"
#include "shad/runtime/mapping_traits.h"
#include "shad/runtime/mappings/available_traits_mappings.h"

namespace shad {

namespace rt {

/// @brief A Locality of the system.
///
/// The Locality abstraction represents a block of the computing system that
/// contains memory and processors.  The abstraction can serve to model
/// different systems with different granularity.  For example on a cluster, the
/// concept can be used to model a node of the cluster or a NUMA domain.
///
/// Typical Usage:
/// @code
/// shad::rt::Locality & thisLocality = shad::rt::thisLocality();
/// if (thisLocality == Locality(0)) {
/// ...
/// } else {
/// ...
/// }
/// @endcode
class Locality {
 public:
  /// @brief Constructor.
  ///
  /// Initialize the newly created object to a Locality::kNullLocality.
  Locality()
      : id_(impl::RuntimeInternalsTrait<TargetSystemTag>::NullLocality()) {}

  /// @brief Constructor.
  ///
  /// Initialize the newly created object with a specific locality ID.
  ///
  /// @param id The locality ID to be assigned.
  explicit constexpr Locality(const uint32_t id) : id_(id) {}

  /// @brief Copy-Constructor.
  Locality(const Locality& rhs) = default;

  /// @brief Copy-Assigment.
  Locality& operator=(const Locality& rhs) = default;

  /// @brief Move-Constructor.
  Locality(Locality&& rhs) = default;

  /// @brief Move-Assignment.
  Locality& operator=(Locality&& rhs) = default;

  /// @brief Operator less than.
  ///
  /// @param lhs The left-hand side of the operator.
  /// @param rhs The right-hand side of the operator.
  /// @return true when lhs is less than rhs.
  friend bool operator<(const Locality& lhs, const Locality& rhs) {
    return (lhs.id_ < rhs.id_);
  }

  /// @brief Operator equal.
  ///
  /// @param lhs The left-hand side of the operator.
  /// @param rhs The right-hand side of the operator.
  /// @return true when lhs and rhs are equal.
  friend bool operator==(const Locality& lhs, const Locality& rhs) {
    return (lhs.id_ == rhs.id_);
  }

  /// @brief Stream insertion operator.
  ///
  /// @param os The output stream to be used.
  /// @param rhs The right-hand side of the operator.
  /// @return A reference to the output stream used.
  friend std::ostream& operator<<(std::ostream& os, const Locality& rhs) {
    return os << "Locality[" << rhs.id_ << "]";
  }

  /// @brief Explicit conversion to uint32_t.
  explicit operator uint32_t() const { return id_; }

  /// @brief Null Test.
  /// @return true if the Handle is null, false otherwise.
  bool IsNull() const {
    return id_ == impl::RuntimeInternalsTrait<TargetSystemTag>::NullLocality();
  }

  Locality& operator++() {
    ++id_;
    return *this;
  }

  Locality& operator--() {
    --id_;
    return *this;
  }

  Locality& operator+=(std::size_t n) {
    id_ += n;
    return *this;
  }

  Locality& operator-=(std::size_t n) {
    id_ -= n;
    return *this;
  }

  Locality operator-(std::size_t n) {
    Locality tmp = *this;
    tmp.operator-=(1);
    return tmp;
  }

  Locality operator+(std::size_t n) {
    Locality tmp = *this;
    tmp.operator+=(1);
    return tmp;
  }

 private:
  uint32_t id_;
};

/// @memberof Locality
/// @brief Operator not equal.
///
/// @param lhs The left-hand side of the operator.
/// @param rhs The right-hand side of the operator.
/// @return true when lhs and rhs are not equal.
inline bool operator!=(const Locality& lhs, const Locality& rhs) {
  return !(lhs == rhs);
}

/// @memberof Locality
/// @brief Operator greater than.
///
/// @param lhs The left-hand side of the operator.
/// @param rhs The right-hand side of the operator.
/// @return true when the lhs ID is greater than the rhs ID.
inline bool operator>(const Locality& lhs, const Locality& rhs) {
  return rhs < lhs;
}

/// @memberof Locality
/// @brief Operator less than or equal.
///
/// @param lhs The left-hand side of the operator.
/// @param rhs The right-hand side of the operator.
/// @return true when the lhs ID is less than or equal to the rhs ID.
inline bool operator<=(const Locality& lhs, const Locality& rhs) {
  return !(lhs > rhs);
}

/// @memberof Locality
/// @brief Operator greater than or equal.
///
/// @param lhs The left-hand side of the operator.
/// @param rhs The right-hand side of the operator.
/// @return true when the lhs ID is greater than or equal to the rhs ID.
inline bool operator>=(const Locality& lhs, const Locality& rhs) {
  return !(lhs < rhs);
}

/// @brief A range of localities.
class localities_range {
 public:
  /// @brief Constructor.
  /// @param B The begin.
  /// @param E the end.
  localities_range(const Locality& B, const Locality& E) : begin_(B), end_(E) {}

  /// @brief Default constructor.
  localities_range()
      : localities_range(Locality(0),
                         Locality(impl::RuntimeInternalsTrait<
                                  TargetSystemTag>::NumLocalities())) {}

  /// @brief The begin of the sequence of Localities.
  Locality begin() const { return begin_; }
  /// @brief The end of the sequence of Localities.
  Locality end() const { return end_; }

  size_t size() const {
    return static_cast<uint32_t>(end_) - static_cast<uint32_t>(begin_);
  }

 private:
  Locality begin_;
  Locality end_;
};

}  // namespace rt

}  // namespace shad

#endif  // INCLUDE_SHAD_RUNTIME_LOCALITY_H_
