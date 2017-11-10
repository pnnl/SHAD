//===------------------------------------------------------------*- C++ -*-===//
//
//                                     SHAD
//
//      The Scalable High-performance Algorithms and Data Structure Library
//
//===----------------------------------------------------------------------===//
//
// Copyright 2017 Pacific Northwest National Laboratory
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


#ifndef INCLUDE_SHAD_DATA_STRUCTURES_OBJECT_IDENTIFIER_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_OBJECT_IDENTIFIER_H_

#include <atomic>
#include <limits>
#include "shad/runtime/runtime.h"

namespace shad {
/// @brief The ObjectIdentifier.
///
/// The most significant 12bits store the id of the Locality
/// where the object has been created.  The remaining 48bits contain a unique
/// number identifying the object.
///
/// @warning Enforcing the previous policy is not responsibility of
/// ObjectIdentifier but of its users.
///
/// @tparam T The type of the object that must be uniquely identified.
template <typename T>
class ObjectIdentifier {
 public:
  /// Null value for the ObjectIdentifier.  This value is used during
  /// ObjectIdentifier creation when the ID can't be known yet or the ID is
  /// invalid.
  static const ObjectIdentifier<T> kNullID;

  /// The number of bits used to store the Locality where the Object is stored.
  static constexpr uint8_t kLocalityIdBitsize = 16u;

  /// The number of bits in the RowID used for the row identifier in each
  /// Locality.
  static constexpr uint8_t kIdentifierBitsize = 48u;

  /// @brief Constructor.
  explicit constexpr ObjectIdentifier(uint64_t id) : id_(id) {}

  /// @brief Constructor.
  /// @param[in] locality The locality identifier part.
  /// @param[in] localID The local identifier part.
  ObjectIdentifier(const rt::Locality & locality, uint64_t localID) {
    id_ = static_cast<uint32_t>(locality);
    id_ <<= kIdentifierBitsize;
    id_ |= localID;
  }

  /// @brief Copy Constructor
  ObjectIdentifier(const ObjectIdentifier & oid) = default;

  /// @brief Move constructor
  ObjectIdentifier(ObjectIdentifier &&) noexcept = default;

  /// @brief Move assignment
  ObjectIdentifier & operator=(ObjectIdentifier &&) noexcept = default;

  /// @brief Operator less than.
  ///
  /// @param[in] lhs Left hand side of the operator.
  /// @param[in] rhs Right hand side of the operator.
  /// @return true if lhs < rhs, false otherwise.
  friend bool operator<(const ObjectIdentifier & lhs,
                        const ObjectIdentifier & rhs) {
    return lhs.id_ < rhs.id_;
  }

  /// @brief Assignment operator.
  ///
  /// @param[in] rhs Right hand side of the operator.
  ObjectIdentifier & operator=(const ObjectIdentifier & rhs) = default;

  /// @brief Conversion operator to uint64_t
  explicit operator uint64_t() const { return static_cast<uint64_t>(id_); }

  /// @brief Get the Locality owing the Object.
  /// @return The Locality owing the Object.
  rt::Locality GetOwnerLocality() const {
    return rt::Locality(uint32_t(id_ >> kIdentifierBitsize));
  }

  /// Get the Local part of the ObjectIdentifier.
  ///
  /// @return The lowest ObjectIdentifier::kIdentifierBitsize bits of the
  /// ObjectID.
  size_t GetLocalID() const {
    return id_ & kIdentifierBitMask;
  }

 private:
  /// The Bitmask used to estract the Object Identifier.
  static constexpr uint64_t kIdentifierBitMask =
      ((uint64_t(1) << kIdentifierBitsize) - 1);

  uint64_t id_;
};

template<typename T>
const ObjectIdentifier<T> ObjectIdentifier<T>::kNullID =
    ObjectIdentifier<T>(std::numeric_limits<uint64_t>::max());

template<typename T>
constexpr uint8_t ObjectIdentifier<T>::kIdentifierBitsize;

template<typename T>
constexpr uint64_t ObjectIdentifier<T>::kIdentifierBitMask;


/// @brief Operator greater than.
///
/// @tparam T The type of Object to which the identifier refers.
///
/// @param[in] lhs Left hand side of the operator.
/// @param[in] rhs Right hand side of the operator.
/// @return true if lhs > rhs, false otherwise.
template<typename T>
inline bool
operator>(const ObjectIdentifier<T> & lhs, const ObjectIdentifier<T> & rhs) {
  return rhs < lhs;
}

/// @brief Operator less than or equal.
///
/// @tparam T The type of Object to which the identifier refers.
///
/// @param[in] lhs Left hand side of the operator.
/// @param[in] rhs Right hand side of the operator.
/// @return true if lhs <= rhs, false otherwise.
template<typename T>
inline bool
operator<=(const ObjectIdentifier<T> & lhs, const ObjectIdentifier<T> & rhs) {
  return !(lhs > rhs);
}

/// @brief Operator greater than or equal.
///
/// @tparam T The type of Object to which the identifier refers.
///
/// @param[in] lhs Left hand side of the operator.
/// @param[in] rhs Right hand side of the operator.
/// @return true if lhs >= rhs, false otherwise.
template<typename T>
inline bool
operator>=(const ObjectIdentifier<T> & lhs, const ObjectIdentifier<T> & rhs) {
  return !(lhs < rhs);
}

/// @brief Operator equal.
///
/// @tparam T The type of Object to which the identifier refers.
///
/// @param[in] lhs Left hand side of the operator.
/// @param[in] rhs Right hand side of the operator.
/// @return true if lhs == rhs, false otherwise.
template<typename T>
inline bool
operator==(const ObjectIdentifier<T> & lhs, const ObjectIdentifier<T> & rhs) {
  return !(rhs < lhs) && !(lhs < rhs);
}

/// @brief Operator not equal.
///
/// @tparam T The type of Object to which the identifier refers.
///
/// @param[in] lhs Left hand side of the operator.
/// @param[in] rhs Right hand side of the operator.
/// @return true if lhs != rhs, false otherwise.
template<typename T>
inline bool
operator!=(const ObjectIdentifier<T> & lhs, const ObjectIdentifier<T> & rhs) {
  return !(lhs == rhs);
}

/// @brief Output operator to streams.
///
/// @tparam T The type of Object to which the identifier refers.
///
/// @param[in] out The output stream.
/// @param[in] rhs Right hand side of the operator.
/// @return A reference to the used output stream.
template <typename T>
std::ostream &
operator<<(std::ostream & out, const ObjectIdentifier<T> & rhs) {
  auto node = rhs.GetOwnerLocality();
  size_t objectId = rhs.GetLocalID();
  return out << "NodeOwner[" << node << "] id = " << objectId;
}


/// @brief Object representing counters to obtain object IDs.
///
/// The ObjectIdentifier counter enforces that the most significant 12bits store
/// the id of the Locality where the object has been created.  The remaining
/// 48bits contain a unique number identifying the object.
template <typename T>
class ObjectIdentifierCounter {
 public:
  /// @brief Get the singleton instance of the counter for the type T.
  /// @return A reference to the singleton counter object for the type T.
  static ObjectIdentifierCounter<T> & Instance() {
    static ObjectIdentifierCounter<T> instance;
    return instance;
  }
  /// @brief Operator post-increment.
  ObjectIdentifier<T> operator++(int) {
    uint64_t value = counter_.fetch_add(1);
    return ObjectIdentifier<T>(value);
  }

  /// @brief Conversion operator to uint64_t
  explicit operator uint64_t() const { return static_cast<uint64_t>(counter_); }

 private:
  ObjectIdentifierCounter() {
    // note that in the following, the cast to uint32_t invokes the conversion
    // operator of the rt::thisLocality object, but we need a uint64_t.
    counter_ = static_cast<uint64_t>(static_cast<uint32_t>(rt::thisLocality()))
               << ObjectIdentifier<T>::kIdentifierBitsize;
  }

  std::atomic<uint64_t> counter_;
};

/// Output operator to streams.
///
/// @tparam T The type of Object to which the identifier refers.
///
/// @param[out] out The output stream.
/// @param[in] rhs Right hand side of the operator.
/// @return A reference to the used output stream.
template <typename T>
std::ostream &
operator<<(std::ostream & out, const ObjectIdentifierCounter<T> & rhs) {
  uint64_t objectIdentifier = static_cast<uint64_t>(rhs);
  uint64_t node = objectIdentifier >> ObjectIdentifier<T>::kIdentifierBitsize;
  uint64_t objectId =
      ObjectIdentifier<T>::kIdentifierBitMask & objectIdentifier;
  return out << "NodeOwner[" << node << "] id = " << objectId;
}

}  // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_OBJECT_IDENTIFIER_H_
