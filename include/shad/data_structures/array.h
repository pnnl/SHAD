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

#ifndef INCLUDE_SHAD_DATA_STRUCTURES_ARRAY_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_ARRAY_H_

#include <algorithm>
#include <cstring>
#include <functional>
#include <iterator>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "shad/data_structures/abstract_data_structure.h"
#include "shad/data_structures/buffer.h"
#include "shad/runtime/runtime.h"

namespace shad {

/// @brief The Array data structure.
///
/// SHAD's Array is a fixed size distributed container.
///
/// @warning obects of type T need to be trivially copiable.
///
/// @tparam T type of the entries stored in the Array.
template <typename T>
class Array : public AbstractDataStructure<Array<T>> {
  template <typename>
  friend class AbstractDataStructure;

 public:
  constexpr static size_t kMaxChunkSize =
      constants::max(constants::kBufferNumBytes / sizeof(T), 1lu);
  using ObjectID = typename AbstractDataStructure<Array<T>>::ObjectID;
  using BuffersVector = impl::BuffersVector<std::tuple<size_t, T>, Array<T>>;
  using ShadArrayPtr = typename AbstractDataStructure<Array<T>>::SharedPtr;

  /// @brief Retrieve the Global Identifier.
  ///
  /// @return The global identifier associated with the array instance.
  ObjectID GetGlobalID() const { return oid_; }

  /// @brief Return the size of the shad::Array.
  /// @return The size of the shad::Array.
  size_t Size() const noexcept { return size_; }

#ifdef DOXYGEN_IS_RUNNING
  /// @brief Create method.
  ///
  /// Creates a new array instance.
  ///
  /// @param size The size of the array..
  /// @param initValue Initialization value.
  /// @return A shared pointer to the newly created array instance.
  static ShadArrayPtr Create(size_t size, const T &initValue);
#endif

  /// @brief Synchronous insert method.
  ///
  /// Inserts an element at the specified position synchronously.
  ///
  /// Typical usage:
  /// @code
  /// auto arrayPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  /// for (size_t i = 0; i < kArraySize; i++) {
  ///   arrayPtr->InsertAt(i, i+1);
  /// }
  /// @endcode
  ///
  /// @param[in] pos The target position.
  /// @param[in] value The value of the element to be inserted.
  void InsertAt(const size_t pos, const T &value);

  /// @brief Synchronous bulk insert method.
  ///
  /// Inserts multiple elements starting at the specified position.
  ///
  /// Typical usage:
  /// @code
  /// std::vector<size_t> values(10, 1);
  /// auto arrayPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  /// arrayPtr->InsertAt(0, values.data(), values.size());
  /// @endcode
  ///
  /// @param[in] pos The target position.
  /// @param[in] values Pointer to the values to be inserted.
  /// @param[in] numValues Number of values to be inserted.
  void InsertAt(const size_t pos, const T *values, const size_t numValues);

  /// @brief Asynchronous Insert method.
  ///
  /// Asynchronously inserts an element at the specified position.
  ///
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle& handle) method.
  ///
  /// Typical usage:
  /// @code
  /// shad::rt::Handle handle;
  /// auto arrayPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  /// for (size_t i = 0; i < kArraySize; i++) {
  ///   arrayPtr->AsyncInsertAt(handle, i, i+1);
  /// }
  /// // ... more work here and then finally wait.
  /// shad::rt::waitForCompletion(handle);
  /// @endcode
  ///
  /// @param[in,out] handle The handle to be used to wait for completion.
  /// @param[in] pos The target position.
  /// @param[in] value The value to be inserted.
  void AsyncInsertAt(rt::Handle &handle, const size_t pos, const T &value);

  /// @brief Asynchronous bulk insert method.
  ///
  /// Asynchronously inserts multiple elements starting at the specified
  /// position.
  ///
  /// Typical usage:
  /// @code
  /// std::vector<size_t> values(10, 1);
  /// auto arrayPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  ///
  /// shad::rt::Handle handle;
  /// arrayPtr->AsyncInsertAt(handle, 0, values.data(), values.size());
  /// // ... more work here and then finally wait.
  /// shad::rt::waitForCompletion(handle);;
  /// @endcode
  ///
  /// @param[in,out] handle The handle to be used to wait for completion.
  /// @param[in] pos The target position.
  /// @param[in] values Pointer to the values to be inserted.
  /// @param[in] numValues Number of values to be inserted.
  void AsyncInsertAt(rt::Handle &handle, const size_t pos, const T *values,
                     const size_t numValues);

  /// @brief Buffered insert method.
  ///
  /// Inserts an element at the specified position, using aggregation buffers.
  ///
  /// @warning Insertions are finalized only after calling the
  /// WaitForBufferedInsert() method.
  ///
  /// Typical usage:
  /// @code
  /// auto arrayPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  /// for (size_t i = 0; i < kArraySize; i++) {
  ///   arrayPtr->BufferedInsertAt(i, i+1);
  /// }
  /// arrayPtr->WaitForBufferedInsert();
  /// @endcode
  ///
  /// @param[in] pos The target position.
  /// @param[in] value The value of the element to be inserted.
  void BufferedInsertAt(const size_t pos, const T &value);

  /// @brief Asynchronous buffered insert method.
  ///
  /// Asynchronously inserts an element at the specified position,
  /// using aggregation buffers.
  ///
  /// @warning asynchronous buffered insertions are finalized only after calling
  /// the rt::waitForCompletion(rt::Handle &handle) method <b>and</b> the
  /// WaitForBufferedInsert() method, in this order.
  ///
  /// Typical usage:
  /// @code
  /// auto arrayPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  ///
  /// shad::rt::Handle handle;
  /// for (size_t i = 0; i < kArraySize; i++) {
  ///   arrayPtr->BufferedAsyncInsertAt(handle, i, i+1);
  /// }
  /// // ... more work here and then finally wait.
  /// shad::rt::waitForCompletion(handle);
  /// arrayPtr->WaitForBufferedInsert();
  /// @endcode
  ///
  /// @param[in,out] handle The handle to be used to wait for completion.
  /// @param[in] pos The target position.
  /// @param[in] value The value to be inserted.
  void BufferedAsyncInsertAt(rt::Handle &handle, const size_t pos,
                             const T &value);

  /// @brief Finalize method for buffered insertions.
  void WaitForBufferedInsert() { buffers_.FlushAll(); }

  /// @brief Lookup Method.
  ///
  /// Retireve an element at a given position.
  ///
  /// Typical usage:
  /// @code
  /// auto arrayPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  /// // ... fill the array with useful values ...
  ///
  /// for (size_t i = 0; i < arrayPtr->Size(); ++i)
  ///   PrintValue(arrayPtr->At(i));  // Don't do this !
  /// @endcode
  ///
  /// @param[in] pos The target position.
  /// @return The value of the element at position pos.
  T At(const size_t pos);

  /// @brief Asynchronous Lookup Method.
  ///
  /// Retireve an element at a given position asynchronously.
  ///
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  ///
  /// Typical usage:
  /// @code
  /// auto arrayPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  /// // ... fill the array with useful values ...
  ///
  /// std::vector<size_t> top(k);
  /// shad::rt::Handle handle;
  /// for (size_t i = 0; i < k; ++i)
  ///   arrayPtr->AsyncAt(i, &top[i]);
  ///
  /// shad::rt::waitForCompletion(handle);
  /// for (auto v : top)
  ///   PrintValue(v);
  /// @endcode
  ///
  /// @param[in,out] handle The handle to be used to wait for completion.
  /// @param[in] pos The target position.
  /// @param[out] result pointer to the region where the element value is
  /// written; result must point to a valid memory allocation.
  void AsyncAt(rt::Handle &handle, const size_t pos, T *result);

  /// @brief Applies a user-defined function to an element.
  ///
  /// Applies a user-defined function to the element at the specified position.
  ///
  /// Typical usage:
  /// @code
  /// void fn(size_t i, size_t& elem, size_t & incr) { /* do something */ }
  /// ...
  /// auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  /// for (size_t i = 0; i < kArraySize; i++) {
  ///   edsPtr->Apply(i, fn, kInitValue);
  /// }
  /// ...
  /// @endcode
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// must be:
  /// @code
  /// void(size_t, T&, Args& args);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in] pos The target position.
  /// @param[in] function The function to apply.
  /// @param[in] args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void Apply(const size_t pos, ApplyFunT &&function, Args &... args);

  /// @brief Asynchronously applies a user-defined function to an element.
  ///
  /// Asynchronously applies a user-defined function to the
  /// element at the specified position.
  ///
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  ///
  /// Typical usage:
  /// @code
  /// void fn(shad::rt::Handle&, size_t i, size_t& elem, size_t & incr) {
  ///   /* do something */
  /// }
  /// ...
  /// auto arrayPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  /// shad::rt::Handle handle;
  /// for (size_t i = 0; i < kArraySize; i++) {
  ///   arrayPtr->AsyncApply(handle, i, fn, kInitValue);
  /// }
  /// shad::rt::waitForCompletion(handle);
  /// ...
  /// @endcode
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// must be:
  /// @code
  /// void(shad::rt::Handle&, size_t, T&, Args& args);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle Reference to the handle to be used to wait for
  /// completion.
  /// @param[in] pos The target position.
  /// @param[in] function The function to apply.
  /// @param[in] args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncApply(rt::Handle &handle, const size_t pos, ApplyFunT &&function,
                  Args &... args);

  /// @brief Applies a user-defined function to every element
  /// in the specified range.
  ///
  /// Applies a user-defined function to all the elements in the specified range
  /// of positions.
  ///
  /// Typical usage:
  /// @code
  /// static void fn(size_t i, size_t& elem, size_t & incr) {
  ///   /* do something */
  /// }
  /// ...
  /// auto arrayPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  /// arrayPtr->ForEachInRange(0, kArraySize, fn, kInitValue);
  /// ...
  /// @endcode
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype must
  /// be:
  /// @code
  /// void(size_t, T&, Args& args);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in] first The first position of the range.
  /// @param[in] last The last position of the range.
  /// @param[in] function The function to apply.
  /// @param[in] args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void ForEachInRange(const size_t first, const size_t last,
                      ApplyFunT &&function, Args &... args);

  /// @brief Asynchronously applies a user-defined function to every element
  /// in the specified range.
  ///
  /// Asynchronously applies a user-defined function to all the elements
  /// in the specified range of positions.
  ///
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  ///
  /// Typical usage:
  /// @code
  /// void fn(shad::rt::Handle&, size_t i, size_t& elem, size_t& incr) {
  ///   /* do something */
  /// }
  /// ...
  /// auto arrayPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  /// shad::rt::Handle handle;
  /// arrayPtr->AsyncForEachInRange(handle, 0, kArraySize, fn, kInitValue);
  /// ...
  /// rt::waitForCompletion(handle);
  /// ...
  /// @endcode
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype must
  /// be:
  /// @code
  /// void(shad::rt::Handle&, size_t, T&, Args& args);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle Reference to the handle to be used to wait for
  /// completion.
  /// @param[in] first The first position of the range.
  /// @param[in] last The last position of the range.
  /// @param[in] function The function to apply.
  /// @param[in] args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncForEachInRange(rt::Handle &handle, const size_t first,
                           const size_t last, ApplyFunT &&function,
                           Args &... args);

  /// @brief Applies a user-defined function to every element.
  ///
  /// Applies a user-defined function to all the elements.
  ///
  /// Typical usage:
  /// @code
  /// void fn(size_t i, size_t& elem, size_t& incr) {
  ///   /* do something */
  /// }
  /// ...
  /// auto arrayPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  /// arrayPtr->ForEach(fn, someValue);
  /// @endcode
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype must
  /// be:
  /// @code
  /// void(size_t pos, T&, Args& args);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in] function The function to apply.
  /// @param[in] args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void ForEach(ApplyFunT &&function, Args &... args);

  /// @brief Asynchronously applies a user-defined function to every element.
  ///
  /// Asynchronously applies a user-defined function to all the elements.
  ///
  /// @warning Asynchronous operations are guaranteed to have completed only
  /// after calling the rt::waitForCompletion(rt::Handle &handle) method.
  ///
  /// Typical usage:
  /// @code
  /// void fn(shad::rt::Handle&, size_t i, size_t& elem, size_t& incr) {
  ///   /* do something */
  /// }
  /// ...
  /// auto arrayPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  /// rt::Handle handle;
  /// arrayPtr->AsyncForEach(handle, fn, someValue);
  /// ...
  /// shad::rt::waitForCompletion(handle);
  /// ...
  /// @endcode
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype must
  /// be:
  /// @code
  /// void(shad::rt::Handle&, size_t pos, T&, Args& args);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle The handle to be used to wait for completion.
  /// @param[in] function The function to apply.
  /// @param[in] args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncForEach(rt::Handle &handle, ApplyFunT &&function, Args &... args);

  // FIXME it should be protected
  void BufferEntryInsert(const std::tuple<size_t, T> entry) {
    data_[std::get<0>(entry)] = std::get<1>(entry);
  }

 protected:
  Array(ObjectID oid, size_t size, const T &initValue)
      : oid_(oid),
        size_(size),
        pivot_((size % rt::numLocalities() == 0)
                   ? rt::numLocalities()
                   : rt::numLocalities() - (size % rt::numLocalities())),
        data_(),
        dataDistribution_(),
        buffers_(oid) {
    rt::Locality pivot(pivot_);
    size_t start = 0;
    size_t chunkSize = size / rt::numLocalities();
    auto localities = rt::allLocalities();

    for (auto &locality : localities) {
      if (locality < pivot) {
        dataDistribution_.emplace_back(
            std::make_pair(start, start + chunkSize - 1));
      } else {
        dataDistribution_.emplace_back(
            std::make_pair(start, start + chunkSize));
        ++start;
      }

      start += chunkSize;
    }
    if (rt::thisLocality() < pivot)
      data_.resize(chunkSize, initValue);
    else
      data_.resize(chunkSize + 1, initValue);
  }

 private:
  ObjectID oid_;
  size_t size_;
  uint32_t pivot_;
  std::vector<T> data_;
  std::vector<std::pair<size_t, size_t>> dataDistribution_;
  BuffersVector buffers_;

  struct InsertAtArgs {
    ObjectID oid;
    size_t pos;
    T value;
  };

  struct AtArgs {
    ObjectID oid;
    size_t pos;
  };

  static void InsertAtFun(const InsertAtArgs &args) {
    ShadArrayPtr ptr = Array<T>::GetPtr(args.oid);
    ptr->data_[args.pos] = args.value;
  }

  static void RangedInsertAtFun(const uint8_t *args, const uint32_t size) {
    uint8_t *argsPtr = const_cast<uint8_t *>(args);
    ObjectID &oid = *reinterpret_cast<ObjectID *>(argsPtr);
    argsPtr += sizeof(oid);
    size_t pos = *reinterpret_cast<size_t *>(argsPtr);
    argsPtr += sizeof(size_t);
    size_t chunkSize = *reinterpret_cast<size_t *>(argsPtr);
    argsPtr += sizeof(size_t);
    ShadArrayPtr ptr = Array<T>::GetPtr(oid);
    memcpy(&(ptr->data_[pos]), argsPtr, chunkSize * sizeof(T));
  }

  static void AsyncRangedInsertAtFun(rt::Handle &, const uint8_t *args,
                                     const uint32_t size) {
    uint8_t *argsPtr = const_cast<uint8_t *>(args);
    ObjectID &oid = *reinterpret_cast<ObjectID *>(argsPtr);
    argsPtr += sizeof(oid);
    size_t pos = *reinterpret_cast<size_t *>(argsPtr);
    argsPtr += sizeof(size_t);
    size_t chunkSize = *reinterpret_cast<size_t *>(argsPtr);
    argsPtr += sizeof(size_t);
    ShadArrayPtr ptr = Array<T>::GetPtr(oid);
    memcpy(&(ptr->data_[pos]), argsPtr, chunkSize * sizeof(T));
  }

  static void AsyncInsertAtFun(shad::rt::Handle &, const InsertAtArgs &args) {
    ShadArrayPtr ptr = Array<T>::GetPtr(args.oid);
    ptr->data_[args.pos] = args.value;
  }

  static void AtFun(const AtArgs &args, T *result) {
    ShadArrayPtr ptr = Array<T>::GetPtr(args.oid);
    *result = ptr->data_[args.pos];
  }

  static void AsyncAtFun(rt::Handle &handle, const AtArgs &args, T *result) {
    ShadArrayPtr ptr = Array<T>::GetPtr(args.oid);
    *result = ptr->data_[args.pos];
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void CallApplyFun(ObjectID &oid, size_t pos, size_t loffset,
                           ApplyFunT function, std::tuple<Args...> &args,
                           std::index_sequence<is...>) {
    // Get a local instance on the remote node.
    auto arrayPtr = Array<T>::GetPtr(oid);
    T &element = arrayPtr->data_[loffset];
    function(pos, element, std::get<is>(args)...);
  }

  template <typename Tuple, typename... Args>
  static void ApplyFunWrapper(const Tuple &args) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<4>(args))>::type>::value;

    Tuple &tuple = const_cast<Tuple &>(args);

    CallApplyFun(std::get<0>(tuple), std::get<1>(tuple), std::get<2>(tuple),
                 std::get<3>(tuple), std::get<4>(tuple),
                 std::make_index_sequence<Size>{});
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void AsyncCallApplyFun(rt::Handle &handle, ObjectID &oid, size_t pos,
                                size_t loffset, ApplyFunT function,
                                std::tuple<Args...> &args,
                                std::index_sequence<is...>) {
    // Get a local instance on the remote node.
    auto arrayPtr = Array<T>::GetPtr(oid);
    T &element = arrayPtr->data_[loffset];
    function(handle, pos, element, std::get<is>(args)...);
  }

  template <typename Tuple, typename... Args>
  static void AsyncApplyFunWrapper(rt::Handle &handle, const Tuple &args) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<4>(args))>::type>::value;

    Tuple &tuple = const_cast<Tuple &>(args);

    AsyncCallApplyFun(handle, std::get<0>(tuple), std::get<1>(tuple),
                      std::get<2>(tuple), std::get<3>(tuple),
                      std::get<4>(tuple), std::make_index_sequence<Size>{});
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void CallForEachInRangeFun(size_t i, ObjectID &oid, size_t pos,
                                    size_t lpos, ApplyFunT function,
                                    std::tuple<Args...> &args,
                                    std::index_sequence<is...>) {
    // Get a local instance on the remote node.
    auto arrayPtr = Array<T>::GetPtr(oid);
    T &element = arrayPtr->data_[i + lpos];
    function(i + pos, element, std::get<is>(args)...);
  }

  template <typename Tuple, typename... Args>
  static void ForEachInRangeFunWrapper(const Tuple &args, size_t i) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<4>(args))>::type>::value;

    Tuple &tuple = const_cast<Tuple &>(args);

    CallForEachInRangeFun(i, std::get<0>(tuple), std::get<1>(tuple),
                          std::get<2>(tuple), std::get<3>(tuple),
                          std::get<4>(tuple), std::make_index_sequence<Size>{});
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void AsyncCallForEachInRangeFun(rt::Handle &handle, size_t i,
                                         ObjectID &oid, size_t pos, size_t lpos,
                                         ApplyFunT function,
                                         std::tuple<Args...> &args,
                                         std::index_sequence<is...>) {
    // Get a local instance on the remote node.
    auto arrayPtr = Array<T>::GetPtr(oid);
    T &element = arrayPtr->data_[i + lpos];
    function(handle, i + pos, element, std::get<is>(args)...);
  }

  template <typename Tuple, typename... Args>
  static void AsyncForEachInRangeFunWrapper(rt::Handle &handle,
                                            const Tuple &args, size_t i) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<4>(args))>::type>::value;

    Tuple &tuple = const_cast<Tuple &>(args);

    AsyncCallForEachInRangeFun(handle, i, std::get<0>(tuple),
                               std::get<1>(tuple), std::get<2>(tuple),
                               std::get<3>(tuple), std::get<4>(tuple),
                               std::make_index_sequence<Size>{});
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void AsyncCallForEachFun(rt::Handle &handle, const size_t i,
                                  T *arrayPtr, ApplyFunT function, size_t pos,
                                  std::tuple<Args...> &args,
                                  std::index_sequence<is...>) {
    function(handle, pos + i, arrayPtr[i], std::get<is>(args)...);
  }

  template <typename Tuple, typename... Args>
  static void AsyncForEachFunWrapper(rt::Handle &handle, const Tuple &args,
                                     size_t i) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<3>(args))>::type>::value;

    Tuple &tuple = const_cast<Tuple &>(args);

    AsyncCallForEachFun(handle, i, std::get<0>(tuple), std::get<1>(tuple),
                        std::get<2>(tuple), std::get<3>(tuple),
                        std::make_index_sequence<Size>{});
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void CallForEachFun(size_t i, T *arrayPtr, ApplyFunT function,
                             size_t pos, std::tuple<Args...> &args,
                             std::index_sequence<is...>) {
    function(i + pos, arrayPtr[i], std::get<is>(args)...);
  }

  template <typename Tuple, typename... Args>
  static void ForEachFunWrapper(const Tuple &args, size_t i) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<3>(args))>::type>::value;

    Tuple &tuple = const_cast<Tuple &>(args);

    CallForEachFun(i, std::get<0>(tuple), std::get<1>(tuple),
                   std::get<2>(tuple), std::get<3>(tuple),
                   std::make_index_sequence<Size>{});
  }
};

static std::pair<rt::Locality, size_t> getTargetLocalityFromTargePosition(
    const std::vector<std::pair<size_t, size_t>> &dataDistribution,
    size_t position) {
  auto itr = std::lower_bound(
      dataDistribution.begin(), dataDistribution.end(), position,
      [](const std::pair<size_t, size_t> &lhs, const size_t position) -> bool {
        return lhs.second < position;
      });

  rt::Locality dest(std::distance(dataDistribution.begin(), itr));
  size_t off = position - itr->first;
  return std::make_pair(dest, off);
}

template <typename T>
void Array<T>::InsertAt(const size_t pos, const T &value) {
  auto target = getTargetLocalityFromTargePosition(dataDistribution_, pos);

  if (target.first == rt::thisLocality()) {
    data_[target.second] = value;
  } else {
    InsertAtArgs args = {oid_, target.second, value};
    rt::executeAt(target.first, InsertAtFun, args);
  }
}

template <typename T>
void Array<T>::AsyncInsertAt(rt::Handle &handle, const size_t pos,
                             const T &value) {
  auto target = getTargetLocalityFromTargePosition(dataDistribution_, pos);
  if (target.first == rt::thisLocality()) {
    data_[target.second] = value;
  } else {
    InsertAtArgs args = {oid_, target.second, value};
    rt::asyncExecuteAt(handle, target.first, AsyncInsertAtFun, args);
  }
}

template <typename T>
void Array<T>::BufferedInsertAt(const size_t pos, const T &value) {
  auto target = getTargetLocalityFromTargePosition(dataDistribution_, pos);
  if (target.first == rt::thisLocality()) {
    data_[target.second] = value;
  } else {
    buffers_.Insert(std::tuple<size_t, T>(target.second, value), target.first);
  }
}

template <typename T>
void Array<T>::InsertAt(const size_t pos, const T *values,
                        const size_t numValues) {
  size_t tgtPos = 0, firstPos = pos;
  rt::Locality tgtLoc;
  size_t remainingValues = numValues;
  size_t chunkSize = 0;
  T *valuesPtr = const_cast<T *>(values);

  while ((remainingValues > 0) &&
         (firstPos < pivot_ * (size_ / rt::numLocalities()))) {
    tgtLoc = rt::Locality(firstPos / (size_ / rt::numLocalities()));
    tgtPos = firstPos % (size_ / rt::numLocalities());
    chunkSize =
        std::min((size_ / rt::numLocalities() - tgtPos), remainingValues);
    if (tgtLoc == rt::thisLocality()) {
      memcpy(&data_[tgtPos], valuesPtr, chunkSize * sizeof(T));
    } else {
      chunkSize = constants::min(chunkSize, kMaxChunkSize);
      size_t argsSize =
          sizeof(oid_) + sizeof(size_t) * 2 + sizeof(T) * chunkSize;
      std::shared_ptr<uint8_t> args(new uint8_t[argsSize],
                                    std::default_delete<uint8_t[]>());
      uint8_t *argsPtr = args.get();
      memcpy(argsPtr, &oid_, sizeof(oid_));
      argsPtr += sizeof(oid_);
      memcpy(argsPtr, &tgtPos, sizeof(size_t));
      argsPtr += sizeof(size_t);
      memcpy(argsPtr, &chunkSize, sizeof(size_t));
      argsPtr += sizeof(size_t);
      memcpy(argsPtr, valuesPtr, sizeof(T) * chunkSize);

      rt::executeAt(tgtLoc, RangedInsertAtFun, args, argsSize);
    }

    firstPos += chunkSize;
    remainingValues -= chunkSize;
    valuesPtr += chunkSize;
  }
  while (remainingValues > 0) {
    size_t newPos = firstPos - (pivot_ * (size_ / rt::numLocalities()));
    tgtLoc =
        rt::Locality(pivot_ + newPos / ((size_ / rt::numLocalities() + 1)));
    tgtPos = newPos % ((size_ / rt::numLocalities() + 1));
    chunkSize =
        std::min((size_ / rt::numLocalities() + 1 - tgtPos), remainingValues);
    if (tgtLoc == rt::thisLocality()) {
      memcpy(&data_[tgtPos], valuesPtr, chunkSize * sizeof(T));
    } else {
      chunkSize = constants::min(chunkSize, kMaxChunkSize);
      size_t argsSize =
          sizeof(oid_) + sizeof(size_t) * 2 + sizeof(T) * chunkSize;
      // FIXME(SHAD-125)
      std::shared_ptr<uint8_t> args(new uint8_t[argsSize],
                                    std::default_delete<uint8_t[]>());
      uint8_t *argsPtr = args.get();
      memcpy(argsPtr, &oid_, sizeof(oid_));
      argsPtr += sizeof(oid_);
      memcpy(argsPtr, &tgtPos, sizeof(size_t));
      argsPtr += sizeof(size_t);
      memcpy(argsPtr, &chunkSize, sizeof(size_t));
      argsPtr += sizeof(size_t);
      memcpy(argsPtr, valuesPtr, sizeof(T) * chunkSize);

      rt::executeAt(tgtLoc, RangedInsertAtFun, args, argsSize);
    }

    firstPos += chunkSize;
    remainingValues -= chunkSize;
    valuesPtr += chunkSize;
  }
}

template <typename T>
void Array<T>::AsyncInsertAt(rt::Handle &handle, const size_t pos,
                             const T *values, const size_t numValues) {
  size_t tgtPos = 0, firstPos = pos;
  rt::Locality tgtLoc;
  size_t remainingValues = numValues;
  size_t chunkSize = 0;
  T *valuesPtr = const_cast<T *>(values);

  while (remainingValues > 0) {
    if (firstPos < pivot_ * (size_ / rt::numLocalities())) {
      tgtLoc = rt::Locality(firstPos / (size_ / rt::numLocalities()));
      tgtPos = firstPos % (size_ / rt::numLocalities());
      chunkSize =
          std::min((size_ / rt::numLocalities() - tgtPos), remainingValues);
    } else {
      size_t newPos = firstPos - (pivot_ * (size_ / rt::numLocalities()));
      tgtLoc =
          rt::Locality(pivot_ + newPos / ((size_ / rt::numLocalities() + 1)));
      tgtPos = newPos % ((size_ / rt::numLocalities() + 1));
      chunkSize =
          std::min((size_ / rt::numLocalities() + 1 - tgtPos), remainingValues);
    }
    if (tgtLoc == rt::thisLocality()) {
      memcpy(&data_[tgtPos], valuesPtr, chunkSize * sizeof(T));
    } else {
      chunkSize = constants::min(chunkSize, kMaxChunkSize);
      size_t argsSize =
          sizeof(oid_) + sizeof(size_t) * 2 + sizeof(T) * chunkSize;
      // FIXME(SHAD-125)
      std::shared_ptr<uint8_t> args(new uint8_t[argsSize],
                                    std::default_delete<uint8_t[]>());
      ;
      uint8_t *argsPtr = args.get();
      memcpy(argsPtr, &oid_, sizeof(oid_));
      argsPtr += sizeof(oid_);
      memcpy(argsPtr, &tgtPos, sizeof(size_t));
      argsPtr += sizeof(size_t);
      memcpy(argsPtr, &chunkSize, sizeof(size_t));
      argsPtr += sizeof(size_t);
      memcpy(argsPtr, valuesPtr, sizeof(T) * chunkSize);

      rt::asyncExecuteAt(handle, tgtLoc, AsyncRangedInsertAtFun, args,
                         argsSize);
    }

    firstPos += chunkSize;
    remainingValues -= chunkSize;
    valuesPtr += chunkSize;
  }
}

template <typename T>
void Array<T>::BufferedAsyncInsertAt(rt::Handle &handle, const size_t pos,
                                     const T &value) {
  auto target = getTargetLocalityFromTargePosition(dataDistribution_, pos);
  if (target.first == rt::thisLocality()) {
    data_[target.second] = value;
  } else {
    buffers_.AsyncInsert(handle, std::tuple<size_t, T>(target.second, value),
                         target.first);
  }
}

template <typename T>
T Array<T>::At(const size_t pos) {
  if (rt::numLocalities() == 1) {
    return data_[pos];
  }
  auto target = getTargetLocalityFromTargePosition(dataDistribution_, pos);
  T retValue;
  if (target.first == rt::thisLocality()) {
    retValue = data_[target.second];
  } else {
    AtArgs args{oid_, target.second};
    rt::executeAtWithRet(target.first, AtFun, args, &retValue);
  }
  return retValue;
}

template <typename T>
void Array<T>::AsyncAt(rt::Handle &handle, const size_t pos, T *result) {
  auto target = getTargetLocalityFromTargePosition(dataDistribution_, pos);
  if (target.first == rt::thisLocality()) {
    *result = data_[target.second];
  } else {
    AtArgs args = {oid_, target.second};
    rt::asyncExecuteAtWithRet(handle, target.first, AsyncAtFun, args, result);
  }
}

template <typename T>
template <typename ApplyFunT, typename... Args>
void Array<T>::Apply(const size_t pos, ApplyFunT &&function, Args &... args) {
  auto target = getTargetLocalityFromTargePosition(dataDistribution_, pos);
  if (target.first == rt::thisLocality()) {
    function(pos, data_[target.second], args...);
    return;
  }
  using FunctionTy = void (*)(size_t, T &, Args & ...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using ArgsTuple =
      std::tuple<ObjectID, size_t, size_t, FunctionTy, std::tuple<Args...>>;
  ArgsTuple argsTuple{oid_, pos, target.second, fn,
                      std::tuple<Args...>(args...)};

  rt::executeAt(target.first, ApplyFunWrapper<ArgsTuple, Args...>, argsTuple);
}

template <typename T>
template <typename ApplyFunT, typename... Args>
void Array<T>::AsyncApply(rt::Handle &handle, const size_t pos,
                          ApplyFunT &&function, Args &... args) {
  auto target = getTargetLocalityFromTargePosition(dataDistribution_, pos);

  using FunctionTy = void (*)(rt::Handle &, size_t, T &, Args & ...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using ArgsTuple =
      std::tuple<ObjectID, size_t, size_t, FunctionTy, std::tuple<Args...>>;
  ArgsTuple argsTuple{oid_, pos, target.second, fn,
                      std::tuple<Args...>(args...)};

  rt::asyncExecuteAt(handle, target.first,
                     AsyncApplyFunWrapper<ArgsTuple, Args...>, argsTuple);
}

template <typename T>
template <typename ApplyFunT, typename... Args>
void Array<T>::ForEachInRange(const size_t first, const size_t last,
                              ApplyFunT &&function, Args &... args) {
  using FunctionTy = void (*)(size_t, T &, Args & ...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using ArgsTuple =
      std::tuple<ObjectID, size_t, size_t, FunctionTy, std::tuple<Args...>>;

  size_t tgtPos = 0, firstPos = first;
  rt::Locality tgtLoc;
  size_t remainingValues = last - first;
  size_t chunkSize = 0;

  ArgsTuple argsTuple{oid_, firstPos, tgtPos, fn, std::tuple<Args...>(args...)};
  while (remainingValues > 0) {
    if (firstPos < pivot_ * (size_ / rt::numLocalities())) {
      tgtLoc = rt::Locality(firstPos / (size_ / rt::numLocalities()));
      tgtPos = firstPos % (size_ / rt::numLocalities());
      chunkSize =
          std::min((size_ / rt::numLocalities() - tgtPos), remainingValues);
    } else {
      size_t newPos = firstPos - (pivot_ * (size_ / rt::numLocalities()));
      tgtLoc =
          rt::Locality(pivot_ + newPos / ((size_ / rt::numLocalities() + 1)));
      tgtPos = newPos % ((size_ / rt::numLocalities() + 1));
      chunkSize =
          std::min((size_ / rt::numLocalities() + 1 - tgtPos), remainingValues);
    }

    std::get<1>(argsTuple) = firstPos;
    std::get<2>(argsTuple) = tgtPos;

    rt::forEachAt(tgtLoc, ForEachInRangeFunWrapper<ArgsTuple, Args...>,
                  argsTuple, chunkSize);

    firstPos += chunkSize;
    remainingValues -= chunkSize;
  }
}

template <typename T>
template <typename ApplyFunT, typename... Args>
void Array<T>::AsyncForEachInRange(rt::Handle &handle, const size_t first,
                                   const size_t last, ApplyFunT &&function,
                                   Args &... args) {
  using FunctionTy = void (*)(rt::Handle &, size_t, T &, Args & ...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using ArgsTuple =
      std::tuple<ObjectID, size_t, size_t, FunctionTy, std::tuple<Args...>>;

  size_t tgtPos = 0, firstPos = first;
  rt::Locality tgtLoc;
  size_t remainingValues = last - first;
  size_t chunkSize = 0;
  // first it
  ArgsTuple argsTuple{oid_, firstPos, tgtPos, fn, std::tuple<Args...>(args...)};

  while (remainingValues > 0) {
    if (firstPos < pivot_ * (size_ / rt::numLocalities())) {
      tgtLoc = rt::Locality(firstPos / (size_ / rt::numLocalities()));
      tgtPos = firstPos % (size_ / rt::numLocalities());
      chunkSize =
          std::min((size_ / rt::numLocalities() - tgtPos), remainingValues);
    } else {
      size_t newPos = firstPos - (pivot_ * (size_ / rt::numLocalities()));
      tgtLoc =
          rt::Locality(pivot_ + newPos / ((size_ / rt::numLocalities() + 1)));
      tgtPos = newPos % ((size_ / rt::numLocalities() + 1));
      chunkSize =
          std::min((size_ / rt::numLocalities() + 1 - tgtPos), remainingValues);
    }

    std::get<1>(argsTuple) = firstPos;
    std::get<2>(argsTuple) = tgtPos;

    rt::asyncForEachAt(handle, tgtLoc,
                       AsyncForEachInRangeFunWrapper<ArgsTuple, Args...>,
                       argsTuple, chunkSize);

    firstPos += chunkSize;
    remainingValues -= chunkSize;
  }
}

template <typename T>
template <typename ApplyFunT, typename... Args>
void Array<T>::AsyncForEach(rt::Handle &handle, ApplyFunT &&function,
                            Args &... args) {
  using FunctionTy = void (*)(rt::Handle &, size_t, T &, Args & ...);
  FunctionTy fn = std::forward<decltype(function)>(function);

  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  using ArgsTuple = std::tuple<T *, FunctionTy, size_t, std::tuple<Args...>>;

  feArgs arguments{oid_, fn, std::tuple<Args...>(args...)};

  auto feLambda = [](rt::Handle &handle, const feArgs &args) {
    auto arrayPtr = Array<T>::GetPtr(std::get<0>(args));

    size_t currentLocality = static_cast<uint32_t>(rt::thisLocality());
    ArgsTuple argsTuple(arrayPtr->data_.data(), std::get<1>(args),
                        arrayPtr->dataDistribution_[currentLocality].first,
                        std::get<2>(args));

    rt::asyncForEachAt(handle, rt::thisLocality(),
                       AsyncForEachFunWrapper<ArgsTuple, Args...>, argsTuple,
                       arrayPtr->data_.size());
  };
  rt::asyncExecuteOnAll(handle, feLambda, arguments);
}

template <typename T>
template <typename ApplyFunT, typename... Args>
void Array<T>::ForEach(ApplyFunT &&function, Args &... args) {
  using FunctionTy = void (*)(size_t, T &, Args & ...);
  FunctionTy fn = std::forward<decltype(function)>(function);

  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  using ArgsTuple = std::tuple<T *, FunctionTy, size_t, std::tuple<Args...>>;

  feArgs arguments{oid_, fn, std::tuple<Args...>(args...)};

  auto feLambda = [](const feArgs &args) {
    auto arrayPtr = Array<T>::GetPtr(std::get<0>(args));
    size_t currentLocality = static_cast<uint32_t>(rt::thisLocality());
    ArgsTuple argsTuple(arrayPtr->data_.data(), std::get<1>(args),
                        arrayPtr->dataDistribution_[currentLocality].first,
                        std::get<2>(args));

    rt::forEachAt(rt::thisLocality(), ForEachFunWrapper<ArgsTuple, Args...>,
                  argsTuple, arrayPtr->data_.size());
  };
  rt::executeOnAll(feLambda, arguments);
}

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

          std::copy(Other->chunk_,
                    Other->chunk_ + (N / rt::numLocalities() + 1),
                    This->churk_);
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
    return iterator{rt::Locality(0), 0, oid_};
  }

  /// @brief The iterator to the beginning of the sequence.
  /// @return a ::const_iterator to the beginning of the sequence.
  constexpr const_iterator begin() const noexcept { return cbegin(); }

  /// @brief The iterator to the end of the sequence.
  /// @return an ::iterator to the end of the sequence.
  constexpr iterator end() noexcept {
    return iterator{rt::Locality(rt::numLocalities() - 1), N % chunk_size(),
                    oid_};
  }

  /// @brief The iterator to the end of the sequence.
  /// @return a ::const_iterator to the end of the sequence.
  constexpr const_iterator end() const noexcept { return cend(); }

  /// @brief The iterator to the beginning of the sequence.
  /// @return a ::const_iterator to the beginning of the sequence.
  constexpr const_iterator cbegin() const noexcept {
    return const_iterator{rt::Locality(0), 0, oid_};
  }

  /// @brief The iterator to the end of the sequence.
  /// @return a ::const_iterator to the end of the sequence.
  constexpr const_iterator cend() const noexcept {
    return const_iterator{rt::Locality(rt::numLocalities() - 1),
                          N % chunk_size(), oid_};
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
    rt::Locality dest{static_cast<uint32_t>(n / chunk_size())};
    std::size_t offset{static_cast<uint32_t>(n % chunk_size())};
    return reference(dest, offset, oid_);
  }

  /// @brief Unchecked element access operator.
  /// @return a ::const_reference to the n-th element in the array.
  constexpr const_reference operator[](size_type n) const {
    rt::Locality dest{static_cast<uint32_t>(n / chunk_size())};
    std::size_t offset{static_cast<uint32_t>(n % chunk_size())};
    return const_reference(dest, offset, oid_);
  }

  /// @brief Checked element access operator.
  /// @return a ::reference to the n-th element in the array.
  constexpr reference at(size_type n) {
    if (n > size()) throw std::out_of_range("Array::at()");
    return operator[](n);
  }

  /// @brief Checked element access operator.
  /// @return a ::const_reference to the n-th element in the array.
  constexpr const_reference at(size_type n) const {
    if (n > size()) throw std::out_of_range("Array::at() const");
    return operator[](n);
  }

  /// @brief the first element in the array.
  /// @return a ::reference to the element in position 0.
  constexpr reference front() { return operator[](0); }
  /// @brief the first element in the array.
  /// @return a ::const_reference to the element in position 0.
  constexpr const_reference front() const { return operator[](0); }

  /// @brief the last element in the array.
  /// @return a ::reference to the element in position N - 1.
  constexpr reference back() { return operator[](N - 1); }
  /// @brief the last element in the array.
  /// @return a ::const_reference to the element in position N - 1.
  constexpr const_reference back() const { return operator[](N - 1); }
  /// @}

  /// @brief DataStructure identifier getter.
  ///
  /// Returns the global object identifier associated to a DataStructure
  /// instance.
  ///
  /// @warning It must be implemented in the inheriting DataStructure.
  ObjectID GetGlobalID() const { return oid_; }

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
    return N / rt::numLocalities() + 1;
  }

  /// @brief Constructor.
  explicit array(ObjectID oid) : chunk_{new T[chunk_size()]}, oid_{oid} {}

 private:
  std::unique_ptr<T[]> chunk_;
  ObjectID oid_;
};

template <typename T, std::size_t N>
template <typename U>
class array<T, N>::BaseArrayRef {
 public:
  using value_type = U;
  using ObjectID = typename array<T, N>::ObjectID;

  std::size_t pos_;
  rt::Locality loc_;
  ObjectID oid_;

  BaseArrayRef(rt::Locality l, std::size_t p, ObjectID oid)
      : pos_(p), loc_(l), oid_(oid) {}

  value_type get() const {
    value_type result;
    rt::executeAtWithRet(
        loc_,
        [](const std::pair<ObjectID, std::size_t> &args, T *result) {
          auto This = array<T, N>::GetPtr(std::get<0>(args));
          *result = This->chunk_[std::get<1>(args)];
        },
        std::make_pair(oid_, pos_), &result);
    return result;
  }
};

template <typename T, std::size_t N>
template <typename U>
class array<T, N>::ArrayRef : public array<T, N>::template BaseArrayRef<U> {
 public:
  using value_type = U;
  using ObjectID = typename array<T, N>::ObjectID;

  ArrayRef(rt::Locality l, std::size_t p, ObjectID oid)
      : array<T, N>::template BaseArrayRef<U>(l, p, oid) {}

  operator value_type() { return array<T, N>::template BaseArrayRef<U>::get(); }

  bool operator==(const value_type &v) const {
    return array<T, N>::template BaseArrayRef<U>::get() == v;
  }

  ArrayRef &operator=(const T &v) {
    rt::executeAt(this->loc_,
                  [](const std::tuple<ObjectID, std::size_t, T> &args) {
                    auto This = array<T, N>::GetPtr(std::get<0>(args));
                    This->chunk_[std::get<1>(args)] = std::get<2>(args);
                  },
                  std::make_tuple(this->oid_, this->pos_, v));
    return *this;
  }
};

template <typename T, std::size_t N>
template <typename U>
class array<T, N>::ArrayRef<const U>
    : public array<T, N>::template BaseArrayRef<U> {
 public:
  using value_type = const U;
  using ObjectID = typename array<T, N>::ObjectID;

  ArrayRef(rt::Locality l, std::size_t p, ObjectID oid)
      : array<T, N>::template BaseArrayRef<U>(l, p, oid) {}

  operator value_type() { return array<T, N>::template BaseArrayRef<U>::get(); }
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
class array<T, N>::array_iterator {
 public:
  using reference = typename array<T, N>::template ArrayRef<U>;
  using pointer = typename array<T, N>::pointer;
  using difference_type = std::ptrdiff_t;
  using value_type = typename array<T, N>::value_type;
  using iterator_category = std::random_access_iterator_tag;

  array_iterator() = default;
  array_iterator(rt::Locality &&l, std::size_t offset, ObjectID oid)
      : locality_(l), offset_(offset), oid_(oid) {}

  bool operator==(const array_iterator &O) const {
    return locality_ == O.locality_ && oid_ == O.oid_ && offset_ == O.offset_;
  }

  bool operator!=(const array_iterator &O) const { return !(*this == O); }

  reference operator*() { return reference(locality_, offset_, oid_); }

  array_iterator &operator++() {
    ++offset_;
    if (offset_ == chunk_size()) {
      ++locality_;
      offset_ = 0;
    }
    return *this;
  }

  array_iterator operator++(int) {
    array_iterator tmp = *this;
    operator++();
    return tmp;
  }

  array_iterator &operator--() { return *this; }
  array_iterator operator--(int) {
    array_iterator tmp = *this;
    operator--();
    return tmp;
  }

  array_iterator &operator+=(std::size_t n) {
    if (n == 0) return *this;

    locality_ += (offset_ + n) / chunk_size();
    offset_ = (offset_ + n) % chunk_size();

    return *this;
  }

  array_iterator &operator-=(std::size_t n) {
    if (n == 0) return *this;

    if (n > offset_) {
      locality_ -= ((n - offset_) / chunk_size()) + 1;
      std::size_t delta = (n % chunk_size());
      offset_ =
          delta > offset_ ? chunk_size() - (delta - offset_) : offset_ - delta;
    } else {
      offset_ -= n;
    }

    return *this;
  }

  array_iterator operator+(std::size_t n) {
    if (n == 0) return *this;

    return array_iterator{rt::Locality((offset_ + n) / chunk_size()),
                          offset_ + (n % chunk_size()), oid_};
  }

  array_iterator operator-(std::size_t n) {
    if (n == 0) return *this;

    array_iterator tmp = *this;
    if (n > offset_) {
      tmp.locality_ -= ((n - offset_) / chunk_size()) + 1;
      std::size_t delta = (n % chunk_size());
      tmp.offset_ =
          delta > offset_ ? chunk_size() - (delta - offset_) : offset_ - delta;
    } else {
      tmp.offset_ -= n;
    }

    return tmp;
  }

  difference_type operator-(const array_iterator &O) const {
    if (oid_ != O.oid_)
      return std::numeric_limits<difference_type>::min();

    difference_type distance = (static_cast<uint32_t>(this->locality_) -
                                static_cast<uint32_t>(O.locality_)) *
                               chunk_size();
    distance -= this->offset_;
    distance += O.offset_;

    return distance;
  }

  bool operator<(const array_iterator &O) const {
    return oid_ == O.oid_ && locality_ <= O.locality_ && offset_ < O.offset_;
  }

  bool operator>(const array_iterator &O) const {
    return oid_ == O.oid_ && locality_ >= O.locality_ && offset_ > O.offset_;
  }

  bool operator<=(const array_iterator &O) const {
    return oid_ == O.oid_ && !(*this > O);
  }

  bool operator>=(const array_iterator &O) const {
    return oid_ == O.oid_ && !(*this < O);
  }

 private:
  rt::Locality locality_{static_cast<uint32_t>(-1)};
  ObjectID oid_{static_cast<std::size_t>(-1)};
  std::size_t offset_{static_cast<std::size_t>(-1)};
};

}  // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_ARRAY_H_
