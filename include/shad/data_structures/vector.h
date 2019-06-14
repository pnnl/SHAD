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

#ifndef INCLUDE_SHAD_DATA_STRUCTURES_VECTOR_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_VECTOR_H_

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

#include "shad/data_structures/abstract_data_structure.h"
#include "shad/data_structures/buffer.h"
#include "shad/runtime/runtime.h"

namespace shad {

/// @brief The Vector data Structure.
///
/// ::shad::Vector is a distributed container that can grow dynamically.
///
/// @warning The contained type must be trivially copiable.
///
/// @tparam T The type of the entries stored in a ::shad::Vector.
/// @tparam Allocator The allocator to be used.
template <typename T, typename Allocator = std::allocator<T>>
class Vector : public AbstractDataStructure<Vector<T, Allocator>> {
  template <typename ValueType>
  class Iterator;

 public:
  /// @brief The type of the allocator.
  using allocator_type = Allocator;
  /// @brief The type of the elements stored in the ::shad::Vector.
  using value_type = T;
  /// @brief A signed integral type used the difference between iterators.
  using difference_type = typename allocator_type::difference_type;
  /// @brief An unsigned integral type that can represent any non-negative value
  /// of difference_type.
  using size_type = typename allocator_type::size_type;
  /// @brief A random access iterator to shad::Vector::value_type
  using iterator = Iterator<T>;
  /// @brief A random access iterator to const shad::Vector::value_type
  using const_iterator = Iterator<const T>;
  /// @brief The type of the unique identifier for the Vector.
  using ObjectID =
      typename AbstractDataStructure<Vector<T, Allocator>>::ObjectID;

  static_assert(
      (std::is_same<typename allocator_type::value_type, value_type>::value),
      "Allocator::value_type must be the same as value_type");

  /// @name Capacity
  /// @{

  /// @brief Returns the number of element stored in the shad::Vector.
  /// @return the number of element in the container.
  size_type Size() const noexcept;

  /// @brief Returns the maximum number of elements that shad::Vector can hold.
  ///
  /// This methods returns the maximum potential size of the container.
  /// However, there is no guarantee that a shad::Vector will be able to reach
  /// that size (i.e., the system might fail to allocate memory before that size
  /// is reached).
  size_type MaxSize() const noexcept;

  /// @brief Return the size of the allocated storage capacity.
  ///
  /// The capacity of a shad::Vector represent the maximum number of element
  /// that the currently allocated storage can hold without the need to expand.
  /// Notice that this does not imply a limit on the Vector size.  In fact, when
  /// the current capacity is exhausted and more is needed, the container will
  /// expand its capacity allocating more memory.
  ///
  /// @return The size of the currently allocated storage capacity.
  size_type Capacity() const noexcept;

  /// @brief Returns whether the shad::Vector is empty.
  /// @return true if the container Size is 0, false otherwise.
  bool Empty() const noexcept;

  /// @brief Request that the Capacity is at least n.
  ///
  /// Request that the Capacity is enough to store n elements.  If n is greater
  /// than the current Capacity, the function will cause the container to expand
  /// increasing its Capacity to n.  In all the other cases, the function does
  /// not affect the Capacity of the shad::Vector.
  ///
  /// @warning Reserve is not thread safe, so don't try to reserve from multiple
  /// concurrent tasks.
  ///
  /// @param[in] n The requested minimum Capacity for the shad::Vector.
  void Reserve(size_type n);

  /// @brief Resize the container so that it contains n elements.
  ///
  /// Resize the container so that it contains n elements.  If n is smaller than
  /// the container Size, the content is reduced to the first n elements.  If n
  /// is greater than the current Size, the container is expanded by inserting
  /// at the end as many elements as needed to reach a size of n.
  ///
  /// If n is greater than the current capacity, more data blocks will be
  /// allocated to extend the capacity to at least n elements.
  ///
  /// @param[in] n The new container size, expressend in number of elements.
  void Resize(size_type n);

  /// @}

  /// @name Element Access
  /// @{

  /// @brief Return the element in position n in the shad::Vector.
  ///
  /// This methods check whether the requested position is within the bounds of
  /// the vector.  If the position is out of bounds, the method will throw an
  /// std::out_of_range exception.
  ///
  /// @param[in] n The position of an element in the container.
  /// @return the element in position n.
  value_type At(size_type n) const;

  /// @brief Return the element in position n in the shad::Vector.
  ///
  /// This methods is similar to At but it is not bound checked.
  ///
  /// @param[in] n The position of an element in the container.
  /// @return the element in position n.
  value_type operator[](size_type n) const;

  /// @brief Return the first element in the shad::Vector.
  ///
  /// @warning Calling this method on an empty container causes undefined
  /// behavior.
  ///
  /// @return The first element in the shad::Vector.
  value_type Front() const;

  /// @brief Return the last element in the shad::Vector.
  ///
  /// @warning Calling this method on an empty container causes undefined
  /// behavior.
  ///
  /// @return The last element in the shad::Vector.
  value_type Back() const;

  /// @brief Return the element in position n in the shad::Vector.
  ///
  /// This methods check whether the requested position is within the bounds of
  /// the vector.  If the position is out of bounds, the method will throw an
  /// std::out_of_range exception.
  ///
  /// @param[in,out] handle The handle that will be used for the spawned tasks.
  /// @param[in] n The position of an element in the container.
  /// @param[out] result The address where to store the result.
  void AsyncAt(rt::Handle &handle, size_type n, T *result) const;

  /// @}

  /// @name Modifiers
  /// @{

  /// @brief Removes all the elements from the shad::Vector.
  ///
  /// Removes all the elements from the container (destroying them).
  /// It leaves the container with a size and a capacity of 0.
  void Clear() noexcept;

  /// @brief Adds an element at the end of the shad::Vector.
  void PushBack(const T &value);

  /// @brief Write a value at the specified position.
  ///
  /// This method overwrite the element at the specified position.
  ///
  /// Typical usage:
  /// @code
  /// auto vectorPtr = shad::Vector<size_t>::Create(kVectorSize);
  /// for (size_t i = 0; i < kVectorSize; i++) {
  ///   vectorPtr->InsertAt(i, i+1);
  /// }
  /// @endcode
  ///
  /// @warning The semantic of this method is different from the insert method
  /// of the std::vector.  The shad::Vector will overwrite the element at
  /// poisition.
  ///
  /// @param[in] position Position where to write the given value.
  /// @param[in] value Value to be written at the specified position.
  /// @return An iterator that points to the inserted value.
  iterator InsertAt(shad::Vector<T, Allocator>::size_type position,
                    const shad::Vector<T, Allocator>::value_type &value);

  /// @brief Write a sequence of elements starting at the specified position.
  ///
  /// Typical usage:
  /// @code
  /// auto vectorPtr = shad::Vector<size_t>::Create(kVectorSize);
  /// vectorPtr->InsertAt(
  ///   aPosition, std::begin(someSequence), std::end(someSequence));
  /// @endcode
  ///
  /// @warning The semantic of this method is different from the insert method
  /// of the std::vector.  The shad::Vector <b>will not</b> make room for the
  /// newly inserted elements shifting all the element from position to the end
  /// for performance reasons.
  ///
  /// @param[in] position Position where to write the given value.
  /// @param[in] position Position where to write the given value.
  /// @param[in] begin An input iterator to the start of the sequence to insert.
  /// @param[in] end An input iterator to the end of the sequence to insert.
  /// @return An iterator that points to the first inserted value.
  template <typename InputIterator>
  iterator InsertAt(size_type position, InputIterator begin, InputIterator end);

  /// @brief Write a value at the specified position asynchronously.
  ///
  /// This method overwrite the element at the specified position.  When writing
  /// one past the last element, the method will grow the container size by one
  /// inserting the value at the end.
  ///
  /// Typical usage:
  /// @code
  /// shad::rt::Handle handle;
  /// auto vectorPtr = shad::Vector<size_t>::Create(kVectorSize);
  /// for (size_t i = 0; i < kVectorSize; i++) {
  ///   vectorPtr->AsyncInsertAt(handle, i, i+1);
  /// }
  /// // ... do other work ...
  /// shad::rt::waitForCompletion(handle);
  /// @endcode
  ///
  /// @warning The semantic of this method is different from the insert method
  /// of the std::vector.  The shad::Vector will NOT make room for the newly
  /// inserted element and shift all the element by 1 from position to the end
  /// for performance reasons.
  ///
  /// @param[in,out] handle The handle that will be used for the spawned tasks.
  /// @param[in] position Position where to write the given value.
  /// @param[in] value Value to be written at the specified position.
  void AsyncInsertAt(rt::Handle &handle,
                     shad::Vector<T, Allocator>::size_type position,
                     const shad::Vector<T, Allocator>::value_type &value);

  /// @brief Write a sequence of elements starting at the specified position
  /// asynchronously.
  ///
  /// Typical usage:
  /// @code
  /// shad::rt::Handle handle;
  /// auto vectorPtr = shad::Vector<size_t>::Create(kVectorSize);
  /// vectorPtr->AsyncInsertAt(
  ///   handle, aPosition, std::begin(someSequence), std::end(someSequence));
  /// // ... do other work ...
  /// shad::rt::waitForCompletion(handle);
  /// @endcode
  ///
  /// @warning The semantic of this method is different from the insert method
  /// of the std::vector.  The shad::Vector <b>will not</b> make room for the
  /// newly inserted elements shifting all the element from position to the end
  /// for performance reasons.
  ///
  /// @param[in,out] handle The handle that will be used for the spawned tasks.
  /// @param[in] position Position where to write the given value.
  /// @param[in] begin An input iterator to the start of the sequence to insert.
  /// @param[in] end An input iterator to the end of the sequence to insert.
  template <typename InputIterator>
  void AsyncInsertAt(rt::Handle &handle, size_type position,
                     InputIterator begin, InputIterator end);

  /// @brief Buffered Insert method.
  ///
  /// Inserts an element at the specified position, using aggregation buffers.
  ///
  /// Typical usage:
  /// @code
  /// auto vectorPtr = shad::Vector<size_t>::Create(kVectorSize);
  /// for (size_t i = 0; i < kVectorSize; i++) {
  ///   vectorPtr->BufferedInsertAt(handle, i, i+1);
  /// }
  /// vectorPtr->WaitForBufferedInsert();
  /// @endcode
  ///
  /// @warning Insertions are finalized only after calling the
  /// WaitForBufferedInsert() method.
  ///
  /// @param[in] pos The target position.
  /// @param[in] value The value of the element to be inserted.
  void BufferedInsertAt(const size_type pos, const value_type &value);

  /// @brief Asynchronous Buffered Insert method.
  ///
  /// Asynchronously inserts an element at the specified position,
  /// using aggregation buffers.
  ///
  /// Typical usage:
  /// @code
  /// shad::rt::Handle handle;
  /// auto vectorPtr = shad::Vector<size_t>::Create(kVectorSize);
  /// for (size_t i = 0; i < kVectorSize; i++) {
  ///   vectorPtr->BufferedAsyncInsertAt(handle, i, i+1);
  /// }
  /// // ... do other work ...
  /// shad::rt::waitForCompletion(handle);
  /// vectorPtr->WaitForBufferedInsert();
  /// @endcode
  ///
  /// @warning asynchronous buffered insertions are finalized only after calling
  /// the shad::rt::waitForCompletion(rt::Handle &handle) method <b>and</b> the
  /// WaitForBufferedInsert() method, in this order.
  ///
  /// @param[in,out] handle Reference to the handle to be used to wait for
  /// completion.
  /// @param[in] pos The target position.
  /// @param[in] value The value to be inserted.
  void BufferedAsyncInsertAt(rt::Handle &handle, const size_type pos,
                             const value_type &value);

  /// @brief Finalize method for buffered insertions.
  void WaitForBufferedInsert() { buffers_.FlushAll(); }

  /// @}

  /// @name Algorithms
  /// @{

  /// @brief Applies a user-defined function to an element.
  ///
  /// Applies a user-defined function to the element at the specified position.
  /// Typical usage:
  /// @code
  /// void fn(size_t, size_t& elem, size_t& aValue) {
  ///   // ... do something ...
  /// }
  /// // ...
  /// auto vectorPtr = shad::Vector<size_t>::Create(kVectorSize);
  /// for (size_t i = 0; i < kVectorSize; i++) {
  ///   vectorPtr->Apply(i, fn, aValue);
  /// }
  /// @endcode
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(size_t, T&, Args& args);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in] position The target position.
  /// @param[in] function The function to apply.
  /// @param[in] args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void Apply(const size_type position, ApplyFunT &&function, Args &... args);

  /// @brief Asynchronously applies a user-defined function to an element.
  ///
  /// Asynchronously applies a user-defined function to the element at the
  /// specified position.
  ///
  /// Typical usage:
  /// @code
  /// void fn(shad::rt::Handle&, size_t i, size_t& elem, size_t& aValue) {
  ///   // ... do something ...
  /// }
  /// // ...
  /// auto vectorPtr = shad::Vector<size_t>::Create(kVectorSize);
  /// shad::rt::Handle handle;
  /// for (size_t i = 0; i < kVectorSize; i++) {
  ///   vectorPtr->AsyncApply(handle, i, fn, aValue);
  /// }
  /// shad::rt::waitForCompletion(handle);
  /// @endcode
  ///
  /// @warning Asynchronous operations are guaranteed to have completed only
  /// after calling the shad::rt::waitForCompletion(rt::Handle &handle) method.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(shad::rt::Handle&, size_t, T&, Args& args);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle Reference to the handle to be used to wait for
  /// completion.
  /// @param[in] position The target position.
  /// @param[in] function The function to apply.
  /// @param[in] args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncApply(rt::Handle &handle, const size_type position,
                  ApplyFunT &&function, Args &... args);

  /// @brief Applies a user-defined function to every element in the specified
  /// range.
  ///
  /// Applies a user-defined function to all the elements in the specified range
  /// of positions.
  ///
  /// Typical usage:
  /// @code
  /// void fn(size_t& elem, size_t& aValue) {
  ///   // ... do something ...
  /// }
  /// // ...
  /// auto vectorPtr = shad::Vector<size_t>::Create(kVectorSize);
  /// edsPtr->ForEachInRange(0, kVectorSize, fn, aValue);
  /// @endcode
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(T&, Args& args);
  /// @endcode
  ///
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in] first The first position of the range.
  /// @param[in] last The last position of the range.
  /// @param[in] function The function to apply.
  /// @param[in] args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void ForEachInRange(const size_type first, const size_type last,
                      ApplyFunT &&function, Args &... args);

  /// @brief Asynchronously applies a user-defined function to every element
  /// in the specified range.
  ///
  /// Asynchronously applies a user-defined function to all the elements in the
  /// specified range of positions.
  ///
  /// Typical usage:
  /// @code
  /// void asyncApplyFun(rt::Handle&, size_t& elem, size_t& aValue) {
  ///   // ... do something ...
  /// }
  /// // ...
  /// auto vectorPtr = shad::Vector<size_t>::Create(kVectorSize);
  /// rt::Handle handle;
  /// vectorPtr->AsyncForEachInRange(0, kVectorSize, kVectorSize, aValue);
  /// // ... do other work ...
  /// shad::rt::waitForCompletion(handle);
  /// @endcode
  ///
  /// @warning Asynchronous operations are guaranteed to have completed only
  /// after calling the shad::rt::waitForCompletion(rt::Handle &handle) method.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(shad::rt::Handle&, T&, Args& args);
  /// @endcode
  ///
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle Reference to the handle to be used to wait for
  /// completion.
  /// @param[in] first The first position of the range.
  /// @param[in] last The last position of the range.
  /// @param[in] function The function to apply.
  /// @param[in] args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncForEachInRange(rt::Handle &handle, const size_type first,
                           const size_type last, ApplyFunT &&function,
                           Args &... args);

  /// @}

  /// @brief Destructor.
  ~Vector() { _clear(); }

  void BufferEntryInsert(const std::tuple<size_type, value_type> entry) {
    auto blockOffsetPair = _blockOffsetFromPosition(std::get<0>(entry));
    size_type localBlock = _globlalBlockToLocalBlock(blockOffsetPair.first);
    dataBlocks_.at(localBlock)[blockOffsetPair.second] = std::get<1>(entry);
  }

 protected:
  Vector(ObjectID oid, size_type n)
      : AbstractDataStructure<Vector<T, Allocator>>(oid),
        dataBlocks_(),
        mainLocality_(static_cast<uint64_t>(oid) % rt::numLocalities()),
        sizeCapacityLock_(),
        size_(n),
        capacity_(0),
        allocator_(),
        buffers_(oid) {
    size_t blocksToAllocate = std::max(_sizeToLocalBlocks(n, kBlockSize), 1UL);
    capacity_ =
        std::max(kBlockSize * _blockOffsetFromPosition(n).first, kBlockSize);

    if (n == 0 && static_cast<uint32_t>(rt::thisLocality()) != 0) return;

    for (size_t i = 0; i < blocksToAllocate; ++i)
      dataBlocks_.emplace_back(std::allocator_traits<allocator_type>::allocate(
          allocator_, kBlockSize));
  }

 private:
  friend class AbstractDataStructure<Vector<T, Allocator>>;
  static const size_type kBlockSize;

  std::tuple<rt::Locality, size_t, size_t> _targetFromPosition(
      size_type position, size_type blockSize) const {
    size_t blockNumber = position / blockSize;
    size_t destination = blockNumber % rt::numLocalities();
    size_t offset = position % blockSize;
    return std::make_tuple(rt::Locality(destination), blockNumber, offset);
  }

  size_t _sizeToLocalBlocks(size_type n, size_type blockSize) const {
    size_t fullyUsedBlocks(0);
    rt::Locality pivot(0);

    if (n == 0) return 0;

    std::tie(pivot, fullyUsedBlocks, std::ignore) =
        _targetFromPosition(n - 1, blockSize);

    size_t localBlocks = fullyUsedBlocks / rt::numLocalities();
    if (rt::thisLocality() <= pivot) localBlocks += 1;

    return localBlocks;
  }

  std::pair<size_type, size_type> _blockOffsetFromPosition(size_type n) const {
    size_type blockNumber = n / kBlockSize;
    size_type offset = n % kBlockSize;
    return std::make_pair(blockNumber, offset);
  }

  size_type _globlalBlockToLocalBlock(size_type n) const {
    size_type i = 0;
    while (static_cast<uint32_t>(rt::thisLocality()) < n) {
      n -= rt::numLocalities();
      ++i;
    }
    return i;
  }

  void _reserve(size_type n) {
    size_type currentCapacity = Capacity();
    if (currentCapacity >= n) return;

    rt::Locality insertLocality(0);
    size_t lastBlock(0);
    std::tie(insertLocality, lastBlock, std::ignore) =
        _targetFromPosition(currentCapacity, kBlockSize);
    size_t newLastBlock = n / kBlockSize;

    size_type blocksToAllocate = 1;
    blocksToAllocate += newLastBlock - lastBlock;

    std::vector<size_type> newBlocks(rt::numLocalities(), 0);
    for (size_t i = static_cast<uint32_t>(insertLocality), j = blocksToAllocate;
         j > 0; ++i, --j) {
      newBlocks[i % rt::numLocalities()]++;
    }

    rt::Handle handle;
    for (size_t i = 0; i < rt::numLocalities(); ++i) {
      if (newBlocks[i] == 0) continue;

      rt::asyncExecuteAt(
          handle, rt::Locality(i),
          [](rt::Handle &, const std::pair<ObjectID, size_type> &args) {
            auto This = Vector<T, Allocator>::GetPtr(args.first);

            for (size_t i = 0; i < args.second; ++i) {
              This->dataBlocks_.emplace_back(
                  std::allocator_traits<allocator_type>::allocate(
                      This->allocator_, kBlockSize));
            }
          },
          std::make_pair(this->oid_, newBlocks[i]));
    }

    rt::waitForCompletion(handle);

    capacity_ += kBlockSize * blocksToAllocate;
  }

  void _clear() {
    for (auto block : dataBlocks_) {
      for (T *toDestroy = block; toDestroy < block + kBlockSize; ++toDestroy) {
        std::allocator_traits<allocator_type>::destroy(allocator_, toDestroy);
      }
      std::allocator_traits<allocator_type>::deallocate(allocator_, block,
                                                        kBlockSize);
    }
    dataBlocks_.clear();
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void CallApplyFun(const ObjectID &oid, const size_type position,
                           ApplyFunT function, std::tuple<Args...> &args,
                           std::index_sequence<is...>) {
    auto This = Vector<T, Allocator>::GetPtr(oid);
    auto blockOffsetPair = This->_blockOffsetFromPosition(position);
    size_type localBlock =
        This->_globlalBlockToLocalBlock(blockOffsetPair.first);
    value_type &element =
        This->dataBlocks_.at(localBlock)[blockOffsetPair.second];
    function(position, element, std::get<is>(args)...);
  }

  template <typename Tuple, typename... Args>
  static void ApplyFunWrapper(const Tuple &args) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<3>(args))>::type>::value;
    Tuple &tuple = const_cast<Tuple &>(args);
    CallApplyFun(std::get<0>(tuple), std::get<1>(tuple), std::get<2>(tuple),
                 std::get<3>(tuple), std::make_index_sequence<Size>{});
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void AsyncCallApplyFun(rt::Handle &handle, const ObjectID &oid,
                                const size_type position, ApplyFunT function,
                                std::tuple<Args...> &args,
                                std::index_sequence<is...>) {
    auto This = Vector<T, Allocator>::GetPtr(oid);
    auto blockOffsetPair = This->_blockOffsetFromPosition(position);
    size_type localBlock =
        This->_globlalBlockToLocalBlock(blockOffsetPair.first);
    value_type &element = This->dataBlocks_[localBlock][blockOffsetPair.second];
    function(handle, position, element, std::get<is>(args)...);
  }

  template <typename Tuple, typename... Args>
  static void AsyncApplyFunWrapper(rt::Handle &handle, const Tuple &args) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<3>(args))>::type>::value;
    Tuple &tuple = const_cast<Tuple &>(args);
    AsyncCallApplyFun(handle, std::get<0>(tuple), std::get<1>(tuple),
                      std::get<2>(tuple), std::get<3>(tuple),
                      std::make_index_sequence<Size>{});
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void CallForEachInRangeFun(const size_t i, const ObjectID &oid,
                                    const size_t position, ApplyFunT function,
                                    std::tuple<Args...> &args,
                                    std::index_sequence<is...>) {
    // Get a local instance on the remote node.
    auto This = Vector<T, Allocator>::GetPtr(oid);
    auto blockOffsetPair = This->_blockOffsetFromPosition(position + i);
    size_type localBlock =
        This->_globlalBlockToLocalBlock(blockOffsetPair.first);
    value_type &element =
        This->dataBlocks_.at(localBlock)[blockOffsetPair.second];
    function(position + i, element, std::get<is>(args)...);
  }

  template <typename Tuple, typename... Args>
  static void ForEachInRangeFunWrapper(const Tuple &args, size_t i) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<3>(args))>::type>::value;
    Tuple &tuple = const_cast<Tuple &>(args);
    CallForEachInRangeFun(i, std::get<0>(tuple), std::get<1>(tuple),
                          std::get<2>(tuple), std::get<3>(tuple),
                          std::make_index_sequence<Size>{});
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void AsyncCallForEachInRangeFun(rt::Handle &handle, const size_t i,
                                         const ObjectID &oid,
                                         const size_t position,
                                         ApplyFunT function,
                                         std::tuple<Args...> &args,
                                         std::index_sequence<is...>) {
    // Get a local instance on the remote node.
    auto This = Vector<T, Allocator>::GetPtr(oid);
    auto blockOffsetPair = This->_blockOffsetFromPosition(position + i);
    size_type localBlock =
        This->_globlalBlockToLocalBlock(blockOffsetPair.first);
    value_type &element =
        This->dataBlocks_.at(localBlock)[blockOffsetPair.second];
    function(handle, position + i, element, std::get<is>(args)...);
  }

  template <typename Tuple, typename... Args>
  static void AsyncForEachInRangeFunWrapper(rt::Handle &handle,
                                            const Tuple &args, size_t i) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<3>(args))>::type>::value;
    Tuple &tuple = const_cast<Tuple &>(args);
    AsyncCallForEachInRangeFun(
        handle, i, std::get<0>(tuple), std::get<1>(tuple), std::get<2>(tuple),
        std::get<3>(tuple), std::make_index_sequence<Size>{});
  }

  using BuffersVector =
      impl::BuffersVector<std::tuple<size_t, T>, Vector<T, Allocator>>;
  friend class impl::BuffersVector<std::tuple<size_t, T>, Vector<T, Allocator>>;

  rt::Locality mainLocality_;
  std::vector<value_type *> dataBlocks_;
  rt::Lock sizeCapacityLock_;
  size_type size_;
  size_type capacity_;
  allocator_type allocator_;
  BuffersVector buffers_;
};

template <typename T, typename Allocator>
const typename Vector<T, Allocator>::size_type
    Vector<T, Allocator>::kBlockSize = constants::max((1024 << 6) / sizeof(T),
                                                      1lu);

template <typename T, typename Allocator>
template <typename ValueType>
class Vector<T, Allocator>::Iterator
    : std::iterator<std::random_access_iterator_tag, ValueType> {
 public:
  Iterator(Vector<T, Allocator>::size_type n,
           Vector<T, Allocator>::ObjectID oid)
      : position_(n), oid_(oid) {}

  Iterator(const Iterator &itr) = default;

  Iterator &operator=(const Iterator &itr) = default;

  explicit operator bool() const {
    if (position_ == Vector<T, Allocator>::MaxSize())
      return false;
    else
      return true;
  }

  bool operator==(const Iterator &rhs) const {
    if (oid_ != rhs.oid_) return false;

    if (position_ != rhs.position_)
      return false;
    else
      return true;
  }

  bool operator!=(const Iterator &rhs) const { return !(*this == rhs); }

  Iterator &operator+=(const ptrdiff_t &movement) {
    position_ += movement;
    return *this;
  }

  Iterator &operator-=(const ptrdiff_t &movement) {
    position_ -= movement;
    return *this;
  }

  Iterator &operator++() {
    ++position_;
    return *this;
  }
  Iterator &operator--() {
    --position_;
    return *this;
  }

  Iterator operator++(int) {
    auto tmp(*this);
    ++position_;
    return tmp;
  }

  Iterator operator--(int) {
    auto tmp(*this);
    --position_;
    tmp;
  }

  Iterator operator+(const ptrdiff_t &movement) {
    Iterator tmp(*this);
    tmp.position_ += movement;
    return tmp;
  }

  Iterator operator-(const ptrdiff_t &movement) {
    Iterator tmp(*this);
    tmp.position_ -= movement;
    return tmp;
  }

  ptrdiff_t operator-(const Iterator &rhs) {
    return std::distance(rhs.position_, this->position_);
  }

  const value_type operator*() const {
    auto ptr = Vector<T, Allocator>::GetPtr(oid_);
    return ptr->At(position_);
  }

  const value_type *operator->() const {
    auto ptr = Vector<T, Allocator>::GetPtr(oid_);
    return &*(*this);
  }

 private:
  Vector<T, Allocator>::size_type position_;
  Vector<T, Allocator>::ObjectID oid_;
};

template <typename T, typename Allocator>
typename Vector<T, Allocator>::size_type Vector<T, Allocator>::Size() const
    noexcept {
  if (rt::thisLocality() == mainLocality_) return size_;

  size_type size = 0;
  rt::executeAtWithRet(mainLocality_,
                       [](const ObjectID &oid, size_type *size) {
                         auto This = Vector<T, Allocator>::GetPtr(oid);
                         *size = This->size_;
                       },
                       this->oid_, &size);
  return size;
}

template <typename T, typename Allocator>
typename Vector<T, Allocator>::size_type Vector<T, Allocator>::MaxSize() const
    noexcept {
  return std::numeric_limits<Vector<T, Allocator>::size_type>::max() - 1;
}

template <typename T, typename Allocator>
typename Vector<T, Allocator>::size_type Vector<T, Allocator>::Capacity() const
    noexcept {
  if (rt::thisLocality() == mainLocality_) return capacity_;

  size_type capacity = 0;
  rt::executeAtWithRet(mainLocality_,
                       [](const ObjectID &oid, size_type *capacity) {
                         auto ptr = Vector<T, Allocator>::GetPtr(oid);
                         *capacity = ptr->capacity_;
                       },
                       this->oid_, &capacity);
  return capacity;
}

template <typename T, typename Allocator>
bool Vector<T, Allocator>::Empty() const noexcept {
  return Size() == 0;
}

template <typename T, typename Allocator>
void Vector<T, Allocator>::Reserve(Vector<T, Allocator>::size_type n) {
  rt::executeAt(mainLocality_,
                [](const std::pair<ObjectID, size_type> &args) {
                  auto This = Vector<T, Allocator>::GetPtr(args.first);
                  auto n = args.second;

                  std::lock_guard<rt::Lock> _(This->sizeCapacityLock_);
                  This->_reserve(n);
                },
                std::make_pair(this->oid_, n));
}

template <typename T, typename Allocator>
void Vector<T, Allocator>::Resize(Vector<T, Allocator>::size_type n) {
  rt::executeAt(mainLocality_,
                [](const std::pair<ObjectID, size_type> &args) {
                  auto This = Vector<T, Allocator>::GetPtr(args.first);
                  auto n = args.second;

                  std::lock_guard<rt::Lock> _(This->sizeCapacityLock_);
                  if (n <= This->size_) return;

                  This->_reserve(n);
                  This->size_ = n;
                },
                std::make_pair(this->oid_, n));
}

template <typename T, typename Allocator>
typename Vector<T, Allocator>::value_type Vector<T, Allocator>::At(
    Vector<T, Allocator>::size_type n) const {
  rt::Locality target(0);
  size_t blockNumber(0);
  size_t offset(0);
  std::tie(target, blockNumber, offset) = _targetFromPosition(n, kBlockSize);

  if (target == rt::thisLocality()) {
    size_type localBlock = _globlalBlockToLocalBlock(blockNumber);
    return dataBlocks_[localBlock][offset];
  } else {
    T value;
    rt::executeAtWithRet(
        target,
        [](const std::tuple<ObjectID, size_type, size_type> &args, T *result) {
          auto This = Vector<T, Allocator>::GetPtr(std::get<0>(args));
          size_type localBlock =
              This->_globlalBlockToLocalBlock(std::get<1>(args));

          *result = This->dataBlocks_[localBlock][std::get<2>(args)];
        },
        std::make_tuple(this->oid_, blockNumber, offset), &value);
    return value;
  }
}

template <typename T, typename Allocator>
typename Vector<T, Allocator>::value_type Vector<T, Allocator>::operator[](
    Vector<T, Allocator>::size_type n) const {
  return At(n);
}

template <typename T, typename Allocator>
typename Vector<T, Allocator>::value_type Vector<T, Allocator>::Front() const {
  return At(0);
}

template <typename T, typename Allocator>
typename Vector<T, Allocator>::value_type Vector<T, Allocator>::Back() const {
  size_type last_position = Size() - 1;
  return At(last_position);
}

template <typename T, typename Allocator>
void Vector<T, Allocator>::AsyncAt(
    rt::Handle &handle, Vector<T, Allocator>::size_type n,
    Vector<T, Allocator>::value_type *result) const {
  rt::Locality target(0);
  size_type blockNumber(0);
  size_type offset(0);
  std::tie(target, blockNumber, offset) = _targetFromPosition(n, kBlockSize);

  rt::asyncExecuteAtWithRet(
      handle, target,
      [](rt::Handle &handle,
         const std::tuple<ObjectID, size_type, size_type> &args, T *result) {
        auto This = Vector<T, Allocator>::GetPtr(std::get<0>(args));
        size_type localBlock =
            This->_globlalBlockToLocalBlock(std::get<1>(args));

        *result = This->dataBlocks_[localBlock][std::get<2>(args)];
      },
      std::make_tuple(this->oid_, blockNumber, offset), result);
}

template <typename T, typename Allocator>
void Vector<T, Allocator>::Clear() noexcept {
  rt::executeAt(mainLocality_,
                [](const ObjectID &args) {
                  auto This = Vector<T, Allocator>::GetPtr(args);
                  std::lock_guard<rt::Lock> _(This->sizeCapacityLock_);

                  This->size_ = 0;
                  This->capacity_ = 0;

                  rt::executeOnAll(
                      [](const ObjectID &args) {
                        auto This = Vector<T, Allocator>::GetPtr(args);
                        This->_clear();
                      },
                      args);
                },
                this->oid_);
}

template <typename T, typename Allocator>
void Vector<T, Allocator>::PushBack(
    const typename Vector<T, Allocator>::value_type &value) {
  size_type newSize(0);

  rt::executeAtWithRet(mainLocality_,
                       [](const ObjectID &args, size_type *size) {
                         auto This = Vector<T, Allocator>::GetPtr(args);
                         std::lock_guard<rt::Lock> _(This->sizeCapacityLock_);

                         *size = ++This->size_;
                         if (This->size_ > This->capacity_)
                           This->_reserve(This->size_);
                       },
                       this->oid_, &newSize);

  size_type position = newSize - 1;

  rt::Locality target(0);
  size_t blockNumber(0);
  size_t offset(0);
  std::tie(target, blockNumber, offset) =
      _targetFromPosition(position, kBlockSize);

  if (target == rt::thisLocality()) {
    size_type localBlock = _globlalBlockToLocalBlock(blockNumber);
    dataBlocks_[localBlock][offset] = value;
  } else {
    using MessageTuple = std::tuple<ObjectID, size_type, size_type, value_type>;
    rt::executeAt(target,
                  [](const MessageTuple &args) {
                    auto This = Vector<T, Allocator>::GetPtr(std::get<0>(args));
                    size_type localBlock =
                        This->_globlalBlockToLocalBlock(std::get<1>(args));

                    This->dataBlocks_[localBlock][std::get<2>(args)] =
                        std::get<3>(args);
                  },
                  std::make_tuple(this->oid_, blockNumber, offset, value));
  }
}

template <typename T, typename Allocator>
typename Vector<T, Allocator>::iterator Vector<T, Allocator>::InsertAt(
    Vector<T, Allocator>::size_type position,
    const Vector<T, Allocator>::value_type &value) {
  rt::Locality target(0);
  size_t blockNumber(0);
  size_t offset(0);
  std::tie(target, blockNumber, offset) =
      _targetFromPosition(position, kBlockSize);

  if (target == rt::thisLocality()) {
    size_type localBlock = _globlalBlockToLocalBlock(blockNumber);
    dataBlocks_[localBlock][offset] = value;
  } else {
    using MessageTuple = std::tuple<ObjectID, size_type, size_type, value_type>;
    rt::executeAt(target,
                  [](const MessageTuple &args) {
                    auto This = Vector<T, Allocator>::GetPtr(std::get<0>(args));
                    size_type localBlock =
                        This->_globlalBlockToLocalBlock(std::get<1>(args));

                    This->dataBlocks_[localBlock][std::get<2>(args)] =
                        std::get<3>(args);
                  },
                  std::make_tuple(this->oid_, blockNumber, offset, value));
  }

  return Vector<T, Allocator>::iterator(position, this->GetGlobalID());
}

template <typename T, typename Allocator>
template <typename IteratorType>
typename Vector<T, Allocator>::iterator Vector<T, Allocator>::InsertAt(
    Vector<T, Allocator>::size_type position, IteratorType begin,
    IteratorType end) {
  rt::Handle handle;

  AsyncInsertAt(handle, position, begin, end);

  rt::waitForCompletion(handle);

  return Vector<T, Allocator>::iterator(position, this->GetGlobalID());
}

template <typename T, typename Allocator>
void Vector<T, Allocator>::AsyncInsertAt(
    rt::Handle &handle, Vector<T, Allocator>::size_type position,
    const Vector<T, Allocator>::value_type &value) {
  size_type currentSize(0);

  rt::Locality target(0);
  size_t blockNumber(0);
  size_t offset(0);
  std::tie(target, blockNumber, offset) =
      _targetFromPosition(position, kBlockSize);

  if (target == rt::thisLocality()) {
    size_type localBlock = _globlalBlockToLocalBlock(blockNumber);
    dataBlocks_[localBlock][offset] = value;
  } else {
    using MessageTuple = std::tuple<ObjectID, size_type, size_type, value_type>;
    rt::asyncExecuteAt(
        handle, target,
        [](rt::Handle &, const MessageTuple &args) {
          auto This = Vector<T, Allocator>::GetPtr(std::get<0>(args));
          size_type localBlock =
              This->_globlalBlockToLocalBlock(std::get<1>(args));

          This->dataBlocks_[localBlock][std::get<2>(args)] = std::get<3>(args);
        },
        std::make_tuple(this->oid_, blockNumber, offset, value));
  }
}

template <typename T, typename Allocator>
template <typename IteratorType>
void Vector<T, Allocator>::AsyncInsertAt(
    rt::Handle &handle, Vector<T, Allocator>::size_type position,
    IteratorType begin, IteratorType end) {
  size_type newElements = std::distance(begin, end);
  size_type newSize(0);

  rt::executeAtWithRet(
      mainLocality_,
      [](const std::tuple<ObjectID, size_type, size_type> &args,
         size_type *size) {
        auto This = Vector<T, Allocator>::GetPtr(std::get<0>(args));
        std::lock_guard<rt::Lock> _(This->sizeCapacityLock_);
        auto position = std::get<1>(args);
        auto newElements = std::get<2>(args);

        if (position > This->size_ || position + newElements <= This->size_) {
          *size = This->size_;
          return;
        }

        size_type end = position + newElements;
        size_type growth = This->size_ > end ? 0 : end - This->size_;

        This->size_ += growth;
        if (This->size_ > This->capacity_) This->_reserve(This->size_);

        *size = This->size_;
      },
      std::make_tuple(this->oid_, position, newElements), &newSize);

  // Trying to insert in an invalid position.
  if (position > newSize) {
    throw std::out_of_range("AsyncInsertAt: position out of range");
  }

  size_type startingPoint = newSize - newElements;

  constexpr size_t kNumElements = 4000 / sizeof(value_type);
  static_assert(kNumElements >= 1,
                "We can't do this due to a series of unfortunate events");

  struct InsertMessage {
    ObjectID objID;
    size_type startPosition;
    size_type numElements;
    value_type elements[kNumElements];

    InsertMessage()
        : objID(ObjectID::kNullID), startPosition(0), numElements(0) {}
  };

  auto insertFunction = [](rt::Handle &, const InsertMessage &args) {
    auto This = Vector<T, Allocator>::GetPtr(args.objID);
    auto blockOffsetPair = This->_blockOffsetFromPosition(args.startPosition);
    size_type localBlock =
        This->_globlalBlockToLocalBlock(blockOffsetPair.first);
    std::copy(&args.elements[0], &args.elements[args.numElements],
              &This->dataBlocks_.at(localBlock)[blockOffsetPair.second]);
  };

  InsertMessage args;
  size_t spaceLeftInBlock = kBlockSize - (startingPoint % kBlockSize);
  // first block
  args.objID = this->oid_;
  args.startPosition = startingPoint;
  args.numElements =
      std::min(newElements, std::min(kNumElements, spaceLeftInBlock));

  std::copy(begin, begin + args.numElements, args.elements);

  rt::Locality target(0);
  size_t blockNumber(0);
  size_t offset(0);
  std::tie(target, blockNumber, offset) =
      _targetFromPosition(position, kBlockSize);

  rt::asyncExecuteAt(handle, target, insertFunction, args);

  begin += args.numElements;
  newElements -= args.numElements;
  startingPoint += args.numElements;

  // inner blocks
  while (begin != end && newElements > 0) {
    std::tie(target, blockNumber, offset) =
        _targetFromPosition(startingPoint, kBlockSize);
    args.startPosition = startingPoint;

    spaceLeftInBlock = kBlockSize - (startingPoint % kBlockSize);
    args.numElements =
        std::min(newElements, std::min(kNumElements, spaceLeftInBlock));

    std::copy(begin, begin + args.numElements, args.elements);
    rt::asyncExecuteAt(handle, target, insertFunction, args);

    begin += args.numElements;
    newElements -= args.numElements;
    startingPoint += args.numElements;
  }
}

template <typename T, typename Allocator>
void Vector<T, Allocator>::BufferedInsertAt(const size_type position,
                                            const value_type &value) {
  rt::Locality target(0);
  size_t blockNumber(0);
  size_t offset(0);
  std::tie(target, blockNumber, offset) =
      _targetFromPosition(position, kBlockSize);

  if (target == rt::thisLocality()) {
    size_type localBlock = _globlalBlockToLocalBlock(blockNumber);
    dataBlocks_[localBlock][offset] = value;
  } else {
    buffers_.Insert(std::make_tuple(position, value), target);
  }
}

template <typename T, typename Allocator>
void Vector<T, Allocator>::BufferedAsyncInsertAt(rt::Handle &handle,
                                                 const size_type position,
                                                 const value_type &value) {
  rt::Locality target(0);
  size_t blockNumber(0);
  size_t offset(0);
  std::tie(target, blockNumber, offset) =
      _targetFromPosition(position, kBlockSize);

  if (target == rt::thisLocality()) {
    size_type localBlock = _globlalBlockToLocalBlock(blockNumber);
    dataBlocks_[localBlock][offset] = value;
  } else {
    buffers_.AsyncInsert(handle, std::make_tuple(position, value), target);
  }
}

template <typename T, typename Allocator>
template <typename ApplyFunT, typename... Args>
void Vector<T, Allocator>::Apply(const Vector<T, Allocator>::size_type position,
                                 ApplyFunT &&function, Args &... args) {
  rt::Locality target(0);
  size_t blockNumber(0);
  size_t offset(0);
  std::tie(target, blockNumber, offset) =
      _targetFromPosition(position, kBlockSize);

  using FunctionTy = void (*)(size_type, T &, Args & ...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using ArgsTuple =
      std::tuple<ObjectID, size_t, FunctionTy, std::tuple<Args...>>;
  ArgsTuple argsTuple{this->oid_, position, fn, std::tuple<Args...>(args...)};

  rt::executeAt(target, ApplyFunWrapper<ArgsTuple, Args...>, argsTuple);
}

template <typename T, typename Allocator>
template <typename ApplyFunT, typename... Args>
void Vector<T, Allocator>::AsyncApply(
    rt::Handle &handle, const Vector<T, Allocator>::size_type position,
    ApplyFunT &&function, Args &... args) {
  rt::Locality target(0);
  size_t blockNumber(0);
  size_t offset(0);
  std::tie(target, blockNumber, offset) =
      _targetFromPosition(position, kBlockSize);

  using FunctionTy = void (*)(rt::Handle &, size_type, T &, Args & ...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using ArgsTuple =
      std::tuple<ObjectID, size_t, FunctionTy, std::tuple<Args...>>;
  ArgsTuple argsTuple{this->oid_, position, fn, std::tuple<Args...>(args...)};

  rt::asyncExecuteAt(handle, target, AsyncApplyFunWrapper<ArgsTuple, Args...>,
                     argsTuple);
}

template <typename T, typename Allocator>
template <typename ApplyFunT, typename... Args>
void Vector<T, Allocator>::ForEachInRange(const size_type begin,
                                          const size_type end,
                                          ApplyFunT &&function,
                                          Args &... args) {
  using FunctionTy = void (*)(size_type, value_type &, Args & ...);

  FunctionTy fn = std::forward<decltype(function)>(function);
  using ArgsTuple =
      std::tuple<ObjectID, size_t, FunctionTy, std::tuple<Args...>>;

  size_type numElements = end - begin;
  size_type start = begin;

  // first block
  size_type blockSize =
      std::min(numElements, kBlockSize - (start % kBlockSize));

  rt::Locality target(0);
  std::tie(target, std::ignore, std::ignore) =
      _targetFromPosition(start, kBlockSize);

  ArgsTuple argsTuple{this->oid_, start, fn, std::tuple<Args...>(args...)};

  rt::forEachAt(target, ForEachInRangeFunWrapper<ArgsTuple, Args...>, argsTuple,
                blockSize);

  start += blockSize;
  numElements -= blockSize;

  // inner blocks
  while (start != end && numElements > 0) {
    std::tie(target, std::ignore, std::ignore) =
        _targetFromPosition(start, kBlockSize);
    blockSize = std::min(kBlockSize, numElements);

    std::get<1>(argsTuple) = start;
    rt::forEachAt(target, ForEachInRangeFunWrapper<ArgsTuple, Args...>,
                  argsTuple, blockSize);

    start += blockSize;
    numElements -= blockSize;
  }
}

template <typename T, typename Allocator>
template <typename ApplyFunT, typename... Args>
void Vector<T, Allocator>::AsyncForEachInRange(rt::Handle &handle,
                                               const size_type begin,
                                               const size_type end,
                                               ApplyFunT &&function,
                                               Args &... args) {
  using FunctionTy =
      void (*)(rt::Handle &, size_type, value_type &, Args & ...);

  FunctionTy fn = std::forward<decltype(function)>(function);
  using ArgsTuple =
      std::tuple<ObjectID, size_t, FunctionTy, std::tuple<Args...>>;

  size_type numElements = end - begin;
  size_type start = begin;

  // first block
  size_type blockSize =
      std::min(numElements, kBlockSize - (start % kBlockSize));

  rt::Locality target(0);
  std::tie(target, std::ignore, std::ignore) =
      _targetFromPosition(start, kBlockSize);

  ArgsTuple argsTuple{this->oid_, start, fn, std::tuple<Args...>(args...)};

  rt::asyncForEachAt(handle, target,
                     AsyncForEachInRangeFunWrapper<ArgsTuple, Args...>,
                     argsTuple, blockSize);

  start += blockSize;
  numElements -= blockSize;

  // inner blocks
  while (start != end && numElements > 0) {
    std::tie(target, std::ignore, std::ignore) =
        _targetFromPosition(start, kBlockSize);
    blockSize = std::min(kBlockSize, numElements);

    std::get<1>(argsTuple) = start;
    rt::asyncForEachAt(handle, target,
                       AsyncForEachInRangeFunWrapper<ArgsTuple, Args...>,
                       argsTuple, blockSize);

    start += blockSize;
    numElements -= blockSize;
  }
}

}  // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_VECTOR_H_
