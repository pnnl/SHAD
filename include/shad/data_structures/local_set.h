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

#ifndef INCLUDE_SHAD_DATA_STRUCTURES_LOCAL_SET_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_LOCAL_SET_H_

#include <algorithm>
#include <atomic>
#include <cassert>
#include <functional>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "shad/data_structures/compare_and_hash_utils.h"
#include "shad/runtime/runtime.h"

namespace shad {

namespace constants {
constexpr size_t kSetDefaultNumEntriesPerBucket = 128;
}

template <typename LSet, typename T>
class lset_iterator;

/// @brief The LocalSet data structure.
///
/// SHAD's LocalSet is a "local", unordered, set.
/// LocalSets can be used ONLY on the Locality
/// on which they are created.
/// @tparam T type of the entries stored in the set.
/// @tparam ELEM_COMPARE key comparison function; default is MemCmp<T>.
template <typename T, typename ELEM_COMPARE = MemCmp<T>>
class LocalSet {
  template <typename, typename>
  friend class Set;
  template <typename, typename, typename>
  friend class LocalEdgeIndex;
  template <typename, typename>
  friend class AttrEdgesPair;

  friend class lset_iterator<LocalSet<T, ELEM_COMPARE>, const T>;
  friend class lset_iterator<LocalSet<T, ELEM_COMPARE>, const T>;
  template <typename, typename, typename>
  friend class set_iterator;

 public:
  using value_type = T;
  using iterator = lset_iterator<LocalSet<T, ELEM_COMPARE>, const T>;
  using const_iterator = lset_iterator<LocalSet<T, ELEM_COMPARE>, const T>;

  /// @brief Constructor.
  /// @param numInitBuckets initial number of Buckets.
  explicit LocalSet(const size_t numInitBuckets = 16)
      : numBuckets_(numInitBuckets), buckets_array_(numInitBuckets), size_(0) {}

  /// @brief Size of the set (number of entries).
  /// @return the size of the set.
  size_t Size() const { return size_.load(); }

  /// @brief Insert an element in the set.
  /// @param[in] element the element to insert.
  /// @return a pair consisting of an iterator to the inserted element (or to
  /// the element that prevented the insertion) and a bool denoting whether the
  /// insertion took place.
  std::pair<iterator, bool> Insert(const T& element);

  /// @brief Asynchronously Insert an element in the set.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle
  /// to be used to wait for completion.
  /// @param[in] element the element to insert.
  void AsyncInsert(rt::Handle& handle, const T& element);

  /// @brief Remove an element from the set.
  /// @param[in] element the element to remove.
  void Erase(const T& element);

  /// @brief Asynchronously removen element from the set.
  /// @warning Asynchronous operations are guaranteed to have completed.
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle.
  /// to be used to wait for completion.
  /// @param[in] element the element to insert.
  void AsyncErase(rt::Handle& handle, const T& element);

  /// @brief Clear the content of the set.
  void Clear() {
    size_ = 0;
    buckets_array_.clear();
    //     buckets_array_(numBuckets_);
  }

  /// @brief Clear the content of the set.
  void Reset(size_t expectedEntries) {
    size_ = 0;
    buckets_array_.clear();
    numBuckets_ = std::max(1lu, expectedEntries / 16);
    buckets_array_ = std::vector<Bucket>(numBuckets_);
  }
  /// @brief Check if the set contains a given element.
  /// @param[in] element the element to find.
  /// @return true if the element is found, false otherwise.
  bool Find(const T& element);

  /// @brief Asynchronously check if the set contains a given element.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle
  /// to be used to wait for completion.
  /// @param[in] element the element to find.
  /// @param[out] found the address where to store the result of the operation.
  void AsyncFind(rt::Handle& handle, const T& element, bool* found);

  /// @brief Apply a user-defined function to each element in the set.
  /// @tparam ApplyFunT User-defined function type.
  /// The function prototype should be:
  /// @code
  /// void(const T&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void ForEachElement(ApplyFunT&& function, Args&... args);

  /// @brief Asynchronously apply a user-defined function
  /// to each element in the set.
  /// @tparam ApplyFunT User-defined function type.
  /// The function prototype should be:
  /// @code
  /// void(shad::rt::Handle&, const T&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  /// @warning Asynchronous operations are guaranteed to have completed.
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle.
  /// to be used to wait for completion.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncForEachElement(rt::Handle& handle, ApplyFunT&& function,
                           Args&... args);

  /// @brief Print all the entries in the set.
  /// @warning std::ostream & operator<< must be defined for T.
  void PrintAllElements();

  iterator begin() {
    Entry* firstEntry = &buckets_array_[0].getEntry(0);
    iterator cbeg(this, 0, 0, &buckets_array_[0], firstEntry);
    if (firstEntry->state == USED) {
      return cbeg;
    }
    return ++cbeg;
  }

  iterator end() { return iterator::lset_end(numBuckets_); }

  const_iterator cbegin() {
    Entry* firstEntry = &buckets_array_[0].getEntry(0);
    const_iterator cbeg(this, 0, 0, &buckets_array_[0], firstEntry);
    if (firstEntry->state == USED) {
      return cbeg;
    }
    return ++cbeg;
  }

  const_iterator cend() { return const_iterator::lset_end(numBuckets_); }

 private:
  static const size_t kNumEntriesPerBucket =
      constants::kSetDefaultNumEntriesPerBucket;
  static const size_t kAllocPending = 0x1;
  static const uint32_t kKeyWords = sizeof(T) > sizeof(uint64_t)
                                        ? sizeof(T) / sizeof(uint64_t)
                                        : 1;
  static const uint8_t kHashSeed = 0;
  typedef ELEM_COMPARE ElemCompare;

  enum State { EMPTY, USED, PENDING_INSERT };

  struct Entry {
    T element;
    volatile State state;
    Entry() : state(EMPTY) {}
  };

  struct Bucket {
    std::shared_ptr<Bucket> next;
    bool isNextAllocated;

    explicit Bucket(size_t bsize = kNumEntriesPerBucket)
        : next(nullptr),
          isNextAllocated(false),
          entries(nullptr),
          bucketSize_(bsize) {}

    Entry& getEntry(size_t i) {
      if (!entries) {
        std::lock_guard<rt::Lock> _(_entriesLock);
        if (!entries) {
          entries = std::move(std::shared_ptr<Entry>(
              new Entry[bucketSize_], std::default_delete<Entry[]>()));
        }
      }
      return entries.get()[i];
    }

    size_t BucketSize() const { return bucketSize_; }

   private:
    size_t bucketSize_;
    std::shared_ptr<Entry> entries;
    rt::Lock _entriesLock;
  };

  ElemCompare ElemComp_;
  size_t numBuckets_;
  std::vector<Bucket> buckets_array_;
  std::atomic<size_t> size_;

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void AsyncCallForEachElementFun(rt::Handle& handle, const size_t i,
                                         LocalSet<T>* setPtr,
                                         ApplyFunT function,
                                         std::tuple<Args...>& args,
                                         std::index_sequence<is...>) {
    Bucket* bucket = &setPtr->buckets_array_[i];
    while (bucket != nullptr) {
      Bucket* next_bucket = bucket->next.get();
      uint64_t j;
      for (j = 0; j < bucket->BucketSize(); ++j) {
        Entry* entry = &bucket->getEntry(j);
        if (entry->state == USED) {
          function(handle, entry->element, std::get<is>(args)...);
        } else if (entry->state != EMPTY) {
          printf(
              "Entry in PENDING state"
              " while iterating over entries\n");
        }
      }
      bucket = next_bucket;
    }
  }

  template <typename Tuple, typename... Args>
  static void AsyncForEachElementFunWrapper(rt::Handle& handle,
                                            const Tuple& args, size_t i) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<2>(args))>::type>::value;
    Tuple& tuple = const_cast<Tuple&>(args);
    AsyncCallForEachElementFun(handle, i, std::get<0>(tuple),
                               std::get<1>(tuple), std::get<2>(tuple),
                               std::make_index_sequence<Size>{});
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void CallForEachElementFun(const size_t i, LocalSet<T>* setPtr,
                                    ApplyFunT function,
                                    std::tuple<Args...>& args,
                                    std::index_sequence<is...>) {
    Bucket* bucket = &setPtr->buckets_array_.data()[i];
    while (bucket != nullptr) {
      Bucket* next_bucket = bucket->next.get();
      uint64_t j;
      for (j = 0; j < bucket->BucketSize(); ++j) {
        Entry* entry = &bucket->getEntry(j);
        if (entry->state == USED) {
          function(entry->element, std::get<is>(args)...);
        } else if (entry->state != EMPTY) {
          printf(
              "Entry in PENDING state"
              " while iterating over entries\n");
        }
      }
      bucket = next_bucket;
    }
  }

  template <typename Tuple, typename... Args>
  static void ForEachElementFunWrapper(const Tuple& args, size_t i) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<2>(args))>::type>::value;
    Tuple& tuple = const_cast<Tuple&>(args);
    CallForEachElementFun(i, std::get<0>(tuple), std::get<1>(tuple),
                          std::get<2>(tuple), std::make_index_sequence<Size>{});
  }

 protected:
  // Custom ForEach for the Local Edge Index
  template <typename ApplyFunT, typename SrcT, typename... Args>
  void AsyncForEachNeighbor(rt::Handle& handle, ApplyFunT&& function, SrcT src,
                            Args... args) {
    for (auto& curr_bucket : buckets_array_) {
      Bucket* bucket = &curr_bucket;
      while (bucket != nullptr) {
        Bucket* next_bucket = bucket->next.get();
        uint64_t j;
        for (j = 0; j < bucket->BucketSize(); ++j) {
          Entry* entry = &bucket->getEntry(j);
          if (entry->state == USED) {
            function(handle, src, entry->element, args...);
          } else if (entry->state != EMPTY) {
            printf(
                "Entry in PENDING state"
                " while iterating over entries\n");
          }
        }
        bucket = next_bucket;
      }
    }
  }
  // Custom ForEach for the Local Edge Index
  template <typename ApplyFunT, typename SrcT, typename... Args>
  void ForEachNeighbor(ApplyFunT&& function, SrcT src, Args... args) {
    for (auto& curr_bucket : buckets_array_) {
      Bucket* bucket = &curr_bucket;
      while (bucket != nullptr) {
        Bucket* next_bucket = bucket->next.get();
        uint64_t j;
        for (j = 0; j < bucket->BucketSize(); ++j) {
          Entry* entry = &bucket->getEntry(j);
          if (entry->state == USED) {
            function(src, entry->element, args...);
          } else if (entry->state != EMPTY) {
            printf(
                "Entry in PENDING state"
                " while iterating over entries\n");
          }
        }
        bucket = next_bucket;
      }
    }
  }
};

template <typename T, typename ELEM_COMPARE>
bool LocalSet<T, ELEM_COMPARE>::Find(const T& element) {
  size_t bucketIdx = shad::hash<T>{}(element) % numBuckets_;
  Bucket* bucket = &(buckets_array_[bucketIdx]);

  while (bucket != nullptr) {
    for (size_t i = 0; i < bucket->BucketSize(); ++i) {
      Entry* entry = &bucket->getEntry(i);
      // Stop at the first empty entry.
      if (entry->state == EMPTY) return false;
      // Yield on pending entries.
      while (entry->state == PENDING_INSERT) rt::impl::yield();
      // Entry is USED.
      if (ElemComp_(&entry->element, &element) == 0) {
        return true;
      }
    }
    bucket = bucket->next.get();
  }
  return false;
}

template <typename T, typename ELEM_COMPARE>
void LocalSet<T, ELEM_COMPARE>::PrintAllElements() {
  for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx++) {
    size_t pos = 0;
    Bucket* bucket = &(buckets_array_[bucketIdx]);
    std::cout << "Bucket: " << bucketIdx << std::endl;
    while (bucket != nullptr) {
      for (size_t i = 0; i < bucket->BucketSize(); ++i, ++pos) {
        Entry* entry = &bucket->getEntry(i);
        // Stop at the first empty entry.
        if (entry->state == EMPTY) break;
        // Yield on pending entries.
        while (entry->state == PENDING_INSERT) rt::impl::yield();
        std::cout << pos << ": [" << entry->element << "]\n";
      }
      bucket = bucket->next.get();
    }
  }
}

template <typename T, typename ELEM_COMPARE>
void LocalSet<T, ELEM_COMPARE>::Erase(const T& element) {
  size_t bucketIdx = shad::hash<T>{}(element) % numBuckets_;
  Bucket* bucket = &(buckets_array_[bucketIdx]);
  Entry* prevEntry = nullptr;
  Entry* toDelete = nullptr;
  Entry* lastEntry = nullptr;
  for (;;) {
    for (size_t i = 0; i < bucket->BucketSize(); ++i) {
      Entry* entry = &bucket->getEntry(i);
      // 1. Key not found, returning
      if (entry->state == EMPTY) {
        if (toDelete != nullptr)
          throw std::logic_error(
              "A problem occured with"
              "the set erase operation");
        return;
      }
      while (entry->state == PENDING_INSERT) {
        rt::impl::yield();
      }
      if (ElemComp_(&entry->element, &element) == 0) {
        // 2. Key found, try to acquire a lock on it
        if (!__sync_bool_compare_and_swap(&entry->state, USED,
                                          PENDING_INSERT)) {
          Erase(element);
          return;
        }
        // 3. The entry to remove has been found,
        // and its status set to PENDING_INSERT
        toDelete = entry;
        prevEntry = entry;
        --size_;

        // now look for the last two entries.
        size_t j = i + 1;
        for (;;) {
          for (; j < bucket->BucketSize(); ++j) {
            lastEntry = &bucket->getEntry(j);
            if (__sync_bool_compare_and_swap(&lastEntry->state, EMPTY,
                                             PENDING_INSERT)) {
              // 3. Last entry found (EMPTY->PENDING)
              if (prevEntry == toDelete) {  // just set it to EMPTY and return;
                lastEntry->state = EMPTY;
                toDelete->state = EMPTY;
                return;
              }
              // STATUS:
              // toDelete found and status is PENDING_INSERT
              // lastEntry found and status is PENDING_INSERT
              // need to find prevEntry

              if (!__sync_bool_compare_and_swap(&prevEntry->state, USED,
                                                PENDING_INSERT)) {
                rt::impl::yield();
                lastEntry->state = EMPTY;
                toDelete->state = USED;
                ++size_;
                Erase(element);
                return;
              }
              // now prevEntry is locked
              // 4. free the last entry
              lastEntry->state = EMPTY;
              // move prevEntry into toDelete
              toDelete->element = std::move(prevEntry->element);
              toDelete->state = USED;
              // free prevEntry
              prevEntry->state = EMPTY;
              return;
            } else {
              if (lastEntry->state == PENDING_INSERT) {
                toDelete->state = USED;
                ++size_;
                Erase(element);
                return;
              }
            }
            prevEntry = lastEntry;
          }
          j = 0;
          if (bucket->next != nullptr) {
            bucket = bucket->next.get();
          } else {
            // STATUS last entry is not empty and has not been locked
            if (lastEntry == nullptr) {
              // toDelete has not been found or
              // it is the last entry at the end of the last bucket
              if (toDelete != nullptr) toDelete->state = EMPTY;
              return;
            }
            if (!__sync_bool_compare_and_swap(&lastEntry->state, USED,
                                              PENDING_INSERT)) {
              toDelete->state = USED;
              ++size_;
              Erase(element);
              return;
            }
            if (lastEntry == prevEntry) {
              if (toDelete == prevEntry) {
                // No move is necessary, just set to EMPTY
                lastEntry->state = EMPTY;
                toDelete->state = EMPTY;
                return;
              } else {
                toDelete->element = std::move(lastEntry->element);
                toDelete->state = USED;
                lastEntry->state = EMPTY;
                return;
              }
            } else {
              // FIXME double check if this state is reachable
              if (toDelete == prevEntry) {
                toDelete->element = std::move(lastEntry->element);
                toDelete->state = USED;
                lastEntry->state = EMPTY;
                return;
              } else {
                // Need to lock prev entry as well
                while (!__sync_bool_compare_and_swap(&prevEntry->state, USED,
                                                     PENDING_INSERT)) {
                  rt::impl::yield();
                }
                lastEntry->state = EMPTY;
                toDelete->element = std::move(prevEntry->element);
                toDelete->state = USED;
                prevEntry->state = EMPTY;
              }
            }
            return;
          }
        }
      }
      prevEntry = entry;
    }
    if (bucket->next != nullptr) {
      bucket = bucket->next.get();
    } else {
      return;
    }
  }
}

template <typename T, typename ELEM_COMPARE>
void LocalSet<T, ELEM_COMPARE>::AsyncErase(rt::Handle& handle,
                                           const T& element) {
  auto args = std::tuple<LocalSet<T, ELEM_COMPARE>*, T>(this, element);
  auto eraseLambda = [](rt::Handle&,
                        const std::tuple<LocalSet<T, ELEM_COMPARE>*, T>& t) {
    (std::get<0>(t))->Erase(std::get<1>(t));
  };
  rt::asyncExecuteAt(handle, rt::thisLocality(), eraseLambda, args);
}

template <typename T, typename ELEM_COMPARE>
std::pair<typename LocalSet<T, ELEM_COMPARE>::iterator, bool>
LocalSet<T, ELEM_COMPARE>::Insert(const T& element) {
  size_t bucketIdx = shad::hash<T>{}(element) % numBuckets_;
  Bucket* bucket = &(buckets_array_[bucketIdx]);

  // Forever or until we find an insertion point.
  for (;;) {
    for (size_t i = 0; i < bucket->BucketSize(); ++i) {
      Entry* entry = &bucket->getEntry(i);

      if (__sync_bool_compare_and_swap(&entry->state, EMPTY, PENDING_INSERT)) {
        entry->element = std::move(element);
        ++size_;
        entry->state = USED;
        return std::make_pair(iterator(this, bucketIdx, i, bucket, entry),
                              true);
      } else {
        while (entry->state == PENDING_INSERT) rt::impl::yield();
        if (ElemComp_(&entry->element, &element) == 0) {
          return std::make_pair(iterator(this, bucketIdx, i, bucket, entry),
                                false);
        }
      }
    }

    if (bucket->next == nullptr) {
      // We need to allocate a new buffer
      if (__sync_bool_compare_and_swap(&bucket->isNextAllocated, false, true)) {
        // Allocate the bucket
        std::shared_ptr<Bucket> newBucket(
            new Bucket(constants::kSetDefaultNumEntriesPerBucket));
        bucket->next.swap(newBucket);
      } else {
        // Wait for the allocation to happen
        while (bucket->next == nullptr) rt::impl::yield();
      }
    }

    bucket = bucket->next.get();
  }
}

template <typename T, typename ELEM_COMPARE>
void LocalSet<T, ELEM_COMPARE>::AsyncInsert(rt::Handle& handle,
                                            const T& element) {
  auto args = std::tuple<LocalSet<T, ELEM_COMPARE>*, T>(this, element);
  auto insertLambda = [](rt::Handle&,
                         const std::tuple<LocalSet<T, ELEM_COMPARE>*, T>& t) {
    (std::get<0>(t))->Insert(std::get<1>(t));
  };
  rt::asyncExecuteAt(handle, rt::thisLocality(), insertLambda, args);
}

template <typename T, typename ELEM_COMPARE>
void LocalSet<T, ELEM_COMPARE>::AsyncFind(rt::Handle& handle, const T& element,
                                          bool* found) {
  auto args =
      std::tuple<LocalSet<T, ELEM_COMPARE>*, T, bool*>(this, element, found);
  auto findLambda =
      [](rt::Handle&,
         const std::tuple<LocalSet<T, ELEM_COMPARE>*, T, bool*>& t) {
        *std::get<2>(t) = (std::get<0>(t))->Find(std::get<1>(t));
      };
  rt::asyncExecuteAt(handle, rt::thisLocality(), findLambda, args);
}

template <typename T, typename ELEM_COMPARE>
template <typename ApplyFunT, typename... Args>
void LocalSet<T, ELEM_COMPARE>::ForEachElement(ApplyFunT&& function,
                                               Args&... args) {
  using FunctionTy = void (*)(const T&, Args&...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using ArgsTuple =
      std::tuple<LocalSet<T, ELEM_COMPARE>*, FunctionTy, std::tuple<Args...>>;
  ArgsTuple argsTuple(this, fn, std::tuple<Args...>(args...));
  // FIXME for "small" sizes it may be better to go serially
  rt::forEachAt(rt::thisLocality(),
                ForEachElementFunWrapper<ArgsTuple, Args...>, argsTuple,
                numBuckets_);
}

template <typename T, typename ELEM_COMPARE>
template <typename ApplyFunT, typename... Args>
void LocalSet<T, ELEM_COMPARE>::AsyncForEachElement(rt::Handle& handle,
                                                    ApplyFunT&& function,
                                                    Args&... args) {
  using FunctionTy = void (*)(rt::Handle&, const T&, Args&...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using ArgsTuple =
      std::tuple<LocalSet<T, ELEM_COMPARE>*, FunctionTy, std::tuple<Args...>>;
  ArgsTuple argsTuple(this, fn, std::tuple<Args...>(args...));
  // FIXME for "small" sizes it may be better to go serially
  rt::asyncForEachAt(handle, rt::thisLocality(),
                     AsyncForEachElementFunWrapper<ArgsTuple, Args...>,
                     argsTuple, numBuckets_);
}

template <typename LSet, typename T>
class lset_iterator : public std::iterator<std::forward_iterator_tag, T> {
  template <typename, typename, typename>
  friend class set_iterator;

 public:
  using value_type = T;
  using Entry = typename LSet::Entry;
  using State = typename LSet::State;
  using Bucket = typename LSet::Bucket;

  lset_iterator(){};
  lset_iterator(const LSet* setPtr, size_t bId, size_t pos, Bucket* cb,
                Entry* ePtr)
      : setPtr_(setPtr),
        bucketId_(bId),
        position_(pos),
        currBucket_(cb),
        entryPtr_(ePtr) {}

  static lset_iterator lset_begin(const LSet* setPtr) {
    Bucket* rootPtr = &(const_cast<LSet*>(setPtr)->buckets_array_[0]);
    Entry* firstEntry = &(rootPtr->getEntry(0));
    lset_iterator beg(setPtr, 0, 0, rootPtr, firstEntry);
    if (firstEntry->state == LSet::USED) {
      return beg;
    }
    return ++beg;
  }

  static lset_iterator lset_end(const LSet* setPtr) {
    return lset_end(setPtr->numBuckets_);
  }

  static lset_iterator lset_end(size_t numBuckets) {
    return lset_iterator(nullptr, numBuckets, 0, nullptr, nullptr);
  }
  bool operator==(const lset_iterator& other) const {
    return entryPtr_ == other.entryPtr_;
  }
  bool operator!=(const lset_iterator& other) const {
    return !(*this == other);
  }

  T operator*() const { return entryPtr_->element; }

  lset_iterator& operator++() {
    ++position_;
    if (position_ < constants::kSetDefaultNumEntriesPerBucket) {
      entryPtr_++;
      if (entryPtr_->state == LSet::USED) {
        return *this;
      }
      position_ = 0;
    } else {
      position_ = 0;
      currBucket_ = currBucket_->next.get();
      if (currBucket_ != nullptr) {
        entryPtr_ = &currBucket_->getEntry(position_);
        if (entryPtr_->state == LSet::USED) {
          return *this;
        }
      }
    }
    // check the first entry of the following bucket lists
    for (++bucketId_; bucketId_ < setPtr_->numBuckets_; ++bucketId_) {
      currBucket_ = &const_cast<LSet*>(setPtr_)->buckets_array_[bucketId_];
      entryPtr_ = &currBucket_->getEntry(position_);
      if (entryPtr_->state == LSet::USED) {
        return *this;
      }
    }
    // next it not found, returning end iterator (n, 0, nullptr)
    setPtr_ = nullptr;
    entryPtr_ = nullptr;
    currBucket_ = nullptr;
    return *this;
  }
  lset_iterator operator++(int) {
    lset_iterator tmp = *this;
    operator++();
    return tmp;
  }

  class partition_range {
   public:
    partition_range(const lset_iterator& begin, const lset_iterator& end)
        : begin_(begin), end_(end) {}
    lset_iterator begin() { return begin_; }
    lset_iterator end() { return end_; }

   private:
    lset_iterator begin_;
    lset_iterator end_;
  };

  // split a range into at most n_parts non-empty sub-ranges
  static std::vector<partition_range> partitions(lset_iterator begin,
                                                 lset_iterator end,
                                                 size_t n_parts) {
    std::vector<partition_range> res;

    auto n_buckets = n_spanned_buckets(begin, end);

    if (n_buckets && n_parts) {
      auto part_step =
          (n_buckets >= n_parts) ? (n_buckets + n_parts - 1) / n_parts : 1;
      auto set_ptr = begin.setPtr_;
      auto& buckets = set_ptr->buckets_array_;
      auto b_end =
          (end != lset_end(set_ptr)) ? end.bucketId_ : set_ptr->numBuckets_;
      auto bi = begin.bucketId_;
      auto pbegin = begin;
      while (true) {
        bi = first_used_bucket(set_ptr, bi + part_step);
        if (bi < b_end) {
          auto pend = first_in_bucket(set_ptr, bi);
          assert(pbegin != pend);
          res.push_back(partition_range{pbegin, pend});
          pbegin = pend;
        } else {
          if (pbegin != end) {
            res.push_back(partition_range{pbegin, end});
          }
          break;
        }
      }
    }

    return res;
  }

 private:
  const LSet* setPtr_;
  size_t bucketId_;
  size_t position_;
  Bucket* currBucket_;
  Entry* entryPtr_;

  // returns a pointer to the first entry of a bucket
  static typename LSet::Entry& first_bucket_entry(const LSet* setPtr_,
                                                  size_t bi) {
    assert(setPtr_);
    assert(bi < setPtr_->numBuckets_);
    return const_cast<LSet*>(setPtr_)->buckets_array_[bi].getEntry(0);
  }

  // returns an iterator pointing to the beginning of the first active bucket
  // from the input bucket (included)
  static lset_iterator first_in_bucket(const LSet* setPtr_, size_t bi) {
    assert(setPtr_);
    assert(bi < setPtr_->numBuckets_);

    auto& entry = first_bucket_entry(setPtr_, bi);

    // sanity check - bucket is used
    assert(entry.state == LSet::USED);

    return lset_iterator(setPtr_, bi, 0,
                         &const_cast<LSet*>(setPtr_)->buckets_array_[bi],
                         &entry);
  }

  // returns the index of the first active bucket, starting from the input
  // bucket (included). If not such bucket, it returns the number of buckets.
  static size_t first_used_bucket(const LSet* setPtr_, size_t bi) {
    assert(setPtr_);
    // scan for the first used entry with the same logic as operator++
    for (; bi < setPtr_->numBuckets_; ++bi)
      if (first_bucket_entry(setPtr_, bi).state == LSet::USED) return bi;
    return setPtr_->numBuckets_;
  }

  // returns the number of buckets spanned by the input range
  static size_t n_spanned_buckets(const lset_iterator& begin,
                                  const lset_iterator& end) {
    if (begin != end) {
      auto set_ptr = begin.setPtr_;
      assert(set_ptr);

      // invariant check - end is either:
      // - the end of the set; or
      // - an iterator pointing to an used entry
      assert(end == lset_end(set_ptr) ||
             first_bucket_entry(set_ptr, end.bucketId_).state == LSet::USED);

      if (end != lset_end(set_ptr)) {
        // count one more if end is not on a bucket edge
        return end.bucketId_ - begin.bucketId_ +
               (end.entryPtr_ !=
                &first_bucket_entry(end.setPtr_, end.bucketId_));
      }
      return set_ptr->numBuckets_ - begin.bucketId_;
    }
    return 0;
  }
};

}  // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_LOCAL_SET_H_
