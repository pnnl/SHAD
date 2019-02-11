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

#ifndef INCLUDE_SHAD_DATA_STRUCTURES_LOCAL_HASHMAP_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_LOCAL_HASHMAP_H_

#include <algorithm>
#include <atomic>
#include <functional>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "shad/data_structures/compare_and_hash_utils.h"
#include "shad/runtime/runtime.h"

namespace shad {

namespace constants {
constexpr size_t kDefaultNumEntriesPerBucket = 128;
}

template <typename LMap, typename T>
class lmap_iterator;

template <typename T>
struct Overwriter {
  bool operator()(T *const lhs, const T &rhs, bool) {
    *lhs = std::move(rhs);
    return true;
  }
  static bool Insert(T *const lhs, const T &rhs, bool) {
    *lhs = std::move(rhs);
    return true;
  }
};

template <typename T>
struct Updater {
  bool operator()(T *const lhs, const T &rhs, bool same_key) {
    if (!same_key) {
      *lhs = std::move(rhs);
      return true;
    }
    return false;
  }
  static bool Insert(T *const lhs, const T &rhs, bool same_key) {
    if (!same_key) {
      *lhs = std::move(rhs);
      return true;
    }
    return false;
  }
};

/// @brief The LocalHashmap data structure.
///
/// SHAD's LocalHashmap is a "local", thread-safe, associative container.
/// LocalHashmaps can be used ONLY on the Locality
/// on which they are created.
/// @tparam KTYPE type of the hashmap keys.
/// @tparam VTYPE type of the hashmap values.
/// @tparam KEY_COMPARE key comparison function; default is MemCmp<KTYPE>.
/// @tparam INSERTER default is Overwriter
/// (i.e. insertions overwrite previous values
///  associated to the same key, if any).
template <typename KTYPE, typename VTYPE, typename KEY_COMPARE = MemCmp<KTYPE>,
          typename INSERTER = Overwriter<VTYPE>>
class LocalHashmap {
  template <typename, typename, typename, typename>
  friend class Hashmap;
  friend class lmap_iterator<LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>,
                             const std::pair<KTYPE, VTYPE>>;
  friend class lmap_iterator<LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>,
                             const std::pair<KTYPE, VTYPE>>;
  template <typename, typename, typename>
  friend class map_iterator;

 public:
  using value_type = std::pair<KTYPE, VTYPE>;
  using iterator =
      lmap_iterator<LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>,
                    const std::pair<KTYPE, VTYPE>>;
  using const_iterator =
      lmap_iterator<LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>,
                    const std::pair<KTYPE, VTYPE>>;
  /// @brief Constructor.
  /// @param numInitBuckets initial number of Buckets.
  explicit LocalHashmap(const size_t numInitBuckets)
      : numBuckets_(numInitBuckets), buckets_array_(numInitBuckets), size_(0) {}

  /// @brief Size of the hashmap (number of entries).
  /// @return the size of the hashmap.
  size_t Size() const { return size_.load(); }

  /// @brief Insert a key-value pair in the hashmap.
  /// @param[in] key the key.
  /// @param[in] value the value to copy into the hashMap.
  /// @return an iterator to the inserted value
  std::pair<iterator, bool> Insert(const KTYPE &key, const VTYPE &value);

  template <typename ELTYPE>
  std::pair<iterator, bool> Insert(const KTYPE &key, const ELTYPE &value);

  /// @brief Asynchronously Insert a key-value pair in the hashmap.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle
  /// to be used to wait for completion.
  /// @param[in] key the key.
  /// @param[in] value the value to copy into the hashMap.
  /// @return a pointer to the value if the the key-value was inserted
  ///        or a pointer to a previously inserted value.
  void AsyncInsert(rt::Handle &handle, const KTYPE &key, const VTYPE &value);

  template <typename ELTYPE>
  void AsyncInsert(rt::Handle &handle, const KTYPE &key, const ELTYPE &value);
  /// @brief Remove a key-value pair from the hashmap.
  /// @param[in] key the key.
  void Erase(const KTYPE &key);

  /// @brief Asynchronously remove a key-value pair from the hashmap.
  /// @warning Asynchronous operations are guaranteed to have completed.
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle.
  /// to be used to wait for completion.
  /// @param[in] key the key.
  void AsyncErase(rt::Handle &handle, const KTYPE &key);

  /// @brief Clear the content of the hashmap.
  void Clear() {
    size_ = 0;
    buckets_array_.clear();
    buckets_array_ = std::vector<Bucket>(numBuckets_);
  }
  /// @brief Get the value associated to a key.
  /// @param[in] key the key.
  /// @param[out] res the address where to store the value,
  /// if the the key-value is found.
  /// @return true if the entry is found, false otherwise.
  bool Lookup(const KTYPE &key, VTYPE *res) {
    VTYPE *result = Lookup(key);
    if (result != nullptr) *res = *result;
    return result != nullptr;
  }

  /// @brief Get the value associated to a key.
  /// @param[in] key the key.
  /// @return a pointer to the value if the the key-value is found
  ///         and nullptr if it does not exists.
  VTYPE *Lookup(const KTYPE &key);

  /// @brief Asynchronously get the value associated to a key.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle
  /// to be used to wait for completion.
  /// @param[in] key the key.
  /// @param[out] res the address where to storethe pointer to the value
  ///                 if the the key-value was found,
  ///                 or a nullptr otherwise.
  void AsyncLookup(rt::Handle &handle, const KTYPE &key, VTYPE **res);

  /// @brief Result for the
  /// Lookup(const KTYPE&, LookupResult*) and
  /// AsyncLookup(rt::Handle&, const KTYPE&, LookupResult*) methods.
  struct LookupResult {
    /// True if the key has been found in the Hashmap
    bool found;
    /// The value associated with the key.
    VTYPE value;
  };

  /// @brief Lookup method.
  /// @param[in] key The key.
  /// @param[out] res The result of the lookup operation.
  void Lookup(const KTYPE &key, LookupResult *res) {
    VTYPE empty;
    VTYPE *result = Lookup(key);
    if (result != nullptr) res->value = *result;
    res->found = (result != nullptr);
  }

  /// @brief Asynchronous lookup method.
  ///
  /// @warning Asynchronous operations are guaranteed to have completed.  only
  /// after calling the rt::waitForCompletion(rt::Handle &handle) method.
  ///
  /// @param[in,out] handle Reference to the handle.  to be used to wait for
  /// completion.
  /// @param[in] key The key.
  /// @param[out] res The result of the lookup operation.
  void AsyncLookup(rt::Handle &handle, const KTYPE &key, LookupResult *res);

  /// @brief Apply a user-defined function to a key-value pair.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(const KTYPE&, VTYPE&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in] key The key.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void Apply(const KTYPE &key, ApplyFunT &&function, Args &... args) {
    VTYPE *value = Lookup(key);
    if (value != nullptr) {
      function(key, *value, args...);
    }
  }

  /// @brief Asynchronously apply a user-defined function to a key-value pair.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(rt::Handle &handle, const KTYPE&, VTYPE&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle Reference to the handle.
  /// @param[in] key The key.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncApply(rt::Handle &handle, const KTYPE &key, ApplyFunT &&function,
                  Args &... args);

  /// @brief Apply a user-defined function to each key-value pair.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(const KTYPE&, VTYPE&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void ForEachEntry(ApplyFunT &&function, Args &... args);

  /// @brief Asynchronously apply a user-defined function to each key-value
  /// pair.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(shad::rt::Handle&, const KTYPE&, VTYPE&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @warning Asynchronous operations are guaranteed to have completed.  only
  /// after calling the rt::waitForCompletion(rt::Handle &handle) method.
  ///
  /// @param[in,out] handle Reference to the handle.  to be used to wait for
  /// completion.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncForEachEntry(rt::Handle &handle, ApplyFunT &&function,
                         Args &... args);

  /// @brief Apply a user-defined function to each key.
  /// @tparam ApplyFunT User-defined function type.
  /// The function prototype should be:
  /// @code
  /// void(const KTYPE&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void ForEachKey(ApplyFunT &&function, Args &... args);

  /// @brief Asynchronously apply a user-defined function to each key.
  /// @tparam ApplyFunT User-defined function type.
  /// The function prototype should be:
  /// @code
  /// void(shad::rt::Handle&, const KTYPE&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  /// @warning Asynchronous operations are guaranteed to have completed.
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle.
  /// to be used to wait for completion.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncForEachKey(rt::Handle &handle, ApplyFunT &&function,
                       Args &... args);

  /// @brief Print all the entries in the hashmap.
  /// @warning std::ostream & operator<< must be defined for both
  /// KTYPE and VTYPE
  void PrintAllEntries();

  iterator begin() {
    Entry *firstEntry = &buckets_array_[0].getEntry(0);
    iterator cbeg(this, 0, 0, &buckets_array_[0], firstEntry);
    if (firstEntry->state == USED) {
      return cbeg;
    }
    return ++cbeg;
  }

  iterator end() { return iterator::lmap_end(numBuckets_); }

  const_iterator cbegin() {
    Entry *firstEntry = &buckets_array_[0].getEntry(0);
    const_iterator cbeg(this, 0, 0, &buckets_array_[0], firstEntry);
    if (firstEntry->state == USED) {
      return cbeg;
    }
    return ++cbeg;
  }

  const_iterator cend() { return const_iterator::lmap_end(numBuckets_); }

 private:
  static const size_t kNumEntriesPerBucket =
      constants::kDefaultNumEntriesPerBucket;
  static const size_t kAllocPending = 0x1;
  static const uint32_t kKeyWords = sizeof(KTYPE) > sizeof(uint64_t)
                                        ? sizeof(KTYPE) / sizeof(uint64_t)
                                        : 1;
  static const uint8_t kHashSeed = 0;

  typedef KEY_COMPARE KeyCompare;

  enum State { EMPTY, USED, PENDING_INSERT, PENDING_UPDATE };

  struct Entry {
    KTYPE key;
    VTYPE value;
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

    Entry &getEntry(size_t i) {
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

  INSERTER InsertPolicy_;
  KeyCompare KeyComp_;
  size_t numBuckets_;
  std::vector<Bucket> buckets_array_;
  std::atomic<size_t> size_;

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void CallForEachEntryFun(
      const size_t i, LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *mapPtr,
      ApplyFunT function, std::tuple<Args...> &args,
      std::index_sequence<is...>) {
    Bucket *bucket = &mapPtr->buckets_array_[i];
    while (bucket != nullptr) {
      Bucket *next_bucket = bucket->next.get();
      uint64_t j;
      for (j = 0; j < bucket->BucketSize(); ++j) {
        Entry *entry = &bucket->getEntry(j);
        if (entry->state == USED) {
          function(entry->key, entry->value, std::get<is>(args)...);
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
  static void ForEachEntryFunWrapper(const Tuple &args, size_t i) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<2>(args))>::type>::value;
    Tuple &tuple = const_cast<Tuple &>(args);
    CallForEachEntryFun(i, std::get<0>(tuple), std::get<1>(tuple),
                        std::get<2>(tuple), std::make_index_sequence<Size>{});
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void AsyncCallForEachEntryFun(
      rt::Handle &handle, const size_t i,
      LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *mapPtr,
      ApplyFunT function, std::tuple<Args...> &args,
      std::index_sequence<is...>) {
    Bucket *bucket = &mapPtr->buckets_array_[i];
    while (bucket != nullptr) {
      Bucket *next_bucket = bucket->next.get();
      uint64_t j;
      for (j = 0; j < bucket->BucketSize(); ++j) {
        Entry *entry = &bucket->getEntry(j);
        if (entry->state == USED) {
          function(handle, entry->key, entry->value, std::get<is>(args)...);
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
  static void AsyncForEachEntryFunWrapper(rt::Handle &handle, const Tuple &args,
                                          size_t i) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<2>(args))>::type>::value;
    Tuple &tuple = const_cast<Tuple &>(args);
    AsyncCallForEachEntryFun(handle, i, std::get<0>(tuple), std::get<1>(tuple),
                             std::get<2>(tuple),
                             std::make_index_sequence<Size>{});
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void CallForEachKeyFun(
      const size_t i, LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *mapPtr,
      ApplyFunT function, std::tuple<Args...> &args,
      std::index_sequence<is...>) {
    Bucket *buckets_array = mapPtr->buckets_array_.data();
    Bucket *bucket = &buckets_array[i];
    size_t cnt = 0;
    while (bucket != nullptr) {
      Bucket *next_bucket = bucket->next.get();
      uint64_t j;
      for (j = 0; j < bucket->BucketSize(); ++j) {
        Entry *entry = &bucket->getEntry(j);
        if (entry->state == USED) {
          function(entry->key, std::get<is>(args)...);
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
  static void ForEachKeyFunWrapper(const Tuple &args, size_t i) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<2>(args))>::type>::value;
    Tuple &tuple = const_cast<Tuple &>(args);
    CallForEachKeyFun(i, std::get<0>(tuple), std::get<1>(tuple),
                      std::get<2>(tuple), std::make_index_sequence<Size>{});
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void AsyncCallForEachKeyFun(
      rt::Handle &handle, const size_t i,
      LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *mapPtr,
      ApplyFunT function, std::tuple<Args...> &args,
      std::index_sequence<is...>) {
    Bucket *buckets_array = mapPtr->buckets_array_.data();
    Bucket *bucket = &buckets_array[i];
    while (bucket != nullptr) {
      Bucket *next_bucket = bucket->next.get();
      uint64_t j;
      for (j = 0; j < bucket->BucketSize(); ++j) {
        Entry *entry = &bucket->getEntry(j);
        if (entry->state == USED) {
          function(handle, entry->key, std::get<is>(args)...);
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
  static void AsyncForEachKeyFunWrapper(rt::Handle &handle, const Tuple &args,
                                        size_t i) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<2>(args))>::type>::value;
    Tuple &tuple = const_cast<Tuple &>(args);
    AsyncCallForEachKeyFun(handle, i, std::get<0>(tuple), std::get<1>(tuple),
                           std::get<2>(tuple),
                           std::make_index_sequence<Size>{});
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void AsyncCallApplyFun(
      rt::Handle &handle,
      LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *mapPtr,
      const KTYPE &key, ApplyFunT function, std::tuple<Args...> &args,
      std::index_sequence<is...>) {
    size_t bucketIdx = shad::hash<KTYPE>{}(key) % mapPtr->numBuckets_;
    Bucket *bucket = &(mapPtr->buckets_array_[bucketIdx]);

    while (bucket != nullptr) {
      for (size_t i = 0; i < bucket->BucketSize(); ++i) {
        Entry *entry = &bucket->getEntry(i);

        // Stop at the first empty entry.
        if (entry->state == EMPTY) break;

        // Yield on pending entries.
        while (entry->state == PENDING_INSERT) rt::impl::yield();

        // Entry is USED.
        if (mapPtr->KeyComp_(&entry->key, &key) == 0) {
          // wait for updates before returning
          while (entry->state == PENDING_UPDATE) rt::impl::yield();
          function(handle, key, entry->value, std::get<is>(args)...);
          return;
        }
      }

      bucket = bucket->next.get();
    }
    return;
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void CallApplyFun(
      LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *mapPtr,
      const KTYPE &key, ApplyFunT function, std::tuple<Args...> &args,
      std::index_sequence<is...>) {
    size_t bucketIdx = shad::hash<KTYPE>{}(key) % mapPtr->numBuckets_;
    Bucket *bucket = &(mapPtr->buckets_array_[bucketIdx]);

    while (bucket != nullptr) {
      for (size_t i = 0; i < bucket->BucketSize(); ++i) {
        Entry *entry = &bucket->getEntry(i);

        // Stop at the first empty entry.
        if (entry->state == EMPTY) break;

        // Yield on pending entries.
        while (entry->state == PENDING_INSERT) rt::impl::yield();

        // Entry is USED.
        if (mapPtr->KeyComp_(&entry->key, &key) == 0) {
          // wait for updates before returning
          while (entry->state == PENDING_UPDATE) rt::impl::yield();
          function(key, entry->value, std::get<is>(args)...);
          return;
        }
      }

      bucket = bucket->next.get();
    }
    return;
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
};

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERTER>
VTYPE *LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::Lookup(
    const KTYPE &key) {
  size_t bucketIdx = shad::hash<KTYPE>{}(key) % numBuckets_;
  Bucket *bucket = &(buckets_array_[bucketIdx]);

  VTYPE *result = nullptr;
  while (bucket != nullptr && result == nullptr) {
    for (size_t i = 0; i < bucket->BucketSize(); ++i) {
      Entry *entry = &bucket->getEntry(i);

      // Stop at the first empty entry.
      if (entry->state == EMPTY) break;

      // Yield on pending entries.
      while (entry->state == PENDING_INSERT) rt::impl::yield();

      // Entry is USED.
      if (KeyComp_(&entry->key, &key) == 0) {
        // wait for updates before returning
        while (entry->state == PENDING_UPDATE) rt::impl::yield();
        result = &entry->value;
      }
    }

    bucket = bucket->next.get();
  }
  return result;
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERTER>
void LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::PrintAllEntries() {
  for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx++) {
    size_t pos = 0;
    Bucket *bucket = &(buckets_array_[bucketIdx]);
    std::cout << "Bucket: " << bucketIdx << std::endl;
    while (bucket != nullptr) {
      for (size_t i = 0; i < bucket->BucketSize(); ++i, ++pos) {
        Entry *entry = &bucket->getEntry(i);
        // Stop at the first empty entry.
        if (entry->state == EMPTY) break;
        // Yield on pending entries.
        while (entry->state == PENDING_INSERT ||
               entry->state == PENDING_UPDATE) {
          rt::impl::yield();
        }
        std::cout << pos << ": [" << entry->key << "] [" << entry->value
                  << "]\n";
      }
      bucket = bucket->next.get();
    }
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERTER>
void LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::Erase(
    const KTYPE &key) {
  size_t bucketIdx = shad::hash<KTYPE>{}(key) % numBuckets_;
  Bucket *bucket = &(buckets_array_[bucketIdx]);
  Entry *prevEntry = nullptr;
  Entry *toDelete = nullptr;
  Entry *lastEntry = nullptr;
  auto printEntryState = [](size_t num, Entry *todel, Entry *last,
                            Entry *prev) {
    size_t tds = todel->state, ls = 42, ps = 42;
    if (last != nullptr) ls = last->state;
    if (prev != nullptr) ps = prev->state;
    printf("loop %lu, todel-s: %lu, last-s: %lu, prev-s: %lu\n", num, tds, ls,
           ps);
  };
  for (;;) {
    for (size_t i = 0; i < bucket->BucketSize(); ++i) {
      Entry *entry = &bucket->getEntry(i);

      // 1. Key not found, returning
      if (entry->state == EMPTY) {
        if (toDelete != nullptr)
          throw std::logic_error(
              "A problem occured with"
              "the map erase operation");
        return;
      }
      while (entry->state == PENDING_INSERT) {
        rt::impl::yield();
      }

      if (KeyComp_(&entry->key, &key) == 0) {
        // 2. Key found, try to acquire a lock on it
        if (!__sync_bool_compare_and_swap(&entry->state, USED,
                                          PENDING_INSERT)) {
          // entry has already been deleted by another operation
          Erase(key);
          return;
        }
        // 3. The entry to remove has been found,
        // and its status set to PENDING_INSERT
        toDelete = entry;
        prevEntry = entry;
        size_--;

        // now look for the last two entries.
        size_t j = i + 1;
        for (;;) {
          size_t numBuck = 0;
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
                printEntryState(2, toDelete, lastEntry, prevEntry);
                rt::impl::yield();
                lastEntry->state = EMPTY;
                toDelete->state = USED;
                size_++;
                Erase(key);
                return;
              }
              // now prevEntry is locked
              // 4. free the last entry
              lastEntry->state = EMPTY;
              // move prevEntry into toDelete
              toDelete->key = std::move(prevEntry->key);
              toDelete->value = std::move(prevEntry->value);
              toDelete->state = USED;
              // free prevEntry
              prevEntry->state = EMPTY;
              return;
            } else {
              if (lastEntry->state == PENDING_INSERT) {
                toDelete->state = USED;
                size_++;
                Erase(key);
                return;
              }
            }
            prevEntry = lastEntry;
          }
          j = 0;
          numBuck++;
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
              size_++;
              Erase(key);
              return;
            }
            if (lastEntry == prevEntry) {
              if (toDelete == prevEntry) {
                // No move is necessary, just set to EMPTY
                lastEntry->state = EMPTY;
                toDelete->state = EMPTY;
                return;
              } else {
                toDelete->key = std::move(lastEntry->key);
                toDelete->value = std::move(lastEntry->value);
                toDelete->state = USED;
                lastEntry->state = EMPTY;
                return;
              }
            } else {
              // FIXME check if this state is reachable
              if (toDelete == prevEntry) {
                toDelete->key = std::move(lastEntry->key);
                toDelete->value = std::move(lastEntry->value);
                toDelete->state = USED;
                lastEntry->state = EMPTY;
                return;
              } else {
                // Need to lock prev entry as well
                while (!__sync_bool_compare_and_swap(&prevEntry->state, USED,
                                                     PENDING_INSERT)) {
                  rt::impl::yield();
                  printEntryState(6, toDelete, lastEntry, prevEntry);
                }
                lastEntry->state = EMPTY;
                toDelete->key = std::move(prevEntry->key);
                toDelete->value = std::move(prevEntry->value);
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

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERTER>
void LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::AsyncErase(
    rt::Handle &handle, const KTYPE &key) {
  using LMapPtr = LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *;
  auto args = std::tuple<LMapPtr, KTYPE>(this, key);
  auto eraseLambda = [](rt::Handle &, const std::tuple<LMapPtr, KTYPE> &t) {
    (std::get<0>(t))->Erase(std::get<1>(t));
  };
  rt::asyncExecuteAt(handle, rt::thisLocality(), eraseLambda, args);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERTER>
std::pair<typename LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::iterator,
          bool>
LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::Insert(const KTYPE &key,
                                                          const VTYPE &value) {
  size_t bucketIdx = shad::hash<KTYPE>{}(key) % numBuckets_;
  Bucket *bucket = &(buckets_array_[bucketIdx]);

  // Forever or until we find an insertion point.
  for (;;) {
    for (size_t i = 0; i < bucket->BucketSize(); ++i) {
      Entry *entry = &bucket->getEntry(i);

      if (__sync_bool_compare_and_swap(&entry->state, EMPTY, PENDING_INSERT)) {
        // First time insertion.
        entry->key = std::move(key);
        bool inserted = InsertPolicy_(&entry->value, value, false);
        size_ += 1;
        entry->state = USED;
        return std::make_pair(iterator(this, bucketIdx, i, bucket, entry),
                              inserted);
      } else {
        // Update of an existing entry
        while (entry->state == PENDING_INSERT) rt::impl::yield();

        if (KeyComp_(&entry->key, &key) == 0) {
          while (!__sync_bool_compare_and_swap(&entry->state, USED,
                                               PENDING_UPDATE))
            rt::impl::yield();

          auto inserted = InsertPolicy_(&entry->value, value, true);
          entry->state = USED;
          return std::make_pair(iterator(this, bucketIdx, i, bucket, entry),
                                inserted);
        }
      }
    }

    if (bucket->next == nullptr) {
      // We need to allocate a new buffer
      if (__sync_bool_compare_and_swap(&bucket->isNextAllocated, false, true)) {
        // Allocate the bucket
        std::shared_ptr<Bucket> newBucket(
            new Bucket(constants::kDefaultNumEntriesPerBucket));
        bucket->next.swap(newBucket);
      } else {
        // Wait for the allocation to happen
        while (bucket->next == nullptr) rt::impl::yield();
      }
    }

    bucket = bucket->next.get();
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERTER>
void LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::AsyncInsert(
    rt::Handle &handle, const KTYPE &key, const VTYPE &value) {
  using LMapPtr = LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *;
  auto args = std::tuple<LMapPtr, KTYPE, VTYPE>(this, key, value);
  auto insertLambda = [](rt::Handle &,
                         const std::tuple<LMapPtr, KTYPE, VTYPE> &t) {
    (std::get<0>(t))->Insert(std::get<1>(t), std::get<2>(t));
  };
  rt::asyncExecuteAt(handle, rt::thisLocality(), insertLambda, args);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERTER>
void LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::AsyncLookup(
    rt::Handle &handle, const KTYPE &key, VTYPE **result) {
  using LMapPtr = LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *;
  auto args = std::tuple<LMapPtr, KTYPE, VTYPE **>(this, key, result);
  auto lookupLambda = [](rt::Handle &,
                         const std::tuple<LMapPtr, KTYPE, VTYPE **> &t) {
    *std::get<2>(t) = (std::get<0>(t))->Lookup(std::get<1>(t));
  };
  rt::asyncExecuteAt(handle, rt::thisLocality(), lookupLambda, args);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERTER>
void LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::AsyncLookup(
    rt::Handle &handle, const KTYPE &key, LookupResult *result) {
  using LMapPtr = LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *;
  auto args = std::tuple<LMapPtr, KTYPE, LookupResult *>(this, key, result);
  auto lookupLambda = [](rt::Handle &,
                         const std::tuple<LMapPtr, KTYPE, LookupResult *> &t) {
    (std::get<0>(t))->Lookup(std::get<1>(t), std::get<2>(t));
  };
  rt::asyncExecuteAt(handle, rt::thisLocality(), lookupLambda, args);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERTER>
template <typename ApplyFunT, typename... Args>
void LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::ForEachEntry(
    ApplyFunT &&function, Args &... args) {
  using FunctionTy = void (*)(const KTYPE &, VTYPE &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using LMapPtr = LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *;
  using ArgsTuple = std::tuple<LMapPtr, FunctionTy, std::tuple<Args...>>;
  ArgsTuple argsTuple(this, fn, std::tuple<Args...>(args...));
  rt::forEachAt(rt::thisLocality(), ForEachEntryFunWrapper<ArgsTuple, Args...>,
                argsTuple, numBuckets_);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERTER>
template <typename ApplyFunT, typename... Args>
void LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::AsyncForEachEntry(
    rt::Handle &handle, ApplyFunT &&function, Args &... args) {
  using FunctionTy = void (*)(rt::Handle &, const KTYPE &, VTYPE &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using LMapPtr = LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *;
  using ArgsTuple = std::tuple<LMapPtr, FunctionTy, std::tuple<Args...>>;
  ArgsTuple argsTuple(this, fn, std::tuple<Args...>(args...));
  rt::asyncForEachAt(handle, rt::thisLocality(),
                     AsyncForEachEntryFunWrapper<ArgsTuple, Args...>, argsTuple,
                     numBuckets_);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERTER>
template <typename ApplyFunT, typename... Args>
void LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::ForEachKey(
    ApplyFunT &&function, Args &... args) {
  using FunctionTy = void (*)(const KTYPE &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);

  using LMapPtr = LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *;
  using ArgsTuple = std::tuple<LMapPtr, FunctionTy, std::tuple<Args...>>;
  ArgsTuple argsTuple(this, fn, std::tuple<Args...>(args...));

  rt::forEachAt(rt::thisLocality(), ForEachKeyFunWrapper<ArgsTuple, Args...>,
                argsTuple, numBuckets_);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERTER>
template <typename ApplyFunT, typename... Args>
void LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::AsyncForEachKey(
    rt::Handle &handle, ApplyFunT &&function, Args &... args) {
  using FunctionTy = void (*)(rt::Handle &, const KTYPE &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using LMapPtr = LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *;
  using ArgsTuple = std::tuple<LMapPtr, FunctionTy, std::tuple<Args...>>;
  ArgsTuple argsTuple(this, fn, std::tuple<Args...>(args...));
  rt::asyncForEachAt(handle, rt::thisLocality(),
                     AsyncForEachKeyFunWrapper<ArgsTuple, Args...>, argsTuple,
                     numBuckets_);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERTER>
template <typename ApplyFunT, typename... Args>
void LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::AsyncApply(
    rt::Handle &handle, const KTYPE &key, ApplyFunT &&function,
    Args &... args) {
  using FunctionTy = void (*)(rt::Handle &, const KTYPE &, VTYPE &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using LMapPtr = LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *;
  using ArgsTuple =
      std::tuple<LMapPtr, const KTYPE, FunctionTy, std::tuple<Args...>>;

  ArgsTuple argsTuple(this, key, fn, std::tuple<Args...>(args...));
  rt::asyncExecuteAt(handle, rt::thisLocality(),
                     AsyncApplyFunWrapper<ArgsTuple, Args...>, argsTuple);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERTER>
template <typename ELTYPE>
std::pair<typename LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::iterator,
          bool>
LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::Insert(const KTYPE &key,
                                                          const ELTYPE &value) {
  size_t bucketIdx = shad::hash<KTYPE>{}(key) % numBuckets_;
  Bucket *bucket = &(buckets_array_[bucketIdx]);

  // Forever or until we find an insertion point.
  for (;;) {
    for (size_t i = 0; i < bucket->BucketSize(); ++i) {
      Entry *entry = &bucket->getEntry(i);

      if (__sync_bool_compare_and_swap(&entry->state, EMPTY, PENDING_INSERT)) {
        // First time insertion.
        entry->key = std::move(key);
        auto inserted = INSERTER::Insert(&entry->value, value, false);
        size_ += 1;
        entry->state = USED;
        return std::make_pair(iterator(this, bucketIdx, i, bucket, entry),
                              inserted);
      } else {
        // Update of an existing entry
        while (entry->state == PENDING_INSERT) rt::impl::yield();

        if (KeyComp_(&entry->key, &key) == 0) {
          while (!__sync_bool_compare_and_swap(&entry->state, USED,
                                               PENDING_UPDATE))
            rt::impl::yield();

          auto inserted = INSERTER::Insert(&entry->value, value, true);
          entry->state = USED;
          return std::make_pair(iterator(this, bucketIdx, i, bucket, entry),
                                inserted);
        }
      }
    }

    if (bucket->next == nullptr) {
      // We need to allocate a new buffer
      if (__sync_bool_compare_and_swap(&bucket->isNextAllocated, false, true)) {
        // Allocate the bucket
        std::shared_ptr<Bucket> newBucket(
            new Bucket(constants::kDefaultNumEntriesPerBucket));
        bucket->next.swap(newBucket);
      } else {
        // Wait for the allocation to happen
        while (bucket->next == nullptr) rt::impl::yield();
      }
    }

    bucket = bucket->next.get();
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERTER>
template <typename ELTYPE>
void LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER>::AsyncInsert(
    rt::Handle &handle, const KTYPE &key, const ELTYPE &value) {
  using LMapPtr = LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERTER> *;
  auto args = std::tuple<LMapPtr, KTYPE, ELTYPE>(this, key, value);
  auto insertLambda = [](rt::Handle &,
                         const std::tuple<LMapPtr, KTYPE, ELTYPE> &t) {
    (std::get<0>(t))->Insert(std::get<1>(t), std::get<2>(t));
  };
  rt::asyncExecuteAt(handle, rt::thisLocality(), insertLambda, args);
}

template <typename LMap, typename T>
class lmap_iterator : public std::iterator<std::forward_iterator_tag, T> {
  template <typename, typename, typename>
  friend class map_iterator;

 public:
  using value_type = T;
  using Entry = typename LMap::Entry;
  using State = typename LMap::State;
  using Bucket = typename LMap::Bucket;

  lmap_iterator() {}
  lmap_iterator(const LMap *mapPtr, size_t bId, size_t pos, Bucket *cb,
                Entry *ePtr)
      : mapPtr_(mapPtr),
        bucketId_(bId),
        position_(pos),
        currBucket_(cb),
        entryPtr_(ePtr) {}

  static lmap_iterator lmap_begin(const LMap *mapPtr) {
    Bucket *rootPtr = &(const_cast<LMap *>(mapPtr)->buckets_array_[0]);
    Entry *firstEntry = &(rootPtr->getEntry(0));
    lmap_iterator beg(mapPtr, 0, 0, rootPtr, firstEntry);
    if (firstEntry->state == LMap::USED) {
      return beg;
    }
    return ++beg;
  }

  static lmap_iterator lmap_end(const LMap *mapPtr) {
    return lmap_end(mapPtr->numBuckets_);
  }

  static lmap_iterator lmap_end(size_t numBuckets) {
    return lmap_iterator(nullptr, numBuckets, 0, nullptr, nullptr);
  }
  bool operator==(const lmap_iterator &other) const {
    return entryPtr_ == other.entryPtr_;
  }
  bool operator!=(const lmap_iterator &other) const {
    return !(*this == other);
  }

  T operator*() const { return T(entryPtr_->key, entryPtr_->value); }

  lmap_iterator &operator++() {
    ++position_;
    if (position_ < constants::kDefaultNumEntriesPerBucket) {
      entryPtr_++;
      if (entryPtr_->state == LMap::USED) {
        return *this;
      }
      position_ = 0;
    } else {
      position_ = 0;
      currBucket_ = currBucket_->next.get();
      if (currBucket_ != nullptr) {
        entryPtr_ = &currBucket_->getEntry(position_);
        if (entryPtr_->state == LMap::USED) {
          return *this;
        }
      }
    }
    // check the first entry of the following bucket lists
    for (++bucketId_; bucketId_ < mapPtr_->numBuckets_; ++bucketId_) {
      currBucket_ = &const_cast<LMap *>(mapPtr_)->buckets_array_[bucketId_];
      entryPtr_ = &currBucket_->getEntry(position_);
      if (entryPtr_->state == LMap::USED) {
        return *this;
      }
    }
    // next it not found, returning end iterator (n, 0, nullptr)
    mapPtr_ = nullptr;
    entryPtr_ = nullptr;
    currBucket_ = nullptr;
    return *this;
  }
  lmap_iterator operator++(int) {
    lmap_iterator tmp = *this;
    operator++();
    return tmp;
  }

  class partition_range {
   public:
    partition_range(const lmap_iterator &begin, const lmap_iterator &end)
        : begin_(begin), end_(end) {}
    lmap_iterator begin() { return begin_; }
    lmap_iterator end() { return end_; }

   private:
    lmap_iterator begin_;
    lmap_iterator end_;
  };

  // split a range into at most n_parts non-empty sub-ranges
  static std::vector<partition_range> partitions(lmap_iterator begin,
                                                 lmap_iterator end,
                                                 size_t n_parts) {
    std::vector<partition_range> res;

    auto n_buckets = n_spanned_buckets(begin, end);

    if (n_buckets && n_parts) {
      auto part_step =
          (n_buckets >= n_parts) ? (n_buckets + n_parts - 1) / n_parts : 1;
      auto map_ptr = begin.mapPtr_;
      auto &buckets = map_ptr->buckets_array_;
      auto b_end =
          (end != lmap_end(map_ptr)) ? end.bucketId_ : map_ptr->numBuckets_;
      auto bi = begin.bucketId_;
      auto pbegin = begin;
      while (true) {
        bi = first_used_bucket(map_ptr, bi + part_step);
        if (bi < b_end) {
          auto pend = first_in_bucket(map_ptr, bi);
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
  const LMap *mapPtr_;
  size_t bucketId_;
  size_t position_;
  Bucket *currBucket_;
  Entry *entryPtr_;

  // returns a pointer to the first entry of a bucket
  static typename LMap::Entry &first_bucket_entry(const LMap *mapPtr_,
                                                  size_t bi) {
    assert(mapPtr_);
    assert(bi < mapPtr_->numBuckets_);
    return const_cast<LMap *>(mapPtr_)->buckets_array_[bi].getEntry(0);
  }

  // returns an iterator pointing to the beginning of the first active bucket
  // from the input bucket (included)
  static lmap_iterator first_in_bucket(const LMap *mapPtr_, size_t bi) {
    assert(mapPtr_);
    assert(bi < mapPtr_->numBuckets_);

    auto &entry = first_bucket_entry(mapPtr_, bi);

    // sanity check - bucket is used
    assert(entry.state == LMap::USED);

    return lmap_iterator(mapPtr_, bi, 0,
                         &const_cast<LMap *>(mapPtr_)->buckets_array_[bi],
                         &entry);
  }

  // returns the index of the first active bucket, starting from the input
  // bucket (included). If not such bucket, it returns the number of buckets.
  static size_t first_used_bucket(const LMap *mapPtr_, size_t bi) {
    assert(mapPtr_);
    // scan for the first used entry with the same logic as operator++
    for (; bi < mapPtr_->numBuckets_; ++bi)
      if (first_bucket_entry(mapPtr_, bi).state == LMap::USED) return bi;
    return mapPtr_->numBuckets_;
  }

  // returns the number of buckets spanned by the input range
  static size_t n_spanned_buckets(const lmap_iterator &begin,
                                  const lmap_iterator &end) {
    if (begin != end) {
      auto map_ptr = begin.mapPtr_;
      assert(map_ptr);

      // invariant check - end is either:
      // - the end of the set; or
      // - an iterator pointing to an used entry
      assert(end == lmap_end(map_ptr) ||
             first_bucket_entry(map_ptr, end.bucketId_).state == LMap::USED);

      if (end != lmap_end(map_ptr)) {
        // count one more if end is not on a bucket edge
        return end.bucketId_ - begin.bucketId_ +
               (end.entryPtr_ !=
                &first_bucket_entry(end.mapPtr_, end.bucketId_));
      }
      return map_ptr->numBuckets_ - begin.bucketId_;
    }
    return 0;
  }
};

}  // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_LOCAL_HASHMAP_H_
