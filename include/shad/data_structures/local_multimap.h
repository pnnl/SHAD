//===------------------------------------------------------------*- C++ -*-===//
//
//                                     SHAD
//
//      The Scalable High-performance Algorithms and Data Structure Library
//
//===----------------------------------------------------------------------===//
//
// Copyright 2021 Battelle Memorial Institute
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

#ifndef INCLUDE_SHAD_DATA_STRUCTURES_LOCAL_MULTIMAP_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_LOCAL_MULTIMAP_H_

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
constexpr size_t kMMapDefaultNumEntriesPerBucket = 128;
}

template <typename LMap, typename T>
class lmultimap_iterator;

template <typename LMap, typename T>
class lmultimap_key_iterator;

/// @brief The LocalMultimap data structure.
///
/// SHAD's LocalMultimap is a "local", thread-safe, associative container.
/// LocalMultimaps can be used ONLY on the Locality on which they are created.
/// @tparam KTYPE type of the multimap keys.
/// @tparam VTYPE type of the multimap values.
/// @tparam KEY_COMPARE key comparison function; default is MemCmp<KTYPE>.
template <typename KTYPE, typename VTYPE, typename KEY_COMPARE = MemCmp<KTYPE>>
class LocalMultimap {
  template <typename, typename, typename>

  friend class Multimap;
  friend class lmultimap_iterator<LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>,
                                  const std::pair<KTYPE, VTYPE>>;
  friend class lmultimap_key_iterator<
      LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>,
      const std::pair<KTYPE, std::vector<VTYPE>>>;

  template <typename, typename, typename>
  friend class multimap_iterator;

  template <typename, typename, typename>
  friend class multimap_key_iterator;

 public:
  using inner_type = VTYPE;
  using iterator = lmultimap_iterator<LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>,
                                      const std::pair<KTYPE, VTYPE>>;
  using const_iterator =
      lmultimap_iterator<LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>,
                         const std::pair<KTYPE, VTYPE>>;
  using key_iterator =
      lmultimap_key_iterator<LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>,
                             const std::pair<KTYPE, std::vector<VTYPE>>>;
  using const_key_iterator =
      lmultimap_key_iterator<LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>,
                             const std::pair<KTYPE, std::vector<VTYPE>>>;

  /// @brief Constructor.
  /// @param numInitBuckets initial number of Buckets.
  explicit LocalMultimap(const size_t numInitBuckets)
      : numBuckets_(numInitBuckets),
        buckets_array_(numInitBuckets),
        deleter_array_(numInitBuckets),
        inserter_array_(numInitBuckets),
        numberKeys_(0) {
    for (uint64_t i = 0; i < numInitBuckets; i++) {
      deleter_array_[i] = 0;
      inserter_array_[i] = 0;
    }
  }

  /// @brief Size of the multimap (number of entries).
  /// @return the size of the multimap.
  size_t Size() {
    size_t size = 0;

   for (auto itr = key_begin(); itr != key_end(); ++ itr)
     size += (* itr).second.size();

   return size;
  };

  /// @brief Number of keys in the multimap.
  /// @return the number of keys in the multimap.
  size_t NumberKeys() const { return numberKeys_.load(); };

  /// @brief Insert a key-value pair in the multimap.
  /// @param[in] key the key.
  /// @param[in] value the value to copy into the multimap.
  /// @return an iterator to the inserted value
  std::pair<iterator, bool> Insert(const KTYPE &key, const VTYPE &value);

  template <typename ELTYPE>
  std::pair<iterator, bool> Insert(const KTYPE &key, const ELTYPE &value);

  /// @brief Asynchronously Insert a key-value pair in the multimap.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle to be used to wait for
  /// completion.
  /// @param[in] key the key.
  /// @param[in] value the value to copy into the multimap.
  /// @return a pointer to the inserted value
  void AsyncInsert(rt::Handle &handle, const KTYPE &key, const VTYPE &value);

  template <typename ELTYPE>
  void AsyncInsert(rt::Handle &handle, const KTYPE &key, const ELTYPE &value);

  /// @brief Remove a key and value from the multimap.
  /// @param[in] key the key.
  void Erase(const KTYPE &key);

  /// @brief Asynchronously remove a key and value from the multimap.
  /// @warning Asynchronous operations are guaranteed to have completed.
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle to be used to wait for
  /// completion.
  /// @param[in] key the key.
  void AsyncErase(rt::Handle &handle, const KTYPE &key);

  /// @brief Clear the content of the multimap.
  void Clear() {
    buckets_array_.clear();
    numberKeys_ = 0;
    buckets_array_ = std::vector<Bucket>(numBuckets_);
    for (uint64_t i = 0; i < numBuckets_; i++) {
      deleter_array_[i] = 0;
      inserter_array_[i] = 0;
    }
  }

  /// @brief Result for the Lookup and AsyncLookup methods.
  struct LookupResult {
    bool found;
    size_t size;               /// Size of the value vector.
    std::vector<VTYPE> value;  /// A copy of the value vector.
  };

  /// @brief Result for the Remote Lookup method.
  struct LookupRemoteResult {
    bool found;
    size_t size;            /// Size of the value vector.
    rt::Locality localLoc;  /// Locality of the local site.
    VTYPE *local_elems;     /// Address of the value vector
                            /// elements at the local site
    VTYPE *remote_elems;    /// Address of a copy of the
                            /// value vector elements at the remote site
  };

  /// @brief Asynchronously get the values associated to a key.
  /// @warning Asynchronous operations are guaranteed to have completed. only
  /// after calling the rt::waitForCompletion(rt::Handle &handle) method.
  ///
  /// @param[in,out] handle Reference to the handle to be used
  ///                       to wait for completion.
  /// @param[in] key The key.
  /// @param[out] res The result of the lookup operation.
  void AsyncLookup(rt::Handle &handle, const KTYPE &key, LookupResult *result);

  /// @brief Get the values associated to a key.
  /// @param[in] key The key.
  /// @param[out] res The result of the lookup operation.
  bool Lookup(const KTYPE &key, LookupResult *result);

  /// @brief Make a local copy of the values associated to a key and
  /// return the size and address of the local copy.
  ///
  /// @param[in] key The key.
  /// @param[out] local_result The result of the lookup operation.
  void LookupFromRemote(const KTYPE &key, LookupRemoteResult *remote_result);

  /// @brief Apply a user-defined function to every element of an entry's value
  /// array.
  /// @tparam ApplyFunT User-defined function type. The function prototype
  /// should be:
  /// @code
  /// void(const KTYPE&, std::vector<VTYPE> &, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in] key The key.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void Apply(const KTYPE &key, ApplyFunT &&function, Args &...args) {
    LookupResult result;
    Lookup(key, &result);
    function(key, result.value, args...);
  }

  /// @brief Asynchronously apply a user-defined function to a key-value pair.
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(rt::Handle &handle, std::vector<VTYPE> &, VTYPE&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle Reference to the handle.
  /// @param[in] key The key.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncApply(rt::Handle &handle, const KTYPE &key, ApplyFunT &&function,
                  Args &...args);

 /// @brief Apply a user-defined function to every element of an entry's value
  /// array. Thread safe wrt other operations.
  /// @tparam ApplyFunT User-defined function type. The function prototype
  /// should be:
  /// @code
  /// void(const KTYPE&, std::vector<VTYPE> &, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in] key The key.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void BlockingApply(const KTYPE &key, ApplyFunT &&function, Args &...args);

  /// @brief Asynchronously apply a user-defined function to a key-value pair.
  /// Thread safe wrt other operations.
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(rt::Handle &handle, std::vector<VTYPE> &, VTYPE&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle Reference to the handle.
  /// @param[in] key The key.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncBlockingApply(rt::Handle &handle, const KTYPE &key, ApplyFunT &&function,
                          Args &...args);
 
  /// @brief Asynchronously apply a user-defined function to a key-value pair.
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(rt::Handle &handle, const KTYPE&, std::vector<VTYPE>&, Args&,
  ///      uint8_t*, uint32_t*);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle Reference to the handle.
  /// @param[in] key The key.
  /// @param function The function to apply.
  /// @param[out] result Pointer to the region where the result is
  /// written; result must point to a valid memory allocation.
  /// @param[out] resultSize Pointer to the region where the result size is
  /// written; resultSize must point to a valid memory allocation. 
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncApplyWithRetBuff(rt::Handle &handle, const KTYPE &key,
                             ApplyFunT &&function, uint8_t* result,
                             uint32_t* resultSize, Args &...args);

  /// @brief Apply a user-defined function to each key-value pair.
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
  void ForEachEntry(ApplyFunT &&function, Args &...args);

  /// @brief Asynchronously apply a user-defined function to each key-value
  /// pair.
  ///
  /// @tparam ApplyFunT User-defined function type. The function prototype
  ///         should be:
  /// @code
  /// void(shad::rt::Handle&, const KTYPE&, VTYPE&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @warning Asynchronous operations are guaranteed to have completed only
  /// after calling the rt::waitForCompletion(rt::Handle &handle) method.
  ///
  /// @param[in,out] handle Reference to the handle to be used to wait for
  /// completion.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncForEachEntry(rt::Handle &handle, ApplyFunT &&function,
                         Args &...args);

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
  void ForEachKey(ApplyFunT &&function, Args &...args);

  /// @brief Asynchronously apply a user-defined function to each key.
  /// @tparam ApplyFunT User-defined function type.
  /// The function prototype should be:
  /// @code
  /// void(shad::rt::Handle&, const KTYPE&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  /// @warning Asynchronous operations are guaranteed to have completed.
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle to be used to wait for
  /// completion.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncForEachKey(rt::Handle &handle, ApplyFunT &&function, Args &...args);

  /// @brief Print all the entries in the multimap.
  /// @warning std::ostream & operator<< must be defined for both KTYPE and
  /// VTYPE
  void PrintAllEntries();

  /// @brief Print all the keys in the multimap.
  /// @warning std::ostream & operator<< must be defined for both KTYPE and
  /// VTYPE
  void PrintAllKeys();

  iterator begin() {
    Entry *firstEntry = &buckets_array_[0].getEntry(0);

    if (firstEntry->state == USED) {
      iterator cbeg(this, 0, 0, &buckets_array_[0], firstEntry,
                    firstEntry->value.begin());
      return cbeg;
    }

    iterator cbeg(this, 0, 0, &buckets_array_[0], firstEntry,
                  typename std::vector<inner_type>::iterator());
    return ++cbeg;
  }

  iterator end() { return iterator::lmultimap_end(numBuckets_); }

  key_iterator key_begin() {
    Entry *firstEntry = &buckets_array_[0].getEntry(0);

    if (firstEntry->state == USED) {
      key_iterator cbeg(this, 0, 0, &buckets_array_[0], firstEntry);
      return cbeg;
    }

    key_iterator cbeg(this, 0, 0, &buckets_array_[0], firstEntry);
    return ++cbeg;
  }

  key_iterator key_end() {
    return key_iterator::lmultimap_key_end(numBuckets_);
  }

  const_iterator cbegin() { return begin(); }
  const_iterator cend() { return const_iterator::lmultimap_end(numBuckets_); }

  void allow_inserter(size_t i) {
    while (true) {
      uint32_t prev_inserters = inserter_array_[i];

      if ((prev_inserters != has_deleter) && (deleter_array_[i] == 0)) {
        uint32_t new_inserters = prev_inserters + 1;
        if (inserter_array_[i].compare_exchange_weak(prev_inserters,
                                                     new_inserters)) {
          return;
        } else {
          rt::impl::yield();
        }
      } else {
        rt::impl::yield();
      }
    }
  }

  void release_inserter(size_t i) { inserter_array_[i]--; }

  void allow_deleter(size_t i) {
    deleter_array_[i]++;

    while (true) {
      uint32_t prev_inserters = inserter_array_[i];

      if (prev_inserters == 0) {
        if (inserter_array_[i].compare_exchange_weak(prev_inserters,
                                                     has_deleter)) {
          deleter_array_[i]--;
          return;
        } else {
          rt::impl::yield();
        }
      } else {
        rt::impl::yield();
      }
    }
  }

  void release_deleter(size_t i) { inserter_array_[i] = 0; }

 private:
  static const uint8_t kHashSeed = 0;
  static const size_t kAllocPending = 0x1;
  static const size_t kNumEntriesPerBucket =
      constants::kMMapDefaultNumEntriesPerBucket;
  static const uint32_t kKeyWords = sizeof(KTYPE) > sizeof(uint64_t)
                                        ? sizeof(KTYPE) / sizeof(uint64_t)
                                        : 1;

  typedef KEY_COMPARE KeyCompare;
  enum State { EMPTY, USED, PENDING_INSERT, PENDING_UPDATE};

  struct Entry {
    KTYPE key;
    std::vector<VTYPE> value;
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
    rt::Lock _entriesLock;
    std::shared_ptr<Entry> entries;
  };

  uint32_t has_deleter = 0xffffffff;

  KeyCompare KeyComp_;
  size_t numBuckets_;
  std::atomic<size_t> numberKeys_;
  std::vector<Bucket> buckets_array_;
  std::vector<std::atomic<uint32_t>> deleter_array_;
  std::vector<std::atomic<uint32_t>> inserter_array_;

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void CallForEachEntryFun(
      const size_t i, LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *mapPtr,
      ApplyFunT function, std::tuple<Args...> &args,
      std::index_sequence<is...>) {
    Bucket *bucket = &mapPtr->buckets_array_[i];

    while (bucket != nullptr) {
      Bucket *next_bucket = bucket->next.get();

      for (uint64_t j = 0; j < bucket->BucketSize(); ++j) {
        Entry *entry = &bucket->getEntry(j);

        if (entry->state == USED) {
          function(entry->key, entry->value, std::get<is>(args)...);
        } else if (entry->state != EMPTY) {
          printf("Entry in PENDING state while iterating over entries\n");
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
      LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *mapPtr, ApplyFunT function,
      std::tuple<Args...> &args, std::index_sequence<is...>) {
    Bucket *bucket = &mapPtr->buckets_array_[i];

    while (bucket != nullptr) {
      Bucket *next_bucket = bucket->next.get();

      for (uint64_t j = 0; j < bucket->BucketSize(); ++j) {
        Entry *entry = &bucket->getEntry(j);

        if (entry->state == USED) {
          function(handle, entry->key, entry->value, std::get<is>(args)...);
        } else if (entry->state != EMPTY) {
          printf("Entry in PENDING state while iterating over entries\n");
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
      const size_t i, LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *mapPtr,
      ApplyFunT function, std::tuple<Args...> &args,
      std::index_sequence<is...>) {
    size_t cnt = 0;
    Bucket *buckets_array = mapPtr->buckets_array_.data();
    Bucket *bucket = &buckets_array[i];

    while (bucket != nullptr) {
      Bucket *next_bucket = bucket->next.get();
      for (uint64_t j = 0; j < bucket->BucketSize(); ++j) {
        Entry *entry = &bucket->getEntry(j);

        if (entry->state == USED) {
          function(entry->key, std::get<is>(args)...);
        } else if (entry->state != EMPTY) {
          printf("Entry in PENDING state while iterating over entries\n");
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
      LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *mapPtr, ApplyFunT function,
      std::tuple<Args...> &args, std::index_sequence<is...>) {
    Bucket *buckets_array = mapPtr->buckets_array_.data();
    Bucket *bucket = &buckets_array[i];

    while (bucket != nullptr) {
      Bucket *next_bucket = bucket->next.get();

      for (uint64_t j = 0; j < bucket->BucketSize(); ++j) {
        Entry *entry = &bucket->getEntry(j);
        if (entry->state == USED) {
          function(handle, entry->key, std::get<is>(args)...);
        } else if (entry->state != EMPTY) {
          printf("Entry in PENDING state while iterating over entries\n");
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
      rt::Handle &handle, LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *mapPtr,
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
          while (entry->state == PENDING_INSERT) rt::impl::yield();
          function(handle, key, entry->value, std::get<is>(args)...);
          return;
        }
      }

      bucket = bucket->next.get();
    }
    return;
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void AsyncCallApplyWithRetBuffFun(
      rt::Handle &handle, LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *mapPtr,
      const KTYPE &key, ApplyFunT function, std::tuple<Args...> &args,
      std::index_sequence<is...>, uint8_t* result, uint32_t* resultSize) {
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
          while (entry->state == PENDING_INSERT) rt::impl::yield();
          function(handle, key, entry->value,
                   std::get<is>(args)..., result, resultSize);
          return;
        }
      }

      bucket = bucket->next.get();
    }
    return;
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void CallApplyFun(LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *mapPtr,
                           const KTYPE &key, ApplyFunT function,
                           std::tuple<Args...> &args,
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
          while (entry->state == PENDING_INSERT) rt::impl::yield();
          function(key, entry->value, std::get<is>(args)...);
          return;
        }
      }

      bucket = bucket->next.get();
    }
    return;
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void CallBlockingApplyFun(LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *mapPtr,
                           const KTYPE &key, ApplyFunT function,
                           std::tuple<Args...> &args,
                           std::index_sequence<is...>) {
    mapPtr->BlockingApply(key, function, std::get<is>(args)...);
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
  static void CallAsyncBlockingApplyFun(rt::Handle &h,
                                        LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *mapPtr,
                                        const KTYPE &key, ApplyFunT function,
                                        std::tuple<Args...> &args,
                                        std::index_sequence<is...>) {
    mapPtr->AsyncBlockingApply(h, key, function, std::get<is>(args)...);
  }

  template <typename Tuple, typename... Args>
  static void AsyncApplyWRBFunWrapper(rt::Handle &handle, const Tuple &args,
                                      uint8_t* result, uint32_t* resultSize) {
    constexpr auto Size = std::tuple_size<
        typename std::decay<decltype(std::get<3>(args))>::type>::value;
    Tuple &tuple = const_cast<Tuple &>(args);

    AsyncCallApplyWithRetBuffFun(handle, std::get<0>(tuple),
                                 std::get<1>(tuple),
                                 std::get<2>(tuple), std::get<3>(tuple),
                                 std::make_index_sequence<Size>{},
                                 result, resultSize);
  }
};

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::AsyncLookup(
    rt::Handle &handle, const KTYPE &key, LookupResult *result) {
  using LMapPtr = LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *;
  auto args = std::tuple<LMapPtr, KTYPE, LookupResult *>(this, key, result);

  auto lookupLambda = [](rt::Handle &,
                         const std::tuple<LMapPtr, KTYPE, LookupResult *> &t) {
    (std::get<0>(t))->Lookup(std::get<1>(t), std::get<2>(t));
  };

  rt::asyncExecuteAt(handle, rt::thisLocality(), lookupLambda, args);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
bool LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::Lookup(const KTYPE &key,
                                                      LookupResult *result) {
  size_t bucketIdx = shad::hash<KTYPE>{}(key) % numBuckets_;
  Bucket *bucket = &(buckets_array_[bucketIdx]);
  // concurrent inserts okay; concurrent delete not okay
  allow_inserter(bucketIdx);

  while (bucket != nullptr) {
    for (size_t i = 0; i < bucket->BucketSize(); ++i) {
      Entry *entry = &bucket->getEntry(i);

      // Reached first unused entry, key not found, break
      if (entry->state == EMPTY) break;

      // Wait for some other thread to finish with this entry
      while (entry->state == PENDING_INSERT) rt::impl::yield();

      // if key matches this entry's key, return entry's value
      if (KeyComp_(&entry->key, &key) == 0) {
        while (!__sync_bool_compare_and_swap(&entry->state, USED,
                                             PENDING_INSERT)) {
          rt::impl::yield();
        }

        result->found = true;
        result->size = entry->value.size();
        result->value = entry->value;

        entry->state = USED;
        release_inserter(bucketIdx);
        return true;
      }
    }

    bucket = bucket->next.get();
  }

  result->found = false;
  result->size = 0;

  release_inserter(bucketIdx);
  return false;  // key not found
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::LookupFromRemote(
    const KTYPE &key, LookupRemoteResult *result) {
  size_t bucketIdx = shad::hash<KTYPE>{}(key) % numBuckets_;
  Bucket *bucket = &(buckets_array_[bucketIdx]);
  // concurrent inserts okay; concurrent delete not okay
  allow_inserter(bucketIdx);

  while (bucket != nullptr) {
    for (size_t i = 0; i < bucket->BucketSize(); ++i) {
      Entry *entry = &bucket->getEntry(i);

      // Reached first unused entry, key not found, return
      if (entry->state == EMPTY) {
        result->found = false;
        result->size = 0;
        release_inserter(bucketIdx);
      }

      // Wait for some other thread to finish with this entry
      while (entry->state == PENDING_INSERT) {
        rt::impl::yield();
      }
      // if key matches this entry's key, return entry's value
      if (KeyComp_(&entry->key, &key) == 0) {
        while (!__sync_bool_compare_and_swap(&entry->state, USED,
                                             PENDING_INSERT)) {
          rt::impl::yield();
        }

        result->found = true;
        result->size = entry->value.size();
        result->remote_elems = (VTYPE *)malloc(result->size * sizeof(VTYPE));
        memcpy(result->remote_elems, entry->value.data(),
               result->size * sizeof(VTYPE));

        entry->state = USED;
        release_inserter(bucketIdx);
        return;
      }
    }

    bucket = bucket->next.get();
  }

  // key not found
  result->found = false;
  result->size = 0;

  release_inserter(bucketIdx);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::PrintAllEntries() {
  for (auto itr = begin(); itr != end(); ++itr) {
    auto key = (*itr).first;
    std::cout << std::get<0>(key) << " " << std::get<1>(key);
    std::cout << "\n";
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::PrintAllKeys() {
  for (auto itr = key_begin(); itr != key_end(); ++itr) {
    auto key = (*itr).first;
    auto value = (*itr).second;
    std::cout << value.size() << " " << std::get<0>(key) << " "
              << std::get<1>(key);
    std::cout << "\n";
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::Erase(const KTYPE &key) {
  size_t bucketIdx = shad::hash<KTYPE>{}(key) % numBuckets_;
  Bucket *bucket = &(buckets_array_[bucketIdx]);
  std::vector<VTYPE> emptyValue;
  allow_deleter(bucketIdx);
  // loop over linked buckets
  for (;;) {
    // loop over entries in this bucket
    for (size_t i = 0; i < bucket->BucketSize(); ++i) {
      Entry *entry = &bucket->getEntry(i);

      // Reached first unused entry, key not found, return
      if (entry->state == EMPTY) {
        release_deleter(bucketIdx);
        return;
      }

      // If key does not match this entry's key, continue inner for loop
      if (KeyComp_(&entry->key, &key) != 0) continue;

      // Key found
      numberKeys_ -= 1;

      // find last USED entry in this bucket and swap with this entry
      //    entry     == entry being deleted
      //    lastEntry == last entry in this bucket
      //    nextEntry == first unused entry in this bucket

      Entry *lastEntry = entry;
      size_t j = i + 1;
      // loop over linked buckets
      for (;;) {
        // loop over entries in this bucket
        for (; j < bucket->BucketSize(); ++j) {
          Entry *nextEntry = &bucket->getEntry(j);
          if (nextEntry->state == USED) {
            lastEntry = nextEntry;
            continue;
          }

          // entry is last entry, so set it to empty
          if (entry == lastEntry) {
            entry->state = EMPTY;
            std::swap(entry->value, emptyValue);
            release_deleter(bucketIdx);
            return;

            // set entry {key, value} to lastEntry {key, value}
            // and set lastEntry to empty
          } else {
            entry->key = std::move(lastEntry->key);
            entry->value = std::move(lastEntry->value);

            lastEntry->state = EMPTY;
            std::swap(lastEntry->value, emptyValue);
            release_deleter(bucketIdx);
            return;
          }
        }  // Second inner for loop
        // Exhausted linked buckets, so lastEntry is last USED entry
        if (bucket->next == nullptr) {
          entry->key = std::move(lastEntry->key);
          entry->value = std::move(lastEntry->value);

          lastEntry->state = EMPTY;
          std::swap(lastEntry->value, emptyValue);
          release_deleter(bucketIdx);
          return;

        } else {
          bucket = bucket->next.get();
        }
      }  // Second outer for loop
    }    // First inner for loop

    // Exhausted linked buckets without finding key, return
    if (bucket->next == nullptr) {
      release_deleter(bucketIdx);
      return;
    } else {
      bucket = bucket->next.get();
    }
  }  // First outer for loop
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::AsyncErase(rt::Handle &handle,
                                                          const KTYPE &key) {
  using LMapPtr = LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *;
  auto args = std::tuple<LMapPtr, KTYPE>(this, key);

  auto eraseLambda = [](rt::Handle &, const std::tuple<LMapPtr, KTYPE> &t) {
    (std::get<0>(t))->Erase(std::get<1>(t));
  };

  rt::asyncExecuteAt(handle, rt::thisLocality(), eraseLambda, args);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
std::pair<typename LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::iterator, bool>
LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::Insert(const KTYPE &key,
                                                 const VTYPE &value) {
  size_t bucketIdx = shad::hash<KTYPE>{}(key) % numBuckets_;
  Bucket *bucket = &(buckets_array_[bucketIdx]);
  allow_inserter(bucketIdx);
  // loop over linked buckets
  for (;;) {
    // loop over entries in this bucket
    for (size_t i = 0; i < bucket->BucketSize(); ++i) {
      Entry *entry = &bucket->getEntry(i);

      // Reached end of used entries without finding key, so new key
      if (__sync_bool_compare_and_swap(&entry->state, EMPTY, PENDING_INSERT)) {
        entry->key = std::move(key);
        entry->value.push_back(value);

        numberKeys_ += 1;
        entry->state = USED;
        release_inserter(bucketIdx);
        return std::make_pair(
            iterator(this, bucketIdx, i, bucket, entry, entry->value.end() - 1),
            true);
      }

      // Wait for some other thread to finish with this entry
      while (entry->state == PENDING_INSERT) rt::impl::yield();

      // if key matches this entry's key, insert value; else continue inner for
      // loop
      if (KeyComp_(&entry->key, &key) == 0) {
        while (!__sync_bool_compare_and_swap(&entry->state, USED,
                                             PENDING_INSERT)) {
          rt::impl::yield();
        }
        entry->value.push_back(value);

        entry->state = USED;
        release_inserter(bucketIdx);
        return std::make_pair(
            iterator(this, bucketIdx, i, bucket, entry, entry->value.end() - 1),
            true);
      }
    }  // Inner for loop

    // Exhausted entries in this buckets
    // ...  if no more buckets, link new bucket
    if (bucket->next == nullptr) {
      if (__sync_bool_compare_and_swap(&bucket->isNextAllocated, false, true)) {
        // Allocate new bucket
        std::shared_ptr<Bucket> newBucket(
            new Bucket(constants::kMMapDefaultNumEntriesPerBucket));
        bucket->next.swap(newBucket);
      } else {
        // Wait for pending allocation to finish
        while (bucket->next == nullptr) rt::impl::yield();
      }
    }

    // move to next bucket
    bucket = bucket->next.get();
  }  // Outer for loop
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::AsyncInsert(rt::Handle &handle,
                                                           const KTYPE &key,
                                                           const VTYPE &value) {
  using LMapPtr = LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *;
  auto args = std::tuple<LMapPtr, KTYPE, VTYPE>(this, key, value);

  auto insertLambda = [](rt::Handle &,
                         const std::tuple<LMapPtr, KTYPE, VTYPE> &t) {
    (std::get<0>(t))->Insert(std::get<1>(t), std::get<2>(t));
  };

  rt::asyncExecuteAt(handle, rt::thisLocality(), insertLambda, args);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ApplyFunT, typename... Args>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::ForEachEntry(
    ApplyFunT &&function, Args &...args) {
  using FunctionTy = void (*)(const KTYPE &, std::vector<VTYPE> &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using LMapPtr = LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *;
  using ArgsTuple = std::tuple<LMapPtr, FunctionTy, std::tuple<Args...>>;

  ArgsTuple argsTuple(this, fn, std::tuple<Args...>(args...));
  rt::forEachAt(rt::thisLocality(), ForEachEntryFunWrapper<ArgsTuple, Args...>,
                argsTuple, numBuckets_);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ApplyFunT, typename... Args>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::AsyncForEachEntry(
    rt::Handle &handle, ApplyFunT &&function, Args &...args) {
  using FunctionTy = void (*)(rt::Handle &, const KTYPE &,
                              std::vector<VTYPE> &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using LMapPtr = LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *;
  using ArgsTuple = std::tuple<LMapPtr, FunctionTy, std::tuple<Args...>>;
  ArgsTuple argsTuple(this, fn, std::tuple<Args...>(args...));

  rt::asyncForEachAt(handle, rt::thisLocality(),
                     AsyncForEachEntryFunWrapper<ArgsTuple, Args...>, argsTuple,
                     numBuckets_);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ApplyFunT, typename... Args>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::ForEachKey(ApplyFunT &&function,
                                                          Args &...args) {
  using FunctionTy = void (*)(const KTYPE &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using LMapPtr = LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *;
  using ArgsTuple = std::tuple<LMapPtr, FunctionTy, std::tuple<Args...>>;
  ArgsTuple argsTuple(this, fn, std::tuple<Args...>(args...));

  rt::forEachAt(rt::thisLocality(), ForEachKeyFunWrapper<ArgsTuple, Args...>,
                argsTuple, numBuckets_);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ApplyFunT, typename... Args>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::AsyncForEachKey(
    rt::Handle &handle, ApplyFunT &&function, Args &...args) {
  using FunctionTy = void (*)(rt::Handle &, const KTYPE &,
                              std::vector<VTYPE> &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using LMapPtr = LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *;
  using ArgsTuple = std::tuple<LMapPtr, FunctionTy, std::tuple<Args...>>;
  ArgsTuple argsTuple(this, fn, std::tuple<Args...>(args...));

  rt::asyncForEachAt(handle, rt::thisLocality(),
                     AsyncForEachKeyFunWrapper<ArgsTuple, Args...>, argsTuple,
                     numBuckets_);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ApplyFunT, typename... Args>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::AsyncApply(rt::Handle &handle,
                                                          const KTYPE &key,
                                                          ApplyFunT &&function,
                                                          Args &...args) {
  using FunctionTy = void (*)(rt::Handle &, const KTYPE &,
                              std::vector<VTYPE> &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using LMapPtr = LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *;
  using ArgsTuple =
      std::tuple<LMapPtr, const KTYPE, FunctionTy, std::tuple<Args...>>;

  ArgsTuple argsTuple(this, key, fn, std::tuple<Args...>(args...));
  rt::asyncExecuteAt(handle, rt::thisLocality(),
                     AsyncApplyFunWrapper<ArgsTuple, Args...>, argsTuple);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ApplyFunT, typename... Args>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::
AsyncApplyWithRetBuff(rt::Handle &handle, const KTYPE &key,
                      ApplyFunT &&function, uint8_t* result,
                      uint32_t* resultSize, Args &...args) {
  using FunctionTy = void (*)(rt::Handle &, const KTYPE &,
                              std::vector<VTYPE> &, Args &...,
                              uint8_t*, uint32_t*);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using LMapPtr = LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *;
  using ArgsTuple =
      std::tuple<LMapPtr, const KTYPE, FunctionTy, std::tuple<Args...>>;

  ArgsTuple argsTuple(this, key, fn, std::tuple<Args...>(args...));
  rt::asyncExecuteAtWithRetBuff(handle, rt::thisLocality(),
                     AsyncApplyWRBFunWrapper<ArgsTuple, Args...>, argsTuple,
                     result, resultSize);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ELTYPE>
std::pair<typename LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::iterator, bool>
LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::Insert(const KTYPE &key,
                                                 const ELTYPE &value) {
  size_t bucketIdx = shad::hash<KTYPE>{}(key) % numBuckets_;
  Bucket *bucket = &(buckets_array_[bucketIdx]);
  allow_inserter(bucketIdx);
  // loop over linked buckets
  for (;;) {
    // loop over entries in this bucket
    for (size_t i = 0; i < bucket->BucketSize(); ++i) {
      Entry *entry = &bucket->getEntry(i);

      // Reached end of used entries without finding key, so new key
      if (__sync_bool_compare_and_swap(&entry->state, EMPTY, PENDING_INSERT)) {
        entry->key = std::move(key);
        entry->value.push_back(value);

        numberKeys_ += 1;
        entry->state = USED;
        release_inserter(bucketIdx);
        iterator itr(this, bucketIdx, i, bucket, entry,
                     entry->value.begin());
        return std::make_pair(itr, true);
      }

      // Wait for some other thread to finish with this entry
      while (entry->state == PENDING_INSERT) rt::impl::yield();

      // if key matches entry's key, insert value; else continue inner for loop
      if (KeyComp_(&entry->key, &key) == 0) {
        while (!__sync_bool_compare_and_swap(&entry->state, USED,
                                             PENDING_INSERT)) {
          rt::impl::yield();
        }
        entry->value.push_back(value);

        entry->state = USED;
        release_inserter(bucketIdx);
        iterator itr(this, bucketIdx, i, bucket, entry,
                     entry->value.begin());
        return std::make_pair(itr, true);
      }
    }  // Inner for loop

    // Exhausted linked buckets, so link new bucket
    // Exhausted entries in this buckets
    // ...  if no more buckets, link new bucket
    if (bucket->next == nullptr) {
      if (__sync_bool_compare_and_swap(&bucket->isNextAllocated, false, true)) {
        // Allocate new bucket
        std::shared_ptr<Bucket> newBucket(
            new Bucket(constants::kMMapDefaultNumEntriesPerBucket));
        bucket->next.swap(newBucket);
      } else {
        // Wait for pending allocation to finish
        while (bucket->next == nullptr) rt::impl::yield();
      }
    }

    // move to next bucket
    bucket = bucket->next.get();
  }  // Outer for loop
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ELTYPE>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::AsyncInsert(
    rt::Handle &handle, const KTYPE &key, const ELTYPE &value) {
  using LMapPtr = LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> *;
  auto args = std::tuple<LMapPtr, KTYPE, ELTYPE>(this, key, value);

  auto insertLambda = [](rt::Handle &,
                         const std::tuple<LMapPtr, KTYPE, ELTYPE> &t) {
    (std::get<0>(t))->Insert(std::get<1>(t), std::get<2>(t));
  };

  rt::asyncExecuteAt(handle, rt::thisLocality(), insertLambda, args);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ApplyFunT, typename... Args>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::BlockingApply(const KTYPE &key,
                                                             ApplyFunT &&function,
                                                             Args &...args) {
  size_t bucketIdx = shad::hash<KTYPE>{}(key) % numBuckets_;
  Bucket *bucket = &(buckets_array_[bucketIdx]);
  allow_inserter(bucketIdx);
  // loop over linked buckets
  while (bucket != nullptr) {
    // loop over entries in this bucket
    for (size_t i = 0; i < bucket->BucketSize(); ++i) {
      Entry *entry = &bucket->getEntry(i);

      // Stop at the first empty or pending insert entry.
      if ((entry->state == EMPTY) or (entry->state == PENDING_INSERT)) {
        break;
      }

      // if key matches this entry's key, apply function; else continue inner for
      // loop
      if (KeyComp_(&entry->key, &key) == 0) {
        // tagging as pending insert
        // while (entry->state == PENDING_INSERT) {
        //   rt::impl::yield();
        // }
        while (!__sync_bool_compare_and_swap(&entry->state, USED,
                                             PENDING_UPDATE)) {
          rt::impl::yield();
        }
        function(key, entry->value, args...);
        entry->state = USED;
        release_inserter(bucketIdx);
        return;
      }
    }  // Inner for loop

    // move to next bucket
    bucket = bucket->next.get();
  }  // Outer for loop
  release_inserter(bucketIdx);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ApplyFunT, typename... Args>
void LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::AsyncBlockingApply(rt::Handle &h,
                                                                  const KTYPE &key,
                                                                  ApplyFunT &&function,
                                                                  Args &...args) {
  size_t bucketIdx = shad::hash<KTYPE>{}(key) % numBuckets_;
  Bucket *bucket = &(buckets_array_[bucketIdx]);
  allow_inserter(bucketIdx);
  // loop over linked buckets
  while (bucket != nullptr) {
    // loop over entries in this bucket
    for (size_t i = 0; i < bucket->BucketSize(); ++i) {
      Entry *entry = &bucket->getEntry(i);

      // Stop at the first empty or pending insert entry.
      if ((entry->state == EMPTY) or (entry->state == PENDING_INSERT)) {
        break;
      }

      // if key matches this entry's key, apply function; else continue inner for
      // loop
      if (KeyComp_(&entry->key, &key) == 0) {
        // tagging as pending insert
        while (!__sync_bool_compare_and_swap(&entry->state, USED,
                                             PENDING_UPDATE)) {
          rt::impl::yield();
        }
        function(h, key, entry->value, args...);
        entry->state = USED;
        release_inserter(bucketIdx);
        return;
      }
    }  // Inner for loop

    // move to next bucket
    bucket = bucket->next.get();
  }  // Outer for loop
  release_inserter(bucketIdx);
}

template <typename LMap, typename T>
class lmultimap_iterator : public std::iterator<std::forward_iterator_tag, T> {
  template <typename, typename, typename>
  friend class multimap_iterator;

 public:
  using inner_type = typename LMap::inner_type;
  using Entry = typename LMap::Entry;
  using State = typename LMap::State;
  using Bucket = typename LMap::Bucket;

  lmultimap_iterator() {}
  lmultimap_iterator(const LMap *mapPtr, size_t bId, size_t pos, Bucket *cb,
                     Entry *ePtr,
                     typename std::vector<inner_type>::iterator valueItr)
      : mapPtr_(mapPtr),
        bucketId_(bId),
        position_(pos),
        currBucket_(cb),
        entryPtr_(ePtr),
        valueItr_(valueItr) {}

  static lmultimap_iterator lmultimap_begin(const LMap *mapPtr) {
    Bucket *rootPtr = &(const_cast<LMap *>(mapPtr)->buckets_array_[0]);
    Entry *firstEntry = &(rootPtr->getEntry(0));
    lmultimap_iterator beg(mapPtr, 0, 0, rootPtr, firstEntry,
                           firstEntry->value.begin());
    if (firstEntry->state == LMap::USED)
      return beg;
    else
      return ++beg;
  }

  static lmultimap_iterator lmultimap_end(const LMap *mapPtr) {
    return lmultimap_end(mapPtr->numBuckets_);
  }

  static lmultimap_iterator lmultimap_end(size_t numBuckets) {
    return lmultimap_iterator(nullptr, numBuckets, 0, nullptr, nullptr,
                              typename std::vector<inner_type>::iterator());
  }

  bool operator==(const lmultimap_iterator &other) const {
    return valueItr_ == other.valueItr_;
  }

  bool operator!=(const lmultimap_iterator &other) const {
    return !(*this == other);
  }

  T operator*() const { return T(entryPtr_->key, *valueItr_); }

  // Returns the key and value of the next value vector entry;
  //     Entry values are stored in a four-level data structure  ---
  //         for each bucket list ... for each bucket in list ...
  //           for each entry in bucket ... for each value in entry's value
  //           array
  lmultimap_iterator &operator++() {
    auto null_iter = typename std::vector<inner_type>::iterator();

    // if iterator points to an VTYPE, move to next VTYPE; else move to next
    // bucket list
    if (valueItr_ != null_iter) {
      ++valueItr_;

      // if there is another VTYPE in entry's value array, return it
      if (valueItr_ != entryPtr_->value.end()) return *this;

      // ... else move to next entry in bucket
      ++position_;

      // if there is another entry in this bucket ...
      // FIXME
      if (position_ < constants::kMMapDefaultNumEntriesPerBucket) {
        entryPtr_++;

        // if this entry is used, return begin of its value array
        // ... else no more entries in this bucket or buckets in this bucket
        // list
        if (entryPtr_->state == LMap::USED) {
          valueItr_ = entryPtr_->value.begin();
          return *this;
        }

        // ... else move to next bucket in this bucket list
      } else {
        currBucket_ = currBucket_->next.get();
        position_ = 0;

        // if bucket is not empty and first entry is used, return begin of its
        // value array
        // ... else no more entries in this bucket or buckets in this bucket
        // list
        if (currBucket_ != nullptr) {
          entryPtr_ = &currBucket_->getEntry(position_);
          if (entryPtr_->state == LMap::USED) {
            valueItr_ = entryPtr_->value.begin();
            return *this;
          }
        }
      }
    }

    // move to next bucket list
    ++bucketId_;
    position_ = 0;

    // search for a bucket list with a used entry
    for (; bucketId_ < mapPtr_->numBuckets_; ++bucketId_) {
      currBucket_ = &const_cast<LMap *>(mapPtr_)->buckets_array_[bucketId_];
      entryPtr_ = &currBucket_->getEntry(position_);
      if (entryPtr_->state == LMap::USED) {
        valueItr_ = entryPtr_->value.begin();
        return *this;
      }
    }

    // next is not found, return end iterator
    mapPtr_ = nullptr;
    entryPtr_ = nullptr;
    currBucket_ = nullptr;
    valueItr_ = typename std::vector<inner_type>::iterator();
    return *this;
  }

  lmultimap_iterator operator++(int) {
    lmultimap_iterator tmp = *this;
    operator++();
    return tmp;
  }

  class partition_range {
   public:
    partition_range(const lmultimap_iterator &begin,
                    const lmultimap_iterator &end)
        : begin_(begin), end_(end) {}
    lmultimap_iterator begin() { return begin_; }
    lmultimap_iterator end() { return end_; }

   private:
    lmultimap_iterator begin_;
    lmultimap_iterator end_;
  };

  // split a range into at most n_parts non-empty sub-ranges
  static std::vector<partition_range> partitions(lmultimap_iterator begin,
                                                 lmultimap_iterator end,
                                                 size_t n_parts) {
    std::vector<partition_range> res;
    auto n_buckets = n_spanned_buckets(begin, end);

    if (n_buckets && n_parts) {
      auto part_step =
          (n_buckets >= n_parts) ? (n_buckets + n_parts - 1) / n_parts : 1;
      auto map_ptr = begin.mapPtr_;
      auto &buckets = map_ptr->buckets_array_;
      auto b_end = (end != lmultimap_end(map_ptr)) ? end.bucketId_
                                                   : map_ptr->numBuckets_;
      auto bi = begin.bucketId_;
      auto pbegin = begin;

      while (true) {
        bi = first_used_bucket(map_ptr, bi + part_step);
        if (bi < b_end) {
          auto pend = first_in_bucket(map_ptr, bi);
          res.push_back(partition_range{pbegin, pend});
          pbegin = pend;
        } else {
          if (pbegin != end) res.push_back(partition_range{pbegin, end});
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
  typename std::vector<inner_type>::iterator valueItr_;

  // returns a pointer to the first entry of a bucket
  static typename LMap::Entry &first_bucket_entry(const LMap *mapPtr_,
                                                  size_t bi) {
    return const_cast<LMap *>(mapPtr_)->buckets_array_[bi].getEntry(0);
  }

  // returns an iterator pointing to the beginning of the first active bucket
  // from the input bucket (included)
  static lmultimap_iterator first_in_bucket(const LMap *mapPtr_, size_t bi) {
    auto &entry = first_bucket_entry(mapPtr_, bi);

    return lmultimap_iterator(mapPtr_, bi, 0,
                              &const_cast<LMap *>(mapPtr_)->buckets_array_[bi],
                              &entry, entry.value.begin());
  }

  // returns the index of the first active bucket, starting from the input
  // bucket (included). If not such bucket, it returns the number of buckets.
  static size_t first_used_bucket(const LMap *mapPtr_, size_t bi) {
    // scan for the first used entry with the same logic as operator++
    for (; bi < mapPtr_->numBuckets_; ++bi)
      if (first_bucket_entry(mapPtr_, bi).state == LMap::USED) return bi;

    return mapPtr_->numBuckets_;
  }

  // returns the number of buckets spanned by the input range
  static size_t n_spanned_buckets(const lmultimap_iterator &begin,
                                  const lmultimap_iterator &end) {
    if (begin != end) {
      auto map_ptr = begin.mapPtr_;

      // invariant check - end is either:
      // - the end of the set; or
      // - an iterator pointing to an used entry

      if (end != lmultimap_end(map_ptr)) {
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

template <typename LMap, typename T>
class lmultimap_key_iterator
    : public std::iterator<std::forward_iterator_tag, T> {
  template <typename, typename, typename>
  friend class multimap_key_iterator;

 public:
  using inner_type = typename LMap::inner_type;
  using Entry = typename LMap::Entry;
  using State = typename LMap::State;
  using Bucket = typename LMap::Bucket;

  lmultimap_key_iterator() {}
  lmultimap_key_iterator(const LMap *mapPtr, size_t bId, size_t pos, Bucket *cb,
                         Entry *ePtr)
      : mapPtr_(mapPtr),
        bucketId_(bId),
        position_(pos),
        currBucket_(cb),
        entryPtr_(ePtr) {}

  static lmultimap_key_iterator lmultimap_key_begin(const LMap *mapPtr) {
    Bucket *rootPtr = &(const_cast<LMap *>(mapPtr)->buckets_array_[0]);
    Entry *firstEntry = &(rootPtr->getEntry(0));
    lmultimap_key_iterator beg(mapPtr, 0, 0, rootPtr, firstEntry);
    if (firstEntry->state == LMap::USED)
      return beg;
    else
      return ++beg;
  }

  static lmultimap_key_iterator lmultimap_key_end(const LMap *mapPtr) {
    return lmultimap_key_end(mapPtr->numBuckets_);
  }

  static lmultimap_key_iterator lmultimap_key_end(size_t numBuckets) {
    return lmultimap_key_iterator(nullptr, numBuckets, 0, nullptr, nullptr);
  }

  bool operator==(const lmultimap_key_iterator &other) const {
    return entryPtr_ == other.entryPtr_;
  }

  bool operator!=(const lmultimap_key_iterator &other) const {
    return !(*this == other);
  }

  T operator*() const { return T(entryPtr_->key, entryPtr_->value); }

  // Returns next entry (a pointer to {KEY, VALUE};
  //     Entries are stored in three-level data structure  ---
  //     for each bucket list ... for each bucket in list
  //     ... for each entry in bucket
  lmultimap_key_iterator &operator++() {
    // move to next entry in bucket
    ++position_;

    // if there is another entry in this bucket ...
    if (position_ < constants::kMMapDefaultNumEntriesPerBucket) {
      entryPtr_++;

      // if this entry is used, return it
      // ... else no more entries in this bucket or buckets in this bucket list
      if (entryPtr_->state == LMap::USED) return *this;

      // ... else move to next bucket in this bucket list
    } else {
      currBucket_ = currBucket_->next.get();
      position_ = 0;

      // if bucket is not empty and first entry is used, return it
      // ... else no more entries in this bucket or buckets in this bucket list
      if (currBucket_ != nullptr) {
        entryPtr_ = &currBucket_->getEntry(position_);
        if (entryPtr_->state == LMap::USED) return *this;
      }
    }

    // move to next bucket list
    ++bucketId_;
    position_ = 0;

    // search for a bucket list with a used entity
    for (; bucketId_ < mapPtr_->numBuckets_; ++bucketId_) {
      currBucket_ = &const_cast<LMap *>(mapPtr_)->buckets_array_[bucketId_];
      entryPtr_ = &currBucket_->getEntry(position_);
      if (entryPtr_->state == LMap::USED) return *this;
    }

    // next is not found, return end iterator
    mapPtr_ = nullptr;
    entryPtr_ = nullptr;
    currBucket_ = nullptr;
    return *this;
  }

  lmultimap_key_iterator operator++(int) {
    lmultimap_key_iterator tmp = *this;
    operator++();
    return tmp;
  }

  class partition_range {
   public:
    partition_range(const lmultimap_key_iterator &begin,
                    const lmultimap_key_iterator &end)
        : begin_(begin), end_(end) {}
    lmultimap_key_iterator begin() { return begin_; }
    lmultimap_key_iterator end() { return end_; }

   private:
    lmultimap_key_iterator begin_;
    lmultimap_key_iterator end_;
  };

  // split a range into at most n_parts non-empty sub-ranges
  static std::vector<partition_range> partitions(lmultimap_key_iterator begin,
                                                 lmultimap_key_iterator end,
                                                 size_t n_parts) {
    std::vector<partition_range> res;
    auto n_buckets = n_spanned_buckets(begin, end);

    if (n_buckets && n_parts) {
      auto part_step =
          (n_buckets >= n_parts) ? (n_buckets + n_parts - 1) / n_parts : 1;
      auto map_ptr = begin.mapPtr_;
      auto &buckets = map_ptr->buckets_array_;
      auto b_end = (end != lmultimap_key_end(map_ptr)) ? end.bucketId_
                                                       : map_ptr->numBuckets_;
      auto bi = begin.bucketId_;
      auto pbegin = begin;

      while (true) {
        bi = first_used_bucket(map_ptr, bi + part_step);
        if (bi < b_end) {
          auto pend = first_in_bucket(map_ptr, bi);
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
    return const_cast<LMap *>(mapPtr_)->buckets_array_[bi].getEntry(0);
  }

  // returns an iterator pointing to the beginning
  // of the first active bucket from the input bucket (included)
  static lmultimap_key_iterator first_in_bucket(const LMap *mapPtr_,
                                                size_t bi) {
    auto &entry = first_bucket_entry(mapPtr_, bi);

    return lmultimap_key_iterator(
        mapPtr_, bi, 0, &const_cast<LMap *>(mapPtr_)->buckets_array_[bi],
        &entry);
  }

  // returns the index of the first active bucket, starting from the input
  // bucket (included). If not such bucket, it returns the number of buckets.
  static size_t first_used_bucket(const LMap *mapPtr_, size_t bi) {
    // scan for the first used entry with the same logic as operator++
    for (; bi < mapPtr_->numBuckets_; ++bi)
      if (first_bucket_entry(mapPtr_, bi).state == LMap::USED) return bi;

    return mapPtr_->numBuckets_;
  }

  // returns the number of buckets spanned by the input range
  static size_t n_spanned_buckets(const lmultimap_key_iterator &begin,
                                  const lmultimap_key_iterator &end) {
    if (begin != end) {
      auto map_ptr = begin.mapPtr_;

      // invariant check - end is either:
      // - the end of the set; or
      // - an iterator pointing to an used entry

      if (end != lmultimap_key_end(map_ptr)) {
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

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_LOCAL_MULTIMAP_H_
