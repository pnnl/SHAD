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

#ifndef INCLUDE_SHAD_DATA_STRUCTURES_HASHMAP_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_HASHMAP_H_

#include <algorithm>
#include <functional>
#include <tuple>
#include <utility>
#include <vector>

#include "shad/data_structures/abstract_data_structure.h"
#include "shad/data_structures/buffer.h"
#include "shad/data_structures/compare_and_hash_utils.h"
#include "shad/data_structures/local_hashmap.h"
#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {

template <typename Map, typename T, typename NonConstT>
class map_iterator;

/// @brief The Hashmap data structure.
///
/// SHAD's Hashmap is a distributed, thread-safe, associative container.
/// @tparam KTYPE type of the hashmap keys.
/// @tparam VTYPE type of the hashmap values.
/// @tparam KEY_COMPARE key comparison function; default is MemCmp<KTYPE>.
/// @warning obects of type KTYPE and VTYPE need to be trivially copiable.
/// @tparam INSERT_POLICY insertion policy; default is overwrite
/// (i.e. insertions overwrite previous values
///  associated to the same key, if any).
template <typename KTYPE, typename VTYPE, typename KEY_COMPARE = MemCmp<KTYPE>,
          typename INSERT_POLICY = Overwriter<VTYPE>>
class Hashmap : public AbstractDataStructure<
                    Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>> {
  template <typename>
  friend class AbstractDataStructure;
  friend class map_iterator<Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>,
                            const std::pair<KTYPE, VTYPE>,
                            std::pair<KTYPE, VTYPE>>;
  friend class map_iterator<Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>,
                            const std::pair<KTYPE, VTYPE>,
                            std::pair<KTYPE, VTYPE>>;

 public:
  using value_type = std::pair<KTYPE, VTYPE>;
  using HmapT = Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>;
  using LMapT = LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>;
  using ObjectID = typename AbstractDataStructure<HmapT>::ObjectID;
  using ShadHashmapPtr = typename AbstractDataStructure<HmapT>::SharedPtr;

  using iterator =
      map_iterator<Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>,
                   const std::pair<KTYPE, VTYPE>, std::pair<KTYPE, VTYPE>>;
  using const_iterator =
      map_iterator<Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>,
                   const std::pair<KTYPE, VTYPE>, std::pair<KTYPE, VTYPE>>;
  using local_iterator =
      lmap_iterator<LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>,
                    const std::pair<KTYPE, VTYPE>>;
  using const_local_iterator =
      lmap_iterator<LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>,
                    const std::pair<KTYPE, VTYPE>>;
  struct EntryT {
    EntryT(const KTYPE &k, const VTYPE &v) : key(k), value(v) {}
    EntryT() = default;
    KTYPE key;
    VTYPE value;
  };
  using BuffersVector = typename impl::BuffersVector<EntryT, HmapT>;

  /// @brief Create method.
  ///
  /// Creates a newhashmap instance.
  /// @param numEntries Expected number of entries.
  /// @return A shared pointer to the newly created hashmap instance.
#ifdef DOXYGEN_IS_RUNNING
  static ShadHashmapPtr Create(const size_t numEntries);
#endif

  /// @brief Overall size of the hashmap (number of entries).
  /// @warning Calling the size method may result in one-to-all
  /// communication among localities to retrieve consinstent information.
  /// @return the size of the hashmap.
  size_t Size() const;

  /// @brief Insert a key-value pair in the hashmap.
  /// @param[in] key the key.
  /// @param[in] value the value to copy into the hashmap.
  /// @return an iterator either to the inserted value or to the previously
  /// inserted value that prevented the insertion.
  std::pair<iterator, bool> Insert(const KTYPE &key, const VTYPE &value);

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

  /// @brief Buffered Insert method.
  /// Inserts a key-value pair, using aggregation buffers.
  /// @warning Insertions are finalized only after calling
  /// the WaitForBufferedInsert() method.
  /// @param[in] key The key.
  /// @param[in] value The value.
  void BufferedInsert(const KTYPE &key, const VTYPE &value);

  /// @brief Asynchronous Buffered Insert method.
  /// Asynchronously inserts a key-value pair, using aggregation buffers.
  /// @warning asynchronous buffered insertions are finalized only after
  /// calling the rt::waitForCompletion(rt::Handle &handle) method AND
  /// the WaitForBufferedInsert() method, in this order.
  /// @param[in,out] handle Reference to the handle
  /// @param[in] key The key.
  /// @param[in] value The value.
  void BufferedAsyncInsert(rt::Handle &handle, const KTYPE &key,
                           const VTYPE &value);

  /// @brief Finalize method for buffered insertions.
  void WaitForBufferedInsert() {
    auto flushLambda_ = [](const ObjectID &oid) {
      auto ptr = HmapT::GetPtr(oid);
      ptr->buffers_.FlushAll();
    };
    rt::executeOnAll(flushLambda_, this->oid_);
  }
  /// @brief Remove a key-value pair from the hashmap.
  /// @param[in] key the key.
  void Erase(const KTYPE &key);

  /// @brief Asynchronously remove a key-value pair from the hashmap.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle
  /// to be used to wait for completion.
  /// @param[in] key the key.
  void AsyncErase(rt::Handle &handle, const KTYPE &key);

  /// @brief Clear the content of the hashmap.
  void Clear() {
    auto clearLambda = [](const ObjectID &oid) {
      auto mapPtr = HmapT::GetPtr(oid);
      mapPtr->localMap_.Clear();
    };
    rt::executeOnAll(clearLambda, this->oid_);
  }

  using LookupResult =
      typename LocalHashmap<KTYPE, VTYPE, KEY_COMPARE>::LookupResult;

  /// @brief Get the value associated to a key.
  /// @param[in] key the key.
  /// @param[out] res a pointer to the value if the the key-value was found
  ///             and NULL if it does not exists.
  /// @return true if the entry is found, false otherwise.
  bool Lookup(const KTYPE &key, VTYPE *res);

  /// @brief Asynchronous lookup method.
  /// @warning Asynchronous operations are guaranteed to have completed.
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle.
  /// to be used to wait for completion.
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
  /// @param key The key.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void Apply(const KTYPE &key, ApplyFunT &&function, Args &... args);

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
  /// @param key The key.
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
  /// @param[in,out] handle Reference to the handle.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncForEachEntry(rt::Handle &handle, ApplyFunT &&function,
                         Args &... args);

  /// @brief Apply a user-defined function to each key.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(const KTYPE&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void ForEachKey(ApplyFunT &&function, Args &... args);

  /// @brief Asynchronously apply a user-defined function to each key.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(shad::rt::Handle&, const KTYPE&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle Reference to the handle.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncForEachKey(rt::Handle &handle, ApplyFunT &&function,
                       Args &... args);

  void PrintAllEntries() {
    auto printLambda = [](const ObjectID &oid) {
      auto mapPtr = HmapT::GetPtr(oid);
      std::cout << "---- Locality: " << rt::thisLocality() << std::endl;
      mapPtr->localMap_.PrintAllEntries();
    };
    rt::executeOnAll(printLambda, this->oid_);
  }

  // FIXME it should be protected
  void BufferEntryInsert(const EntryT &entry) {
    localMap_.Insert(entry.key, entry.value);
  }

  iterator begin() { return iterator::map_begin(this); }
  iterator end() { return iterator::map_end(this); }
  const_iterator cbegin() const { return const_iterator::map_begin(this); }
  const_iterator cend() const { return const_iterator::map_end(this); }
  const_iterator begin() const { return cbegin(); }
  const_iterator end() const { return cend(); }
  local_iterator local_begin() {
    return local_iterator::lmap_begin(&localMap_);
  }
  local_iterator local_end() { return local_iterator::lmap_end(&localMap_); }
  const_local_iterator clocal_begin() {
    return const_local_iterator::lmap_begin(&localMap_);
  }
  const_local_iterator clocal_end() {
    return const_local_iterator::lmap_end(&localMap_);
  }

  std::pair<iterator, bool> insert(const value_type &value) {
    return Insert(value.first, value.second);
  }

  std::pair<iterator, bool> insert(const_iterator, const value_type &value) {
    return insert(value);
  }

  void buffered_async_insert(rt::Handle &h, const value_type &value) {
    BufferedAsyncInsert(h, value.first, value.second);
  }

  void buffered_async_wait(rt::Handle &h) { rt::waitForCompletion(h); }

  void buffered_async_flush() { WaitForBufferedInsert(); }

 private:
  LocalHashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY> localMap_;
  BuffersVector buffers_;

  struct InsertArgs {
    ObjectID oid;
    KTYPE key;
    VTYPE value;
  };

  struct LookupArgs {
    ObjectID oid;
    KTYPE key;
  };

 protected:
  Hashmap(ObjectID oid, const size_t numEntries)
      : AbstractDataStructure<
            Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>>(oid),
        localMap_(std::max(
            numEntries /
                (constants::kDefaultNumEntriesPerBucket * rt::numLocalities()),
            1lu)),
        buffers_(oid) {}
};

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
inline size_t Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::Size() const {
  size_t size = localMap_.size_;
  size_t remoteSize;
  auto sizeLambda = [](const ObjectID &oid, size_t *res) {
    auto mapPtr = HmapT::GetPtr(oid);
    *res = mapPtr->localMap_.size_;
  };
  for (auto tgtLoc : rt::allLocalities()) {
    if (tgtLoc != rt::thisLocality()) {
      rt::executeAtWithRet(tgtLoc, sizeLambda, this->oid_, &remoteSize);
      size += remoteSize;
    }
  }
  return size;
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
inline std::pair<
    typename Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::iterator, bool>
Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::Insert(const KTYPE &key,
                                                          const VTYPE &value) {
  using itr_traits = distributed_iterator_traits<iterator>;
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  std::pair<iterator, bool> res;

  if (targetLocality == rt::thisLocality()) {
    auto lres = localMap_.Insert(key, value);
    res.first = itr_traits::iterator_from_local(begin(), end(), lres.first);
    res.second = lres.second;
  } else {
    auto insertLambda =
        [](const std::tuple<iterator, iterator, InsertArgs> &args_,
           std::pair<iterator, bool> *res_ptr) {
          auto &args(std::get<2>(args_));
          auto mapPtr = HmapT::GetPtr(args.oid);
          auto lres = mapPtr->localMap_.Insert(args.key, args.value);
          res_ptr->first = itr_traits::iterator_from_local(
              std::get<0>(args_), std::get<1>(args_), lres.first);
          res_ptr->second = lres.second;
        };
    rt::executeAtWithRet(
        targetLocality, insertLambda,
        std::make_tuple(begin(), end(), InsertArgs{this->oid_, key, value}),
        &res);
  }
  return res;
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
inline void Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::AsyncInsert(
    rt::Handle &handle, const KTYPE &key, const VTYPE &value) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localMap_.AsyncInsert(handle, key, value);
  } else {
    auto insertLambda = [](rt::Handle &handle, const InsertArgs &args) {
      auto mapPtr = HmapT::GetPtr(args.oid);
      mapPtr->localMap_.AsyncInsert(handle, args.key, args.value);
    };
    InsertArgs args = {this->oid_, key, value};
    rt::asyncExecuteAt(handle, targetLocality, insertLambda, args);
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
inline void Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::BufferedInsert(
    const KTYPE &key, const VTYPE &value) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  buffers_.Insert(EntryT(key, value), targetLocality);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
inline void Hashmap<KTYPE, VTYPE, KEY_COMPARE,
                    INSERT_POLICY>::BufferedAsyncInsert(rt::Handle &handle,
                                                        const KTYPE &key,
                                                        const VTYPE &value) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  buffers_.AsyncInsert(handle, EntryT(key, value), targetLocality);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
inline void Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::Erase(
    const KTYPE &key) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localMap_.Erase(key);
  } else {
    auto eraseLambda = [](const LookupArgs &args) {
      auto mapPtr = HmapT::GetPtr(args.oid);
      mapPtr->localMap_.Erase(args.key);
    };
    LookupArgs args = {this->oid_, key};
    rt::executeAt(targetLocality, eraseLambda, args);
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
inline void Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::AsyncErase(
    rt::Handle &handle, const KTYPE &key) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localMap_.AsyncErase(handle, key);
  } else {
    auto eraseLambda = [](rt::Handle &handle, const LookupArgs &args) {
      auto mapPtr = HmapT::GetPtr(args.oid);
      mapPtr->localMap_.AsyncErase(handle, args.key);
    };
    LookupArgs args = {this->oid_, key};
    rt::asyncExecuteAt(handle, targetLocality, eraseLambda, args);
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
inline bool Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::Lookup(
    const KTYPE &key, VTYPE *res) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    return localMap_.Lookup(key, res);
  } else {
    auto lookupLambda = [](const LookupArgs &args, LookupResult *res) {
      auto mapPtr = HmapT::GetPtr(args.oid);
      res->found = mapPtr->localMap_.Lookup(args.key, &res->value);
    };
    LookupArgs args = {this->oid_, key};
    LookupResult lres;
    rt::executeAtWithRet(targetLocality, lookupLambda, args, &lres);
    if (lres.found) {
      *res = std::move(lres.value);
    }
    return lres.found;
  }
  return false;
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
inline void Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::AsyncLookup(
    rt::Handle &handle, const KTYPE &key, LookupResult *res) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localMap_.AsyncLookup(handle, key, res);
  } else {
    auto lookupLambda = [](rt::Handle &, const LookupArgs &args,
                           LookupResult *res) {
      auto mapPtr = HmapT::GetPtr(args.oid);
      LookupResult tres;
      mapPtr->localMap_.Lookup(args.key, &tres);
      *res = tres;
    };
    LookupArgs args = {this->oid_, key};
    rt::asyncExecuteAtWithRet(handle, targetLocality, lookupLambda, args, res);
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
template <typename ApplyFunT, typename... Args>
void Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::ForEachEntry(
    ApplyFunT &&function, Args &... args) {
  using FunctionTy = void (*)(const KTYPE &, VTYPE &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  using LMapPtr = LocalHashmap<KTYPE, VTYPE, KEY_COMPARE> *;
  using ArgsTuple = std::tuple<LMapT *, FunctionTy, std::tuple<Args...>>;
  feArgs arguments(this->oid_, fn, std::tuple<Args...>(args...));
  auto feLambda = [](const feArgs &args) {
    auto mapPtr = HmapT::GetPtr(std::get<0>(args));
    ArgsTuple argsTuple(&mapPtr->localMap_, std::get<1>(args),
                        std::get<2>(args));
    rt::forEachAt(rt::thisLocality(),
                  LMapT::template ForEachEntryFunWrapper<ArgsTuple, Args...>,
                  argsTuple, mapPtr->localMap_.numBuckets_);
  };
  rt::executeOnAll(feLambda, arguments);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
template <typename ApplyFunT, typename... Args>
void Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::AsyncForEachEntry(
    rt::Handle &handle, ApplyFunT &&function, Args &... args) {
  using FunctionTy = void (*)(rt::Handle &, const KTYPE &, VTYPE &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  using ArgsTuple = std::tuple<LMapT *, FunctionTy, std::tuple<Args...>>;
  feArgs arguments(this->oid_, fn, std::tuple<Args...>(args...));
  auto feLambda = [](rt::Handle &handle, const feArgs &args) {
    auto mapPtr = HmapT::GetPtr(std::get<0>(args));
    ArgsTuple argsTuple(&mapPtr->localMap_, std::get<1>(args),
                        std::get<2>(args));
    rt::asyncForEachAt(
        handle, rt::thisLocality(),
        LMapT::template AsyncForEachEntryFunWrapper<ArgsTuple, Args...>,
        argsTuple, mapPtr->localMap_.numBuckets_);
  };
  rt::asyncExecuteOnAll(handle, feLambda, arguments);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
template <typename ApplyFunT, typename... Args>
void Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::ForEachKey(
    ApplyFunT &&function, Args &... args) {
  using FunctionTy = void (*)(const KTYPE &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  using ArgsTuple = std::tuple<LMapT *, FunctionTy, std::tuple<Args...>>;
  feArgs arguments(this->oid_, fn, std::tuple<Args...>(args...));
  auto feLambda = [](const feArgs &args) {
    auto mapPtr = HmapT::GetPtr(std::get<0>(args));
    ArgsTuple argsTuple(&mapPtr->localMap_, std::get<1>(args),
                        std::get<2>(args));
    rt::forEachAt(rt::thisLocality(),
                  LMapT::template ForEachKeyFunWrapper<ArgsTuple, Args...>,
                  argsTuple, mapPtr->localMap_.numBuckets_);
  };
  rt::executeOnAll(feLambda, arguments);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
template <typename ApplyFunT, typename... Args>
void Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::AsyncForEachKey(
    rt::Handle &handle, ApplyFunT &&function, Args &... args) {
  using FunctionTy = void (*)(rt::Handle &, const KTYPE &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  using ArgsTuple = std::tuple<LMapT *, FunctionTy, std::tuple<Args...>>;
  feArgs arguments(this->oid_, fn, std::tuple<Args...>(args...));
  auto feLambda = [](rt::Handle &handle, const feArgs &args) {
    auto mapPtr = HmapT::GetPtr(std::get<0>(args));
    ArgsTuple argsTuple(&mapPtr->localMap_, std::get<1>(args),
                        std::get<2>(args));
    rt::asyncForEachAt(
        handle, rt::thisLocality(),
        LMapT::template AsyncForEachKeyFunWrapper<ArgsTuple, Args...>,
        argsTuple, mapPtr->localMap_.numBuckets_);
  };
  rt::asyncExecuteOnAll(handle, feLambda, arguments);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
template <typename ApplyFunT, typename... Args>
void Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::Apply(
    const KTYPE &key, ApplyFunT &&function, Args &... args) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    localMap_.Apply(key, function, args...);
  } else {
    using FunctionTy = void (*)(const KTYPE &, VTYPE &, Args &...);
    FunctionTy fn = std::forward<decltype(function)>(function);
    using ArgsTuple =
        std::tuple<ObjectID, const KTYPE, FunctionTy, std::tuple<Args...>>;
    ArgsTuple arguments(this->oid_, key, fn, std::tuple<Args...>(args...));
    auto feLambda = [](const ArgsTuple &args) {
      constexpr auto Size = std::tuple_size<
          typename std::decay<decltype(std::get<3>(args))>::type>::value;
      ArgsTuple &tuple = const_cast<ArgsTuple &>(args);
      LMapT *mapPtr = &(HmapT::GetPtr(std::get<0>(tuple))->localMap_);
      LMapT::CallApplyFun(mapPtr, std::get<1>(tuple), std::get<2>(tuple),
                          std::get<3>(tuple), std::make_index_sequence<Size>{});
    };
    rt::executeAt(targetLocality, feLambda, arguments);
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE,
          typename INSERT_POLICY>
template <typename ApplyFunT, typename... Args>
void Hashmap<KTYPE, VTYPE, KEY_COMPARE, INSERT_POLICY>::AsyncApply(
    rt::Handle &handle, const KTYPE &key, ApplyFunT &&function,
    Args &... args) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localMap_.AsyncApply(handle, key, function, args...);
  } else {
    using FunctionTy =
        void (*)(rt::Handle &, const KTYPE &, VTYPE &, Args &...);
    FunctionTy fn = std::forward<decltype(function)>(function);
    using ArgsTuple =
        std::tuple<ObjectID, const KTYPE, FunctionTy, std::tuple<Args...>>;
    ArgsTuple arguments(this->oid_, key, fn, std::tuple<Args...>(args...));
    auto feLambda = [](rt::Handle &handle, const ArgsTuple &args) {
      constexpr auto Size = std::tuple_size<
          typename std::decay<decltype(std::get<3>(args))>::type>::value;
      ArgsTuple &tuple(const_cast<ArgsTuple &>(args));
      LMapT *mapPtr = &(HmapT::GetPtr(std::get<0>(tuple))->localMap_);
      LMapT::AsyncCallApplyFun(handle, mapPtr, std::get<1>(tuple),
                               std::get<2>(tuple), std::get<3>(tuple),
                               std::make_index_sequence<Size>{});
    };
    rt::asyncExecuteAt(handle, targetLocality, feLambda, arguments);
  }
}

template <typename MapT, typename T, typename NonConstT>
class map_iterator : public std::iterator<std::forward_iterator_tag, T> {
 public:
  using OIDT = typename MapT::ObjectID;
  using LMap = typename MapT::LMapT;
  using local_iterator_type = lmap_iterator<LMap, T>;
  using value_type = NonConstT;

  map_iterator() {}
  map_iterator(uint32_t locID, const OIDT mapOID, local_iterator_type &lit,
               T element) {
    data_ = {locID, mapOID, lit, element};
  }

  map_iterator(uint32_t locID, const OIDT mapOID, local_iterator_type &lit) {
    auto mapPtr = MapT::GetPtr(mapOID);
    const LMap *lmapPtr = &(mapPtr->localMap_);
    if (lit != local_iterator_type::lmap_end(lmapPtr))
      data_ = itData(locID, mapOID, lit, *lit);
    else
      *this = map_end(mapPtr.get());
  }

  static map_iterator map_begin(const MapT *mapPtr) {
    const LMap *lmapPtr = &(mapPtr->localMap_);
    auto localEnd = local_iterator_type::lmap_end(lmapPtr);
    if (static_cast<uint32_t>(rt::thisLocality()) == 0) {
      auto localBegin = local_iterator_type::lmap_begin(lmapPtr);
      if (localBegin != localEnd) {
        return map_iterator(0, mapPtr->oid_, localBegin);
      }
      map_iterator beg(0, mapPtr->oid_, localEnd, T());
      return ++beg;
    }
    auto getItLambda = [](const OIDT &mapOID, map_iterator *res) {
      auto mapPtr = MapT::GetPtr(mapOID);
      const LMap *lmapPtr = &(mapPtr->localMap_);
      auto localEnd = local_iterator_type::lmap_end(lmapPtr);
      auto localBegin = local_iterator_type::lmap_begin(lmapPtr);
      if (localBegin != localEnd) {
        *res = map_iterator(0, mapOID, localBegin);
      } else {
        map_iterator beg(0, mapOID, localEnd, T());
        *res = ++beg;
      }
    };
    map_iterator beg(0, mapPtr->oid_, localEnd, T());
    rt::executeAtWithRet(rt::Locality(0), getItLambda, mapPtr->oid_, &beg);
    return beg;
  }

  static map_iterator map_end(const MapT *mapPtr) {
    local_iterator_type lend =
        local_iterator_type::lmap_end(&(mapPtr->localMap_));
    map_iterator end(rt::numLocalities(), OIDT(0), lend, T());
    return end;
  }

  bool operator==(const map_iterator &other) const {
    return (data_ == other.data_);
  }
  bool operator!=(const map_iterator &other) const { return !(*this == other); }

  T operator*() const { return data_.element_; }

  map_iterator &operator++() {
    auto mapPtr = MapT::GetPtr(data_.oid_);
    if (static_cast<uint32_t>(rt::thisLocality()) == data_.locId_) {
      const LMap *lmapPtr = &(mapPtr->localMap_);
      auto lend = local_iterator_type::lmap_end(lmapPtr);
      if (data_.lmapIt_ != lend) {
        ++(data_.lmapIt_);
      }
      if (data_.lmapIt_ != lend) {
        data_.element_ = *(data_.lmapIt_);
        return *this;
      } else {
        // find the local begin on next localities
        itData itd;
        for (uint32_t i = data_.locId_ + 1; i < rt::numLocalities(); ++i) {
          rt::executeAtWithRet(rt::Locality(i), getLocBeginIt, data_.oid_,
                               &itd);
          if (itd.locId_ != rt::numLocalities()) {
            // It Data is valid
            data_ = itd;
            return *this;
          }
        }
        data_ = itData(rt::numLocalities(), OIDT(0), lend, T());
        return *this;
      }
    }
    itData itd;
    rt::executeAtWithRet(rt::Locality(data_.locId_), getRemoteIt, data_, &itd);
    data_ = itd;
    return *this;
  }

  map_iterator operator++(int) {
    map_iterator tmp = *this;
    operator++();
    return tmp;
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
  static local_iterator_range local_range(map_iterator &B, map_iterator &E) {
    auto mapPtr = MapT::GetPtr(B.data_.oid_);
    local_iterator_type lbeg, lend;
    uint32_t thisLocId = static_cast<uint32_t>(rt::thisLocality());
    if (B.data_.locId_ == thisLocId) {
      lbeg = B.data_.lmapIt_;
    } else {
      lbeg = local_iterator_type::lmap_begin(&(mapPtr->localMap_));
    }
    if (E.data_.locId_ == thisLocId) {
      lend = E.data_.lmapIt_;
    } else {
      lend = local_iterator_type::lmap_end(&(mapPtr->localMap_));
    }
    return local_iterator_range(lbeg, lend);
  }
  static rt::localities_range localities(map_iterator &B, map_iterator &E) {
    return rt::localities_range(rt::Locality(B.data_.locId_),
                                rt::Locality(std::min<uint32_t>(
                                    rt::numLocalities(), E.data_.locId_ + 1)));
  }

  static map_iterator iterator_from_local(map_iterator &B, map_iterator &E,
                                          local_iterator_type itr) {
    return map_iterator(static_cast<uint32_t>(rt::thisLocality()), B.data_.oid_,
                        itr);
  }

 private:
  struct itData {
    itData() : oid_(0), lmapIt_(nullptr, 0, 0, nullptr, nullptr) {}
    itData(uint32_t locId, OIDT oid, local_iterator_type lmapIt, T element)
        : locId_(locId), oid_(oid), lmapIt_(lmapIt), element_(element) {}
    bool operator==(const itData &other) const {
      return (locId_ == other.locId_) && (lmapIt_ == other.lmapIt_);
    }
    bool operator!=(itData &other) const { return !(*this == other); }
    uint32_t locId_;
    OIDT oid_;
    local_iterator_type lmapIt_;
    NonConstT element_;
  };

  itData data_;

  static void getLocBeginIt(const OIDT &mapOID, itData *res) {
    auto mapPtr = MapT::GetPtr(mapOID);
    auto lmapPtr = &(mapPtr->localMap_);
    auto localEnd = local_iterator_type::lmap_end(lmapPtr);
    auto localBegin = local_iterator_type::lmap_begin(lmapPtr);
    if (localBegin != localEnd) {
      *res = itData(static_cast<uint32_t>(rt::thisLocality()), mapOID,
                    localBegin, *localBegin);
    } else {
      *res = itData(rt::numLocalities(), OIDT(0), localEnd, T());
    }
  }

  static void getRemoteIt(const itData &itd, itData *res) {
    auto mapPtr = MapT::GetPtr(itd.oid_);
    auto lmapPtr = &(mapPtr->localMap_);
    auto localEnd = local_iterator_type::lmap_end(lmapPtr);
    local_iterator_type cit = itd.lmapIt_;
    ++cit;
    if (cit != localEnd) {
      *res = itData(static_cast<uint32_t>(rt::thisLocality()), itd.oid_, cit,
                    *cit);
      return;
    } else {
      itData outitd;
      for (uint32_t i = itd.locId_ + 1; i < rt::numLocalities(); ++i) {
        rt::executeAtWithRet(rt::Locality(i), getLocBeginIt, itd.oid_, &outitd);
        if (outitd.locId_ != rt::numLocalities()) {
          // It Data is valid
          *res = outitd;
          return;
        }
      }
      *res = itData(rt::numLocalities(), OIDT(0), localEnd, T());
    }
  }
};

}  // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_HASHMAP_H_
