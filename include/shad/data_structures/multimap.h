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

#ifndef INCLUDE_SHAD_DATA_STRUCTURES_MULTIMAP_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_MULTIMAP_H_

#include <algorithm>
#include <functional>
#include <tuple>
#include <string>
#include <vector>
#include <utility>
#include <fstream>
#include <iostream>

#include "shad/data_structures/abstract_data_structure.h"
#include "shad/data_structures/buffer.h"
#include "shad/data_structures/compare_and_hash_utils.h"
#include "shad/data_structures/local_multimap.h"
#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"
#include "shad/runtime/mappings/gmt/gmt_synchronous_interface.h"

#define PREFIX_SIZE 80

namespace shad {

template <typename Multimap, typename T, typename NonConstT>
class multimap_iterator;

/// @brief The Multimap data structure.
///
/// SHAD's Multimap is a distributed, thread-safe, associative container.
/// @tparam KTYPE type of the multimap keys.
/// @tparam VTYPE type of the multimap values.
/// @tparam KEY_COMPARE key comparison function; default is MemCmp<KTYPE>.
/// @warning obects of type KTYPE and VTYPE need to be trivially copiable.
template < typename KTYPE, typename VTYPE, typename KEY_COMPARE = MemCmp<KTYPE> >
class Multimap : public AbstractDataStructure< Multimap<KTYPE, VTYPE, KEY_COMPARE> > {

  template <typename>
  friend class AbstractDataStructure;
  friend class multimap_iterator<Multimap<KTYPE, VTYPE, KEY_COMPARE>,
                            const std::pair<KTYPE, VTYPE>,
                            std::pair<KTYPE, VTYPE>>;
  friend class multimap_iterator<Multimap<KTYPE, VTYPE, KEY_COMPARE>,
                            const std::pair<KTYPE, VTYPE>,
                            std::pair<KTYPE, VTYPE>>;

 public:
  using value_type = std::pair<KTYPE, VTYPE>;
  using HmapT = Multimap<KTYPE, VTYPE, KEY_COMPARE>;
  using LMapT = LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>;
  using ObjectID = typename AbstractDataStructure<HmapT>::ObjectID;
  using ShadMultimapPtr = typename AbstractDataStructure<HmapT>::SharedPtr;

  using iterator =
      multimap_iterator<Multimap<KTYPE, VTYPE, KEY_COMPARE>,
                   const std::pair<KTYPE, VTYPE>, std::pair<KTYPE, VTYPE>>;
  using const_iterator =
      multimap_iterator<Multimap<KTYPE, VTYPE, KEY_COMPARE>,
                   const std::pair<KTYPE, VTYPE>, std::pair<KTYPE, VTYPE>>;
  using local_iterator =
      lmultimap_iterator<LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>,
                    const std::pair<KTYPE, VTYPE>>;
  using const_local_iterator =
      lmultimap_iterator<LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>,
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
  /// Creates a new multimap instance.
  /// @param numEntries Expected number of entries.
  /// @return A shared pointer to the newly created multimap instance.
#ifdef DOXYGEN_IS_RUNNING
  static ShadMultimapPtr Create(const size_t numEntries);
#endif

  /// @brief Getter of the Global Identifier.
  ///
  /// @return The global identifier associated with the multimap instance.
  ObjectID GetGlobalID() const { return oid_; }

  /// @brief Getter of the local local multimap.
  ///
  /// @return The pointer to the local multimap instance.
  LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> * GetLocalMultimap() {
    return &localMultimap_;
  };

  /// @brief Overall size of the multimap (number of entries).
  /// @warning Calling the size method may result in one-to-all
  /// communication among localities to retrieve consistent information.
  /// @return the size of the multimap.
  size_t Size() const;

  /// @brief Number of keys of the multimap.
  /// @warning Calling this method may result in one-to-all
  /// communication among localities to retrieve consistent information.
  /// @return the size of the multimap.
  size_t NumberKeys() const;

  /// @brief Insert a key-value pair in the multimap.
  /// @param[in] key the key.
  /// @param[in] value the value to copy into the multimap.
  /// @return an iterator either to the inserted value
  std::pair<iterator, bool> Insert(const KTYPE &key, const VTYPE &value);

  /// @brief Asynchronously Insert a key-value pair in the multimap.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle to be used to wait for completion.
  /// @param[in] key the key.
  /// @param[in] value the value to copy into the multimap.
  /// @return a pointer to the value if the the key-value was inserted
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
  void BufferedAsyncInsert(rt::Handle &handle, const KTYPE &key, const VTYPE &value);

  /// @brief Finalize method for buffered insertions.
  void WaitForBufferedInsert() {
    auto flushLambda_ = [](const ObjectID &oid) {
      auto ptr = HmapT::GetPtr(oid);
      ptr->buffers_.FlushAll();
    };
    rt::executeOnAll(flushLambda_, oid_);
  }
  /// @brief Remove a key and all associated values from the multimap.
  /// @param[in] key the key.
  void Erase(const KTYPE &key);

  /// @brief Asynchronously remove a key and associated values from the multimap.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle to be used to wait for completion.
  /// @param[in] key the key.
  void AsyncErase(rt::Handle &handle, const KTYPE &key);

  /// @brief Clear the content of the multimap.
  void Clear() {
    auto clearLambda = [](const ObjectID &oid) {
      auto mapPtr = HmapT::GetPtr(oid);
      mapPtr->localMultimap_.Clear();
    };
    rt::executeOnAll(clearLambda, oid_);
  }

  using LookupResult = typename LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::LookupResult;
  using LookupRemoteResult = typename LocalMultimap<KTYPE, VTYPE, KEY_COMPARE>::LookupRemoteResult;

  /// @brief Get all the values associated to a key.
  /// @param[in] key the key.
  /// @param[out] res a pointer to the values if the the key-value was found, otherwise NULL
  /// @return true if the entry is found, otherwise false
  bool Lookup(const KTYPE &key, LookupResult *res);

  /// @brief Asynchronous lookup method.
  /// @warning Asynchronous operations are guaranteed to have completed.
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle to be used to wait for completion.
  /// @param[in] key the key.
  /// @param[out] res the result of the lookup operation.
  void AsyncLookup(rt::Handle &handle, const KTYPE &key, LookupResult *res);

  /// @brief Read records from multiple files
  /// @param[in] prefix the file prefix
  /// @param[in] lb the lower bound postscript
  /// @param[in] ub the upper bound postscript
  void readFromFiles(rt::Handle & handle, std::string prefix, uint64_t lb, uint64_t ub);


  /// @brief Apply a user-defined function to a key-value pair.
  ///
  /// @tparam ApplyFunT User-defined function type. The function prototype should be:
  /// @code
  /// void(const KTYPE&, std::vector<VTYPE> &, Args&);
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
  /// @tparam ApplyFunT User-defined function type. The function prototype should be:
  /// @code
  /// void(const KTYPE&, std::vector<VTYPE>&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle Reference to the handle.
  /// @param key The key.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncApply(rt::Handle &handle, const KTYPE &key, ApplyFunT &&function, Args &... args);

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
                             uint32_t* resultSize, Args &... args);

  /// @brief Apply a user-defined function to each key-value pair.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype should be:
  /// @code
  /// void(const KTYPE&, std::vector<VTYPE> *, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void ForEachEntry(ApplyFunT &&function, Args &... args);

  /// @brief Asynchronously apply a user-defined function to each key-value pair.
  ///
  /// @tparam ApplyFunT User-defined function type.  he function prototype should be:
  /// @code
  /// void(shad::rt::Handle&, const KTYPE&, std::vector<VTYPE> *, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle Reference to the handle.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncForEachEntry(rt::Handle &handle, ApplyFunT &&function, Args &... args);

  /// @brief Apply a user-defined function to each key.
  ///
  /// @tparam ApplyFunT User-defined function type. The function prototype should be:
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
  /// @tparam ApplyFunT User-defined function type. The function prototype should be:
  /// @code
  /// void(shad::rt::Handle&, const KTYPE&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle Reference to the handle.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncForEachKey(rt::Handle &handle, ApplyFunT &&function, Args &... args);

  void PrintAllEntries() {
    auto printLambda = [](const ObjectID & oid) {
      auto mapPtr = HmapT::GetPtr(oid);
      mapPtr->localMultimap_.PrintAllEntries();
    };

    for (auto loc : rt::allLocalities()) {
      rt::executeAt(loc, printLambda, oid_);
    }
  }

  void PrintAllKeys() {
    auto printLambda = [](const ObjectID & oid) {
      auto mapPtr = HmapT::GetPtr(oid);
      mapPtr->localMultimap_.PrintAllKeys();
    };

    for (auto loc : rt::allLocalities()) rt::executeAt(loc, printLambda, oid_);
  }

  // FIXME it should be protected
  void BufferEntryInsert(const EntryT &entry) {
    localMultimap_.Insert(entry.key, entry.value);
  }

  iterator begin() { return iterator::multimap_begin(this); }
  iterator end() { return iterator::multimap_end(this); }
  const_iterator cbegin() const { return const_iterator::multimap_begin(this); }
  const_iterator cend() const { return const_iterator::multimap_end(this); }
  const_iterator begin() const { return cbegin(); }
  const_iterator end() const { return cend(); }
  local_iterator local_begin() {
    return local_iterator::lmultimap_begin(&localMultimap_);
  }
  local_iterator local_end() { return local_iterator::lmultimap_end(&localMultimap_); }
  const_local_iterator clocal_begin() {
    return const_local_iterator::lmultimap_begin(&localMultimap_);
  }
  const_local_iterator clocal_end() {
    return const_local_iterator::lmultimap_end(&localMultimap_);
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
  ObjectID oid_;
  LocalMultimap<KTYPE, VTYPE, KEY_COMPARE> localMultimap_;
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

  struct RFArgs {
    uint64_t lb;
    ObjectID oid;
    char prefix[PREFIX_SIZE];
  };

 protected:
  Multimap(ObjectID oid, const size_t numEntries)
      : oid_(oid),
        localMultimap_(std::max(numEntries / (constants::kMMapDefaultNumEntriesPerBucket * rt::numLocalities()), 1lu)),
        buffers_(oid) {}
};

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
inline size_t Multimap<KTYPE, VTYPE, KEY_COMPARE>::Size() const {
  size_t size = localMultimap_.size_;
  size_t remoteSize;

  auto sizeLambda = [](const ObjectID &oid, size_t *res) {
    auto mapPtr = HmapT::GetPtr(oid);
    *res = mapPtr->localMultimap_.size_;
  };

  for (auto tgtLoc : rt::allLocalities()) {
    if (tgtLoc != rt::thisLocality()) {
      rt::executeAtWithRet(tgtLoc, sizeLambda, oid_, &remoteSize);
      size += remoteSize;
    }
  }

  return size;
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
inline size_t Multimap<KTYPE, VTYPE, KEY_COMPARE>::NumberKeys() const {
  size_t size = localMultimap_.numberKeys_;
  size_t remoteKeys;

  auto sizeLambda = [](const ObjectID &oid, size_t *res) {
    auto mapPtr = HmapT::GetPtr(oid);
    *res = mapPtr->localMultimap_.numberKeys_;
  };

  for (auto tgtLoc : rt::allLocalities()) {
    if (tgtLoc != rt::thisLocality()) {
      rt::executeAtWithRet(tgtLoc, sizeLambda, oid_, &remoteKeys);
      size += remoteKeys;
    }
  }

  return size;
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
inline std::pair < typename Multimap<KTYPE, VTYPE, KEY_COMPARE>::iterator, bool >
Multimap<KTYPE, VTYPE, KEY_COMPARE>::Insert(const KTYPE &key, const VTYPE &value) {
  using itr_traits = distributed_iterator_traits<iterator>;

  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  std::pair<iterator, bool> res;

  if (targetLocality == rt::thisLocality()) {
    auto lres = localMultimap_.Insert(key, value);
    res.first = itr_traits::iterator_from_local(begin(), end(), lres.first);
    res.second = lres.second;
  } else {
    auto insertLambda =
        [](const std::tuple<iterator, iterator, InsertArgs> &args_,
           std::pair<iterator, bool> *res_ptr) {
          auto &args(std::get<2>(args_));
          auto mapPtr = HmapT::GetPtr(args.oid);
          auto lres = mapPtr->localMultimap_.Insert(args.key, args.value);
          res_ptr->first = itr_traits::iterator_from_local (std::get<0>(args_), std::get<1>(args_), lres.first);
          res_ptr->second = lres.second;
        };
    rt::executeAtWithRet(
        targetLocality, insertLambda,
        std::make_tuple(begin(), end(), InsertArgs{oid_, key, value}), &res);
  }

  return res;
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
inline void Multimap<KTYPE, VTYPE, KEY_COMPARE>::AsyncInsert(
    rt::Handle &handle, const KTYPE &key, const VTYPE &value) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localMultimap_.AsyncInsert(handle, key, value);
  } else {
    auto insertLambda = [](rt::Handle &handle, const InsertArgs &args) {
      auto mapPtr = HmapT::GetPtr(args.oid);
      mapPtr->localMultimap_.AsyncInsert(handle, args.key, args.value);
    };
    InsertArgs args = {oid_, key, value};
    rt::asyncExecuteAt(handle, targetLocality, insertLambda, args);
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
inline void Multimap<KTYPE, VTYPE, KEY_COMPARE>::BufferedInsert(const KTYPE &key, const VTYPE &value) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  buffers_.Insert(EntryT(key, value), targetLocality);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
inline void Multimap<KTYPE, VTYPE, KEY_COMPARE>::
     BufferedAsyncInsert(rt::Handle &handle, const KTYPE &key, const VTYPE &value) {

  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  buffers_.AsyncInsert(handle, EntryT(key, value), targetLocality);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
inline void Multimap<KTYPE, VTYPE, KEY_COMPARE>::Erase(const KTYPE &key) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localMultimap_.Erase(key);
  } else {
    auto eraseLambda = [](const LookupArgs &args) {
      auto mapPtr = HmapT::GetPtr(args.oid);
      mapPtr->localMultimap_.Erase(args.key);
    };
    LookupArgs args = {oid_, key};
    rt::executeAt(targetLocality, eraseLambda, args);
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
inline void Multimap<KTYPE, VTYPE, KEY_COMPARE>::AsyncErase(rt::Handle &handle, const KTYPE &key) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localMultimap_.AsyncErase(handle, key);
  } else {
    auto eraseLambda = [](rt::Handle &handle, const LookupArgs &args) {
      auto mapPtr = HmapT::GetPtr(args.oid);
      mapPtr->localMultimap_.AsyncErase(handle, args.key);
    };
    LookupArgs args = {oid_, key};
    rt::asyncExecuteAt(handle, targetLocality, eraseLambda, args);
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
inline bool Multimap<KTYPE, VTYPE, KEY_COMPARE>::Lookup(const KTYPE &key, LookupResult *res) {
  rt::Handle handle;
  AsyncLookup(handle, key, res);
  waitForCompletion(handle);
  return res->found;
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
inline void Multimap<KTYPE, VTYPE, KEY_COMPARE>::AsyncLookup(
      rt::Handle & handle, const KTYPE & key, LookupResult * result) {

  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
 
  if (targetLocality == rt::thisLocality()) {
    localMultimap_.AsyncLookup(handle, key, result);
 
  } else {

    auto lookupLambda = [](const LookupArgs & args, LookupRemoteResult * ret) {
       auto my_key = args.key;
       auto my_map = HmapT::GetPtr(args.oid);

       LookupRemoteResult remote_result;
       my_map->localMultimap_.LookupFromRemote(my_key, & remote_result);

       * ret = remote_result;
    };

    // executed at remote site, so meaning of remote and local are switched
    auto lookupFetchLambda = [](rt::Handle & handle, const LookupRemoteResult & args) {
       rt::dma<VTYPE>(args.localLoc, args.local_elems, args.remote_elems, args.size);
       free(args.remote_elems);
    };

    LookupArgs args = {oid_, key};
    LookupRemoteResult remote_result;
    rt::executeAtWithRet(targetLocality, lookupLambda, args, & remote_result);

    (* result).found = remote_result.found;
    (* result).size = remote_result.size;
     
    if (remote_result.found) {
        (* result).value.resize(remote_result.size);
        remote_result.localLoc = rt::thisLocality();
        remote_result.local_elems = (* result).value.data();
        rt::asyncExecuteAt(handle, targetLocality, lookupFetchLambda, remote_result);
    }
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
inline void Multimap<KTYPE, VTYPE, KEY_COMPARE>::readFromFiles(
     rt::Handle & handle, std::string prefix, uint64_t lb, uint64_t ub) {

  auto readFileLambda = [](rt::Handle & handle, const RFArgs & args, size_t it) {
     std::string line;
     auto my_map = HmapT::GetPtr(args.oid);
     std::string filename = args.prefix + std::to_string(args.lb + it);
     printf("reading file %s\n", filename.c_str());

     std::ifstream file(filename);
     if (! file.is_open()) { printf("Cannot open file %s\n", filename.c_str()); exit(-1); }

     while (getline(file, line)) {
       if (line[0] == '#') continue;     // skip comments

       VTYPE record = VTYPE(line);
       // my_map->AsyncInsert(handle, record.key(), record);
       my_map->BufferedAsyncInsert(handle, record.key(), record);
  } };

  RFArgs args = {lb, oid_};
  memcpy(args.prefix, prefix.c_str(), prefix.size() + 1);
  rt::asyncForEachOnAll(handle, readFileLambda, args, ub - lb + 1);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ApplyFunT, typename... Args>
void Multimap<KTYPE, VTYPE, KEY_COMPARE>::ForEachEntry(ApplyFunT &&function, Args &... args) {
  using FunctionTy = void (*)(const KTYPE &, std::vector<VTYPE> &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);

  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  using ArgsTuple = std::tuple<LMapT *, FunctionTy, std::tuple<Args...>>;

  feArgs arguments(oid_, fn, std::tuple<Args...>(args...));
  auto feLambda = [](const feArgs &args) {
    auto mapPtr = HmapT::GetPtr(std::get<0>(args));
    ArgsTuple argsTuple(&mapPtr->localMultimap_, std::get<1>(args), std::get<2>(args));
    rt::forEachAt(rt::thisLocality(),
                  LMapT::template ForEachEntryFunWrapper<ArgsTuple, Args...>,
                  argsTuple, mapPtr->localMultimap_.numBuckets_);
  };
  rt::executeOnAll(feLambda, arguments);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ApplyFunT, typename... Args>
void Multimap<KTYPE, VTYPE, KEY_COMPARE>::AsyncForEachEntry(
    rt::Handle &handle, ApplyFunT &&function, Args &... args) {

  using FunctionTy = void (*)(rt::Handle &, const KTYPE &, std::vector<VTYPE> &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);

  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  using ArgsTuple = std::tuple<LMapT *, FunctionTy, std::tuple<Args...>>;

  feArgs arguments(oid_, fn, std::tuple<Args...>(args...));
  auto feLambda = [](rt::Handle &handle, const feArgs &args) {
    auto mapPtr = HmapT::GetPtr(std::get<0>(args));
    ArgsTuple argsTuple(&mapPtr->localMultimap_, std::get<1>(args), std::get<2>(args));

    rt::asyncForEachAt(
        handle, rt::thisLocality(),
        LMapT::template AsyncForEachEntryFunWrapper<ArgsTuple, Args...>,
        argsTuple, mapPtr->localMultimap_.numBuckets_);
  };
  rt::asyncExecuteOnAll(handle, feLambda, arguments);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ApplyFunT, typename... Args>
void Multimap<KTYPE, VTYPE, KEY_COMPARE>::ForEachKey(ApplyFunT &&function, Args &... args) {
  using FunctionTy = void (*)(const KTYPE &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);

  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  using ArgsTuple = std::tuple<LMapT *, FunctionTy, std::tuple<Args...>>;

  feArgs arguments(oid_, fn, std::tuple<Args...>(args...));
  auto feLambda = [](const feArgs &args) {
    auto mapPtr = HmapT::GetPtr(std::get<0>(args));
    ArgsTuple argsTuple(&mapPtr->localMultimap_, std::get<1>(args), std::get<2>(args));
    rt::forEachAt(rt::thisLocality(),
                  LMapT::template ForEachKeyFunWrapper<ArgsTuple, Args...>,
                  argsTuple, mapPtr->localMultimap_.numBuckets_);
  };
  rt::executeOnAll(feLambda, arguments);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ApplyFunT, typename... Args>
void Multimap<KTYPE, VTYPE, KEY_COMPARE>::AsyncForEachKey(
    rt::Handle &handle, ApplyFunT &&function, Args &... args) {
  using FunctionTy = void (*)(rt::Handle &, const KTYPE &, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);

  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  using ArgsTuple = std::tuple<LMapT *, FunctionTy, std::tuple<Args...>>;

  feArgs arguments(oid_, fn, std::tuple<Args...>(args...));
  auto feLambda = [](rt::Handle &handle, const feArgs &args) {
    auto mapPtr = HmapT::GetPtr(std::get<0>(args));
    ArgsTuple argsTuple(&mapPtr->localMultimap_, std::get<1>(args), std::get<2>(args));
    rt::asyncForEachAt(
        handle, rt::thisLocality(),
        LMapT::template AsyncForEachKeyFunWrapper<ArgsTuple, Args...>,
        argsTuple, mapPtr->localMultimap_.numBuckets_);
  };
  rt::asyncExecuteOnAll(handle, feLambda, arguments);
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ApplyFunT, typename... Args>
void Multimap<KTYPE, VTYPE, KEY_COMPARE>::Apply(const KTYPE &key, ApplyFunT &&function, Args &... args) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localMultimap_.Apply(key, function, args...);

  } else {
    using FunctionTy = void (*)(const KTYPE &, std::vector<VTYPE> &, Args &...);
    FunctionTy fn = std::forward<decltype(function)>(function);

    using ArgsTuple = std::tuple<ObjectID, const KTYPE, FunctionTy, std::tuple<Args...>>;
    ArgsTuple arguments(oid_, key, fn, std::tuple<Args...>(args...));

    auto feLambda = [](const ArgsTuple &args) {
      constexpr auto Size = std::tuple_size<typename std::decay<decltype(std::get<3>(args))>::type>::value;
      ArgsTuple &tuple = const_cast<ArgsTuple &>(args);
      LMapT *mapPtr = &(HmapT::GetPtr(std::get<0>(tuple))->localMultimap_);
      LMapT::CallApplyFun(mapPtr, std::get<1>(tuple), std::get<2>(tuple),
                          std::get<3>(tuple), std::make_index_sequence<Size>{});
    };
    rt::executeAt(targetLocality, feLambda, arguments);
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ApplyFunT, typename... Args>
void Multimap<KTYPE, VTYPE, KEY_COMPARE>::AsyncApply(
    rt::Handle &handle, const KTYPE &key, ApplyFunT &&function, Args &... args) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localMultimap_.AsyncApply(handle, key, function, args...);

  } else {
    using FunctionTy = void (*)(rt::Handle &, const KTYPE &, std::vector<VTYPE> &, Args &...);
    FunctionTy fn = std::forward<decltype(function)>(function);
    using ArgsTuple = std::tuple<ObjectID, const KTYPE, FunctionTy, std::tuple<Args...>>;

    ArgsTuple arguments(oid_, key, fn, std::tuple<Args...>(args...));
    auto feLambda = [](rt::Handle &handle, const ArgsTuple &args) {
      constexpr auto Size = std::tuple_size<typename std::decay<decltype(std::get<3>(args))>::type>::value;
      ArgsTuple &tuple(const_cast<ArgsTuple &>(args));
      LMapT *mapPtr = &(HmapT::GetPtr(std::get<0>(tuple))->localMultimap_);
      LMapT::AsyncCallApplyFun(handle, mapPtr, std::get<1>(tuple),
                               std::get<2>(tuple), std::get<3>(tuple),
                               std::make_index_sequence<Size>{});
    };
    rt::asyncExecuteAt(handle, targetLocality, feLambda, arguments);
  }
}

template <typename KTYPE, typename VTYPE, typename KEY_COMPARE>
template <typename ApplyFunT, typename... Args>
void Multimap<KTYPE, VTYPE, KEY_COMPARE>::AsyncApplyWithRetBuff(
                              rt::Handle &handle, const KTYPE &key,
                              ApplyFunT &&function, uint8_t* result,
                              uint32_t* resultSize, Args &... args) {
  size_t targetId = shad::hash<KTYPE>{}(key) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localMultimap_.AsyncApplyWithRetBuff(handle, key,
                                         function, result,
                                         resultSize, args...);
  } else {
    using FunctionTy = void (*)(rt::Handle &, const KTYPE &,
                                std::vector<VTYPE> &, Args &..., uint8_t*, uint32_t*);
    FunctionTy fn = std::forward<decltype(function)>(function);
    using ArgsTuple = std::tuple<ObjectID, const KTYPE, FunctionTy, std::tuple<Args...>>;

    ArgsTuple arguments(oid_, key, fn, std::tuple<Args...>(args...));
    auto feLambda = [](rt::Handle &handle, const ArgsTuple &args,
                       uint8_t* result, uint32_t* resultSize) {
      constexpr auto Size = std::tuple_size<typename std::decay<decltype(std::get<3>(args))>::type>::value;
      ArgsTuple &tuple(const_cast<ArgsTuple &>(args));
      LMapT *mapPtr = &(HmapT::GetPtr(std::get<0>(tuple))->localMultimap_);
      LMapT::AsyncCallApplyWithRetBuffFun(handle, mapPtr, std::get<1>(tuple),
                                          std::get<2>(tuple), std::get<3>(tuple),
                                          std::make_index_sequence<Size>{},
                                          result, resultSize);
    };
    rt::asyncExecuteAtWithRetBuff(handle, targetLocality, feLambda,
                                    arguments, result, resultSize);
  }
}

template <typename MapT, typename T, typename NonConstT>
class multimap_iterator : public std::iterator<std::forward_iterator_tag, T> {
 public:
  using OIDT = typename MapT::ObjectID;
  using LMap = typename MapT::LMapT;
  using inner_type = typename LMap::inner_type;
  using local_iterator_type = lmultimap_iterator<LMap, T>;
  using value_type = NonConstT;

  multimap_iterator() {}
  multimap_iterator(uint32_t locID, const OIDT mapOID, local_iterator_type &lit, T element) {
    data_ = {locID, mapOID, lit, element};
  }

  multimap_iterator(uint32_t locID, const OIDT mapOID, local_iterator_type &lit) {
    auto mapPtr = MapT::GetPtr(mapOID);
    const LMap *lmapPtr = &(mapPtr->localMultimap_);

    if (lit != local_iterator_type::lmultimap_end(lmapPtr))
      data_ = itData(locID, mapOID, lit, *lit);
    else
      *this = multimap_end(mapPtr.get());
  }

  static multimap_iterator multimap_begin(const MapT *mapPtr) {
    const LMap *lmapPtr = &(mapPtr->localMultimap_);
    auto localEnd = local_iterator_type::lmultimap_end(lmapPtr);

    if (static_cast<uint32_t>(rt::thisLocality()) == 0) {
      auto localBegin = local_iterator_type::lmultimap_begin(lmapPtr);
      if (localBegin != localEnd) { return multimap_iterator(0, mapPtr->oid_, localBegin); }
      multimap_iterator beg(0, mapPtr->oid_, localEnd, T());
      return ++beg;
    }

    auto getItLambda = [](const OIDT &mapOID, multimap_iterator *res) {
      auto mapPtr = MapT::GetPtr(mapOID);
      const LMap *lmapPtr = &(mapPtr->localMultimap_);
      auto localEnd = local_iterator_type::lmultimap_end(lmapPtr);
      auto localBegin = local_iterator_type::lmultimap_begin(lmapPtr);
      if (localBegin != localEnd) {
        *res = multimap_iterator(0, mapOID, localBegin);
      } else {
        multimap_iterator beg(0, mapOID, localEnd, T());
        *res = ++beg;
      }
    };

    multimap_iterator beg(0, mapPtr->oid_, localEnd, T());
    rt::executeAtWithRet(rt::Locality(0), getItLambda, mapPtr->oid_, &beg);
    return beg;
  }

  static multimap_iterator multimap_end(const MapT *mapPtr) {
    local_iterator_type lend = local_iterator_type::lmultimap_end(&(mapPtr->localMultimap_));
    multimap_iterator end(rt::numLocalities(), OIDT(0), lend, T());
    return end;
  }

  bool operator==(const multimap_iterator &other) const {
    return (data_ == other.data_);
  }

  bool operator!=(const multimap_iterator &other) const { return !(*this == other); }

  T operator*() const { return data_.element_; }

  multimap_iterator &operator++() {
    auto mapPtr = MapT::GetPtr(data_.oid_);

    if (static_cast<uint32_t>(rt::thisLocality()) == data_.locId_) {
      const LMap *lmapPtr = &(mapPtr->localMultimap_);
      auto lend = local_iterator_type::lmultimap_end(lmapPtr);
      if (data_.lmapIt_ != lend) { ++(data_.lmapIt_); }

      if (data_.lmapIt_ != lend) {
         data_.element_ = *(data_.lmapIt_);
         return *this;

      } else {      // find the local begin on next localities
        itData itd;

        for (uint32_t i = data_.locId_ + 1; i < rt::numLocalities(); ++i) {
          rt::executeAtWithRet(rt::Locality(i), getLocBeginIt, data_.oid_, &itd);
          // if Data is valid
          if (itd.locId_ != rt::numLocalities()) { data_ = itd; return *this; }
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

  multimap_iterator operator++(int) {
    multimap_iterator tmp = *this;
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

  static local_iterator_range local_range(multimap_iterator &B, multimap_iterator &E) {
    auto mapPtr = MapT::GetPtr(B.data_.oid_);
    local_iterator_type lbeg, lend;
    uint32_t thisLocId = static_cast<uint32_t>(rt::thisLocality());

    if (B.data_.locId_ == thisLocId) {
      lbeg = B.data_.lmapIt_;
    } else {
      lbeg = local_iterator_type::lmultimap_begin(&(mapPtr->localMultimap_));
    }

    if (E.data_.locId_ == thisLocId) {
      lend = E.data_.lmapIt_;
    } else {
      lend = local_iterator_type::lmultimap_end(&(mapPtr->localMultimap_));
    }

    return local_iterator_range(lbeg, lend);
  }

  static rt::localities_range localities(multimap_iterator &B, multimap_iterator &E) {
    return rt::localities_range(rt::Locality(B.data_.locId_),
                                rt::Locality(std::min<uint32_t>(rt::numLocalities(), E.data_.locId_ + 1)));
  }

  static multimap_iterator iterator_from_local(multimap_iterator &B, multimap_iterator &E, local_iterator_type itr) {
    return multimap_iterator(static_cast<uint32_t>(rt::thisLocality()), B.data_.oid_, itr);
  }

 private:
  struct itData {
    itData() : oid_(0), lmapIt_(nullptr, 0, 0, nullptr, nullptr, typename std::vector<inner_type>::iterator()) {}
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
    auto lmapPtr = &(mapPtr->localMultimap_);
    auto localEnd = local_iterator_type::lmultimap_end(lmapPtr);
    auto localBegin = local_iterator_type::lmultimap_begin(lmapPtr);
    if (localBegin != localEnd) {
      *res = itData(static_cast<uint32_t>(rt::thisLocality()), mapOID, localBegin, *localBegin);
    } else {
      *res = itData(rt::numLocalities(), OIDT(0), localEnd, T());
    }
  }

  static void getRemoteIt(const itData &itd, itData *res) {
    auto mapPtr = MapT::GetPtr(itd.oid_);
    auto lmapPtr = &(mapPtr->localMultimap_);
    auto localEnd = local_iterator_type::lmultimap_end(lmapPtr);
    local_iterator_type cit = itd.lmapIt_;
    ++cit;

    if (cit != localEnd) {
      *res = itData(static_cast<uint32_t>(rt::thisLocality()), itd.oid_, cit, *cit);
      return;
    } else {
      itData outitd;
      for (uint32_t i = itd.locId_ + 1; i < rt::numLocalities(); ++i) {
        rt::executeAtWithRet(rt::Locality(i), getLocBeginIt, itd.oid_, &outitd);
        // It Data is valid
        if (outitd.locId_ != rt::numLocalities()) { *res = outitd; return; }
      }
      *res = itData(rt::numLocalities(), OIDT(0), localEnd, T());
    }
  }
};

}  // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_MULTIMAP_H_
