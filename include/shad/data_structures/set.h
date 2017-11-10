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


#ifndef INCLUDE_SHAD_DATA_STRUCTURES_SET_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_SET_H_

#include <algorithm>
#include <tuple>
#include <utility>
#include <vector>

#include "shad/data_structures/abstract_data_structure.h"
#include "shad/data_structures/buffer.h"
#include "shad/data_structures/compare_and_hash_utils.h"
#include "shad/data_structures/local_set.h"
#include "shad/runtime/runtime.h"

namespace shad {

/// @brief The Set data structure.
///
/// SHAD's set is a distributed, unordered, set.
/// @tparam T type of the entries stored in the set.
/// @tparam ELEM_COMPARE element comparison function; default is MemCmp<T>.
/// @warning obects of type T need to be trivially copiable.
template <typename T, typename ELEM_COMPARE = MemCmp<T>>
class Set
      : public AbstractDataStructure<Set<T, ELEM_COMPARE>> {
  template<typename> friend class AbstractDataStructure;
 public:
  using SetT = Set<T, ELEM_COMPARE>;
  using LSetT = LocalSet<T, ELEM_COMPARE>;
  using ObjectID =
              typename AbstractDataStructure<SetT>::ObjectID;
  using ShadSetPtr =
              typename AbstractDataStructure<SetT>::SharedPtr;
  using BuffersVector = typename impl::BuffersVector<T, SetT>;

  /// @brief Create method.
  ///
  /// Creates a new set instance.
  /// @param numEntries Expected number of elements.
  /// @return A shared pointer to the newly created set instance.
#ifdef DOXYGEN_IS_RUNNING
  static ShadSetPtr Create(const size_t numEntries);
#endif

  /// @brief Getter of the Global Identifier.
  ///
  /// @return The global identifier associated with the set instance.
  ObjectID GetGlobalID() const {
    return oid_;
  }

  /// @brief Overall size of the set (number of elements).
  /// @warning Calling the size method may result in one-to-all
  /// communication among localities to retrieve consinstent information.
  /// @return the size of the set.
  size_t Size() const;

  /// @brief Insert an element in the set.
  /// @param[in] element the element.
  void Insert(const T& element);

  /// @brief Asynchronously Insert an element in the set.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle
  /// to be used to wait for completion.
  /// @param[in] element the element.
  void AsyncInsert(rt::Handle& handle, const T& element);

  /// @brief Buffered Insert method.
  /// Inserts an element, using aggregation buffers.
  /// @warning Insertions are finalized only after calling
  /// the WaitForBufferedInsert() method.
  /// @param[in] element The element.
  void BufferedInsert(const T& element);

  /// @brief Asynchronous Buffered Insert method.
  /// Asynchronously inserts an element, using aggregation buffers.
  /// @warning asynchronous buffered insertions are finalized only after
  /// calling the rt::waitForCompletion(rt::Handle &handle) method AND
  /// the WaitForBufferedInsert() method, in this order.
  /// @param[in,out] handle Reference to the handle
  /// @param[in] element The element.
  void BufferedAsyncInsert(rt::Handle &handle,
                           const T& element);

  /// @brief Finalize method for buffered insertions.
  void WaitForBufferedInsert() {
    buffers_.FlushAll();
  }
  /// @brief Remove an element from the set.
  /// @param[in] element the element.
  void Erase(const T& element);

  /// @brief Asynchronously remove an element from the set.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in,out] handle Reference to the handle
  /// to be used to wait for completion.
  /// @param[in] element the element.
  void AsyncErase(rt::Handle& handle, const T& element);

  /// @brief Clear the content of the set.
  void Clear() {
    auto clearLambda = [] (const ObjectID& oid) {
      auto setPtr = SetT::GetPtr(oid);
      setPtr->localSet_.Clear();
    };
    rt::executeOnAll(clearLambda, oid_);
  }

  /// @brief Clear the content of the set.
  void Reset(size_t numElements) {
    auto resetLambda = [] (const std::tuple<ObjectID, size_t> &t) {
      auto setPtr = SetT::GetPtr(std::get<0>(t));
      setPtr->localSet_.Reset(std::get<1>(t));
    };
    rt::executeOnAll(resetLambda, std::make_tuple(oid_, numElements));
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
  template<typename ApplyFunT, typename ...Args>
  void ForEachElement(ApplyFunT &&function, Args&... args);


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
  template<typename ApplyFunT, typename ...Args>
  void AsyncForEachElement(rt::Handle & handle, ApplyFunT &&function,
                         Args&... args);

  /// @brief Print all the entries in the set.
  /// @warning std::ostream & operator<< must be defined for T.
  void PrintAllElements() {
    auto printLambda = [] (const ObjectID& oid) {
      auto setPtr = SetT::GetPtr(oid);
      std::cout << "---- Locality: " << rt::thisLocality() << std::endl;
      setPtr->localSet_.PrintAllElements();
    };
    rt::executeOnAll(printLambda, oid_);
  }

  // FIXME it should be protected
  void BufferEntryInsert(const T& element) {
    localSet_.Insert(element);
  }

 private:
  ObjectID oid_;
  LocalSet<T, ELEM_COMPARE> localSet_;
  BuffersVector buffers_;

  struct ExeAtArgs {
    ObjectID oid;
    T element;
  };

 protected:
  Set(ObjectID oid, const size_t numEntries)
      : oid_(oid),
      localSet_(std::max(numEntries/constants::kSetDefaultNumEntriesPerBucket,
                         1lu)),
      buffers_(oid) { }
};

template <typename T, typename ELEM_COMPARE>
inline size_t Set<T, ELEM_COMPARE>::Size() const {
  size_t size = localSet_.size_;
  size_t remoteSize(0);
  auto sizeLambda = [] (const ObjectID &oid, size_t *res) {
    auto setPtr = SetT::GetPtr(oid);
    *res = setPtr->localSet_.size_;
  };
  for (auto tgtLoc : rt::allLocalities()) {
    if (tgtLoc != rt::thisLocality()) {
      rt::executeAtWithRet(tgtLoc, sizeLambda, oid_, &remoteSize);
      size += remoteSize;
    }
  }
  return size;
}

template <typename T, typename ELEM_COMPARE>
inline void
Set<T, ELEM_COMPARE>::Insert(const T& element) {
  uint64_t targetId =
      HashFunction<T>(element, 0) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localSet_.Insert(element);
  } else {
    auto insertLambda = [] (const ExeAtArgs &args) {
      auto setPtr = SetT::GetPtr(args.oid);
      setPtr->localSet_.Insert(args.element);
    };
    ExeAtArgs args = {oid_, element};
    rt::executeAt(targetLocality, insertLambda, args);
  }
}

template <typename T, typename ELEM_COMPARE>
inline void Set<T, ELEM_COMPARE>::AsyncInsert(rt::Handle& handle,
                                             const T& element) {
  uint64_t targetId =
      HashFunction<T>(element, 0) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    localSet_.AsyncInsert(handle, element);
  } else {
    auto insertLambda = [] (rt::Handle& handle, const ExeAtArgs &args) {
      auto setPtr = SetT::GetPtr(args.oid);
      setPtr->localSet_.AsyncInsert(handle, args.element);
    };
    ExeAtArgs args = {oid_, element};
    rt::asyncExecuteAt(handle, targetLocality, insertLambda, args);
  }
}

template <typename T, typename ELEM_COMPARE>
inline void
Set<T, ELEM_COMPARE>::BufferedInsert(const T& element) {
  uint64_t targetId =
      HashFunction<T>(element, 0) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    localSet_.Insert(element);
  } else {
    buffers_.Insert(element, targetLocality);
  }
}

template <typename T, typename ELEM_COMPARE>
inline void Set<T, ELEM_COMPARE>::BufferedAsyncInsert(rt::Handle &handle,
                                                     const T& element) {
  uint64_t targetId =
      HashFunction<T>(element, 0) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    localSet_.AsyncInsert(handle, element);
  } else {
    buffers_.AsyncInsert(handle, element, targetLocality);
  }
}

template <typename T, typename ELEM_COMPARE>
inline void Set<T, ELEM_COMPARE>::Erase(const T& element) {
  uint64_t targetId =
      HashFunction<T>(element, 0) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    localSet_.Erase(element);
  } else {
    auto eraseLambda = [] (const ExeAtArgs &args) {
      auto setPtr = SetT::GetPtr(args.oid);
      setPtr->localSet_.Erase(args.element);
    };
    ExeAtArgs args = {oid_, element};
    rt::executeAt(targetLocality, eraseLambda, args);
  }
}

template <typename T, typename ELEM_COMPARE>
inline void Set<T, ELEM_COMPARE>::AsyncErase(rt::Handle& handle,
                                            const T& element) {
  uint64_t targetId =
      HashFunction<T>(element, 0) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    localSet_.AsyncErase(handle, element);
  } else {
    auto eraseLambda = [] (rt::Handle& handle, const ExeAtArgs &args) {
      auto setPtr = SetT::GetPtr(args.oid);
      setPtr->localSet_.AsyncErase(handle, args.element);
    };
    ExeAtArgs args = {oid_, element};
    rt::asyncExecuteAt(handle, targetLocality, eraseLambda, args);
  }
}

template <typename T, typename ELEM_COMPARE>
inline bool Set<T, ELEM_COMPARE>::Find(const T& element) {
  uint64_t targetId =
      HashFunction<T>(element, 0) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    return localSet_.Find(element);
  } else {
    auto findLambda = [] (const ExeAtArgs &args, bool *res) {
      auto setPtr = SetT::GetPtr(args.oid);
      *res = setPtr->localSet_.Find(args.element);
    };
    ExeAtArgs args = {oid_, element};
    bool found;
    rt::executeAtWithRet(targetLocality, findLambda, args, &found);
    return found;
  }
  return false;
}

template <typename T, typename ELEM_COMPARE>
inline void Set<T, ELEM_COMPARE>::
AsyncFind(rt::Handle& handle, const T& element, bool* found) {
  uint64_t targetId =
      HashFunction<T>(element, 0) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localSet_.AsyncFind(handle, element, found);
  } else {
    auto findLambda = [] (rt::Handle&, const ExeAtArgs &args, bool *res) {
      auto setPtr = SetT::GetPtr(args.oid);
      *res = setPtr->localSet_.Find(args.element);
    };
    ExeAtArgs args = {oid_, element};
    rt::asyncExecuteAtWithRet(handle, targetLocality,
                              findLambda, args, found);
  }
}

template <typename T, typename ELEM_COMPARE>
template<typename ApplyFunT, typename ...Args>
void Set<T, ELEM_COMPARE>::ForEachElement(ApplyFunT &&function, Args&... args) {
  using FunctionTy = void(*)(const T&, Args&...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  using ArgsTuple = std::tuple<LSetT*, FunctionTy, std::tuple<Args...>>;
  feArgs arguments(oid_, fn, std::tuple<Args...>(args...));
  auto feLambda = [](const feArgs &args) {
    auto setPtr = SetT::GetPtr(std::get<0>(args));
    ArgsTuple argsTuple(&setPtr->localSet_,
                        std::get<1>(args),
                        std::get<2>(args));
    rt::forEachAt(rt::thisLocality(),
        LSetT:: template ForEachElementFunWrapper<ArgsTuple, Args...>,
                         argsTuple, setPtr->localSet_.numBuckets_);
  };
  rt::executeOnAll(feLambda, arguments);
}

template <typename T, typename ELEM_COMPARE>
template<typename ApplyFunT, typename ...Args>
void Set<T, ELEM_COMPARE>::AsyncForEachElement(rt::Handle& handle,
                                              ApplyFunT &&function,
                                              Args&... args) {
  using FunctionTy = void(*)(rt::Handle&, const T&, Args&...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  using ArgsTuple = std::tuple<LSetT*, FunctionTy, std::tuple<Args...>>;
  feArgs arguments{oid_, fn, std::tuple<Args...>(args...)};
  auto feLambda = [](rt::Handle& handle, const feArgs &args) {
    auto setPtr = SetT::GetPtr(std::get<0>(args));
    ArgsTuple argsTuple = std::make_tuple(&setPtr->localSet_,
                                          std::get<1>(args),
                                          std::get<2>(args));
    rt::asyncForEachAt(handle, rt::thisLocality(),
        LSetT:: template AsyncForEachElementFunWrapper<ArgsTuple,
                                                       Args...>,
                       argsTuple, setPtr->localSet_.numBuckets_);
  };
  rt::asyncExecuteOnAll(handle, feLambda, arguments);
}

}  // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_SET_H_
