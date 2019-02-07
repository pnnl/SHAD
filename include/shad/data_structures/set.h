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

#ifndef INCLUDE_SHAD_DATA_STRUCTURES_SET_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_SET_H_

#include <algorithm>
#include <functional>
#include <tuple>
#include <utility>
#include <vector>

#include "shad/data_structures/abstract_data_structure.h"
#include "shad/data_structures/buffer.h"
#include "shad/data_structures/compare_and_hash_utils.h"
#include "shad/data_structures/local_set.h"
#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {

template <typename Set, typename T, typename NonConstT>
class set_iterator;

/// @brief The Set data structure.
///
/// SHAD's set is a distributed, unordered, set.
/// @tparam T type of the entries stored in the set.
/// @tparam ELEM_COMPARE element comparison function; default is MemCmp<T>.
/// @warning obects of type T need to be trivially copiable.
template <typename T, typename ELEM_COMPARE = MemCmp<T>>
class Set : public AbstractDataStructure<Set<T, ELEM_COMPARE>> {
  template <typename>
  friend class AbstractDataStructure;

  friend class set_iterator<Set<T, ELEM_COMPARE>, const T, T>;

 public:
  using value_type = T;
  using SetT = Set<T, ELEM_COMPARE>;
  using LSetT = LocalSet<T, ELEM_COMPARE>;
  using ObjectID = typename AbstractDataStructure<SetT>::ObjectID;
  using ShadSetPtr = typename AbstractDataStructure<SetT>::SharedPtr;
  using BuffersVector = typename impl::BuffersVector<T, SetT>;

  using iterator = set_iterator<Set<T, ELEM_COMPARE>, const T, T>;
  using const_iterator = set_iterator<Set<T, ELEM_COMPARE>, const T, T>;
  using local_iterator = lset_iterator<LocalSet<T, ELEM_COMPARE>, const T>;
  using const_local_iterator =
      lset_iterator<LocalSet<T, ELEM_COMPARE>, const T>;
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
  ObjectID GetGlobalID() const { return oid_; }

  /// @brief Overall size of the set (number of elements).
  /// @warning Calling the size method may result in one-to-all
  /// communication among localities to retrieve consinstent information.
  /// @return the size of the set.
  size_t Size() const;

  /// @brief Insert an element in the set.
  /// @param[in] element the element.
  /// @return a pair consisting of an iterator to the inserted element (or to
  /// the element that prevented the insertion) and a bool denoting whether the
  /// insertion took place.
  std::pair<iterator, bool> Insert(const T& element);

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
  void BufferedAsyncInsert(rt::Handle& handle, const T& element);

  /// @brief Finalize method for buffered insertions.
  void WaitForBufferedInsert() { buffers_.FlushAll(); }
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
    auto clearLambda = [](const ObjectID& oid) {
      auto setPtr = SetT::GetPtr(oid);
      setPtr->localSet_.Clear();
    };
    rt::executeOnAll(clearLambda, oid_);
  }

  /// @brief Clear the content of the set.
  void Reset(size_t numElements) {
    auto resetLambda = [](const std::tuple<ObjectID, size_t>& t) {
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
  void PrintAllElements() {
    auto printLambda = [](const ObjectID& oid) {
      auto setPtr = SetT::GetPtr(oid);
      std::cout << "---- Locality: " << rt::thisLocality() << std::endl;
      setPtr->localSet_.PrintAllElements();
    };
    rt::executeOnAll(printLambda, oid_);
  }

  // FIXME it should be protected
  void BufferEntryInsert(const T& element) { localSet_.Insert(element); }

  iterator begin() { return iterator::set_begin(this); }
  iterator end() { return iterator::set_end(this); }
  const_iterator cbegin() const { return const_iterator::set_begin(this); }
  const_iterator cend() const { return const_iterator::set_end(this); }
  const_iterator begin() const { return cbegin(); }
  const_iterator end() const { return cend(); }
  local_iterator local_begin() {
    return local_iterator::lset_begin(&localSet_);
  }
  local_iterator local_end() { return local_iterator::lset_end(&localSet_); }
  const_local_iterator clocal_begin() {
    return const_local_iterator::lset_begin(&localSet_);
  }
  const_local_iterator clocal_end() {
    return const_local_iterator::lset_end(&localSet_);
  }

  std::pair<iterator, bool> insert(const value_type& value) {
    return Insert(value);
  }

  std::pair<iterator, bool> insert(const_iterator, const value_type& value) {
    return insert(value);
  }

  void buffered_async_insert(rt::Handle& h, const value_type& value) {
    BufferedAsyncInsert(h, value);
  }

  void buffered_async_flush(rt::Handle& h) {
    rt::waitForCompletion(h);
    WaitForBufferedInsert();
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
        localSet_(
            std::max(numEntries / (constants::kSetDefaultNumEntriesPerBucket *
                                   rt::numLocalities()),
                     1lu)),
        buffers_(oid) {}
};

template <typename T, typename ELEM_COMPARE>
inline size_t Set<T, ELEM_COMPARE>::Size() const {
  size_t size = localSet_.size_;
  size_t remoteSize(0);
  auto sizeLambda = [](const ObjectID& oid, size_t* res) {
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
inline std::pair<typename Set<T, ELEM_COMPARE>::iterator, bool>
Set<T, ELEM_COMPARE>::Insert(const T& element) {
  size_t targetId = shad::hash<T>{}(element) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  using itr_traits = distributed_iterator_traits<iterator>;
  if (targetLocality == rt::thisLocality()) {
    auto lres = localSet_.Insert(element);
    auto git = itr_traits::iterator_from_local(begin(), end(), lres.first);
    return std::make_pair(git, lres.second);
  }
  std::pair<iterator, bool> res;
  auto insertLambda =
      [](const std::tuple<iterator, iterator, ObjectID, T>& args,
         std::pair<iterator, bool>* res_ptr) {
        auto setPtr = SetT::GetPtr(std::get<2>(args));
        auto lres = setPtr->localSet_.Insert(std::get<3>(args));
        auto git = itr_traits::iterator_from_local(
            std::get<0>(args), std::get<1>(args), lres.first);
        *res_ptr = std::make_pair(git, lres.second);
      };
  rt::executeAtWithRet(targetLocality, insertLambda,
                       std::make_tuple(begin(), end(), oid_, element), &res);
  return res;
}

template <typename T, typename ELEM_COMPARE>
inline void Set<T, ELEM_COMPARE>::AsyncInsert(rt::Handle& handle,
                                              const T& element) {
  size_t targetId = shad::hash<T>{}(element) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    localSet_.AsyncInsert(handle, element);
  } else {
    auto insertLambda = [](rt::Handle& handle, const ExeAtArgs& args) {
      auto setPtr = SetT::GetPtr(args.oid);
      setPtr->localSet_.AsyncInsert(handle, args.element);
    };
    ExeAtArgs args = {oid_, element};
    rt::asyncExecuteAt(handle, targetLocality, insertLambda, args);
  }
}

template <typename T, typename ELEM_COMPARE>
inline void Set<T, ELEM_COMPARE>::BufferedInsert(const T& element) {
  size_t targetId = shad::hash<T>{}(element) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  buffers_.Insert(element, targetLocality);
}

template <typename T, typename ELEM_COMPARE>
inline void Set<T, ELEM_COMPARE>::BufferedAsyncInsert(rt::Handle& handle,
                                                      const T& element) {
  size_t targetId = shad::hash<T>{}(element) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  buffers_.AsyncInsert(handle, element, targetLocality);
}

template <typename T, typename ELEM_COMPARE>
inline void Set<T, ELEM_COMPARE>::Erase(const T& element) {
  size_t targetId = shad::hash<T>{}(element) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    localSet_.Erase(element);
  } else {
    auto eraseLambda = [](const ExeAtArgs& args) {
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
  size_t targetId = shad::hash<T>{}(element) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    localSet_.AsyncErase(handle, element);
  } else {
    auto eraseLambda = [](rt::Handle& handle, const ExeAtArgs& args) {
      auto setPtr = SetT::GetPtr(args.oid);
      setPtr->localSet_.AsyncErase(handle, args.element);
    };
    ExeAtArgs args = {oid_, element};
    rt::asyncExecuteAt(handle, targetLocality, eraseLambda, args);
  }
}

template <typename T, typename ELEM_COMPARE>
inline bool Set<T, ELEM_COMPARE>::Find(const T& element) {
  size_t targetId = shad::hash<T>{}(element) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    return localSet_.Find(element);
  } else {
    auto findLambda = [](const ExeAtArgs& args, bool* res) {
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
inline void Set<T, ELEM_COMPARE>::AsyncFind(rt::Handle& handle,
                                            const T& element, bool* found) {
  size_t targetId = shad::hash<T>{}(element) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localSet_.AsyncFind(handle, element, found);
  } else {
    auto findLambda = [](rt::Handle&, const ExeAtArgs& args, bool* res) {
      auto setPtr = SetT::GetPtr(args.oid);
      *res = setPtr->localSet_.Find(args.element);
    };
    ExeAtArgs args = {oid_, element};
    rt::asyncExecuteAtWithRet(handle, targetLocality, findLambda, args, found);
  }
}

template <typename T, typename ELEM_COMPARE>
template <typename ApplyFunT, typename... Args>
void Set<T, ELEM_COMPARE>::ForEachElement(ApplyFunT&& function, Args&... args) {
  using FunctionTy = void (*)(const T&, Args&...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  using ArgsTuple = std::tuple<LSetT*, FunctionTy, std::tuple<Args...>>;
  feArgs arguments(oid_, fn, std::tuple<Args...>(args...));
  auto feLambda = [](const feArgs& args) {
    auto setPtr = SetT::GetPtr(std::get<0>(args));
    ArgsTuple argsTuple(&setPtr->localSet_, std::get<1>(args),
                        std::get<2>(args));
    rt::forEachAt(rt::thisLocality(),
                  LSetT::template ForEachElementFunWrapper<ArgsTuple, Args...>,
                  argsTuple, setPtr->localSet_.numBuckets_);
  };
  rt::executeOnAll(feLambda, arguments);
}

template <typename T, typename ELEM_COMPARE>
template <typename ApplyFunT, typename... Args>
void Set<T, ELEM_COMPARE>::AsyncForEachElement(rt::Handle& handle,
                                               ApplyFunT&& function,
                                               Args&... args) {
  using FunctionTy = void (*)(rt::Handle&, const T&, Args&...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  using ArgsTuple = std::tuple<LSetT*, FunctionTy, std::tuple<Args...>>;
  feArgs arguments{oid_, fn, std::tuple<Args...>(args...)};
  auto feLambda = [](rt::Handle& handle, const feArgs& args) {
    auto setPtr = SetT::GetPtr(std::get<0>(args));
    ArgsTuple argsTuple = std::make_tuple(&setPtr->localSet_, std::get<1>(args),
                                          std::get<2>(args));
    rt::asyncForEachAt(
        handle, rt::thisLocality(),
        LSetT::template AsyncForEachElementFunWrapper<ArgsTuple, Args...>,
        argsTuple, setPtr->localSet_.numBuckets_);
  };
  rt::asyncExecuteOnAll(handle, feLambda, arguments);
}

template <typename SetT, typename T, typename NonConstT>
class set_iterator : public std::iterator<std::forward_iterator_tag, T> {
 public:
  using value_type = NonConstT;
  using OIDT = typename SetT::ObjectID;
  using LSet = typename SetT::LSetT;
  using local_iterator_type = lset_iterator<LSet, T>;

  set_iterator() {}
  set_iterator(uint32_t locID, const OIDT setOID, local_iterator_type& lit,
               T element) {
    data_ = {locID, setOID, lit, element};
  }

  set_iterator(uint32_t locID, const OIDT setOID, local_iterator_type& lit) {
    auto setPtr = SetT::GetPtr(setOID);
    const LSet* lsetPtr = &(setPtr->localSet_);
    if (lit != local_iterator_type::lset_end(lsetPtr))
      data_ = itData(locID, setOID, lit, *lit);
    else
      *this = set_end(setPtr.get());
  }

  static set_iterator set_begin(const SetT* setPtr) {
    const LSet* lsetPtr = &(setPtr->localSet_);
    auto localEnd = local_iterator_type::lset_end(lsetPtr);
    if (static_cast<uint32_t>(rt::thisLocality()) == 0) {
      auto localBegin = local_iterator_type::lset_begin(lsetPtr);
      if (localBegin != localEnd) {
        return set_iterator(0, setPtr->oid_, localBegin);
      }
      set_iterator beg(0, setPtr->oid_, localEnd, T());
      return ++beg;
    }
    auto getItLambda = [](const OIDT& setOID, set_iterator* res) {
      auto setPtr = SetT::GetPtr(setOID);
      const LSet* lsetPtr = &(setPtr->localSet_);
      auto localEnd = local_iterator_type::lset_end(lsetPtr);
      auto localBegin = local_iterator_type::lset_begin(lsetPtr);
      if (localBegin != localEnd) {
        *res = set_iterator(0, setOID, localBegin);
      } else {
        set_iterator beg(0, setOID, localEnd, T());
        *res = ++beg;
      }
    };
    set_iterator beg(0, setPtr->oid_, localEnd, T());
    rt::executeAtWithRet(rt::Locality(0), getItLambda, setPtr->oid_, &beg);
    return beg;
  }

  static set_iterator set_end(const SetT* setPtr) {
    local_iterator_type lend =
        local_iterator_type::lset_end(&(setPtr->localSet_));
    set_iterator end(rt::numLocalities(), OIDT(0), lend, T());
    return end;
  }

  bool operator==(const set_iterator& other) const {
    return (data_ == other.data_);
  }
  bool operator!=(const set_iterator& other) const { return !(*this == other); }

  T operator*() const { return data_.element_; }

  set_iterator& operator++() {
    auto setPtr = SetT::GetPtr(data_.oid_);
    if (static_cast<uint32_t>(rt::thisLocality()) == data_.locId_) {
      const LSet* lsetPtr = &(setPtr->localSet_);
      auto lend = local_iterator_type::lset_end(lsetPtr);
      if (data_.lsetIt_ != lend) {
        ++(data_.lsetIt_);
      }
      if (data_.lsetIt_ != lend) {
        data_.element_ = *(data_.lsetIt_);
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
  set_iterator operator++(int) {
    set_iterator tmp = *this;
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
  static local_iterator_range local_range(set_iterator& B, set_iterator& E) {
    auto setPtr = SetT::GetPtr(B.data_.oid_);
    local_iterator_type lbeg, lend;
    uint32_t thisLocId = static_cast<uint32_t>(rt::thisLocality());
    if (B.data_.locId_ == thisLocId) {
      lbeg = B.data_.lsetIt_;
    } else {
      lbeg = local_iterator_type::lset_begin(&(setPtr->localSet_));
    }
    if (E.data_.locId_ == thisLocId) {
      lend = E.data_.lsetIt_;
    } else {
      lend = local_iterator_type::lset_end(&(setPtr->localSet_));
    }
    return local_iterator_range(lbeg, lend);
  }
  static rt::localities_range localities(set_iterator& B, set_iterator& E) {
    return rt::localities_range(rt::Locality(B.data_.locId_),
                                rt::Locality(std::min<uint32_t>(
                                    rt::numLocalities(), E.data_.locId_ + 1)));
  }

  static set_iterator iterator_from_local(set_iterator& B, set_iterator& E,
                                          local_iterator_type itr) {
    return set_iterator(static_cast<uint32_t>(rt::thisLocality()), B.data_.oid_,
                        itr);
  }

 private:
  struct itData {
    itData() : oid_(0), lsetIt_(nullptr, 0, 0, nullptr, nullptr) {}
    itData(uint32_t locId, OIDT oid, local_iterator_type lsetIt, T element)
        : locId_(locId), oid_(oid), lsetIt_(lsetIt), element_(element) {}
    bool operator==(const itData& other) const {
      return (locId_ == other.locId_) && (lsetIt_ == other.lsetIt_);
    }
    bool operator!=(itData& other) const { return !(*this == other); }
    uint32_t locId_;
    OIDT oid_;
    local_iterator_type lsetIt_;
    NonConstT element_;
  };

  itData data_;

  static void getLocBeginIt(const OIDT& setOID, itData* res) {
    auto setPtr = SetT::GetPtr(setOID);
    auto lsetPtr = &(setPtr->localSet_);
    auto localEnd = local_iterator_type::lset_end(lsetPtr);
    auto localBegin = local_iterator_type::lset_begin(lsetPtr);
    if (localBegin != localEnd) {
      *res = itData(static_cast<uint32_t>(rt::thisLocality()), setOID,
                    localBegin, *localBegin);
    } else {
      *res = itData(rt::numLocalities(), OIDT(0), localEnd, T());
    }
  }

  static void getRemoteIt(const itData& itd, itData* res) {
    auto setPtr = SetT::GetPtr(itd.oid_);
    auto lsetPtr = &(setPtr->localSet_);
    auto localEnd = local_iterator_type::lset_end(lsetPtr);
    local_iterator_type cit = itd.lsetIt_;
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

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_SET_H_
