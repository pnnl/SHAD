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

#ifndef INCLUDE_SHAD_DATA_STRUCTURES_ATOMIC_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_ATOMIC_H_

#include "shad/data_structures/abstract_data_structure.h"

namespace shad {

/// @brief Wrapper that instantiate one object per Locality in the system.
///
/// This Wrapper triggers the instantiation of one object per Locality in the
/// system.
///
/// @warning Writes are not propagated across the system.
///
/// @tparam T The typen of the objects that will be instantiated.
template <typename T>
class Atomic : public AbstractDataStructure<Atomic<T>> {
 public:
  using ObjectID = typename AbstractDataStructure<Atomic<T>>::ObjectID;
  using SharedPtr =
      typename AbstractDataStructure<Atomic<T>>::SharedPtr;

  /// @brief Create method.
  ///
  /// Creates instances of a T object on each locality.
  ///
  /// @tparam Args The list of types needed to build an instance of type T
  ///
  /// @param args The parameter pack to be forwarded to T' constructor.
  /// @return A shared pointer to the local Atomic instance.
#ifdef DOXYGEN_IS_RUNNING
  template <typename... Args>
  static SharedPtr Create(Args... args);
#endif

  /// @brief Retieve the Global Identifier.
  ///
  /// @return The global identifier associated with the array instance.
  ObjectID GetGlobalID() const { return oid_; }

  /// @brief Atomic Load.
  ///
  /// @return Atomic's value.
  T Load() {
    if (ownerLoc_ == rt::thisLocality()) {
     return localInstance_.load();
    }
    T retValue;
    auto LoadFun = [](const ObjectID &oid, T *result) {
      auto ptr = Atomic<T>::GetPtr(oid);
      *result = ptr->localInstance_.load();
    };
    rt::executeAtWithRet(ownerLoc_, LoadFun, oid_, &retValue);
    return retValue;
  }

  /// @brief Async Atomic Load.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[out] res Pointer to the region where the atomic's value is
  /// written; res must point to a valid memory allocation.
  void AsyncLoad(rt::Handle& h, T* res) {
    if (ownerLoc_ == rt::thisLocality()) {
     *res = localInstance_.load();
     return;
    }
    auto LoadFun = [](rt::Handle&, const ObjectID &oid, T *result) {
      auto ptr = Atomic<T>::GetPtr(oid);
      *result = ptr->localInstance_.load();
    };
    rt::asyncExecuteAtWithRet(h, ownerLoc_, LoadFun, oid_, res);
    return;
  }

  /// @brief Async Atomic Store.
  ///
  /// @param[in] desired Non atomic value to be stored.
  void Store(T desired) {
    if (ownerLoc_ == rt::thisLocality()) {
     localInstance_.store(desired);
     return;
    }
    auto StoreFun = [](const std::pair<ObjectID, T> &args) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      ptr->localInstance_.store(args.second);
    };
    auto args = std::make_pair(oid_, desired);
    rt::executeAt(ownerLoc_, StoreFun, args);
    return;
  }

  /// @brief Atomic Store. Attempts at atomically storing the results of binop.
  ///
  /// @tparam ArgT Type of rhs for the BinaryOp operator.
  /// @tparam BinaryOp User-defined binary operator T (const T& lhs, const ArgT& rhs).
  /// @param[in] desired_arg Non atomic rhs value for binop.
  /// @param[in] binop Binary operator. lhs is atomic's value, rhs is desired_arg.
  /// @return true if the store was successful, false otherwise.
  template <typename ArgT, typename BinaryOp>
  bool Store(ArgT desired_arg, BinaryOp binop) {
    if (ownerLoc_ == rt::thisLocality()) {
      auto old_value = localInstance_.load();
      T desired = binop(old_value, desired_arg);
      return atomic_compare_exchange_strong(&localInstance_,
                                            &old_value, desired);
    }
    using StoreArgs = std::tuple<ObjectID, ArgT, BinaryOp>;
    auto StoreFun = [](const StoreArgs &args, bool *res) {
      auto ptr = Atomic<T>::GetPtr(std::get<0>(args));
      auto old_value = ptr->localInstance_.load();
      auto binop = std::get<2>(args);
      T desired = binop(old_value, std::get<1>(args));
      *res = atomic_compare_exchange_strong(&(ptr->localInstance_),
                                            &old_value, desired);
    };
    StoreArgs args(oid_, desired_arg, binop);
    bool res;
    rt::executeAtWithRet(ownerLoc_, StoreFun, args, &res);
    return res;
  }

  /// @brief Atomic Store. Attempts at atomically storing the results of binop
  ///                      unitil succesful.
  ///
  /// @tparam ArgT Type of rhs for the BinaryOp operator.
  /// @tparam BinaryOp User-defined binary operator T (const T& lhs, const ArgT& rhs).
  /// @param[in] desired_arg Non atomic rhs value for binop.
  /// @param[in] binop Binary operator. lhs is atomic's value, rhs is desired_arg.
  template <typename ArgT, typename BinaryOp>
  void ForceStore(ArgT desired_arg, BinaryOp binop) {
    if (ownerLoc_ == rt::thisLocality()) {
      auto old_value = localInstance_.load();
      T desired = binop(old_value, desired_arg);
      while(!atomic_compare_exchange_weak(&localInstance_,
                                          &old_value, desired)) {
        old_value = localInstance_.load();
        desired = binop(old_value, desired_arg);
      }
      return;
    }
    using StoreArgs = std::tuple<ObjectID, ArgT, BinaryOp>;
    auto StoreFun = [](const StoreArgs &args) {
      auto ptr = Atomic<T>::GetPtr(std::get<0>(args));
      auto old_value = ptr->localInstance_.load();
      auto desired_arg = std::get<1>(args);
      auto binop = std::get<2>(args);
      T desired = binop(old_value, desired_arg);
      while(!atomic_compare_exchange_weak(&ptr->localInstance_,
                                          &old_value, desired)) {
        old_value = ptr->localInstance_.load();
        desired = binop(old_value, desired_arg);
      }
    };
    StoreArgs args(oid_, desired_arg, binop);
    rt::executeAt(ownerLoc_, StoreFun, args);
  }


  /// @brief Async Atomic Fetch-Store. Attempts at atomically storing
  ///                            the results of binop unitil succesful.
  ///
  /// @tparam ArgT Type of rhs for the BinaryOp operator.
  /// @tparam BinaryOp User-defined binary operator T (const T& lhs, const ArgT& rhs).
  /// @param[in,out] h The handle to be used to wait for completion.  
  /// @param[in] desired_arg Non atomic rhs value for binop.
  /// @param[in] binop Binary operator. lhs is atomic's value, rhs is desired_arg.
  /// @return The value fetched when Store was successful.
  template <typename ArgT, typename BinaryOp>
  T ForceFetchStore(ArgT desired_arg, BinaryOp binop) {
    if (ownerLoc_ == rt::thisLocality()) {
      auto old_value = localInstance_.load();
      T desired = binop(old_value, desired_arg);
      while(!atomic_compare_exchange_weak(&localInstance_,
                                          &old_value, desired)) {
        old_value = localInstance_.load();
        desired = binop(old_value, desired_arg);
      }
      return old_value;
    }
    using StoreArgs = std::tuple<ObjectID, ArgT, BinaryOp>;
    auto StoreFun = [](const StoreArgs &args, T*res) {
      auto ptr = Atomic<T>::GetPtr(std::get<0>(args));
      auto old_value = ptr->localInstance_.load();
      auto desired_arg = std::get<1>(args);
      auto binop = std::get<2>(args);
      T desired = binop(old_value, desired_arg);
      while(!atomic_compare_exchange_weak(&ptr->localInstance_,
                                          &old_value, desired)) {
        old_value = ptr->localInstance_.load();
        desired = binop(old_value, desired_arg);
      }
      *res = old_value;
    };
    StoreArgs args(oid_, desired_arg, binop);
    T res;
    rt::executeAtWithRet(ownerLoc_, StoreFun, args, &res);
    return res;
  }

  /// @brief Async Atomic Store.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[in] desired Non atomic value to be stored.
  void AsyncStore(rt::Handle& h, T desired) {
    if (ownerLoc_ == rt::thisLocality()) {
     localInstance_.store(desired);
     return;
    }
    auto StoreFun = [](rt::Handle&, const std::pair<ObjectID, T> &args) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      ptr->localInstance_.store(args.second);
    };
    auto args = std::make_pair(oid_, desired);
    rt::asyncExecuteAt(h, ownerLoc_, StoreFun, args);
    return;
  }

  /// @brief Async Atomic Store. Attempts at atomically storing the results of binop.
  ///
  /// @tparam ArgT Type of rhs for the BinaryOp operator.
  /// @tparam BinaryOp User-defined binary operator T (const T& lhs, const T& rhs).
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[in] desired_arg Non atomic rhs value for binop.
  /// @param[in] binop Binary operator. lhs is atomic's value, rhs is desired_arg.
  /// @param[out] res Pointer to the region where the result is
  /// written; res must point to a valid memory allocation. 
  /// *res is true if the store operation was successful, false otherwise.
  template <typename ArgT, typename BinaryOp>
  void AsyncStore(rt::Handle& h, ArgT desired_arg, BinaryOp binop, bool* res) {
    if (ownerLoc_ == rt::thisLocality()) {
      auto old_value = localInstance_.load();
      T desired = binop(old_value, desired_arg);
      *res = atomic_compare_exchange_strong(&localInstance_,
                                            &old_value, desired);
      return;
    }
    using StoreArgs = std::tuple<ObjectID, ArgT, BinaryOp>;
    auto StoreFun = [](rt::Handle&, const StoreArgs &args, bool *res) {
      auto ptr = Atomic<T>::GetPtr(std::get<0>(args));
      auto old_value = ptr->localInstance_.load();
      auto binop = std::get<2>(args);
      T desired = binop(old_value, std::get<1>(args));
      *res = atomic_compare_exchange_strong(&(ptr->localInstance_),
                                            &old_value, desired);
    };
    StoreArgs args(oid_, desired_arg, binop);
    rt::asyncExecuteAtWithRet(h, ownerLoc_, StoreFun, args, res);
  }

  /// @brief Async Atomic Store. Attempts at atomically storing
  ///                            the results of binop unitil succesful.
  ///
  /// @tparam ArgT Type of rhs for the BinaryOp operator.
  /// @tparam BinaryOp User-defined binary operator T (const T& lhs, const ArgT& rhs).
  /// @param[in,out] h The handle to be used to wait for completion.  
  /// @param[in] desired_arg Non atomic rhs value for binop.
  /// @param[in] binop Binary operator. lhs is atomic's value, rhs is desired_arg.
  template <typename ArgT, typename BinaryOp>
  void AsyncForceStore(rt::Handle &h, ArgT desired_arg, BinaryOp binop) {
    if (ownerLoc_ == rt::thisLocality()) {
      auto old_value = localInstance_.load();
      T desired = binop(old_value, desired_arg);
      while(!atomic_compare_exchange_weak(&localInstance_,
                                          &old_value, desired)) {
        old_value = localInstance_.load();
        desired = binop(old_value, desired_arg);
      }
      return;
    }
    using StoreArgs = std::tuple<ObjectID, ArgT, BinaryOp>;
    auto StoreFun = [](rt::Handle&, const StoreArgs &args) {
      auto ptr = Atomic<T>::GetPtr(std::get<0>(args));
      auto old_value = ptr->localInstance_.load();
      auto desired_arg = std::get<1>(args);
      auto binop = std::get<2>(args);
      T desired = binop(old_value, desired_arg);
      while(!atomic_compare_exchange_weak(&ptr->localInstance_,
                                          &old_value, desired)) {
        old_value = ptr->localInstance_.load();
        desired = binop(old_value, desired_arg);
      }
    };
    StoreArgs args(oid_, desired_arg, binop);
    rt::asyncExecuteAt(h, ownerLoc_, StoreFun, args);
  }

  /// @brief Async Atomic Fetch-Store. Attempts at atomically storing
  ///                            the results of binop unitil succesful.
  ///
  /// @tparam ArgT Type of rhs for the BinaryOp operator.
  /// @tparam BinaryOp User-defined binary operator T (const T& lhs, const ArgT& rhs).
  /// @param[in,out] h The handle to be used to wait for completion.  
  /// @param[in] desired_arg Non atomic rhs value for binop.
  /// @param[in] binop Binary operator. lhs is atomic's value, rhs is desired_arg.
  /// @param[out] res Pointer to the region where the result is
  /// written; res must point to a valid memory allocation.
  /// Result is the value fetched when Store was successful
  template <typename ArgT, typename BinaryOp>
  void AsyncForceFetchStore(rt::Handle &h, ArgT desired_arg,
                            BinaryOp binop, T* res) {
    if (ownerLoc_ == rt::thisLocality()) {
      auto old_value = localInstance_.load();
      T desired = binop(old_value, desired_arg);
      while(!atomic_compare_exchange_weak(&localInstance_,
                                          &old_value, desired)) {
        old_value = localInstance_.load();
        desired = binop(old_value, desired_arg);
      }
      *res = old_value;
      return;
    }
    using StoreArgs = std::tuple<ObjectID, ArgT, BinaryOp>;
    auto StoreFun = [](rt::Handle&, const StoreArgs &args, T* res) {
      auto ptr = Atomic<T>::GetPtr(std::get<0>(args));
      auto old_value = ptr->localInstance_.load();
      auto desired_arg = std::get<1>(args);
      auto binop = std::get<2>(args);
      T desired = binop(old_value, desired_arg);
      while(!atomic_compare_exchange_weak(&ptr->localInstance_,
                                          &old_value, desired)) {
        old_value = ptr->localInstance_.load();
        desired = binop(old_value, desired_arg);
      }
      *res = old_value;
    };
    StoreArgs args(oid_, desired_arg, binop);
    rt::asyncExecuteAtWithRet(h, ownerLoc_, StoreFun, args, res);
  }

  /// @brief Compare and exchange operation.
  ///
  /// @param[in] expected Value expected to be found in the atomic object.
  /// @param[in] desired Value to store in the atomic object if it is as expected.
  /// @return true if the atomic object was equal to expected, false otherwise.
  bool CompareExchange(T expected, T desired) {
    if (ownerLoc_ == rt::thisLocality()) {
      return atomic_compare_exchange_strong(&localInstance_,
                                           &expected, desired );
    }
    bool ret;
    using CasArgs = std::tuple<ObjectID, T, T>;
    auto CasFun = [](const CasArgs &args, bool *result) {
      auto ptr = Atomic<T>::GetPtr(std::get<0>(args));
      T expected = std::get<1>(args);
      *result = atomic_compare_exchange_strong(&(ptr->localInstance_),
                                               &expected,
                                               std::get<2>(args));
    };
    CasArgs args(oid_, expected, desired);
    rt::executeAtWithRet(ownerLoc_, CasFun, args, &ret);
    return ret;
  }

  /// @brief Compare and exchange operation.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[in] expected Value expected to be found in the atomic object.
  /// @param[in] desired Value to store in the atomic object if it is as expected.
  /// @param[out] res Pointer to the region where the result is
  /// written; res must point to a valid memory allocation. 
  /// *res is true if the atomic object was equal to expected, false otherwise.
  void AsyncCompareExchange(rt::Handle &h, T expected, T desired, bool* res) {
    if (ownerLoc_ == rt::thisLocality()) {
      *res = atomic_compare_exchange_strong(&localInstance_,
                                            &expected, desired);
      return;
    }
    using CasArgs = std::tuple<ObjectID, T, T>;
    auto CasFun = [](rt::Handle&, const CasArgs &args, bool *result) {
      auto ptr = Atomic<T>::GetPtr(std::get<0>(args));
      T expected = std::get<1>(args);
      *result = atomic_compare_exchange_strong(&(ptr->localInstance_),
                                               &expected,
                                               std::get<2>(args));
    };
    CasArgs args(oid_, expected, desired);
    rt::asyncExecuteAtWithRet(h, ownerLoc_, CasFun, args, res);
  }

  /// @brief Fetch Add operation.
  ///
  /// @param[in] add Data to be fetch-added.
  /// @return Result of the fect-add operation.
  T FetchAdd(T add) {
    if (ownerLoc_ == rt::thisLocality()) {
     return localInstance_.fetch_add(add);
    }
    T ret;
    auto AddFun = [](const std::pair<ObjectID, T> &args, T *result) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      *result = ptr->localInstance_.fetch_add(args.second);
    };
    auto args = std::make_pair(oid_, add);
    rt::executeAtWithRet(ownerLoc_, AddFun, args, &ret);
    return ret;
  }

  /// @brief Async Fetch Add operation.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[in] add Data to be fetch-added.
  /// @param[out] res Pointer to the region where the result is
  /// written; res must point to a valid memory allocation.
  void AsyncFetchAdd(rt::Handle& h, T add, T* res) {
    if (ownerLoc_ == rt::thisLocality()) {
     *res = localInstance_.fetch_add(add);
     return;
    }
    auto AddFun = [](rt::Handle&,
                     const std::pair<ObjectID, T> &args, T *result) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      *result = ptr->localInstance_.fetch_add(args.second);
    };
    auto args = std::make_pair(oid_, add);
    rt::asyncExecuteAtWithRet(h, ownerLoc_, AddFun, args, res);
    return;
  }

  /// @brief Async Fetch Add operation, with no return value.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[in] add Data to be fetch-added.
  void AsyncFetchAdd(rt::Handle& h, T add) {
    if (ownerLoc_ == rt::thisLocality()) {
     localInstance_.fetch_add(add);
     return;
    }
    auto AddFun = [](rt::Handle&,
                     const std::pair<ObjectID, T> &args) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      ptr->localInstance_.fetch_add(args.second);
    };
    auto args = std::make_pair(oid_, add);
    rt::asyncExecuteAt(h, ownerLoc_, AddFun, args);
    return;
  }

  /// @brief Fetch Sub operation.
  ///
  /// @param[in] sub Data to be fetch-subbed.
  /// @return Result of the fect-sub operation.
  T FetchSub(T sub) {
    if (ownerLoc_ == rt::thisLocality()) {
     return localInstance_.fetch_sub(sub);
    }
    T ret;
    auto SubFun = [](const std::pair<ObjectID, T> &args, T *result) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      *result = ptr->localInstance_.fetch_sub(args.second);
    };
    auto args = std::make_pair(oid_, sub);
    rt::executeAtWithRet(ownerLoc_, SubFun, args, &ret);
    return ret;
  }

  /// @brief Async Fetch Sub operation.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[in] sub Data to be fetch-subbed.
  /// @param[out] res Pointer to the region where the result is
  /// written; res must point to a valid memory allocation.
  void AsyncFetchSub(rt::Handle& h, T sub, T* res) {
    if (ownerLoc_ == rt::thisLocality()) {
     *res = localInstance_.fetch_sub(sub);
     return;
    }
    auto SubFun = [](rt::Handle&,
                     const std::pair<ObjectID, T> &args, T *result) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      *result = ptr->localInstance_.fetch_sub(args.second);
    };
    auto args = std::make_pair(oid_, sub);
    rt::asyncExecuteAtWithRet(h, ownerLoc_, SubFun, args, res);
    return;
  }

  /// @brief Async Fetch Sub operation, with no return value.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[in] sub Data to be fetch-subbed.
  void AsyncFetchSub(rt::Handle& h, T sub) {
    if (ownerLoc_ == rt::thisLocality()) {
     localInstance_.fetch_sub(sub);
     return;
    }
    auto SubFun = [](rt::Handle&,
                     const std::pair<ObjectID, T> &args) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      ptr->localInstance_.fetch_sub(args.second);
    };
    auto args = std::make_pair(oid_, sub);
    rt::asyncExecuteAt(h, ownerLoc_, SubFun, args);
    return;
  }

  /// @brief Fetch And operation.
  ///
  /// @param[in] operand Data to be fetch-anded.
  /// @return Result of the fect-and operation.
  T FetchAnd(T operand) {
    if (ownerLoc_ == rt::thisLocality()) {
     return localInstance_.fetch_and(operand);
    }
    T ret;
    auto AndFun = [](const std::pair<ObjectID, T> &args, T *result) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      *result = ptr->localInstance_.fetch_and(args.second);
    };
    auto args = std::make_pair(oid_, operand);
    rt::executeAtWithRet(ownerLoc_, AndFun, args, &ret);
    return ret;
  }

  /// @brief Async Fetch And operation.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[in] operand Data to be fetch-anded.
  /// @param[out] res Pointer to the region where the result is
  /// written; res must point to a valid memory allocation.
  void AsyncFetchAnd(rt::Handle& h, T operand, T* res) {
    if (ownerLoc_ == rt::thisLocality()) {
     *res = localInstance_.fetch_and(operand);
     return;
    }
    auto AndFun = [](rt::Handle&,
                     const std::pair<ObjectID, T> &args, T *result) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      *result = ptr->localInstance_.fetch_and(args.second);
    };
    auto args = std::make_pair(oid_, operand);
    rt::asyncExecuteAtWithRet(h, ownerLoc_, AndFun, args, res);
    return;
  }

  /// @brief Async Fetch And operation, with no return value.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[in] operand Data to be fetch-anded.
  void AsyncFetchAnd(rt::Handle& h, T operand) {
    if (ownerLoc_ == rt::thisLocality()) {
     localInstance_.fetch_and(operand);
     return;
    }
    auto AndFun = [](rt::Handle&,
                     const std::pair<ObjectID, T> &args) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      ptr->localInstance_.fetch_and(args.second);
    };
    auto args = std::make_pair(oid_, operand);
    rt::asyncExecuteAt(h, ownerLoc_, AndFun, args);
    return;
  }

  /// @brief Fetch Or operation.
  ///
  /// @param[in] operand Data to be fetch-ored.
  /// @return Result of the fect-or operation.
  T FetchOr(T operand) {
    if (ownerLoc_ == rt::thisLocality()) {
     return localInstance_.fetch_or(operand);
    }
    T ret;
    auto OrFun = [](const std::pair<ObjectID, T> &args, T *result) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      *result = ptr->localInstance_.fetch_or(args.second);
    };
    auto args = std::make_pair(oid_, operand);
    rt::executeAtWithRet(ownerLoc_, OrFun, args, &ret);
    return ret;
  }

  /// @brief Async Fetch Or operation.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[in] operand Data to be fetch-ored.
  /// @param[out] res Pointer to the region where the result is
  /// written; res must point to a valid memory allocation.
  void AsyncFetchOr(rt::Handle& h, T operand, T* res) {
    if (ownerLoc_ == rt::thisLocality()) {
     *res = localInstance_.fetch_or(operand);
     return;
    }
    auto OrFun = [](rt::Handle&,
                     const std::pair<ObjectID, T> &args, T *result) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      *result = ptr->localInstance_.fetch_or(args.second);
    };
    auto args = std::make_pair(oid_, operand);
    rt::asyncExecuteAtWithRet(h, ownerLoc_, OrFun, args, res);
    return;
  }

  /// @brief Async Fetch Or operation, with no return value.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[in] operand Data to be fetch-ored.
  void AsyncFetchOr(rt::Handle& h, T operand) {
    if (ownerLoc_ == rt::thisLocality()) {
     localInstance_.fetch_or(operand);
     return;
    }
    auto OrFun = [](rt::Handle&,
                     const std::pair<ObjectID, T> &args) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      ptr->localInstance_.fetch_or(args.second);
    };
    auto args = std::make_pair(oid_, operand);
    rt::asyncExecuteAt(h, ownerLoc_, OrFun, args);
    return;
  }

  /// @brief Fetch Xor operation.
  ///
  /// @param[in] xor Data to be fetch-xored.
  /// @return Result of the fect-xor operation.
  T FetchXor(T operand) {
    if (ownerLoc_ == rt::thisLocality()) {
     return localInstance_.fetch_xor(operand);
    }
    T ret;
    auto XorFun = [](const std::pair<ObjectID, T> &args, T *result) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      *result = ptr->localInstance_.fetch_xor(args.second);
    };
    auto args = std::make_pair(oid_, operand);
    rt::executeAtWithRet(ownerLoc_, XorFun, args, &ret);
    return ret;
  }

  /// @brief Async Fetch Xor operation.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[in] xor Data to be fetch-xored.
  /// @param[out] res Pointer to the region where the result is
  /// written; res must point to a valid memory allocation.
  void AsyncFetchXor(rt::Handle& h, T operand, T* res) {
    if (ownerLoc_ == rt::thisLocality()) {
     *res = localInstance_.fetch_xor(operand);
     return;
    }
    auto XorFun = [](rt::Handle&,
                     const std::pair<ObjectID, T> &args, T *result) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      *result = ptr->localInstance_.fetch_xor(args.second);
    };
    auto args = std::make_pair(oid_, operand);
    rt::asyncExecuteAtWithRet(h, ownerLoc_, XorFun, args, res);
    return;
  }

  /// @brief Async Fetch Xor operation, with no return value.
  ///
  /// @param[in,out] h The handle to be used to wait for completion.
  /// @param[in] xor Data to be fetch-xored.
  void AsyncFetchXor(rt::Handle& h, T operand) {
    if (ownerLoc_ == rt::thisLocality()) {
     localInstance_.fetch_xor(operand);
     return;
    }
    auto XorFun = [](rt::Handle&,
                     const std::pair<ObjectID, T> &args) {
      auto ptr = Atomic<T>::GetPtr(args.first);
      ptr->localInstance_.fetch_xor(args.second);
    };
    auto args = std::make_pair(oid_, operand);
    rt::asyncExecuteAt(h, ownerLoc_, XorFun, args);
    return;
  }

 protected:
  /// @brief Constructor.
  template <typename... Args>
  explicit Atomic(ObjectID oid, T initVal, rt::Locality owner=*(rt::allLocalities().begin()))
      : oid_{oid}, localInstance_{initVal}, ownerLoc_{owner} {}

 private:
  template <typename>
  friend class AbstractDataStructure;

  ObjectID oid_;
  rt::Locality ownerLoc_;
  std::atomic<T> localInstance_;
};

}  // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_ATOMIC_H_
