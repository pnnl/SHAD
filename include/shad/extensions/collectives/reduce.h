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

#ifndef INCLUDE_SHAD_EXTENSIONS_COLLECTIVES_REDUCE_H_
#define INCLUDE_SHAD_EXTENSIONS_COLLECTIVES_REDUCE_H_

#include <algorithm>
#include <numeric>
#include "shad/data_structures/abstract_data_structure.h"

namespace shad {

template <typename T, typename InF,typename OutF>
class Reducer : public AbstractDataStructure<Reducer<T, InF, OutF>> {
 public:
  using RedT = Reducer<T, InF, OutF>;
  using ObjectID = typename AbstractDataStructure<Reducer<T, InF, OutF>>::ObjectID;
  using SharedPtr =
      typename AbstractDataStructure<Reducer<T, InF, OutF>>::SharedPtr;


#ifdef DOXYGEN_IS_RUNNING
  static SharedPtr Create(InF inf, OutF outf);
#endif

  /// @brief Retieve the Global Identifier.
  ///
  /// @return The global identifier associated with the array instance.
  ObjectID GetGlobalID() const { return oid_; }
  
/*  void SetUpPtrs() {
    using setuptuple = std::tuple<ObjectID, uint32_t, T*, T*>;
 */   
//     auto setUpLambda = [](shad::rt::Handle &handle, const ObjectID &oid) {
//       auto ptr = Reducer<T, Binop, InF, OutF>::GetPtr(oid);
//       auto myin = ptr->in_;
//       auto myout = ptr->out_;
//       setuptuple args(oid, (uint32_t)rt::thisLocality(), myin, myout);
//       auto broadcastLambda  = [](shad::rt::Handle &, const setuptuple &args) {
//         auto ptr = Reducer<T, Binop, InF, OutF>::GetPtr(std::get<0>(args));
//         ptr->ins_[std::get<1>(args)] = std::get<2>(args);
//         ptr->ins_[std::get<1>(args)] = std::get<3>(args);
//       };
//       rt::asyncExecuteOnAll(handle, broadcastLambda, args);
//     };
//     rt::Handle h;
//     rt::asyncExecuteOnAll(h, setUpLambda, oid_);
//     rt::waitForCompletion(h);
//   }
  
  template <typename BinOp>
  void AllReduce(BinOp op, const size_t count) {
    auto reduceLambda = [](shad::rt::Handle &handle,
                           const std::pair<ObjectID, BinOp> &args, size_t it) {
      auto ptr = Reducer<T, InF, OutF>::GetPtr(args.first);
      std::vector<T> reduceData(rt::numLocalities());
      // FIXME can be done with rdma
      auto dataLambda =  [](shad::rt::Handle &handle,
                            const std::pair<ObjectID, size_t> &args, T* ret) {
        auto ptr = Reducer<T, InF, OutF>::GetPtr(args.first);
        *ret = ptr->in_[args.second];
      };
      rt::Handle h2;
      for (auto & l : rt::allLocalities()) {
        asyncExecuteAtWithRet(h2, l, dataLambda,
                              std::make_pair(args.first, it),
                              &reduceData[(uint32_t)l]);
      }
      rt::waitForCompletion(h2);
      // FIXME change to reduce
      auto val = std::accumulate(reduceData.begin(), reduceData.end(), 0, args.second);
      std::cout << "red: " << val << std::endl;
      // broadcast the data
      auto broadLambda = [](shad::rt::Handle &,
                           const std::tuple<ObjectID, T, size_t> &args) {
        auto ptr = Reducer<T, InF, OutF>::GetPtr(std::get<0>(args));
        ptr->out_[std::get<2>(args)] = std::get<1>(args);
      };
      rt::asyncExecuteOnAll(handle, broadLambda, std::make_tuple(args.first, val, it));
    };
    rt::Handle h;
    rt::asyncForEachOnAll(h, reduceLambda, std::make_pair(oid_, op), count);
    rt::waitForCompletion(h);
  }

 protected:
  explicit Reducer(ObjectID oid, InF inf, OutF outf)
      : oid_{oid} {
    in_ = inf();
    out_ = outf();
//     ins_.resize(rt::numLocalities());
//     outs_.resize(rt::numLocalities());
//     ins_[(uint32_t)rt::thisLocality()] = in_;
//     outs_[(uint32_t)rt::thisLocality()] = out_;
  }

 private:
  template <typename>
  friend class AbstractDataStructure;

  ObjectID oid_;
  T* in_;
  T* out_;
//   std::vector<T*>ins_;
//   std::vector<T*>outs_;
};

}  // namespace shad

#endif  // INCLUDE_SHAD_EXTENSIONS_COLLECTIVES_REDUCE_H_
