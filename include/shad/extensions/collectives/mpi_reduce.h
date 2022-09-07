//===------------------------------------------------------------*- C++ -*-===//
//
//                                     SHAD
//
//      The Scalable High-performance Algorithms and Data Structure Library
//
//===----------------------------------------------------------------------===//
//
// Copyright 2022 Battelle Memorial Institute
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

#include <mpi.h>
#include <algorithm>
#include <complex>
#include <numeric>
#include "shad/data_structures/abstract_data_structure.h"
// #include "shad/extensions/collectives/reduce.h"

namespace shad {

template <typename InF,typename OutF>
class MPIReducer : public AbstractDataStructure<MPIReducer<InF, OutF>> {
 public:
  using RedT = MPIReducer<InF, OutF>;
  using ObjectID = typename AbstractDataStructure<MPIReducer<InF, OutF>>::ObjectID;
  using SharedPtr =
      typename AbstractDataStructure<MPIReducer<InF, OutF>>::SharedPtr;


#ifdef DOXYGEN_IS_RUNNING
  static SharedPtr Create(MPI_Datatype dt, InF inf, OutF outf);
#endif

  /// @brief Retieve the Global Identifier.
  ///
  /// @return The global identifier associated with the array instance.
  ObjectID GetGlobalID() const { return oid_; }
  
  void* GetOut() {
    return out_;
  }

  void AllReduce(MPI_Op op, const size_t count);

  template <typename T>
  inline void CallReduce(MPI_Op op, const size_t count);

  template <typename T, typename BinOp>
  inline void DoMPIReduce(BinOp op, const size_t count) {
    auto reduceLambda = [](shad::rt::Handle &handle,
                           const std::pair<ObjectID, BinOp> &args, size_t it) {
      auto ptr = MPIReducer<InF, OutF>::GetPtr(args.first);
      std::vector<T> reduceData(rt::numLocalities());
      // FIXME can be done with rdma
      auto dataLambda =  [](shad::rt::Handle &handle,
                            const std::pair<ObjectID, size_t> &args, T* ret) {
        auto ptr = MPIReducer<InF, OutF>::GetPtr(args.first);
        T* in = (T*)(ptr->in_);
        *ret = in[args.second];
      };
      rt::Handle h2;
      for (auto l : rt::allLocalities()) {
        auto addr = &(reduceData[(uint32_t)l]);
        asyncExecuteAtWithRet(h2, l, dataLambda,
                              std::make_pair(args.first, it),
                              addr);
      }
      rt::waitForCompletion(h2);
      T val = args.second(reduceData);
      // broadcast the data
      auto broadLambda = [](shad::rt::Handle &,
                           const std::tuple<ObjectID, T, size_t> &args) {
        auto ptr = MPIReducer<InF, OutF>::GetPtr(std::get<0>(args));
        T* out = (T*)(ptr->out_);
        out[std::get<2>(args)] = std::get<1>(args);
      };
      rt::asyncExecuteOnAll(handle, broadLambda, std::make_tuple(args.first, val, it));
    };
    rt::Handle h;
    rt::asyncForEachOnAll(h, reduceLambda, std::make_pair(oid_, op), count);
    rt::waitForCompletion(h);
  }

  

 protected:
  explicit MPIReducer(ObjectID oid, MPI_Datatype dt, InF inf, OutF outf)
      : oid_{oid}, dt_(dt) {
    in_ = inf();
    out_ = outf();
  }

 private:
  template <typename>
  friend class AbstractDataStructure;

  ObjectID oid_;
  void* in_;
  void* out_;
  MPI_Datatype dt_;
//   std::vector<T*>ins_;
//   std::vector<T*>outs_;
};

template <typename InF,typename OutF>
void MPIReducer<InF, OutF>::AllReduce(MPI_Op op, const size_t count) {
  if (dt_ == MPI_CHAR) { 
    CallReduce<char>(op, count);
    return;
  }
  if (dt_ == MPI_WCHAR) { 
    CallReduce<wchar_t>(op, count);
    return;
  }
  if (dt_ == MPI_SHORT) { 
    CallReduce<signed short>(op, count);
    return;
  }
  if (dt_ == MPI_INT) { 
    CallReduce<signed int>(op, count);
    return;
  }
  if (dt_ == MPI_LONG) { 
    CallReduce<signed long>(op, count);
    return;
  }
  if (dt_ == MPI_SIGNED_CHAR) { 
    CallReduce<signed char>(op, count);
    return;
  }
  if (dt_ == MPI_UNSIGNED_CHAR) { 
    CallReduce<unsigned char>(op, count);
    return;
  }
  if (dt_ == MPI_UNSIGNED_SHORT) { 
    CallReduce<unsigned short>(op, count);
    return;
  }
  if (dt_ == MPI_UNSIGNED) { 
    CallReduce<unsigned int>(op, count);
    return;
  }
  if (dt_ == MPI_UNSIGNED_LONG) { 
    CallReduce<unsigned long int>(op, count);
    return;
  }
  if (dt_ == MPI_FLOAT) { 
    CallReduce<float>(op, count);
    return;
  }
  if (dt_ == MPI_DOUBLE) { 
    CallReduce<double>(op, count);
    return;
  }
  if (dt_ ==  MPI_LONG_DOUBLE) { 
    CallReduce<long double>(op, count);
    return;
  }
  // if (dt_ == MPI_C_BOOL) { 
  //   DoReduce<bool>(op, count);
  //   return;
  // }

  // FIXME Some operators required by algs are not defined for 
  // complex numbers.

  // if (dt_ == MPI_COMPLEX) { 
  //   CallReduce<std::complex<float>>(op, count);
  //   return;
  // }
  // if (dt_ == MPI_DOUBLE_COMPLEX) { 
  //   CallReduce<std::complex<double>>(op, count);
  //   return;
  // }
  // if (dt_ == MPI_C_LONG_DOUBLE_COMPLEX) { 
  //   CallReduce<std::complex<long double>>(op, count);
  //   return;
  // }
  return;
}

template <typename InF,typename OutF>
template <typename T>
void MPIReducer<InF, OutF>::CallReduce(MPI_Op op, const size_t count) {
  if (op == MPI_SUM) {
    auto alg = [](std::vector<T>& redv)->T {
      return std::accumulate(redv.begin(), redv.end(), T{0}, std::plus<T>{});
    };
    DoMPIReduce<T, typeof(alg)>(alg, count);
    return;
  }
  if (op == MPI_PROD) {
    auto alg = [](std::vector<T>& redv)->T {
      return std::accumulate(redv.begin(), redv.end(), T{1}, std::multiplies<T>{});
    };
    DoMPIReduce<T, typeof(alg)>(alg, count);
    return;
  }
  if (op == MPI_MAX) {
    auto alg = [](std::vector<T>& redv)->T {
      return *std::max_element(redv.begin(), redv.end());
    };
    DoMPIReduce<T, typeof(alg)>(alg, count);
    return;
  }
  if (op == MPI_MIN) {
    auto alg = [](std::vector<T>& redv)->T {
      return *std::min_element(redv.begin(), redv.end());
    };
    DoMPIReduce<T, typeof(alg)>(alg, count);
    return;
  }
  return;
}

}  // namespace shad

#endif  // INCLUDE_SHAD_EXTENSIONS_COLLECTIVES_REDUCE_H_
