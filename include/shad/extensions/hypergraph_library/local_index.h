//===------------------------------------------------------------*- C++ -*-===//
//
//                                     SHAD
//
//      The Scalable High-performance Algorithms and Data Structure Library
//
//===----------------------------------------------------------------------===//
//
// Copyright 2020 Battelle Memorial Institute
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

#ifndef INCLUDE_SHAD_EXTENSIONS_HYPERGRAPH_LIBRARY_LOCAL_INDEX_H_
#define INCLUDE_SHAD_EXTENSIONS_HYPERGRAPH_LIBRARY_LOCAL_INDEX_H_

#include <algorithm>
#include <atomic>
#include <memory>
#include <set>
#include <tuple>
#include <utility>
#include <vector>

#include "shad/data_structures/compare_and_hash_utils.h"
#include "shad/data_structures/local_hashmap.h"
#include "shad/data_structures/local_set.h"
#include "shad/runtime/runtime.h"

namespace shad {

template <typename T>
class v2IDCmp {
 public:
  bool operator()(const T* first, const T* sec) const { return *first != *sec; }
};

template <typename SrcT, typename DestT,
          typename NeighborsStorageT = std::set<DestT>>
class v2DefaultEdgeIndexStorage {
 public:
  struct EmptyAttr {};
  using SrcAttributesT = EmptyAttr;
  explicit v2DefaultEdgeIndexStorage(const size_t numVertices)
      : edgeList_(std::max(numVertices / constants::kDefaultNumEntriesPerBucket,
                           1lu)) {}
  v2DefaultEdgeIndexStorage(const size_t numVertices, const SrcAttributesT&)
      : edgeList_(std::max(numVertices / constants::kDefaultNumEntriesPerBucket,
                           1lu)) {}
  static constexpr size_t kEdgeListChunkSize_ = 3072 / sizeof(DestT);
  struct LocalEdgeListChunk {
    LocalEdgeListChunk(size_t _numDest, bool _ow, DestT* _dest)
        : numDest(_numDest), overwrite(_ow) {
      memcpy(destinations.data(), _dest,
             std::min(numDest, destinations.size()) * sizeof(DestT));
    }
    size_t numDest;
    size_t chunkSize;
    bool overwrite;
    std::array<DestT, kEdgeListChunkSize_> destinations;
  };

  struct FlatEdgeList {
    const DestT* values;
    size_t numValues;
    bool overwrite;
  };

  struct ElementInserter {
    void operator()(NeighborsStorageT* const lhs, const NeighborsStorageT&) {}
    static bool Insert(NeighborsStorageT* const lhs, const DestT value, bool) {
      lhs->insert(value);
      return true;
    }
    static bool Insert(NeighborsStorageT* const lhs,
                       const FlatEdgeList values, bool) {
      if (values.overwrite) lhs->Reset(values.numValues);
      for (size_t i = 0; i < values.numValues; i++) {
        lhs->insert(values.values[i]);
      }
      return true;
    }

    static bool Insert(NeighborsStorageT* const lhs,
                       const LocalEdgeListChunk& chunk, bool) {
      if (chunk.overwrite) lhs->reset(chunk.numDest);
      size_t chunkSize = std::min(chunk.numDest, chunk.destinations.size());
      for (size_t i = 0; i < chunkSize; i++) {
        lhs->insert(chunk.destinations[i]);
      }
      return true;
    }
  };

  template <typename ApplyFunT, typename... Args>
  void ForEachAttributedVertexNeighbor(const SrcT& src, ApplyFunT&& function,
                                       Args... args);

  template <typename ApplyFunT, typename... Args>
  void AsyncForEachAttributedVertexNeighbor(rt::Handle& handle, const SrcT& src,
                                            ApplyFunT&& function, Args... args);

  template <typename ApplyFunT, typename... Args>
  void ForEachAttributedVertex(ApplyFunT&& function, Args... args);

  template <typename ApplyFunT, typename... Args>
  void AsyncForEachAttributedVertex(rt::Handle& handle, ApplyFunT&& function,
                                    Args... args);

  SrcAttributesT* GetVertexAttributes(const SrcT& src) {
    printf(
        "WARNING: Trying to lookup attributes from"
        " a non attributed graph\n");
    return nullptr;
  }
  bool GetVertexAttributes(const SrcT& src, SrcAttributesT* attr) {
    return false;
  }

  template <typename ApplyFunT, typename... Args>
  void VertexAttributesApply(const SrcT& src, ApplyFunT&& function,
                             Args... args) {
    printf("WARNING: Function not implemented for non attributed graphs\n");
  }

  using NeighborListStorageT = NeighborsStorageT;
  using EdgeListStorageT =
      LocalHashmap<SrcT, NeighborsStorageT, v2IDCmp<SrcT>, ElementInserter>;
  EdgeListStorageT edgeList_;

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void CallVertexAttributesApplyFun(
      v2DefaultEdgeIndexStorage<SrcT, DestT, NeighborListStorageT>* stPtr,
      const SrcT& key, ApplyFunT function, std::tuple<Args...>& args,
      std::index_sequence<is...>) {
    printf("WARNING: Function not implemented for non attributed graphs\n");
  }
};
/// @brief The LocalIndex data structure.
///
/// SHAD's LocalIndex is a "local", thread-safe, associative container,
/// representive a collection of neighbors lists of a graph.
/// LocalIndexs can be used ONLY on the Locality
/// on which they are created.
template <typename SrcT, typename DestT,
          typename StorageT = v2DefaultEdgeIndexStorage<SrcT, DestT>>
class LocalIndex {
 public:
  using value_type = typename StorageT::EdgeListStorageT::value_type;
  using iterator = typename StorageT::EdgeListStorageT::iterator;
  using const_iterator = typename StorageT::EdgeListStorageT::const_iterator;
  
  iterator begin() {
      return edges_.edgeList_.begin();
  }
  iterator cbegin() {
      return edges_.edgeList_.cbegin();
  }
  iterator end() {
      return edges_.edgeList_.end();
  }
  iterator cend() {
      return edges_.edgeList_.cend();
  }
  
  /// @brief Constructor.
  /// @param numVertices Expected number of vertices.
  explicit LocalIndex(const size_t numVertices)
      : edges_(numVertices), numEdges_(0) {}

  LocalIndex(const size_t numVertices,
                 const typename StorageT::SrcAttributesT& initAttr)
      : edges_(numVertices, initAttr), numEdges_(0) {}

  /// @brief Size of the edgeIndex (number of entries).
  /// @return the size of the edgeIndex.
  size_t Size() const { return edges_.edgeList_.Size(); }

  size_t UpdateNumEdges() {
    size_t numEdges(0);
    size_t* cntPtr = &numEdges;
    LocalIndex<SrcT, DestT, StorageT>* eiptr = this;
    auto countLambda = [](const SrcT& src,
                          LocalIndex<SrcT, DestT, StorageT>*& eiptr,
                          size_t*& edgeCntPtr) {
      auto neigh = eiptr->edges_.edgeList_.Lookup(src);
      size_t nsize = neigh->Size();
      __sync_fetch_and_add(edgeCntPtr, nsize);
    };
    edges_.edgeList_.ForEachKey(countLambda, eiptr, cntPtr);
    // FIXME : This is should be atomic.
    numEdges_ = numEdges;
    return numEdges_;
  }

  void Insert(const SrcT& src, const DestT& dest) {
    edges_.edgeList_.Insert(src, dest);
  }

  void Insert(const SrcT& src,
              const typename StorageT::LocalEdgeListChunk& chunk) {
    edges_.edgeList_.Insert(src, chunk);
  }

  void AsyncInsert(rt::Handle& handle, const SrcT& src,
                   const typename StorageT::LocalEdgeListChunk& chunk) {
    edges_.edgeList_.AsyncInsert(handle, src, chunk);
  }

  void InsertEdgeList(const SrcT& src, const DestT* destinations,
                      size_t numDest, bool overwrite = true) {
    typename StorageT::FlatEdgeList dest = {destinations, numDest, overwrite};
    edges_.edgeList_.Insert(src, dest);
  }

  void AsyncInsertEdgeList(rt::Handle& handle, const SrcT& src,
                           const DestT* destinations, size_t numDest,
                           bool overwrite = true) {
    typename StorageT::FlatEdgeList dest = {destinations, numDest, overwrite};
    edges_.edgeList_.AsyncInsert(handle, src, dest);
  }

  void AsyncInsert(rt::Handle& handle, const SrcT& src, const DestT& dest) {
    edges_.edgeList_.AsyncInsert(handle, src, dest);
  }

  void Erase(const SrcT& src, const DestT& dest) {
    auto edgeList = edges_.edgeList_.Lookup(src);
    if (edgeList != nullptr) edgeList->Erase(dest);
  }

  void AsyncErase(rt::Handle& handle, const SrcT& src, const DestT& dest) {
    auto edgeList = edges_.edgeList_.Lookup(src);
    if (edgeList != nullptr) edgeList->AsyncErase(handle, dest);
  }

  size_t GetDegree(const SrcT& src) {
    auto edgeList = edges_.edgeList_.Lookup(src);
    if (edgeList == nullptr) return 0;
    return edgeList->Size();
  }

  typename StorageT::NeighborListStorageT* GetNeighbors(const SrcT& src) {
    return nullptr; //edges_.edgeList_.Lookup(src);
  }

  void AsyncGetNeighbors(rt::Handle& handle, const SrcT src,
                         typename StorageT::NeighborListStorageT** res) {
    //edges_.edgeList_.AsyncLookup(handle, src, res);
  }

  template <typename ApplyFunT, typename... Args>
  void ForEachNeighbor(const SrcT& src, ApplyFunT&& function, Args&... args) {
    auto edgeList = edges_.edgeList_.Lookup(src);
    if (edgeList == nullptr) {
      printf("EdgeList is null!!\n");
      return;
    }
    edgeList->ForEachNeighbor(function, src, args...);
  }

  template <typename ApplyFunT, typename... Args>
  void AsyncForEachNeighbor(rt::Handle& handle, const SrcT& src,
                            ApplyFunT&& function, Args&... args) {
    auto edgeList = edges_.edgeList_.Lookup(src);
    if (edgeList == nullptr) {
      printf("EdgeList is null!!\n");
      return;
    }
    edgeList->AsyncForEachNeighbor(handle, function, src, args...);
  }

  template <typename ApplyFunT, typename... Args>
  void ForEachVertex(ApplyFunT&& function, Args&... args) {
    edges_.edgeList_.ForEachKey(function, args...);
  }

  template <typename ApplyFunT, typename... Args>
  void AsyncForEachVertex(rt::Handle& handle, ApplyFunT&& function,
                          Args&... args) {
    edges_.edgeList_.AsyncForEachKey(handle, function, args...);
  }

  template <typename ApplyFunT, typename... Args>
  void ForEachEdge(ApplyFunT&& function, Args&... args) {
//     using FunctionTy = void (*)(const SrcT& src, const DestT& dest, Args&...);
//     FunctionTy fn = std::forward<decltype(function)>(function);
//     auto callFE = [](const SrcT& src,
//                      typename StorageT::NeighborListStorageT& neighbors,
//                      FunctionTy& fun, Args&... args) {
//       neighbors.ForEachNeighbor(fun, src, args...);
//     };
//     edges_.edgeList_.ForEachEntry(callFE, fn, args...);
  }

  template <typename ApplyFunT, typename... Args>
  void AsyncForEachEdge(rt::Handle& handle, ApplyFunT&& function,
                        Args&... args) {
//     using FunctionTy = void (*)(rt::Handle & handle, const SrcT& src,
//                                 const DestT& dest, Args&...);
//     FunctionTy fn = std::forward<decltype(function)>(function);
//     auto callFE = [](rt::Handle& handle, const SrcT& src,
//                      typename StorageT::NeighborListStorageT& neighbors,
//                      FunctionTy& fun, Args&... args) {
//       neighbors.AsyncForEachNeighbor(handle, fun, src, args...);
//     };
//     edges_.edgeList_.AsyncForEachEntry(handle, callFE, fn, args...);
  }

  template <typename ApplyFunT, typename... Args>
  void ForEachAttributedVertexNeighbor(const SrcT& src, ApplyFunT&& function,
                                       Args&... args) {
    edges_.ForEachAttributedVertexNeighbor(function, src, args...);
  }

  template <typename ApplyFunT, typename... Args>
  void AsyncForEachAttributedVertexNeighbor(rt::Handle& handle, const SrcT& src,
                                            ApplyFunT&& function,
                                            Args&... args) {
    edges_.AsyncForEachAttributedVertexNeighbor(handle, function, src, args...);
  }

  template <typename ApplyFunT, typename... Args>
  void ForEachAttributedVertex(ApplyFunT&& function, Args&... args) {
    edges_.ForEachAttributedVertex(function, args...);
  }

  template <typename ApplyFunT, typename... Args>
  void AsyncForEachAttributedVertex(rt::Handle& handle, ApplyFunT&& function,
                                    Args&... args) {
    edges_.AsyncForEachAttributedVertex(handle, function, args...);
  }

  typename StorageT::SrcAttributesT* GetVertexAttributes(const SrcT& src) {
    return edges_.GetVertexAttributes(src);
  }

  bool GetVertexAttributes(const SrcT& src,
                           typename StorageT::SrcAttributesT* attr) {
    return edges_.GetVertexAttributes(src, attr);
  }

  template <typename ApplyFunT, typename... Args>
  void VertexAttributesApply(const SrcT& src, ApplyFunT&& function,
                             Args&... args) {
    edges_.VertexAttributesApply(src, function, args...);
  }

  StorageT* GetEdgesPtr() { return &edges_; }

 private:
  size_t numEdges_;
  StorageT edges_;
};

}  // namespace shad

#endif  // INCLUDE_SHAD_EXTENSIONS_HYPERGRAPH_LIBRARY_LOCAL_INDEX_H_
