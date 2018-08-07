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

#ifndef INCLUDE_SHAD_EXTENSIONS_GRAPH_LIBRARY_EDGE_INDEX_H_
#define INCLUDE_SHAD_EXTENSIONS_GRAPH_LIBRARY_EDGE_INDEX_H_

#include <algorithm>
#include <functional>
#include <tuple>
#include <utility>
#include <vector>

#include "shad/data_structures/abstract_data_structure.h"
#include "shad/data_structures/buffer.h"
#include "shad/data_structures/compare_and_hash_utils.h"
#include "shad/data_structures/local_set.h"
#include "shad/extensions/graph_library/local_edge_index.h"
#include "shad/runtime/runtime.h"

namespace shad {

/// @brief The EdgeIndex data structure.
///
/// SHAD's EdgeIndex is a thread-safe, associative container,
/// representing a collection of neighbors lists of a graph.
/// @tparam SrcT type of source vertices (used as identifiers).
/// @tparam DestT type of destination vertices (used as identifiers).
/// @warning obects of type SrcT and DestT need to be trivially copiable.
/// @tparam StorageT EdgeIndex local storage. Default is a map of sets.
template <typename SrcT, typename DestT,
          typename StorageT = DefaultEdgeIndexStorage<SrcT, DestT>>
class EdgeIndex
    : public AbstractDataStructure<EdgeIndex<SrcT, DestT, StorageT>> {
  template <typename>
  friend class AbstractDataStructure;
  template <typename, typename, typename>
  friend class LocalEdgeIndex;

 public:
  using ObjectID = typename AbstractDataStructure<
      EdgeIndex<SrcT, DestT, StorageT>>::ObjectID;
  using EdgeListPtr = typename AbstractDataStructure<
      EdgeIndex<SrcT, DestT, StorageT>>::SharedPtr;
  using SrcType = SrcT;
  using DestType = DestT;
  using LIdxT = LocalEdgeIndex<SrcT, DestT, StorageT>;
  using IdxT = EdgeIndex<SrcT, DestT, StorageT>;
  struct EntryT {
    EntryT(const SrcT &s, const DestT &d) : src(s), dest(d) {}
    EntryT() = default;
    SrcT src;
    DestT dest;
  };
  using BuffersVector =
      typename impl::BuffersVector<EntryT, EdgeIndex<SrcT, DestT, StorageT>>;

  /// @brief Create method.
  ///
  /// Creates a new edge_index instance.
  ///
  /// @param numVertices Expected number of vertices.
  /// @return A shared pointer to the newly created edge_index instance.
#ifdef DOXYGEN_IS_RUNNING
  static EdgeIndexPtr Create(const size_t numVertices);
#endif

  /// @brief Getter of the Global Identifier.
  ///
  /// @return The global identifier associated with the hashmap instance.
  ObjectID GetGlobalID() const { return oid_; }

  /// @brief Overall size of the edge index (number of unique sources).
  /// @return the number of unique source vertices in the index.
  size_t Size() const;

  /// @brief Overall number of edges in the index.
  /// @return the number of edges in the index.
  size_t NumEdges();

  /// @brief Insert an edge in the index.
  /// @param[in] src the source vertex.
  /// @param[in] value the destination vertex.
  void Insert(const SrcT &src, const DestT &dest);

  /// @brief Asynchronously insert an edge in the index.
  /// @param[in,out] handle Reference to the handle
  /// to be used to wait for completion.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in] src the source vertex.
  /// @param[in] value the destination vertex.
  void AsyncInsert(rt::Handle &handle, const SrcT &src, const DestT &dest);

  /// @brief Insert an edge list in the index.
  /// @param[in] src the source vertex.
  /// @param[in] destinations pointer to the destination vertices.
  /// @param numDest number of destinations (edges) to insert.
  /// @param overwrite if true, overwrites the neighbors list of src
  /// with the provided destinations; otherwise, edges are added to the current
  /// neighbors list.
  void InsertEdgeList(const SrcT &src, DestT *destinations, size_t numDest,
                      bool overwrite = true);

  /// @brief Asynchronously insert an edge list in the index.
  /// @param[in,out] handle Reference to the handle
  /// to be used to wait for completion.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in] src the source vertex.
  /// @param[in] destinations pointer to the destination vertices.
  /// @param numDest number of destinations (edges) to insert.
  /// @param overwrite if true, overwrites the neighbors list of src
  /// with the provided destinations; otherwise, edges are added to the current
  /// neighbors list.
  void AsyncInsertEdgeList(rt::Handle &handle, const SrcT &src,
                           DestT *destinations, size_t numDest,
                           bool overwrite = true);

  /// @brief Asynchronously retrieve the neighbors list of a given vertex.
  /// @param[in,out] handle Reference to the handle
  /// to be used to wait for completion.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in] src the source vertex.
  /// @param[out] res pointer to the neighbors list storage where
  /// the neighbors list of src is stored.
  void AsyncGetNeighbors(rt::Handle &handle, const SrcT src,
                         typename StorageT::NeighborListStorageT **res);

  /// @brief Retrieve the neighbors list of a given vertex.
  /// @param[in] src the source vertex.
  /// @param[out] res pointer to the neighbors list storage where
  /// the neighbors list of src is copied.
  void GetNeighbors(const SrcT &src,
                    typename StorageT::NeighborListStorageT *res);

  /// @brief Number of neighbors of a given vertex.
  /// @param[in] src the source vertex.
  /// @return the number of neighbors of vertex src.
  size_t GetDegree(const SrcT &src);

  /// @brief Buffered Insert method.
  ///
  /// Inserts an edge, using aggregation buffers.
  ///
  /// @warning Insertions are finalized only after calling
  /// the WaitForBufferedInsert() method.
  ///
  /// @param[in] src The source vertex.
  /// @param[in] dest The destination vertex.
  void BufferedInsert(const SrcT &src, const DestT &dest);

  /// @brief Asynchronous Buffered Insert method.
  ///
  /// Asynchronously inserts an edge, using aggregation buffers.
  ///
  /// @warning asynchronous buffered insertions are
  /// finalized only after calling
  /// the rt::waitForCompletion(rt::Handle &handle) method <b>and</b> the
  /// WaitForBufferedInsert() method, in this order.
  ///
  /// @param[in,out] handle Reference to the handle
  /// @param[in] src The source vertex.
  /// @param[in] dest The destination vertex.
  void BufferedAsyncInsert(rt::Handle &handle, const SrcT &src,
                           const DestT &dest);

  /// @brief Finalize method for buffered insertions.
  void WaitForBufferedInsert() {
    auto flushLambda_ = [](const ObjectID &oid) {
      auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(oid);
      ptr->buffers_.FlushAll();
    };
    rt::executeOnAll(flushLambda_, oid_);
  }

  /// @brief Delete an edge.
  /// @param[in] src The source vertex.
  /// @param[in] dest The destination vertex.
  void Erase(const SrcT &src, const DestT &dest);

  /// @brief Asynchronously delete an edge.
  /// @param[in,out] handle Reference to the handle
  /// to be used to wait for completion.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the
  /// rt::waitForCompletion(rt::Handle &handle) method.
  /// @param[in] src The source vertex.
  /// @param[in] dest The destination vertex.
  void AsyncErase(rt::Handle &handle, const SrcT &src, const DestT &dest);

  // FIXME implement me
  /// @brief Remove a vertex from the edge index.
  /// @param[in] src the src vertex.
  void Erase(const SrcT &src);

  // FIXME implement me
  /// @brief Clear the content of the edge index.
  void Clear();

  // FIXME it should be protected
  void BufferEntryInsert(const EntryT &entry) {
    localIndex_.Insert(entry.src, entry.dest);
  }

  /// @brief Apply a user-defined function to each neighbor of a given vertex.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(const SrcT&, const DestT&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param src The source vertex
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void ForEachNeighbor(const SrcT &src, ApplyFunT &&function, Args &... args);

  /// @brief Asynchronously apply a user-defined function
  /// to each neighbor of a given vertex.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(Handle&, const SrcT&, const DestT&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle Reference to the handle
  /// to be used to wait for completion.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the
  /// @param src The source vertex.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncForEachNeighbor(rt::Handle &handle, const SrcT &src,
                            ApplyFunT &&function, Args &... args);

  /// @brief Apply a user-defined function to each vertex.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(const SrcT&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void ForEachVertex(ApplyFunT &&function, Args &... args);

  /// @brief Apply a user-defined function to each vertex.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(const SrcT&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle Reference to the handle
  /// to be used to wait for completion.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncForEachVertex(rt::Handle &handle, ApplyFunT &&function,
                          Args &... args);

  /// @brief Apply a user-defined function to each edge.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(const SrcT&, const DestT&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void ForEachEdge(ApplyFunT &&function, Args &... args);

  /// @brief Asynchronously apply a user-defined function
  /// to each edge.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(Handle&, const SrcT&, const DestT&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in,out] handle Reference to the handle
  /// to be used to wait for completion.
  /// @warning Asynchronous operations are guaranteed to have completed
  /// only after calling the
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void AsyncForEachEdge(rt::Handle &handle, ApplyFunT &&function,
                        Args &... args);

  // FIXME for testing purposes only
  LocalEdgeIndex<SrcT, DestT, StorageT> *GetLocalIndexPtr() {
    return &localIndex_;
  }

  /// @brief Retrieve the attributes of a given vertex.
  /// @param[in] src the source vertex.
  /// @param[out] attr pointer to the attributes data structure
  /// where the attributes are copied.
  /// @result true if the vertex has been found, false otherwise.
  bool GetVertexAttributes(const SrcT &src,
                           typename StorageT::SrcAttributesT *attr);

  /// @brief Apply a user-defined function to the attributes of a given vertex.
  ///
  /// @tparam ApplyFunT User-defined function type.  The function prototype
  /// should be:
  /// @code
  /// void(const SrcT&, StorageT::SrcAttributesT&, Args&);
  /// @endcode
  /// @tparam ...Args Types of the function arguments.
  ///
  /// @param[in] src the source vertex.
  /// @param function The function to apply.
  /// @param args The function arguments.
  template <typename ApplyFunT, typename... Args>
  void VertexAttributesApply(const SrcT &src, ApplyFunT &&function,
                             Args &... args);

 private:
  ObjectID oid_;
  LocalEdgeIndex<SrcT, DestT, StorageT> localIndex_;
  BuffersVector buffers_;

  struct InsertArgs {
    ObjectID oid;
    SrcT src;
    DestT dest;
  };

  struct LookupArgs {
    ObjectID oid;
    SrcT src;
  };

  struct LookupResult {
    bool found;
    /// The value associated with the key.
    typename StorageT::SrcAttributesT attr;
  };

  using LocalEdgeListChunk = typename StorageT::LocalEdgeListChunk;
  struct EdgeListChunk {
    EdgeListChunk(ObjectID &_oid, SrcT _src, LocalEdgeListChunk &_chunk)
        : oid(_oid), src(_src), chunk(_chunk) {}
    typename EdgeIndex<SrcT, DestT, StorageT>::ObjectID oid;
    SrcT src;
    LocalEdgeListChunk chunk;
  };

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void ForEachNeighborWrapper(const ObjectID &oid, const SrcT &src,
                                     const ApplyFunT function,
                                     std::tuple<Args...> &args,
                                     std::index_sequence<is...>) {
    auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(oid);
    ptr->localIndex_.ForEachNeighbor(src, function, std::get<is>(args)...);
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void AsyncForEachNeighborWrapper(rt::Handle &handle,
                                          const ObjectID &oid, const SrcT &src,
                                          const ApplyFunT function,
                                          std::tuple<Args...> &args,
                                          std::index_sequence<is...>) {
    auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(oid);
    ptr->localIndex_.AsyncForEachNeighbor(handle, src, function,
                                          std::get<is>(args)...);
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void ForEachVertexWrapper(const ObjectID &oid,
                                   const ApplyFunT function,
                                   std::tuple<Args...> &args,
                                   std::index_sequence<is...>) {
    auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(oid);
    ptr->localIndex_.ForEachVertex(function, std::get<is>(args)...);
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void AsyncForEachVertexWrapper(rt::Handle &handle, const ObjectID &oid,
                                        const ApplyFunT function,
                                        std::tuple<Args...> &args,
                                        std::index_sequence<is...>) {
    auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(oid);
    ptr->localIndex_.AsyncForEachVertex(handle, function,
                                        std::get<is>(args)...);
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void ForEachEdgeWrapper(const ObjectID &oid, const ApplyFunT function,
                                 std::tuple<Args...> &args,
                                 std::index_sequence<is...>) {
    auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(oid);
    ptr->localIndex_.ForEachEdge(function, std::get<is>(args)...);
  }

  template <typename ApplyFunT, typename... Args, std::size_t... is>
  static void AsyncForEachEdgeWrapper(rt::Handle &handle, const ObjectID &oid,
                                      const ApplyFunT function,
                                      std::tuple<Args...> &args,
                                      std::index_sequence<is...>) {
    auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(oid);
    ptr->localIndex_.AsyncForEachEdge(handle, function, std::get<is>(args)...);
  }

 protected:
  EdgeIndex(ObjectID oid, const size_t numVertices)
      : oid_(oid), localIndex_(numVertices), buffers_(oid) {}
  EdgeIndex(ObjectID oid, const size_t numVertices,
            const typename StorageT::SrcAttributesT &initAttr)
      : oid_(oid), localIndex_(numVertices, initAttr), buffers_(oid) {}
};

template <typename SrcT, typename DestT, typename StorageT>
inline size_t EdgeIndex<SrcT, DestT, StorageT>::Size() const {
  size_t size = localIndex_.Size();
  size_t remoteSize;
  auto sizeLambda = [](const ObjectID &oid, size_t *res) {
    auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(oid);
    *res = ptr->localIndex_.Size();
  };
  for (auto tgtLoc : rt::allLocalities()) {
    if (tgtLoc != rt::thisLocality()) {
      rt::executeAtWithRet(tgtLoc, sizeLambda, oid_, &remoteSize);
      size += remoteSize;
    }
  }
  return size;
}

template <typename SrcT, typename DestT, typename StorageT>
inline size_t EdgeIndex<SrcT, DestT, StorageT>::NumEdges() {
  size_t size = localIndex_.UpdateNumEdges();
  size_t remoteSize;
  auto sizeLambda = [](const ObjectID &oid, size_t *res) {
    auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(oid);
    *res = ptr->localIndex_.UpdateNumEdges();
  };
  for (auto tgtLoc : rt::allLocalities()) {
    if (tgtLoc != rt::thisLocality()) {
      rt::executeAtWithRet(tgtLoc, sizeLambda, oid_, &remoteSize);
      size += remoteSize;
    }
  }
  return size;
}

template <typename SrcT, typename DestT, typename StorageT>
inline size_t EdgeIndex<SrcT, DestT, StorageT>::GetDegree(const SrcT &src) {
  size_t targetId = shad::hash<SrcT>{}(src) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  size_t degree = 0;
  if (targetLocality == rt::thisLocality()) {
    return localIndex_.GetDegree(src);
  } else {
    auto degreeLambda = [](const std::tuple<ObjectID, SrcT> &args,
                           size_t *res) {
      auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(std::get<0>(args));
      *res = ptr->localIndex_.GetDegree(std::get<1>(args));
    };
    rt::executeAtWithRet(targetLocality, degreeLambda,
                         std::make_tuple(oid_, src), &degree);
  }
  return degree;
}

template <typename SrcT, typename DestT, typename StorageT>
inline void EdgeIndex<SrcT, DestT, StorageT>::Insert(const SrcT &src,
                                                     const DestT &dest) {
  size_t targetId = shad::hash<SrcT>{}(src) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localIndex_.Insert(src, dest);
  } else {
    auto insertLambda = [](const InsertArgs &args) {
      auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(args.oid);
      ptr->localIndex_.Insert(args.src, args.dest);
    };
    InsertArgs args{oid_, src, dest};
    rt::executeAt(targetLocality, insertLambda, args);
  }
}

template <typename SrcT, typename DestT, typename StorageT>
inline void EdgeIndex<SrcT, DestT, StorageT>::InsertEdgeList(
    const SrcT &src, DestT *destinations, size_t numDest, bool overwrite) {
  size_t targetId = shad::hash<SrcT>{}(src) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    localIndex_.InsertEdgeList(src, destinations, numDest, overwrite);
  } else {
    int toInsert = numDest;
    auto insertLambda = [](const EdgeListChunk &args) {
      auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(args.oid);
      ptr->localIndex_.Insert(args.src, args.chunk);
    };
    size_t locSize = StorageT::kEdgeListChunkSize_;
    size_t chunkSize = std::min<size_t>(locSize, toInsert);
    LocalEdgeListChunk lchunk(numDest, overwrite, destinations);
    EdgeListChunk args(oid_, src, lchunk);
    rt::executeAt(targetLocality, insertLambda, args);
    toInsert -= chunkSize;
    while (toInsert > 0) {
      destinations += chunkSize;
      chunkSize = std::min<size_t>(locSize, toInsert);
      LocalEdgeListChunk lchunk(chunkSize, false, destinations);
      EdgeListChunk args(oid_, src, lchunk);
      rt::executeAt(targetLocality, insertLambda, args);
      toInsert -= chunkSize;
    }
  }
}

template <typename SrcT, typename DestT, typename StorageT>
inline void EdgeIndex<SrcT, DestT, StorageT>::AsyncInsertEdgeList(
    rt::Handle &handle, const SrcT &src, DestT *destinations, size_t numDest,
    bool overwrite) {
  size_t targetId = shad::hash<SrcT>{}(src) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localIndex_.InsertEdgeList(src, destinations, numDest, overwrite);
  } else {
    auto syncInsertLambda = [](const EdgeListChunk &args) {
      auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(args.oid);
      ptr->localIndex_.Insert(args.src, args.chunk);
    };
    auto insertLambda = [](rt::Handle &handle, const EdgeListChunk &args) {
      auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(args.oid);
      ptr->localIndex_.AsyncInsert(handle, args.src, args.chunk);
    };

    int toInsert = numDest;
    size_t locSize = StorageT::kEdgeListChunkSize_;
    size_t chunkSize = std::min<size_t>(locSize, toInsert);
    LocalEdgeListChunk lchunk(numDest, overwrite, destinations);
    EdgeListChunk args(oid_, src, lchunk);
    if (numDest <= locSize) {
      rt::asyncExecuteAt(handle, targetLocality, insertLambda, args);
      return;
    }
    rt::executeAt(targetLocality, syncInsertLambda, args);

    toInsert -= chunkSize;
    while (toInsert > 0) {
      destinations += chunkSize;
      chunkSize = std::min<size_t>(locSize, toInsert);
      lchunk = LocalEdgeListChunk(chunkSize, false, destinations);
      args = EdgeListChunk(oid_, src, lchunk);
      rt::asyncExecuteAt(handle, targetLocality, insertLambda, args);
      toInsert -= chunkSize;
    }
  }
}

template <typename SrcT, typename DestT, typename StorageT>
inline void EdgeIndex<SrcT, DestT, StorageT>::AsyncInsert(rt::Handle &handle,
                                                          const SrcT &src,
                                                          const DestT &dest) {
  size_t targetId = shad::hash<SrcT>{}(src) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localIndex_.AsyncInsert(handle, src, dest);
  } else {
    auto insertLambda = [](rt::Handle &handle, const InsertArgs &args) {
      auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(args.oid);
      ptr->localIndex_.AsyncInsert(handle, args.src, args.dest);
    };
    InsertArgs args = {oid_, src, dest};
    rt::asyncExecuteAt(handle, targetLocality, insertLambda, args);
  }
}

template <typename SrcT, typename DestT, typename StorageT>
inline void EdgeIndex<SrcT, DestT, StorageT>::Erase(const SrcT &src,
                                                    const DestT &dest) {
  size_t targetId = shad::hash<SrcT>{}(src) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localIndex_.Erase(src, dest);
  } else {
    auto eraseLambda = [](const InsertArgs &args) {
      auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(args.oid);
      ptr->localIndex_.Erase(args.src, args.dest);
    };
    InsertArgs args = {oid_, src, dest};
    rt::executeAt(targetLocality, eraseLambda, args);
  }
}

template <typename SrcT, typename DestT, typename StorageT>
inline void EdgeIndex<SrcT, DestT, StorageT>::AsyncErase(rt::Handle &handle,
                                                         const SrcT &src,
                                                         const DestT &dest) {
  size_t targetId = shad::hash<SrcT>{}(src) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    localIndex_.AsyncErase(handle, src, dest);
  } else {
    auto eraseLambda = [](rt::Handle &handle, const InsertArgs &args) {
      auto ptr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(args.oid);
      ptr->localIndex_.AsyncErase(handle, args.src, args.dest);
    };
    InsertArgs args = {oid_, src, dest};
    rt::asyncExecuteAt(handle, targetLocality, eraseLambda, args);
  }
}

template <typename SrcT, typename DestT, typename StorageT>
inline void EdgeIndex<SrcT, DestT, StorageT>::BufferedInsert(
    const SrcT &src, const DestT &dest) {
  size_t targetId = shad::hash<SrcT>{}(src) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    localIndex_.Insert(src, dest);
  } else {
    buffers_.Insert(EntryT(src, dest), targetLocality);
  }
}

template <typename SrcT, typename DestT, typename StorageT>
inline void EdgeIndex<SrcT, DestT, StorageT>::BufferedAsyncInsert(
    rt::Handle &handle, const SrcT &src, const DestT &dest) {
  size_t targetId = shad::hash<SrcT>{}(src) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    localIndex_.AsyncInsert(handle, src, dest);
  } else {
    buffers_.AsyncInsert(handle, EntryT(src, dest), targetLocality);
  }
}

template <typename SrcT, typename DestT, typename StorageT>
template <typename ApplyFunT, typename... Args>
void EdgeIndex<SrcT, DestT, StorageT>::ForEachVertex(ApplyFunT &&function,
                                                     Args &... args) {
  using FunctionTy = void (*)(const SrcT &src, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  feArgs arguments(oid_, fn, std::tuple<Args...>(args...));
  auto feLambda = [](const feArgs &args) {
    constexpr auto size = std::tuple_size<
        typename std::decay<decltype(std::get<2>(args))>::type>::value;
    feArgs &fargs = const_cast<feArgs &>(args);
    ForEachVertexWrapper(std::get<0>(fargs), std::get<1>(fargs),
                         std::get<2>(fargs), std::make_index_sequence<size>());
  };
  rt::executeOnAll(feLambda, arguments);
}

template <typename SrcT, typename DestT, typename StorageT>
template <typename ApplyFunT, typename... Args>
void EdgeIndex<SrcT, DestT, StorageT>::AsyncForEachVertex(rt::Handle &handle,
                                                          ApplyFunT &&function,
                                                          Args &... args) {
  using FunctionTy = void (*)(rt::Handle & h, const SrcT &src, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  feArgs arguments(oid_, fn, std::tuple<Args...>(args...));
  auto feLambda = [](rt::Handle &handle, const feArgs &args) {
    constexpr auto size = std::tuple_size<
        typename std::decay<decltype(std::get<2>(args))>::type>::value;
    feArgs &fargs = const_cast<feArgs &>(args);
    AsyncForEachVertexWrapper(handle, std::get<0>(fargs), std::get<1>(fargs),
                              std::get<2>(fargs),
                              std::make_index_sequence<size>());
  };
  rt::asyncExecuteOnAll(handle, feLambda, arguments);
}

template <typename SrcT, typename DestT, typename StorageT>
template <typename ApplyFunT, typename... Args>
void EdgeIndex<SrcT, DestT, StorageT>::ForEachNeighbor(const SrcT &src,
                                                       ApplyFunT &&function,
                                                       Args &... args) {
  size_t targetId = shad::hash<SrcT>{}(src) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    localIndex_.ForEachNeighbor(src, function, args...);
    return;
  }
  using FunctionTy = void (*)(const SrcT &src, const DestT &dest, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using feArgs = std::tuple<ObjectID, SrcT, FunctionTy, std::tuple<Args...>>;
  feArgs arguments(oid_, src, fn, std::tuple<Args...>(args...));
  auto feLambda = [](const feArgs &args) {
    feArgs &fargs = const_cast<feArgs &>(args);
    constexpr auto size = std::tuple_size<
        typename std::decay<decltype(std::get<3>(fargs))>::type>::value;
    ForEachNeighborWrapper(std::get<0>(fargs), std::get<1>(fargs),
                           std::get<2>(fargs), std::get<3>(fargs),
                           std::make_index_sequence<size>());
  };
  rt::executeAt(targetLocality, feLambda, arguments);
}

template <typename SrcT, typename DestT, typename StorageT>
template <typename ApplyFunT, typename... Args>
void EdgeIndex<SrcT, DestT, StorageT>::AsyncForEachNeighbor(
    rt::Handle &handle, const SrcT &src, ApplyFunT &&function, Args &... args) {
  size_t targetId = shad::hash<SrcT>{}(src) % rt::numLocalities();
  rt::Locality targetLocality(targetId);
  if (targetLocality == rt::thisLocality()) {
    localIndex_.AsyncForEachNeighbor(handle, src, function, args...);
    return;
  }
  using FunctionTy = void (*)(rt::Handle & handle, const SrcT &src,
                              const DestT &dest, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using feArgs = std::tuple<ObjectID, SrcT, FunctionTy, std::tuple<Args...>>;
  feArgs arguments(oid_, src, fn, std::tuple<Args...>(args...));
  auto feLambda = [](rt::Handle &handle, const feArgs &args) {
    feArgs &fargs = const_cast<feArgs &>(args);
    constexpr auto size = std::tuple_size<
        typename std::decay<decltype(std::get<3>(fargs))>::type>::value;
    AsyncForEachNeighborWrapper(handle, std::get<0>(fargs), std::get<1>(fargs),
                                std::get<2>(fargs), std::get<3>(fargs),
                                std::make_index_sequence<size>());
  };
  rt::asyncExecuteAt(handle, targetLocality, feLambda, arguments);
}

template <typename SrcT, typename DestT, typename StorageT>
template <typename ApplyFunT, typename... Args>
void EdgeIndex<SrcT, DestT, StorageT>::ForEachEdge(ApplyFunT &&function,
                                                   Args &... args) {
  using FunctionTy = void (*)(const SrcT &src, const DestT &dest, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  feArgs arguments(oid_, fn, std::tuple<Args...>(args...));
  auto feLambda = [](const feArgs &args) {
    feArgs &fargs = const_cast<feArgs &>(args);
    constexpr auto size = std::tuple_size<
        typename std::decay<decltype(std::get<2>(fargs))>::type>::value;
    ForEachEdgeWrapper(std::get<0>(fargs), std::get<1>(fargs),
                       std::get<2>(fargs), std::make_index_sequence<size>());
  };
  rt::executeOnAll(feLambda, arguments);
}

template <typename SrcT, typename DestT, typename StorageT>
template <typename ApplyFunT, typename... Args>
void EdgeIndex<SrcT, DestT, StorageT>::AsyncForEachEdge(rt::Handle &handle,
                                                        ApplyFunT &&function,
                                                        Args &... args) {
  using FunctionTy = void (*)(rt::Handle & handle, const SrcT &src,
                              const DestT &dest, Args &...);
  FunctionTy fn = std::forward<decltype(function)>(function);
  using feArgs = std::tuple<ObjectID, FunctionTy, std::tuple<Args...>>;
  feArgs arguments(oid_, fn, std::tuple<Args...>(args...));
  auto feLambda = [](rt::Handle &handle, const feArgs &args) {
    feArgs &fargs = const_cast<feArgs &>(args);
    constexpr auto size = std::tuple_size<
        typename std::decay<decltype(std::get<2>(fargs))>::type>::value;
    AsyncForEachEdgeWrapper(handle, std::get<0>(fargs), std::get<1>(fargs),
                            std::get<2>(fargs),
                            std::make_index_sequence<size>());
  };
  rt::asyncExecuteOnAll(handle, feLambda, arguments);
}

template <typename SrcT, typename DestT, typename StorageT>
bool EdgeIndex<SrcT, DestT, StorageT>::GetVertexAttributes(
    const SrcT &src, typename StorageT::SrcAttributesT *attr) {
  size_t targetId = shad::hash<SrcT>{}(src) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    return localIndex_.GetVertexAttributes(src, attr);
  } else {
    auto lookupLambda = [](const LookupArgs &args, LookupResult *res) {
      auto eiPtr = EdgeIndex<SrcT, DestT, StorageT>::GetPtr(args.oid);
      res->found = eiPtr->localIndex_.GetVertexAttributes(args.src, &res->attr);
    };
    LookupArgs args = {oid_, src};
    LookupResult lres;
    rt::executeAtWithRet(targetLocality, lookupLambda, args, &lres);
    if (lres.found) {
      *attr = std::move(lres.attr);
    }
    return lres.found;
  }
  return false;
}

template <typename SrcT, typename DestT, typename StorageT>
template <typename ApplyFunT, typename... Args>
void EdgeIndex<SrcT, DestT, StorageT>::VertexAttributesApply(
    const SrcT &src, ApplyFunT &&function, Args &... args) {
  size_t targetId = shad::hash<SrcT>{}(src) % rt::numLocalities();
  rt::Locality targetLocality(targetId);

  if (targetLocality == rt::thisLocality()) {
    return localIndex_.VertexAttributesApply(src, function, args...);
  } else {
    printf("mmmh.. not local\n");
    using FunctionTy =
        void (*)(const SrcT &, typename StorageT::SrcAttributesT &, Args &...);
    FunctionTy fn = std::forward<decltype(function)>(function);
    using ApplyArgs =
        std::tuple<ObjectID, const SrcT, FunctionTy, std::tuple<Args...>>;
    ApplyArgs arguments(oid_, src, fn, std::tuple<Args...>(args...));
    auto applyLambda = [](const ApplyArgs &args) {
      ApplyArgs &tuple = const_cast<ApplyArgs &>(args);
      constexpr auto Size = std::tuple_size<
          typename std::decay<decltype(std::get<3>(tuple))>::type>::value;
      StorageT *stPtr =
          (IdxT::GetPtr(std::get<0>(tuple))->localIndex_.GetEdgesPtr());
      StorageT::CallVertexAttributesApplyFun(
          stPtr, std::get<1>(tuple), std::get<2>(tuple), std::get<3>(tuple),
          std::make_index_sequence<Size>{});
    };
    rt::executeAt(targetLocality, applyLambda, arguments);
  }
}

}  // namespace shad

#endif  // INCLUDE_SHAD_EXTENSIONS_GRAPH_LIBRARY_EDGE_INDEX_H_
