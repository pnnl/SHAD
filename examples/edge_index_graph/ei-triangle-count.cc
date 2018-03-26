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

// Simple implementation of triangle counting, through graph pattern matching

#include <atomic>
#include <fstream>
#include <iostream>
#include <sstream>
#include <utility>

#include "shad/data_structures/array.h"
#include "shad/extensions/graph_library/edge_index.h"
#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

// This is one for each Locality.
static std::atomic<size_t> TriangleCounter(0);

size_t TriangleCount(shad::EdgeIndex<size_t, size_t>::ObjectID &eid) {
  using ELObjectID = shad::EdgeIndex<size_t, size_t>::ObjectID;
  using SrcT = shad::EdgeIndex<size_t, size_t>::SrcType;
  using DestT = shad::EdgeIndex<size_t, size_t>::DestType;

  // Triangle counting loops:
  // 1 - For each vertex in the graph i
  shad::rt::Handle handle;
  auto elPtr = shad::EdgeIndex<size_t, size_t>::GetPtr(eid);
  elPtr->AsyncForEachEdge(
      handle,
      [](shad::rt::Handle &handle, const SrcT &i, const DestT &j,
         ELObjectID &eid) {
        auto GraphPtr = shad::EdgeIndex<size_t, size_t>::GetPtr(eid);
        // 3 - Visit all the neighbors k of j such that k < j
        GraphPtr->AsyncForEachNeighbor(
            handle, j,
            [](shad::rt::Handle &handle, const SrcT &j, const DestT &k,
               ELObjectID &eid, const SrcT &v) {
              auto GraphPtr = shad::EdgeIndex<size_t, size_t>::GetPtr(eid);

              // 4 - Visit all the neighbors w of k and if w == i
              //     increment the counter.
              GraphPtr->AsyncForEachNeighbor(
                  handle, v,
                  [](shad::rt::Handle &handle, const SrcT &i, const DestT &w,
                     const SrcT &k) {
                    if (w != k) return;
                    TriangleCounter++;
                  },
                  k);
            },
            eid, i);
      },
      eid);
  shad::rt::waitForCompletion(handle);

  // This performs a reduction into a single counter.
  std::vector<size_t> reducer(shad::rt::numLocalities());

  for (auto &locality : shad::rt::allLocalities()) {
    shad::rt::asyncExecuteAtWithRet(
        handle, locality,
        [](shad::rt::Handle &, const size_t &, size_t *value) {
          *value = TriangleCounter.load();
        },
        size_t(0), &reducer[static_cast<uint32_t>(locality)]);
  }
  shad::rt::waitForCompletion(handle);

  size_t counter = 0;
  for (size_t i : reducer) counter += i;

  return counter;
}

// The GraphReader expects an input file in METIS dump format
shad::EdgeIndex<size_t, size_t>::ObjectID GraphReader(std::ifstream &GFS) {
  std::string line;
  unsigned long EdgeNumber, VertexNumber;

  std::getline(GFS, line);

  std::istringstream headlineStream(line);

  headlineStream >> VertexNumber >> EdgeNumber;
  EdgeNumber <<= 1;

  auto eiGraph = shad::EdgeIndex<size_t, size_t>::Create(VertexNumber);
  shad::rt::Handle handle;

  for (size_t i = 0L; i < VertexNumber; i++) {
    size_t destination;

    std::getline(GFS, line);
    std::istringstream lineStream(line);
    std::vector<size_t> edges;
    while (!lineStream.eof()) {
      lineStream >> destination;
      destination--;
      if (destination >= i) continue;
      edges.push_back(destination);
    }
    eiGraph->AsyncInsertEdgeList(handle, i, edges.data(), edges.size());
  }
  shad::rt::waitForCompletion(handle);
  return eiGraph->GetGlobalID();
}

namespace shad {

int main(int argc, char **argv) {
  if (argc != 2) return -1;

  shad::EdgeIndex<size_t, size_t>::ObjectID OID(-1);
  auto loadingTime = shad::measure<std::chrono::seconds>::duration([&]() {
    // The GraphReader expects an input file in METIS dump format
    std::ifstream inputFile;
    inputFile.open(argv[1], std::ifstream::in);
    OID = GraphReader(inputFile);
  });

  std::cout << "Graph loaded in " << loadingTime.count()
            << " seconds\nLet's find some triangles..."
            << std::endl;
  auto eiPtr = shad::EdgeIndex<size_t, size_t>::GetPtr(OID);
  std::cout << "NumVertices: " << eiPtr->Size()
            << " Num Edges: " << eiPtr->NumEdges() << std::endl;
  size_t TC = 0;
  auto duration = shad::measure<std::chrono::seconds>::duration(
      [&]() { TC = TriangleCount(OID); });

  std::cout << "I Found : " << TC << " unique triangles in "
            << duration.count() << " seconds" << std::endl;
  shad::EdgeIndex<size_t, size_t>::Destroy(OID);
  return 0;
}

}  // namespace shad
