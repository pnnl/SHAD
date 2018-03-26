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
//===----------------------------------------------------------------------===/

// Simple implementation of triangle counting, through graph pattern matching,
// using CSR graph representation.

#include <atomic>
#include <fstream>
#include <iostream>
#include <utility>

#include "shad/data_structures/array.h"
#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

struct CSRGraph {
  shad::Array<size_t>::ObjectID vertexOID;
  shad::Array<size_t>::ObjectID edgeOID;
  size_t vertexNumber;
  size_t edgeNumber;

  CSRGraph()
      : vertexOID(shad::rt::Locality(), 0),
        edgeOID(shad::rt::Locality(), 0),
        vertexNumber(0),
        edgeNumber(0) {}

  shad::Array<size_t>::ShadArrayPtr vertexPtr() const {
    return shad::Array<size_t>::GetPtr(vertexOID);
  }

  shad::Array<size_t>::ShadArrayPtr edgePtr() const {
    return shad::Array<size_t>::GetPtr(edgeOID);
  }
};

// This is one for each Locality.
static std::atomic<size_t> TriangleCounter(0);

size_t TriangleCount(CSRGraph &G) {
  // Triangle counting loops:
  // 1 - For each vertex in the graph i
  shad::rt::Handle handle;

  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &handle, const CSRGraph &G, size_t i) {
        auto vertexPtr = G.vertexPtr();

        size_t edgeListStart = vertexPtr->At(i);
        size_t edgeListEnd = vertexPtr->At(i + 1);

        // 2 - Visit all the neighboors j of i such that: j < i
        G.edgePtr()->AsyncForEachInRange(
            handle, edgeListStart, edgeListEnd,
            [](shad::rt::Handle &handle, size_t, size_t &j, size_t &i,
               const CSRGraph &G) {
              if (j >= i) return;

              auto vertexPtr = G.vertexPtr();
              size_t edgeListStart = vertexPtr->At(j);
              size_t edgeListEnd = vertexPtr->At(j + 1);

              // 3 - Visit all the neighboors k of j such that k < j
              G.edgePtr()->AsyncForEachInRange(
                  handle, edgeListStart, edgeListEnd,
                  [](shad::rt::Handle &handle, size_t, size_t &k, size_t &j,
                     size_t &i, const CSRGraph &G) {
                    if (k >= j) return;

                    auto vertexPtr = G.vertexPtr();
                    size_t edgeListStart = vertexPtr->At(k);
                    size_t edgeListEnd = vertexPtr->At(k + 1);

                    // 4 - Visit all the neighboors w of k and if w == i
                    //     increment the counter.
                    G.edgePtr()->AsyncForEachInRange(
                        handle, edgeListStart, edgeListEnd,
                        [](shad::rt::Handle &handle, size_t, size_t &w,
                           size_t &i) {
                          if (w != i) return;

                          TriangleCounter++;
                        },
                        i);
                  },
                  j, i, G);
            },
            i, G);
      },
      G, G.vertexNumber);

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

CSRGraph loadGraph(const std::string &vertexFileName,
                   const std::string &edgeFileName) {
  std::ifstream vertexFile, edgeFile;

  vertexFile.open(vertexFileName, std::fstream::binary);
  edgeFile.open(edgeFileName, std::fstream::binary);

  shad::rt::Handle handle;

  CSRGraph G;

  vertexFile.read(reinterpret_cast<char *>(&G.vertexNumber), sizeof(size_t));
  edgeFile.read(reinterpret_cast<char *>(&G.edgeNumber), sizeof(size_t));

  std::cout << "Loading Graph with " << G.vertexNumber << " vertices and "
            << G.edgeNumber << " edges" << std::endl;

  G.vertexOID =
      shad::Array<size_t>::Create(G.vertexNumber + 1, 0)->GetGlobalID();

  for (size_t i = 0; i <= G.vertexNumber; ++i) {
    size_t value;
    vertexFile.read(reinterpret_cast<char *>(&value), sizeof(size_t));

    G.vertexPtr()->AsyncInsertAt(handle, i, value);
  }
  shad::rt::waitForCompletion(handle);

  /// @todo Fix the size of the edge array.
  G.edgeOID = shad::Array<size_t>::Create(G.edgeNumber, 0)->GetGlobalID();
  for (size_t i = 0; i < G.edgeNumber; ++i) {
    size_t value;
    edgeFile.read(reinterpret_cast<char *>(&value), sizeof(size_t));
    G.edgePtr()->AsyncInsertAt(handle, i, value);
  }
  shad::rt::waitForCompletion(handle);

  vertexFile.close();
  edgeFile.close();

  return G;
}

namespace shad {

int main(int argc, char **argv) {
  if (argc != 3) return -1;

  CSRGraph CSR;
  auto loadingTime = shad::measure<std::chrono::seconds>::duration(
      [&]() { CSR = loadGraph(argv[1], argv[2]); });

  std::cout << "Graph loaded in " << loadingTime.count()
            << " seconds\nLet's find some triangles..."
            << std::endl;

  size_t TC = 0;
  auto duration = shad::measure<std::chrono::seconds>::duration(
      [&]() { TC = TriangleCount(CSR); });

  std::cout << "I Found : " << TC << " unique triangles in " << duration.count()
            << " seconds" << std::endl;

  return 0;
}

}  // namespace shad
