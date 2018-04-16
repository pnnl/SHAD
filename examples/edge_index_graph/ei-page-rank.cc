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

#include <atomic>
#include <cmath>
#include <fstream>
#include <iostream>
#include <sstream>
#include <utility>

#include "shad/data_structures/array.h"
#include "shad/extensions/graph_library/edge_index.h"
#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

// Phase synchronous implementation of PageRank

const float kDamp = 0.85;

shad::rt::Lock errorLock;
static double error(0);

void PageRank(shad::EdgeIndex<uint64_t, uint64_t>::ObjectID &G, int maxIters,
              double epsilon) {
  auto GraphPtr = shad::EdgeIndex<uint64_t, uint64_t>::GetPtr(G);

  float initialScore = 1.0f / GraphPtr->Size();
  auto scores = shad::Array<float>::Create(GraphPtr->Size(), initialScore);
  auto outgoinContrib = shad::Array<float>::Create(GraphPtr->Size(), 0);
  auto incomingTotal = shad::Array<float>::Create(GraphPtr->Size(), 0);

  auto scoresID = scores->GetGlobalID();
  auto outgoingContribID = outgoinContrib->GetGlobalID();
  auto incomingTotalID = incomingTotal->GetGlobalID();

  for (int iteration = 0; iteration < maxIters; ++iteration) {
    shad::rt::Handle handle;

    GraphPtr->AsyncForEachVertex(
        handle,
        [](shad::rt::Handle &handle, const uint64_t &i,
           shad::EdgeIndex<uint64_t, uint64_t>::ObjectID &eoid,
           shad::Array<float>::ObjectID &oid1,
           shad::Array<float>::ObjectID &oid2,
           shad::Array<float>::ObjectID &oid3) {
          auto GraphPtr = shad::EdgeIndex<uint64_t, uint64_t>::GetPtr(eoid);

          auto scores = shad::Array<float>::GetPtr(oid1);
          auto outgoingContrib = shad::Array<float>::GetPtr(oid2);
          auto incomingTotal = shad::Array<float>::GetPtr(oid3);

          size_t outDegree = GraphPtr->GetDegree(i);

          outgoingContrib->InsertAt(i, scores->At(i) / outDegree);

          incomingTotal->InsertAt(i, 0);
          GraphPtr->ForEachNeighbor(
              i,
              [](const uint64_t &src, const uint64_t &dst,
                 shad::Array<float>::ObjectID &oid1,
                 shad::Array<float>::ObjectID &oid2) {
                auto outgoingContrib = shad::Array<float>::GetPtr(oid1);
                auto incomingTotal = shad::Array<float>::GetPtr(oid2);

                size_t value = outgoingContrib->At(dst);
                value += incomingTotal->At(src);
                incomingTotal->InsertAt(src, value);
              },
              oid2, oid3);

          float baseScore = (1.0f - kDamp) / GraphPtr->Size();

          float oldScore = scores->At(i);
          float score = baseScore + kDamp * incomingTotal->At(i);
          scores->InsertAt(i, score);

          std::lock_guard<shad::rt::Lock> _(errorLock);
          error += fabs(score - oldScore);
        },
        G, scoresID, outgoingContribID, incomingTotalID);

    shad::rt::waitForCompletion(handle);

    // This performs a reduction into a single counter.
    std::vector<double> reducer(shad::rt::numLocalities());

    for (auto &locality : shad::rt::allLocalities()) {
      shad::rt::asyncExecuteAtWithRet(
          handle, locality,
          [](shad::rt::Handle &, const size_t &, double *value) {
            *value = error;
          },
          size_t(0), &reducer[static_cast<uint32_t>(locality)]);
    }
    shad::rt::waitForCompletion(handle);

    for (double i : reducer) error += i;

    if (error < epsilon) break;

    shad::rt::asyncExecuteOnAll(
        handle, [](shad::rt::Handle &, const size_t &) { error = 0; },
        size_t(0));

    shad::rt::waitForCompletion(handle);
  }
}

// The GraphReader expects an input file in METIS dump format
shad::EdgeIndex<uint64_t, uint64_t>::ObjectID GraphReader(std::ifstream &GFS) {
  std::string line;
  unsigned long EdgeNumber, VertexNumber;

  std::getline(GFS, line);

  std::istringstream headlineStream(line);

  headlineStream >> VertexNumber >> EdgeNumber;
  EdgeNumber <<= 1;

  auto eiGraph = shad::EdgeIndex<uint64_t, uint64_t>::Create(VertexNumber);
  shad::rt::Handle handle;

  for (size_t i = 0L; i < VertexNumber; i++) {
    size_t destination;

    std::getline(GFS, line);
    std::istringstream lineStream(line);
    std::vector<uint64_t> edges;
    while (!lineStream.eof()) {
      lineStream >> destination;
      edges.push_back(destination - 1);
    }
    eiGraph->AsyncInsertEdgeList(handle, i, edges.data(), edges.size());
  }
  shad::rt::waitForCompletion(handle);
  return eiGraph->GetGlobalID();
}

namespace shad {

int main(int argc, char **argv) {
  if (argc != 2) return -1;

  shad::EdgeIndex<uint64_t, uint64_t>::ObjectID OID(-1);
  auto loadingTime = shad::measure<std::chrono::seconds>::duration([&]() {
    std::ifstream inputFile;
    // The GraphReader expects an input file in METIS dump format
    inputFile.open(argv[1], std::ifstream::in);
    OID = GraphReader(inputFile);
  });

  std::cout << "Graph loaded in " << loadingTime.count()
            << " seconds\nLet's rank some pages..." << std::endl;
  auto eiPtr = shad::EdgeIndex<uint64_t, uint64_t>::GetPtr(OID);
  std::cout << "NumVertices: " << eiPtr->Size()
            << " Num Edges: " << eiPtr->NumEdges() << std::endl;

  auto duration = shad::measure<std::chrono::seconds>::duration(
      [&]() { PageRank(OID, 20, 1e-4); });

  std::cout << "Computed PageRank in " << duration.count() << " seconds"
            << std::endl;

  return 0;
}

}  // namespace shad
