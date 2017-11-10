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

#include <atomic>
#include <cmath>
#include <fstream>
#include <iostream>
#include <utility>

#include "shad/data_structures/array.h"
#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

// Phase synchronous implementation of PageRank, using CSR graph representation

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

const float kDamp = 0.85;

shad::rt::Lock errorLock;
static double error(0);

void PageRank(CSRGraph &G, int maxIters, double epsilon) {
  float initialScore = 1.0f / G.vertexNumber;
  auto scores = shad::Array<float>::Create(G.vertexNumber, initialScore);
  auto outgoinContrib = shad::Array<float>::Create(G.vertexNumber + 1, 0);
  auto incomingTotal = shad::Array<float>::Create(G.vertexNumber + 1, 0);

  auto scoresID = scores->GetGlobalID();
  auto outgoingContribID = outgoinContrib->GetGlobalID();
  auto incomingTotalID = incomingTotal->GetGlobalID();

  for (int iteration = 0; iteration < maxIters; ++iteration) {
    shad::rt::Handle handle;

    outgoinContrib->AsyncForEachInRange(
        handle, 0, G.vertexNumber,
        [](shad::rt::Handle &, size_t i, float &value, CSRGraph &g,
           shad::Array<float>::ObjectID &scoresID) {
          auto scores = shad::Array<float>::GetPtr(scoresID);

          auto vertexPtr = g.vertexPtr();
          size_t outDegree = vertexPtr->At(i + 1) - vertexPtr->At(i);

          value = scores->At(i) / outDegree;
        },
        G, scoresID);

    shad::rt::waitForCompletion(handle);

    incomingTotal->AsyncForEachInRange(
        handle, 0, G.vertexNumber,
        [](shad::rt::Handle &, size_t i, float &value, CSRGraph &g,
           shad::Array<float>::ObjectID &outgoingContribID) {
          auto vertexPtr = g.vertexPtr();

          size_t edgeListStart = vertexPtr->At(i);
          size_t edgeListEnd = vertexPtr->At(i + 1);

          auto outgoingContrib = shad::Array<float>::GetPtr(outgoingContribID);
          value = 0;

          for (size_t j = edgeListStart; j < edgeListEnd; ++j) {
            size_t neighbor = g.edgePtr()->At(j);
            value += outgoingContrib->At(neighbor);
          }
        },
        G, outgoingContribID);

    shad::rt::waitForCompletion(handle);

    float baseScore = (1.0f - kDamp) / G.vertexNumber;

    scores->AsyncForEachInRange(
        handle, 0, G.vertexNumber,
        [](shad::rt::Handle &, size_t i, float &score,
           shad::Array<float>::ObjectID &incomingTotalID, float &baseScore) {
          auto incomingTotal = shad::Array<float>::GetPtr(incomingTotalID);
          float oldScore = score;
          score = baseScore + kDamp * incomingTotal->At(i);

          std::lock_guard<shad::rt::Lock> _(errorLock);
          error += fabs(score - oldScore);
        },
        outgoingContribID, baseScore);

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
            << " seconds\n"
               "Let's compute PageRanks..."
            << std::endl;

  auto duration = shad::measure<std::chrono::seconds>::duration(
      [&]() { PageRank(CSR, 20, 1e-4); });

  std::cout << "Completed in " << duration.count() << " seconds" << std::endl;

  return 0;
}

}  // namespace shad
