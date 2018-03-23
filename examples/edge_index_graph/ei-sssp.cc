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
#include "shad/data_structures/set.h"
#include "shad/extensions/graph_library/algorithms/sssp.h"
#include "shad/extensions/graph_library/edge_index.h"
#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

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
      edges.push_back(destination);
    }
    eiGraph->AsyncInsertEdgeList(handle, i, edges.data(), edges.size());
  }
  shad::rt::waitForCompletion(handle);
  return eiGraph->GetGlobalID();
}

void printHelp(const std::string programName) {
  std::cerr << "Usage: " << programName << " FILENAME SourceID DestinationID"
            << std::endl;
}

namespace shad {

int main(int argc, char **argv) {
  if (argc != 4) {
    printHelp(argv[0]);
    return -1;
  }

  shad::EdgeIndex<size_t, size_t>::ObjectID OID(-1);
  auto loadingTime = shad::measure<std::chrono::microseconds>::duration([&]() {
    // The GraphReader expects an input file in METIS dump format
    std::ifstream inputFile;
    inputFile.open(argv[1], std::ifstream::in);
    OID = GraphReader(inputFile);
  });

  std::cout << "Graph loaded in " << loadingTime.count() / (double)1000000
            << " seconds\n"
               "Let's find some paths..."
            << std::endl;
  auto eiPtr = shad::EdgeIndex<size_t, size_t>::GetPtr(OID);

  size_t num_vertices = eiPtr->Size();
  std::cout << "NumVertices: " << num_vertices
            << " Num Edges: " << eiPtr->NumEdges() << std::endl;

  size_t path_length = 0;
  size_t src = std::stoul(argv[2], nullptr, 0);
  size_t target = std::stoul(argv[3], nullptr, 0);
  auto duration = shad::measure<std::chrono::microseconds>::duration([&]() {
    path_length =
        sssp_length<shad::EdgeIndex<size_t, size_t>, size_t>(OID, src, target);
  });

  if (path_length != std::numeric_limits<size_t>::max()) {
    std::cout << "Found a path between " << src << " and " << target << " in "
              << path_length << " hops in "
              << duration.count() / (double)1000000 << " seconds" << std::endl;
  } else {
    std::cout << "Couldn't find a path between " << src << " and " << target
              << " in " << duration.count() / (double)1000000 << " seconds"
              << std::endl;
  }
  shad::EdgeIndex<size_t, size_t>::Destroy(OID);
  return 0;
}

}  // namespace shad
