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
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <utility>

#include "./ei-vertex_nomination.h"
#include "shad/data_structures/array.h"
#include "shad/data_structures/set.h"
#include "shad/extensions/graph_library/algorithms/sssp.h"
#include "shad/extensions/graph_library/edge_index.h"
#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

std::vector<size_t> seeds;
Score bestLocalScore;
std::mutex localScoreMutex;

Score vertex_nomination(shad::EdgeIndex<size_t, size_t>::ObjectID GID) {
  auto graphPtr = shad::EdgeIndex<size_t, size_t>::GetPtr(GID);
  shad::rt::Handle handle;
  graphPtr->AsyncForEachVertex(
      handle,
      vertex_nomination_step<fusion_mode_t::product, context_sim_t::min_path,
                             content_sim_t::random_content>,
      GID);
  shad::rt::waitForCompletion(handle);
  std::vector<Score> scores(shad::rt::numLocalities());
  for (auto &locality : shad::rt::allLocalities()) {
    shad::rt::asyncExecuteAtWithRet(
        handle, locality,
        [](shad::rt::Handle &, const size_t &, Score *score) {
          *score = bestLocalScore;
        },
        size_t(0), &scores[static_cast<uint32_t>(locality)]);
  }
  shad::rt::waitForCompletion(handle);
  Score bestScore;
  for (auto score : scores) {
    if (score.fusion_score > bestScore.fusion_score) {
      bestScore = score;
    }
  }
  return bestScore;
};

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
  std::cerr << "Usage: " << programName << " --inpath FILENAME [options]\n"
            << "Options:\n"
            << "        --seed SEED (default 123)\n"
            << "        --num_runs NUM_RUNS (default 10)\n"
            << "        --num_seeds NUM_SEEDD (default 5)\n"
            << std::endl;
}

namespace shad {

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cerr << "Missing required argument --inpath FILENAME\n";
    printHelp(argv[0]);
    std::exit(-1);
  }
  // input parameters
  std::string inpath = "";  // required
  size_t seed = 123;
  size_t num_runs = 1;
  size_t num_seeds = 5;

  unsigned argIndex = 1;
  while (argIndex < argc and argv[argIndex][0] == '-') {
    std::string arg(argv[argIndex]);
    if (arg == "--inpath") {
      ++argIndex;
      inpath = std::string(argv[argIndex]);
      ++argIndex;
    } else if (arg == "--seed") {
      ++argIndex;
      seed = std::stoul(argv[argIndex], nullptr, 0);
      ++argIndex;
    } else if (arg == "--num_runs") {
      ++argIndex;
      num_runs = std::stoul(argv[argIndex], nullptr, 0);
      ++argIndex;
    } else if (arg == "--num_seeds") {
      ++argIndex;
      num_seeds = std::stoul(argv[argIndex], nullptr, 0);
      ++argIndex;
    } else {
      std::cerr << "Unknown option [" << arg << "]\n";
      printHelp(argv[0]);
      std::exit(-1);
    }
  }
  if (inpath == "") {
    std::cerr << "Missing required argument --inpath FILENAME\n";
    printHelp(argv[0]);
    std::exit(-1);
  }

  shad::EdgeIndex<size_t, size_t>::ObjectID OID(-1);
  auto loadingTime = shad::measure<std::chrono::seconds>::duration([&]() {
    // The GraphReader expects an input file in METIS dump format
    std::ifstream inputFile;
    inputFile.open(inpath.c_str(), std::ifstream::in);
    OID = GraphReader(inputFile);
  });

  std::cout << "Graph loaded in " << loadingTime.count()
            << " seconds\nLet's nominate some vertices..." << std::endl;
  auto eiPtr = shad::EdgeIndex<size_t, size_t>::GetPtr(OID);

  size_t num_vertices = eiPtr->Size();
  std::cout << "NumVertices: " << num_vertices
            << " Num Edges: " << eiPtr->NumEdges() << std::endl;

  // call srdan on all localities
  auto callSrand = [](const size_t &seed) {
    srand(seed + static_cast<uint32_t>(rt::thisLocality()));
  };
  rt::executeOnAll(callSrand, seed);

  for (size_t i = 0; i < num_runs; ++i) {
    // create seeds
    // using a vector instead of a set to facilitate serialization
    std::vector<size_t> rand_seeds;
    size_t rnd_seed = 0;
    while (rand_seeds.size() < num_seeds) {
      rnd_seed = rand() % num_vertices;
      auto it = std::find(rand_seeds.begin(), rand_seeds.end(), rnd_seed);
      if (it == rand_seeds.end()) {
        rand_seeds.push_back(rnd_seed);
      }
    }
    // send the seeds to all the localities
    std::shared_ptr<uint8_t> seedsBuff(
        reinterpret_cast<uint8_t *>(rand_seeds.data()));
    auto sendSeeds = [](const uint8_t *buff, const uint32_t size) {
      uint32_t num_el = size / sizeof(size_t);
      const size_t *seedsPtr = reinterpret_cast<const size_t *>(buff);
      seeds = std::vector<size_t>(seedsPtr, seedsPtr + num_el);
      bestLocalScore = Score();
    };
    rt::executeOnAll(sendSeeds, seedsBuff, sizeof(size_t) * num_seeds);
    Score bestScore = vertex_nomination(OID);
    std::cout << "Best score: "
              << " node_id = " << bestScore.node_id
              << ", fusion_score = " << bestScore.fusion_score
              << ", content_sim = " << bestScore.content_similarity
              << ", context_sim = " << bestScore.context_similarity
              << std::endl;
  }
  shad::EdgeIndex<size_t, size_t>::Destroy(OID);
  return 0;
}

}  // namespace shad
