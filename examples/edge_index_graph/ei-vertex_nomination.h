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
#include <mutex>
#include <sstream>
#include <utility>

#include "shad/data_structures/array.h"
#include "shad/data_structures/set.h"
#include "shad/extensions/graph_library/algorithms/sssp.h"
#include "shad/extensions/graph_library/edge_index.h"
#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"

struct Score {
  size_t node_id;
  float fusion_score;
  float context_similarity;
  float content_similarity;
  Score()
      : node_id(std::numeric_limits<size_t>::infinity()),
        fusion_score(-std::numeric_limits<size_t>::infinity()),
        context_similarity(std::numeric_limits<size_t>::infinity()),
        content_similarity(std::numeric_limits<size_t>::infinity()) {}
};

extern std::vector<size_t> seeds;
extern Score bestLocalScore;
extern std::mutex localScoreMutex;

enum fusion_mode_t { context, content, product, sum };

template <fusion_mode_t F>
float getFusionScore(float context, float content);

template <>
float getFusionScore<fusion_mode_t::context>(float context, float content) {
  return context;
}

template <>
float getFusionScore<fusion_mode_t::content>(float context, float content) {
  return content;
}

template <>
float getFusionScore<fusion_mode_t::product>(float context, float content) {
  return context * content;
}

template <>
float getFusionScore<fusion_mode_t::sum>(float context, float content) {
  return context + content;
}

enum content_sim_t { random_content };

template <content_sim_t C>
float getContentSimilarity(shad::EdgeIndex<size_t, size_t>::ObjectID GID,
                           size_t node_id);

template <>
float getContentSimilarity<content_sim_t::random_content>(
    shad::EdgeIndex<size_t, size_t>::ObjectID, size_t) {
  return static_cast<float>(rand());
}

enum context_sim_t { min_path };

template <context_sim_t C>
float getContextSimilarity(shad::EdgeIndex<size_t, size_t>::ObjectID GID,
                           size_t node_id);

template <>
float getContextSimilarity<context_sim_t::min_path>(
    shad::EdgeIndex<size_t, size_t>::ObjectID GID, size_t node_id) {
  size_t min_path = std::numeric_limits<size_t>::infinity();
  size_t path_length = min_path;
  for (auto seed : seeds) {
    if (node_id == seed) continue;
    // sssp from node_id to seed for better locality
    path_length = sssp_length<shad::EdgeIndex<size_t, size_t>, size_t>(
        GID, node_id, seed);
    min_path = std::min<size_t>(min_path, path_length);
  }

  return 1.0 / static_cast<float>(min_path);
}

template <fusion_mode_t F, context_sim_t CX, content_sim_t CN>
void vertex_nomination_step(shad::rt::Handle&, const size_t& src,
                            shad::EdgeIndex<size_t, size_t>::ObjectID& GID) {
  auto context_sim = getContextSimilarity<CX>(GID, src);
  auto content_sim = getContentSimilarity<CN>(GID, src);
  auto fusion_score = getFusionScore<F>(context_sim, content_sim);

  if (fusion_score > bestLocalScore.fusion_score) {
    std::lock_guard<std::mutex> lock(localScoreMutex);
    // check again
    if (fusion_score > bestLocalScore.fusion_score) {
      bestLocalScore.node_id = src;
      bestLocalScore.fusion_score = fusion_score;
      bestLocalScore.content_similarity = content_sim;
      bestLocalScore.context_similarity = context_sim;
    }
  }

}  // namespace shad
