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

#ifndef INCLUDE_SHAD_EXTENSIONS_HYPERGRAPH_LIBRARY_HYPERGRAPH_H_
#define INCLUDE_SHAD_EXTENSIONS_HYPERGRAPH_LIBRARY_HYPERGRAPH_H_

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <tuple>
#include <utility>
#include <vector>
#include <climits>
#include "shad/extensions/data_types/data_types.h"
#include "shad/extensions/hypergraph_library/local_index.h"
#include "shad/extensions/hypergraph_library/index.h"
#include "shad/extensions/table_library/local_table.h"
#include "shad/data_structures/local_set.h"


namespace shad {

template <class StorageT>
class Hypergraph {
template <typename>
friend class LocalTable;
 public:
  using schema_t = data_types::schema_t;
  using ENC_t = typename StorageT::ENC_type;
  using row_t = ENC_t*;
  Hypergraph() {}
  Hypergraph(StorageT* hedges,
             StorageT* vertices,
             StorageT* he2v,
             StorageT* v2he) :
               hedges_(hedges), vertices_(vertices),
               he2v_(he2v), v2he_(v2he) { }
  void Collapse(shad::LocalSet<std::pair<ENC_t, ENC_t> >&collapse);
  static void S_LineGraph(uint32_t s,
                          shad::LocalIndex<ENC_t, ENC_t> &in_index,
                          shad::LocalIndex<ENC_t, ENC_t> &s_index);
 private:
  StorageT* hedges_;
  StorageT* vertices_;
  StorageT* he2v_;
  StorageT* v2he_;
};

template <class StorageT>
void Hypergraph<StorageT>::Collapse(shad::LocalSet<std::pair<Hypergraph<StorageT>::ENC_t,
                                                             Hypergraph<StorageT>::ENC_t> >&collapse) {
  using it_t = typename std::vector<row_t>::iterator;
  uint64_t num_edges = hedges_->num_rows();
  uint64_t num_vertices = he2v_->num_rows();
  StorageT table(num_edges, num_vertices+num_edges);

  auto nthreads = shad::rt::impl::getConcurrency();
  uint64_t blocksize = __CEILING(num_edges, nthreads);
  
  using args1_t = std::tuple<size_t, //num vertices
                              row_t*, // vertices rows
                              typename std::vector<row_t>::iterator, // edge rows begin
                              typename std::vector<row_t>::iterator, // edge rows end
                              row_t*, // new table rows
                              ENC_t*, // new table data_
                              size_t  // blocksize
                              >;
  args1_t args1(num_edges, hedges_->rows_.data(),
                he2v_->rows_.begin(), he2v_->rows_.end(),
                table.rows_.data(), table.data_.data(), blocksize);
  
  auto fun_1 = [](/*shad::rt::Handle &, */const args1_t& t, size_t block_id) {
    auto num_edges = std::get<0>(t);
    auto vert_rows = std::get<1>(t);
    auto edges_rows_begin = std::get<2>(t);
    auto edges_rows_end = std::get<3>(t);
    auto table_rows = std::get<4>(t);
    auto table_data = std::get<5>(t);
    auto blocksize = std::get<6>(t);
    size_t offset = blocksize * block_id;
    size_t end = __MIN(offset + blocksize, num_edges);
    for (uint64_t i = offset; i < end; ++i) {
      uint64_t v = vert_rows[i][0];
      auto edgeRange = std::equal_range(edges_rows_begin,
                                        edges_rows_end,
                                        & v, StorageT::__row_t_LT);

      std::sort(edgeRange.first, edgeRange.second, StorageT::__dst_LT);
      
      // # of edges of preveious v's plus markers
      uint64_t start = edgeRange.first - edges_rows_begin + i; 
      table_rows[i] = table_data + start;

      for (auto itr = edgeRange.first; itr < edgeRange.second; ++itr) {
        table_data[start ++] = (* itr)[1];
      }
      table_data[start] = (uint64_t) (- (v + 1));
    }
  };
  uint64_t niter = num_edges/blocksize + (num_edges % blocksize != 0);
  shad::rt::forEachOnAll(fun_1, args1, niter);

  table.Sort(StorageT::__list_LT);  
  using args2_t = std::tuple<size_t, //num vertices
                              row_t*, // new table rows
                              size_t,  // blocksize
                              shad::LocalSet<std::pair <uint64_t, uint64_t> >* // collapse
                              >;
  --num_edges;
  blocksize = __CEILING(num_edges, nthreads);
  niter = num_edges/blocksize + (num_edges % blocksize != 0);
  args2_t args2(num_edges, 
                table.rows_.data(),
                blocksize,
                &collapse);
  auto fun_2 = [](shad::rt::Handle &, const args2_t& t, size_t block_id) {
    auto num_edges = std::get<0>(t);
    auto table_rows = std::get<1>(t);
    auto blocksize = std::get<2>(t);
    auto collapse = std::get<3>(t);
    size_t offset = blocksize * block_id;
    size_t end = __MIN(offset + blocksize, num_edges);
    for (uint64_t i = offset; i < end; ++i) {
      uint64_t ndxA = 0, ndxB = 0;
      row_t A = table_rows[i], B = table_rows[i + 1];
      while (true) {
        // A and B are finished, so neighbor lists are equal
        if ( ((int64_t) A[ndxA] < 0) && ((int64_t) B[ndxB] < 0)) {
          uint64_t vA = (uint64_t) (- ((int64_t) A[ndxA] + 1));
          uint64_t vB = (uint64_t) (- ((int64_t) B[ndxB] + 1));
          collapse->Insert(std::make_pair(vA, vB));
          break;
        }
        else if ((int64_t) A[ndxA] < 0) break;      // A has finished; so A != B
        else if ((int64_t) B[ndxB] < 0) break;      // B has finished; so A != B
        else if (A[ndxA] < B[ndxB]) break;
        else if (B[ndxB] < A[ndxA]) break;          
        // A[ndxA] == B[ndxB], so move to next values skipping duplicates
        uint64_t val = A[ndxA];
        while (A[ndxA] == val) ndxA ++;
        while (B[ndxB] == val) ndxB ++;
      }
    }
  };
  shad::rt::Handle h;
  shad::rt::asyncForEachOnAll(h, fun_2, args2, niter);
  shad::rt::waitForCompletion(h);
}

template <class StorageT>
void Hypergraph<StorageT>::S_LineGraph(uint32_t S,
                                       shad::LocalIndex<ENC_t, ENC_t> &in_index,
                                       shad::LocalIndex<ENC_t, ENC_t> &overlaps) {
  for (auto it = in_index.begin(); it != in_index.end(); ++it) {
    auto vneigh = (*it);
    auto v1beg = vneigh.second.begin();
    auto v1end = vneigh.second.end();
    auto v1Id = vneigh.first;
    auto it2 = it;
    ++it2;
    for (; it2 != in_index.end(); ++it2) {
      auto vneigh2 = (*it2);
      std::vector<int> v_intersection;
      std::set_intersection(v1beg, v1end,
                            vneigh2.second.begin(), vneigh2.second.end(),
                            std::back_inserter(v_intersection));
      if (v_intersection.size() >= S) {
        overlaps.Insert(v1Id, vneigh2.first);
        overlaps.Insert(vneigh2.first, v1Id);
      }
    }
  }
}

}  // namespace shad

#endif  // INCLUDE_SHAD_EXTENSIONS_HYPERGRAPH_LIBRARY_HYPERGRAPH_H_

