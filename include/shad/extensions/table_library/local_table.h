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

#ifndef INCLUDE_SHAD_EXTENSIONS_TABLE_LIBRARY_LOCAL_TABLE_H_
#define INCLUDE_SHAD_EXTENSIONS_TABLE_LIBRARY_LOCAL_TABLE_H_

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <tuple>
#include <utility>
#include <vector>
#include <climits>
#include <omp.h>
#include "shad/extensions/data_types/data_types.h"
#include "shad/extensions/graph_library/local_edge_index.h"
#include "shad/extensions/hypergraph_library/local_index.h"
#include "shad/extensions/hypergraph_library/index.h"
#include "shad/data_structures/local_set.h"


#define __MIN(a,b) (((a)<(b))?(a):(b))
#define __MAX(a,b) (((a)<(b))?(b):(a))
#define __CEILING(x,y) (((x) + (y) - 1) / (y))

namespace shad {

template <typename ENC_t>
class LocalTable {
 public:
  using schema_t = data_types::schema_t;
  using row_t = ENC_t*;
  LocalTable() {}
  LocalTable(const LocalTable<ENC_t>&rhs) : 
                 num_cols_(rhs.num_cols_),
                 num_rows_(rhs.num_rows_),
                 schema_(rhs.schema_),
                 rows_(rhs.rows_),
                 data_(rhs.data_) {
        std::cout << "rhs num rows: " << rhs.num_rows_ <<std::endl;
        std::cout << "rhs num rows: " << rhs.num_rows_ <<std::endl;
  }
  LocalTable(size_t num_rows, schema_t schema) 
               : num_cols_(schema.size()),
                 num_rows_(num_rows),
                 schema_(schema),
                 rows_(num_rows),
                 data_(num_rows*schema.size()) {}
  LocalTable(size_t num_rows, size_t data_size) 
               : num_cols_(1),
                 num_rows_(num_rows),
                 schema_(),
                 rows_(num_rows),
                 data_(data_size) {}
//   LocalTable(size_t num_rows,
//              size_t num_cols,
//              schema_t schema) 
//                : num_cols_(num_cols_),
//                  num_rows_(num_rows),
//                  schema_(schema),
//                  rows_(num_rows_),
//                  data_(num_rows_*num_cols_) {}


 // Creates and populate a table from file                
  LocalTable(std::string filename, schema_t & schema)
            : num_cols_(schema.size()),
              schema_(schema) {
    std::string line;
    std::ifstream file(filename);
    std::vector <std::string> records;

    if (! file.is_open()) {
      printf("Cannot open file %s\n", filename.c_str());
      exit(-1); 
    }
    while (getline(file, line)) {
      if (line[0] != '#') {
        records.push_back(line);     // read file
      }
    }
    file.close();

    num_rows_ = records.size();
    rows_ = std::vector<row_t>(num_rows_);
    data_ = std::vector<ENC_t>(num_rows_*num_cols_);
    
    auto nthreads = shad::rt::impl::getConcurrency();
    std::cout << "concurrency: " << nthreads << std::endl;
    uint64_t blocksize = __CEILING(num_rows_, nthreads);
    using tuple_v2_t = std::tuple<row_t*, // rows
                                  ENC_t*, // data
                                  size_t, // numrows
                                  size_t, //blcoksize
                                  size_t, // numcols
                                  schema_t, // schema
                                  std::string*
                                  >;
    auto myfun_v2 = [](/*shad::rt::Handle &, */const tuple_v2_t& t, size_t block_id) {
      auto rowsptr = std::get<0>(t);
      auto dataptr = std::get<1>(t);
      auto numrows = std::get<2>(t);
      auto blocksize = std::get<3>(t);
      auto num_cols = std::get<4>(t);
      auto schema = std::get<5>(t);
      auto recordsptr = std::get<6>(t);
      size_t offset = blocksize * block_id;
      dataptr += offset * num_cols;
      size_t end = __MIN(offset + blocksize, numrows);
      for (uint64_t it = offset; it < end; ++it) {
        rowsptr[it] = dataptr;
        std::string::iterator start = recordsptr[it].begin();
        std::string::iterator endit = recordsptr[it].end();
        for (uint64_t j = 0; j < num_cols; ++j, ++dataptr) {
          std::string::iterator end = std::find(start, endit, ',');
          std::string field = std::string(start, end);

          *dataptr = shad::data_types::encode<uint64_t, std::string>(field, schema[j].second);
          start = end + 1;
        }
      }
    };
    tuple_v2_t argstuple(rows_.data(), data_.data(),
                         num_rows_, blocksize, num_cols_, schema_, records.data());
    uint64_t niter = num_rows_/blocksize + (num_rows_ % blocksize != 0);
    shad::rt::forEachOnAll(myfun_v2, argstuple, niter);
  using tuple2_t = std::tuple<std::string*,
                              row_t*,
                              schema_t,
                              size_t>;
  tuple2_t argstuple2(records.data(), rows_.data(), schema, num_cols_);
  auto myfun2 = [](/*shad::rt::Handle &, */const tuple2_t &t, size_t i) {
    std::string* recordsptr = std::get<0>(t) + i;
    auto num_cols = std::get<3>(t);
    row_t* rowsptr = std::get<1>(t);
    auto schema = std::get<2>(t);
    std::string::iterator start = recordsptr->begin();
    for (uint64_t j = 0; j < num_cols; j ++) {
      std::string::iterator end = std::find(start, recordsptr->end(), ',');
      std::string field = std::string(start, end);
      (*(rowsptr+i))[j] = shad::data_types::encode<uint64_t, std::string>(field, schema[j].second);
      start = end + 1;
    }
    };
    shad::rt::forEachOnAll(myfun2, argstuple2, num_rows_);
    Sort(__row_t_LT); //shad
  }
                 
  ENC_t* operator[](const size_t pos) {
    return &data_[0] + pos*num_cols_;
  }

template <typename comp>
void Sort(comp cmp) { // shad
  auto nthreads = shad::rt::impl::getConcurrency();
//   std::cout << "concurrency: " << nthreads << std::endl;
  uint64_t blocksize = __CEILING(num_rows_, nthreads);
  using sort_args = std::tuple<
                      size_t, // num_rows
                      size_t, // blocksize
                      typename std::vector<row_t>::iterator, // rows
                      comp
                    >;
  sort_args args(num_rows_, blocksize, rows_.begin(), cmp);
  auto sortfun = [](/*shad::rt::Handle &, */const sort_args &args, size_t i) {
    auto num_rows = std::get<0>(args);
    auto blocksize = std::get<1>(args);
    auto rows_beg = std::get<2>(args);
    auto cmp = std::get<3>(args);
    uint64_t start = blocksize * i;
    uint64_t end = __MIN(start + blocksize, num_rows);
//     std::cout << "chuncksize[" << i << "] = " << end - start << std::endl;
    auto data_start = rows_beg + start;
    auto data_end = rows_beg + end;
    std::sort(data_start, data_end, cmp);
  };

  shad::rt::Handle h;
  uint64_t niter = num_rows_/blocksize + (num_rows_ % blocksize != 0);
    shad::rt::forEachOnAll( sortfun, args, niter);
  std::vector<row_t> outdata(num_rows_);

  for (uint64_t size = blocksize << 1; size < (num_rows_ << 1); size <<= 1) {

    for (uint64_t i = 0; i < __CEILING(num_rows_, size); i ++)
        shad_merge_blocks<comp>(i, size, outdata, cmp);

    std::copy(outdata.begin(), outdata.end(), rows_.begin());
} 
//     std::cout << "done sorting " << std::endl;
  }

  template <typename comp>
  void ompSort(comp cmp) {
    uint64_t blocksize = __CEILING(num_rows_, omp_get_max_threads());
  // #pragma omp parallel for
    for (uint64_t start = 0; start < num_rows_; start += blocksize) {
      uint64_t end = __MIN(start + blocksize, num_rows_);

      auto data_start = rows_.begin() + start;
      auto data_end = rows_.begin() + end;
      std::sort(data_start, data_end, cmp);
    }

    std::vector<row_t> outdata(num_rows_);

    for (uint64_t size = blocksize << 1; size < (num_rows_ << 1); size <<= 1) {

      for (uint64_t i = 0; i < __CEILING(num_rows_, size); i ++)
          merge_blocks<comp>(i, size, outdata, cmp);

      std::copy(outdata.begin(), outdata.end(), rows_.begin());
    } 
  }
  
  size_t num_rows() {
    return num_rows_;
  }
  
  void print_row(size_t n) {
    for (size_t i = 0; i < num_cols_; ++i) {
      std::cout << rows_[n][i] << ", ";
    }
    std::cout << std::endl;
  }
  void print(size_t n) {
    std::cout << std::endl;
    for (size_t i = 0; i < n; ++i) {
      std::cout << "[" << i << "]" << std::endl;
      print_row(i);
    }
    std::cout << std::endl;
  }

  uint64_t findRecord(uint64_t key) {
    using it_t = typename std::vector<row_t>::iterator;
    it_t it;
    auto first = rows_.begin();
    auto last = rows_.end();
    typename std::iterator_traits<it_t>::difference_type count, step;

    count = std::distance(first, last);

    while (count > 0) {
      it = first;
      step = count / 2;
      std::advance (it, step);
      if ((* it)[0] < key) {first = ++it; count -= step + 1;} else count = step;
    }

    return (uint64_t) (first - rows_.begin());
  }

  LocalTable(std::vector<uint64_t>& columnsToCheck,
             std::vector <uint64_t>& columnsToMove,
             schema_t& schema, LocalTable<ENC_t>& rhs, bool discard_duplicates = false):
                 num_cols_(schema.size()),
                 num_rows_(rhs.num_rows_*columnsToCheck.size()),
                 schema_(schema),
                 rows_(rhs.num_rows_*columnsToCheck.size()),
                 data_(rhs.num_rows_*columnsToCheck.size()*schema.size()) {
    
    uint64_t colMove = columnsToMove.size();
    uint64_t colCheck = columnsToCheck.size();
    auto dataptr = data_.data();
// #pragma omp parallel for
    for (uint64_t i = 0; i < num_rows_; i ++)
      rows_[i] = dataptr + i * num_cols_;
    
//   #pragma omp parallel for
    for (uint64_t i = 0; i < rhs.num_rows_; i ++) {     // for each row in table
        uint64_t row_out = i * colCheck;
    for (uint64_t j = 0; j < colCheck;    j ++) {     // create a new row for each checked column
        rows_[row_out + j][0] = rhs.rows_[i][ columnsToCheck[j] ];     // key

        for (uint64_t k = 0; k < colMove; k ++)
            rows_[row_out + j][k + 1] = rhs.rows_[i][ columnsToMove[k] ];
    } }

    Sort(__row_t_LT);  //shad
    
    if (discard_duplicates) {
    std::map <uint64_t, uint64_t> saved;
    uint64_t blocksize = __CEILING(num_rows_, shad::rt::impl::getConcurrency()); //omp_get_max_threads());

  // Save first key of next segment as it may be changed when it is read
    for (uint64_t start = 0; start < num_rows_; start += blocksize) {
      uint64_t end = __MIN(start + blocksize, num_rows_ - 1);     // for last segment, save last element
      saved[start] = rows_[end][0];
    }

//   #pragma omp parallel for
    for (uint64_t start = 0; start < num_rows_; start += blocksize) {
      uint64_t end = __MIN(start + blocksize, num_rows_);

      // do all elements in segment except last
      for (uint64_t j = 0; j < end - 1; j ++)
        if (rows_[j][0] == rows_[j + 1][0]) rows_[j][0] = ULLONG_MAX;

      // compare last element to saved element
      if (rows_[end - 1][0] == saved[start]) rows_[end - 1][0] = ULLONG_MAX;
    }

    Sort(__row_t_LT);  //shad
    num_rows_ = findRecord(ULLONG_MAX);
      }
    return;
  }


  struct row_t_LT {
    constexpr bool operator() (const row_t & A, const row_t & B) const {
      std::cout << "A[0] = " << A[0] << std::endl;
      return A[0] < B[0];
    }
  };
  
//   std::vector<std::vector<uint64_t>> S_Overlaps(LocalTable<ENC_t>& hedges,
//                                                 LocalTable<ENC_t>& edges,
//                                                 size_t vertex_column,
//                                                 size_t S) {
//     auto num_hedges = hedges.num_rows();
//     std::vector<std::mutex> mutexes(num_hedges);
//     std::vector<std::vector<uint64_t>> overlaps(num_hedges);
//     size_t 
//     for (size_t hedge_id = 0; hedge_id < num_hedges; ++hedge_id) {
//       auto edgeRange = std::equal_range(hedges.rows_.begin(),
//                                         hedges.rows_.end(), & v, __row_t_LT);
//     }
//   }
  
  static void ompCollapse(LocalTable<ENC_t> &vertices, LocalTable<ENC_t>& edges) {
    using it_t = typename std::vector<row_t>::iterator;
    uint64_t num_edges = edges.num_rows();
    uint64_t num_vertices = vertices.num_rows();

    LocalTable<ENC_t> table(num_vertices, num_edges+num_vertices);

//   #pragma omp parallel for schedule(dynamic, 4096)
    for (uint64_t i = 0; i < num_vertices; i ++) {
      uint64_t v = vertices.rows_[i][0];
      auto edgeRange = std::equal_range(edges.rows_.begin(),
                                        edges.rows_.end(), & v, __row_t_LT);

      std::sort(edgeRange.first, edgeRange.second, __dst_LT);

      uint64_t start = edgeRange.first - edges.rows_.begin() + i; // # of edges of preveious v's plus markers
      table.rows_[i] = table.data_.data() + start;

      for (auto itr = edgeRange.first; itr < edgeRange.second; itr ++) {
        table.data_[start ++] = (* itr)[1];
      }
      table.data_[start] = (uint64_t) (- (v + 1));;
    }

    table.ompSort(__list_LT);
    shad::LocalSet<std::pair <uint64_t, uint64_t> > collapse(num_vertices/16);

    
//   #pragma omp parallel for schedule(dynamic, 4096)
    for (uint64_t i = 0; i < num_vertices - 1; i ++) {
      uint64_t ndxA = 0, ndxB = 0;
      row_t A = table.rows_[i], B = table.rows_[i + 1];

      while (true) {
        // A and B are finished, so neighbor lists are equal
        if ( ((int64_t) A[ndxA] < 0) && ((int64_t) B[ndxB] < 0)) {
          uint64_t vA = (uint64_t) (- ((int64_t) A[ndxA] + 1));
          uint64_t vB = (uint64_t) (- ((int64_t) B[ndxB] + 1));
//           #pragma omp critical
//               collapse.push_back(std::make_pair(vA, vB));
          collapse.Insert(std::make_pair(vA, vB));
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
    } }

    printf("   Number of collapsed items = %lu\n", collapse.Size());

  }
  

  static bool __dst_LT(const row_t & A, const row_t & B) { return A[1] < B[1]; }
  static bool __row_t_LT(const row_t & A, const row_t & B) { return A[0] < B[0]; }
  static bool __list_LT(const row_t & A, const row_t & B) {
  uint64_t ndxA = 0, ndxB = 0;

  while (true) {

    // A has finished; A < B, if B has not finished; else A == B, so false
    if      ((int64_t) A[ndxA] < 0) return ((int64_t) B[ndxB] >= 0);

    // B has finished; so A > B
    else if ((int64_t) B[ndxB] < 0) return false;

    else if (A[ndxA] < B[ndxB]) return true;
    else if (B[ndxB] < A[ndxA]) return false;
      
    // A[ndxA] == B[ndxB], so move to next values skipping duplicates
    uint64_t val = A[ndxA];
    while (A[ndxA] == val) ndxA ++;
    while (B[ndxB] == val) ndxB ++;
} }
//  private:
  size_t num_cols_, num_rows_;
  std::vector<row_t> rows_;
  std::vector<ENC_t> data_;
  schema_t schema_;


  template <typename comp>
  void corank_sorted(const uint64_t index, uint64_t * corank,
                   const uint64_t leftoffset,  const uint64_t leftsize,
                   const uint64_t rightoffset, const uint64_t rightsize, comp cmp) {
    uint64_t delta;
    uint64_t j    = __MIN(index, leftsize);
    uint64_t jlow = (index <= rightsize) ? 0 : index - rightsize;
    uint64_t k    = index - j;
    uint64_t klow = ULLONG_MAX;
    for ( ; ;) {
      if (j > 0 && k < rightsize) {
        if (cmp(& rows_[rightoffset + k][0], & rows_[leftoffset + j - 1][0])) {
          delta = __CEILING(j - jlow, 2);
          klow = k;
          j   -= delta;
          k   += delta;
          continue;
        }
      }

      if (k > 0 && j < leftsize) {
        if (! cmp(& rows_[rightoffset + k - 1][0], & rows_[leftoffset + j][0])) {
          delta = __CEILING(k - klow, 2);
          jlow  = j;
          j    += delta;
          k    -= delta;
          continue;
        }
      }
      break;
    }
    corank[0] = j;
    corank[1] = k;
  }

  template <typename comp>
  static void shad_corank_sorted(const uint64_t index, uint64_t * corank,
                          const uint64_t leftoffset,  const uint64_t leftsize,
                          const uint64_t rightoffset, const uint64_t rightsize,
                          comp cmp, row_t* rowptr) {
    uint64_t delta;
    uint64_t j    = __MIN(index, leftsize);
    uint64_t jlow = (index <= rightsize) ? 0 : index - rightsize;
    uint64_t k    = index - j;
    uint64_t klow = ULLONG_MAX;
    for ( ; ;) {
      if (j > 0 && k < rightsize) {
//         if (cmp(*(rowptr + rightoffset + k), *(rowptr + leftoffset + j - 1))) {
        if (cmp(& rowptr[rightoffset + k][0], & rowptr[leftoffset + j - 1][0])) {
          delta = __CEILING(j - jlow, 2);
          klow = k;
          j   -= delta;
          k   += delta;
          continue;
        }
      }

      if (k > 0 && j < leftsize) {
//         if (! cmp(rowptr + rightoffset + k - 1, rowptr + leftoffset + j)) {
        if (! cmp(& rowptr[rightoffset + k - 1][0], & rowptr[leftoffset + j][0])) {
          delta = __CEILING(k - klow, 2);
          jlow  = j;
          j    += delta;
          k    -= delta;
          continue;
        }
      }
      break;
    }
    corank[0] = j;
    corank[1] = k;
  }
  template <typename comp>
  void merge_nested_blocks(uint64_t it, uint64_t num_blocks,
                         uint64_t start, uint64_t mid,
                         uint64_t end,
                         std::vector<row_t> & outdata, comp cmp) {
    uint64_t i[2], lower[2], upper[2];
    typename std::vector<row_t>::iterator data_start, data_end, outdata_start, outdata_mid, outdata_end;

    i[0] = it       * (end - start) / num_blocks;
    i[1] = (it + 1) * (end - start) / num_blocks;
    corank_sorted<comp>(i[0], lower, start, mid - start, mid, end - mid, cmp);
    corank_sorted<comp>(i[1], upper, start, mid - start, mid, end - mid, cmp);

    uint64_t leftsize  = upper[0] - lower[0];
    uint64_t rightsize = upper[1] - lower[1];

    if (rightsize == 0) {
      data_start = rows_.begin() + start + lower[0];
      data_end = data_start + leftsize;

      outdata_start = outdata.begin() + start + i[0];
      std::copy(data_start, data_end, outdata_start);

    } else if (leftsize == 0) {
      data_start = rows_.begin() + mid + lower[1];
      data_end = data_start + rightsize;

      outdata_start = outdata.begin() + start + i[0];
      std::copy(data_start, data_end, outdata_start);

    } else {

      data_start = rows_.begin() + start + lower[0];
      data_end = data_start + leftsize;

      outdata_start = outdata.begin() + start + i[0];
      std::copy(data_start, data_end, outdata_start);

      data_start = rows_.begin() + mid + lower[1];
      data_end = data_start + rightsize;

      outdata_start += leftsize;
      std::copy(data_start, data_end, outdata_start);

      outdata_start = outdata.begin() + start + i[0];
      outdata_mid = outdata_start + leftsize;
      outdata_end = outdata_mid + rightsize;
      std::inplace_merge(outdata_start, outdata_mid, outdata_end, cmp);
    }
  }
  
  template <typename comp>
  void shad_merge_blocks(uint64_t it, uint64_t mergesize,
                    std::vector<row_t> & outdata, comp cmp) {
    uint64_t start = it * mergesize;
    uint64_t mid = start + (mergesize >> 1);
    uint64_t end = __MIN(start + mergesize, num_rows_);

    if (end <= mid) {
      auto data_start = rows_.begin() + start;
      auto data_end = rows_.begin() + end;

      auto outdata_start = outdata.begin() + start;
      std::copy(data_start, data_end, outdata_start);

    } else {
      uint64_t num_threads = shad::rt::impl::getConcurrency();

      using args_t = std::tuple<uint64_t, //num_blocks
                                uint64_t, //start
                                uint64_t, //mid
                                uint64_t, //end
                                typename std::vector<row_t>::iterator, //rows
                                typename std::vector<row_t>::iterator, //outdata
                                comp,
                                row_t* //rowptr
      >;
      args_t args(num_threads, start, mid, end,
                  rows_.begin(), outdata.begin(), cmp, rows_.data());
      auto merged_l = [] (/*shad::rt::Handle&, */const args_t& args, uint64_t it) {
        uint64_t num_blocks = std::get<0>(args);
        uint64_t start = std::get<1>(args);
        uint64_t mid = std::get<2>(args);
        uint64_t end = std::get<3>(args);
        auto rows_beg = std::get<4>(args);
        auto outdata_beg = std::get<5>(args);
        comp cmp = std::get<6>(args);
        auto rowptr = std::get<7>(args);
        
        uint64_t i[2], lower[2], upper[2];
        typename std::vector<row_t>::iterator data_start, data_end, outdata_start, outdata_mid, outdata_end;

        i[0] = it       * (end - start) / num_blocks;
        i[1] = (it + 1) * (end - start) / num_blocks;
        shad_corank_sorted<comp>(i[0], lower, start, mid - start, mid, end - mid, cmp, rowptr);
        shad_corank_sorted<comp>(i[1], upper, start, mid - start, mid, end - mid, cmp, rowptr);

        uint64_t leftsize  = upper[0] - lower[0];
        uint64_t rightsize = upper[1] - lower[1];

        if (rightsize == 0) {
          data_start = rows_beg + start + lower[0];
          data_end = data_start + leftsize;

          outdata_start = outdata_beg + start + i[0];
          std::copy(data_start, data_end, outdata_start);

        } else if (leftsize == 0) {
          data_start = rows_beg + mid + lower[1];
          data_end = data_start + rightsize;

          outdata_start = outdata_beg + start + i[0];
          std::copy(data_start, data_end, outdata_start);

        } else {

          data_start = rows_beg + start + lower[0];
          data_end = data_start + leftsize;

          outdata_start = outdata_beg + start + i[0];
          std::copy(data_start, data_end, outdata_start);

          data_start = rows_beg + mid + lower[1];
          data_end = data_start + rightsize;

          outdata_start += leftsize;
          std::copy(data_start, data_end, outdata_start);

          outdata_start = outdata_beg + start + i[0];
          outdata_mid = outdata_start + leftsize;
          outdata_end = outdata_mid + rightsize;
          std::inplace_merge(outdata_start, outdata_mid, outdata_end, cmp);
        }
      };
      shad::rt::forEachOnAll(merged_l, args, num_threads);
      }
    }
  
  template <typename comp>
  void merge_blocks(uint64_t it, uint64_t mergesize,
                    std::vector<row_t> & outdata, comp cmp) {
    uint64_t start = it * mergesize;
    uint64_t mid = start + (mergesize >> 1);
    uint64_t end = __MIN(start + mergesize, num_rows_);

    if (end <= mid) {
      auto data_start = rows_.begin() + start;
      auto data_end = rows_.begin() + end;

      auto outdata_start = outdata.begin() + start;
      std::copy(data_start, data_end, outdata_start);

    } else {
      uint64_t num_threads = shad::rt::impl::getConcurrency(); //omp_get_max_threads();

// #pragma omp parallel for
      for (uint64_t i = 0; i < num_threads; i ++)
          merge_nested_blocks<comp>(i, num_threads, start, mid, end, outdata, cmp);
      }
    }
  
  inline void update_rows() {
      auto nthreads = shad::rt::impl::getConcurrency();
//     std::cout << "concurrency: " << nthreads << std::endl;
    uint64_t blocksize = __CEILING(num_rows_, nthreads);
    using tuple_v2_t = std::tuple<row_t*, // rows
                                  ENC_t*, // data
                                  size_t, // numrows
                                  size_t, //blcoksize
                                  size_t // numcols
                                  >;
    auto myfun_v2 = [](/*shad::rt::Handle &, */const tuple_v2_t& t, size_t block_id) {
      auto rowsptr = std::get<0>(t);
      auto dataptr = std::get<1>(t);
      auto numrows = std::get<2>(t);
      auto blocksize = std::get<3>(t);
      auto num_cols = std::get<4>(t);
      size_t offset = blocksize * block_id;
      dataptr += offset * num_cols;
      size_t end = __MIN(offset + blocksize, numrows);
      for (uint64_t it = offset; it < end; ++it, dataptr += num_cols) {
        rowsptr[it] = dataptr;
      }
    };
    tuple_v2_t argstuple(rows_.data(), data_.data(), num_rows_, blocksize, num_cols_);
    uint64_t niter = num_rows_/blocksize + (num_rows_ % blocksize != 0);

    shad::rt::forEachOnAll(myfun_v2, argstuple, niter);
  }
public:
  static void shadCollapse(LocalTable<ENC_t> &vertices, LocalTable<ENC_t>& edges) {
    using it_t = typename std::vector<row_t>::iterator;
    uint64_t num_edges = edges.num_rows();
    uint64_t num_vertices = vertices.num_rows();

    LocalTable<ENC_t> table(num_vertices, num_edges+num_vertices);

    auto nthreads = shad::rt::impl::getConcurrency();
    uint64_t blocksize = __CEILING(num_vertices, nthreads);
    
    using args1_t = std::tuple<size_t, //num vertices
                               row_t*, // vertices rows
                               typename std::vector<row_t>::iterator, // edge rows begin
                               typename std::vector<row_t>::iterator, // edge rows end
                               row_t*, // new table rows
                               ENC_t*, // new table data_
                               size_t  // blocksize
                               >;
    args1_t args1(num_vertices, vertices.rows_.data(),
                  edges.rows_.begin(), edges.rows_.end(),
                  table.rows_.data(), table.data_.data(), blocksize);
    
    auto fun_1 = [](/*shad::rt::Handle &, */const args1_t& t, size_t block_id) {
      auto num_vertices = std::get<0>(t);
      auto vert_rows = std::get<1>(t);
      auto edges_rows_begin = std::get<2>(t);
      auto edges_rows_end = std::get<3>(t);
      auto table_rows = std::get<4>(t);
      auto table_data = std::get<5>(t);
      auto blocksize = std::get<6>(t);
      size_t offset = blocksize * block_id;
      size_t end = __MIN(offset + blocksize, num_vertices);
      for (uint64_t i = offset; i < end; ++i) {
        uint64_t v = vert_rows[i][0];
        auto edgeRange = std::equal_range(edges_rows_begin,
                                          edges_rows_end,
                                          & v, __row_t_LT);

        std::sort(edgeRange.first, edgeRange.second, __dst_LT);
        
        // # of edges of preveious v's plus markers
        uint64_t start = edgeRange.first - edges_rows_begin + i; 
        table_rows[i] = table_data + start;

        for (auto itr = edgeRange.first; itr < edgeRange.second; ++itr) {
          table_data[start ++] = (* itr)[1];
        }
        table_data[start] = (uint64_t) (- (v + 1));
      }
    };
    uint64_t niter = num_vertices/blocksize + (num_vertices % blocksize != 0);
    shad::rt::forEachOnAll(fun_1, args1, niter);

    table.Sort(__list_LT);  //shad
    shad::LocalSet<std::pair <uint64_t, uint64_t> > collapse(num_vertices/16);
    
    using args2_t = std::tuple<size_t, //num vertices
                               row_t*, // new table rows
                               size_t,  // blocksize
                               shad::LocalSet<std::pair <uint64_t, uint64_t> >* // collapse
                               >;
    --num_vertices;
    blocksize = __CEILING(num_vertices, nthreads);
    niter = num_vertices/blocksize + (num_vertices % blocksize != 0);
    args2_t args2(num_vertices, 
                  table.rows_.data(),
                  blocksize,
                  &collapse);
    auto fun_2 = [](shad::rt::Handle &, const args2_t& t, size_t block_id) {
      auto num_vertices = std::get<0>(t);
      auto table_rows = std::get<1>(t);
      auto blocksize = std::get<2>(t);
      auto collapse = std::get<3>(t);
      size_t offset = blocksize * block_id;
      size_t end = __MIN(offset + blocksize, num_vertices);
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
    printf("   Number of collapsed items = %lu\n", collapse.Size());

  }
  
  static void CreateLocalEdgeIndex(LocalTable<ENC_t>& edges,
                    shad::LocalEdgeIndex<uint64_t, uint64_t>* idx,
                    size_t src_col,
                    size_t dest_col) {
    auto num_edges = edges.num_rows()/2;
    for (size_t i = 0; i < num_edges; ++i) {
      uint64_t src = edges.rows_[i][src_col];
      uint64_t dest = edges.rows_[i][dest_col];
      idx->Insert(src, dest);
    }
   }
   
  static void CreateLocalIndex(LocalTable<ENC_t>& edges,
                    shad::LocalIndex<uint64_t, uint64_t>& idx,
                    size_t src_col,
                    size_t dest_col) {
    auto num_edges = edges.num_rows();
    for (size_t i = 0; i < num_edges; ++i) {
      uint64_t src = edges.rows_[i][src_col];
      uint64_t dest = edges.rows_[i][dest_col];
      idx.Insert(src, dest);
    }
  }

  static void CreateIndex(LocalTable<ENC_t>& edges,
                    shad::Index<uint64_t, uint64_t>::ObjectID oid,
                    size_t src_col,
                    size_t dest_col) {
    auto num_edges = edges.num_rows();
    auto idx = shad::Index<uint64_t, uint64_t>::GetPtr(oid);
    for (size_t i = 0; i < num_edges; ++i) {
      uint64_t src = edges.rows_[i][src_col];
      uint64_t dest = edges.rows_[i][dest_col];
      idx->Insert(src, dest);
    }
  }
};





}  // namespace shad

#endif  // INCLUDE_SHAD_EXTENSIONS_TABLE_LIBRARY_LOCAL_TABLE_H_

