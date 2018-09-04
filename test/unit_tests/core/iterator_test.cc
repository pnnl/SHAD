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

#include <iterator>

#include "gtest/gtest.h"

#include "shad/core/algorithm.h"
#include "shad/core/iterator.h"
#include "shad/core/unordered_map.h"
#include "shad/core/unordered_set.h"

constexpr int batch_size = 128;
std::pair<int, int> kv(int x) { return std::make_pair(x, x); }

///////////////////////////////////////
//
// shad::unordered_set
//
///////////////////////////////////////
TEST(shad_uset, insert_iterator) {
  using T = shad::unordered_set<int>;
  T cnt(3 * batch_size);
  int first = 0;

  // insert into empty container, from begin
  std::insert_iterator<T> ins_begin(cnt, cnt.begin());
  for (auto i = batch_size; i > 0; --i) {
    ins_begin = i;
  }
  for (auto i = batch_size; i > 0; --i) {
    ASSERT_TRUE(shad::find(shad::distributed_parallel_tag{}, cnt.begin(),
                           cnt.end(), i) != cnt.end());
  }

  // insert into non-empty container, from begin
  first = batch_size;
  for (auto i = batch_size; i > 0; --i) {
    ins_begin = first + i;
  }
  for (auto i = batch_size; i > 0; --i) {
    ASSERT_TRUE(shad::find(shad::distributed_parallel_tag{}, cnt.begin(),
                           cnt.end(), first + i) != cnt.end());
  }

  // insert into non-empty container, from end
  std::insert_iterator<T> ins_end(cnt, cnt.begin());
  first = 2 * batch_size;
  for (auto i = batch_size; i > 0; --i) {
    ins_end = first + i;
  }
  for (auto i = batch_size; i > 0; --i) {
    ASSERT_TRUE(shad::find(shad::distributed_parallel_tag{}, cnt.begin(),
                           cnt.end(), first + i) != cnt.end());
  }
}

TEST(shad_uset, buffered_insert_iterator) {
  using T = shad::unordered_set<int>;
  T cnt(4 * batch_size);
  int first = 0;

  // insert into empty container, from begin
  {
    shad::buffered_insert_iterator<T> ins_begin(cnt, cnt.begin());
    for (auto i = batch_size; i > 0; --i) {
      ins_begin = i;
    }
  }
  for (auto i = batch_size; i > 0; --i) {
    ASSERT_TRUE(shad::find(shad::distributed_parallel_tag{}, cnt.begin(),
                           cnt.end(), i) != cnt.end());
  }

  // insert into non-empty container, from begin
  first = batch_size;
  {
    shad::buffered_insert_iterator<T> ins_begin(cnt, cnt.begin());
    for (auto i = batch_size; i > 0; --i) {
      ins_begin = first + i;
    }
  }
  for (auto i = batch_size; i > 0; --i) {
    ASSERT_TRUE(shad::find(shad::distributed_parallel_tag{}, cnt.begin(),
                           cnt.end(), first + i) != cnt.end());
  }

  // insert into non-empty container, from end
  first = 2 * batch_size;
  {
    shad::buffered_insert_iterator<T> ins_end(cnt, cnt.begin());
    for (auto i = batch_size; i > 0; --i) {
      ins_end = first + i;
    }
  }
  for (auto i = batch_size; i > 0; --i) {
    ASSERT_TRUE(shad::find(shad::distributed_parallel_tag{}, cnt.begin(),
                           cnt.end(), first + i) != cnt.end());
  }

  // insert into non-empty container, from end, early flush
  first = 3 * batch_size;
  shad::buffered_insert_iterator<T> ins_end(cnt, cnt.begin());
  for (auto i = batch_size; i > 0; --i) {
    ins_end = first + i;
  }
  ins_end.flush();
  for (auto i = batch_size; i > 0; --i) {
    ASSERT_TRUE(shad::find(shad::distributed_parallel_tag{}, cnt.begin(),
                           cnt.end(), first + i) != cnt.end());
  }
}

///////////////////////////////////////
//
// shad::unordered_map
//
///////////////////////////////////////
TEST(shad_umap, insert_iterator) {
  using T = shad::unordered_map<int, int>;
  T cnt(3 * batch_size);
  int first = 0;

  // insert into empty container, from begin
  std::insert_iterator<T> ins_begin(cnt, cnt.begin());
  for (auto i = batch_size; i > 0; --i) ins_begin = kv(i);
  for (auto i = batch_size; i > 0; --i)
    ASSERT_TRUE(shad::find(shad::distributed_parallel_tag{}, cnt.begin(),
                           cnt.end(), kv(i)) != cnt.end());

  // insert into non-empty container, from begin
  first = batch_size;
  for (auto i = batch_size; i > 0; --i) ins_begin = kv(first + i);
  for (auto i = batch_size; i > 0; --i)
    ASSERT_TRUE(shad::find(shad::distributed_parallel_tag{}, cnt.begin(),
                           cnt.end(), kv(first + i)) != cnt.end());

  // insert into non-empty container, from end
  first = 2 * batch_size;
  std::insert_iterator<T> ins_end(cnt, cnt.begin());
  for (auto i = batch_size; i > 0; --i) ins_end = kv(first + i);
  for (auto i = batch_size; i > 0; --i)
    ASSERT_TRUE(shad::find(shad::distributed_parallel_tag{}, cnt.begin(),
                           cnt.end(), kv(first + i)) != cnt.end());
}

TEST(shad_umap, buffered_insert_iterator) {
  using T = shad::unordered_map<int, int>;
  T cnt(4 * batch_size);
  int first = 0;

  // insert into empty container, from begin
  {
    shad::buffered_insert_iterator<T> ins_begin(cnt, cnt.begin());
    for (auto i = batch_size; i > 0; --i) {
      ins_begin = kv(i);
    }
  }
  for (auto i = batch_size; i > 0; --i)
    ASSERT_TRUE(shad::find(shad::distributed_parallel_tag{}, cnt.begin(),
                           cnt.end(), kv(i)) != cnt.end());

  // insert into non-empty container, from begin
  first = batch_size;
  {
    shad::buffered_insert_iterator<T> ins_begin(cnt, cnt.begin());
    for (auto i = batch_size; i > 0; --i) {
      ins_begin = kv(first + i);
    }
  }
  for (auto i = batch_size; i > 0; --i)
    ASSERT_TRUE(shad::find(shad::distributed_parallel_tag{}, cnt.begin(),
                           cnt.end(), kv(first + i)) != cnt.end());

  // insert into non-empty container, from end
  first = 2 * batch_size;
  {
    shad::buffered_insert_iterator<T> ins_end(cnt, cnt.begin());
    for (auto i = batch_size; i > 0; --i) {
      ins_end = kv(first + i);
    }
  }
  for (auto i = batch_size; i > 0; --i)
    ASSERT_TRUE(shad::find(shad::distributed_parallel_tag{}, cnt.begin(),
                           cnt.end(), kv(first + i)) != cnt.end());

  // insert into non-empty container, from end, early flush
  first = 3 * batch_size;
  shad::buffered_insert_iterator<T> ins_end(cnt, cnt.begin());
  for (auto i = batch_size; i > 0; --i) ins_end = kv(first + i);
  ins_end.flush();
  for (auto i = batch_size; i > 0; --i)
    ASSERT_TRUE(shad::find(shad::distributed_parallel_tag{}, cnt.begin(),
                           cnt.end(), kv(first + i)) != cnt.end());
}
