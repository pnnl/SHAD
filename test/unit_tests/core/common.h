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

#ifndef TEST_UNIT_TESTS_STL_COMMON_H_
#define TEST_UNIT_TESTS_STL_COMMON_H_

#include <array>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"

#include "shad/core/array.h"
#include "shad/core/iterator.h"
#include "shad/core/unordered_map.h"
#include "shad/core/unordered_set.h"

namespace std {
template <class T1, class T2>
struct negate<std::pair<T1, T2>> {
  using T = std::pair<T1, T2>;
  T operator()(const T &p1) {
    return std::make_pair(std::negate<T1>{}(p1.first),
                          std::negate<T2>{}(p1.second));
  }
};

template <class T1, class T2>
struct plus<std::pair<T1, T2>> {
  using T = std::pair<T1, T2>;
  T operator()(const T &p1, const T &p2) {
    return std::make_pair(std::plus<T1>{}(p1.first, p2.first),
                          std::plus<T2>{}(p1.second, p2.second));
  }
};

template <class T1, class T2>
struct minus<std::pair<T1, T2>> {
  using T = std::pair<T1, T2>;
  T operator()(const T &p1, const T &p2) {
    return std::make_pair(std::minus<T1>{}(p1.first, p2.first),
                          std::minus<T2>{}(p1.second, p2.second));
  }
};

template <class T1, class T2>
struct multiplies<std::pair<T1, T2>> {
  using T = std::pair<T1, T2>;
  T operator()(const T &p1, const T &p2) {
    return std::make_pair(std::multiplies<T1>{}(p1.first, p2.first),
                          std::multiplies<T2>{}(p1.second, p2.second));
  }
};
};  // namespace std

namespace shad_test_stl {

static constexpr size_t kNumElements = 1024;
static constexpr size_t substr_len = 32;

// container creation and expected checksum
template <typename T, bool even>
struct create_vector_;

template <typename T, bool even>
struct create_array_;

template <typename T, bool even>
struct create_set_;

template <typename T, bool even>
struct create_map_;

template <typename U, bool even>
struct create_vector_<std::vector<U>, even> {
  using T = std::vector<U>;
  std::shared_ptr<T> operator()(size_t size) {
    auto res = std::make_shared<T>();
    for (size_t i = 0; i < size; ++i) res->push_back(2 * i + !even);
    return res;
  }
};

template <typename U, size_t size, bool even>
struct create_array_<std::array<U, size>, even> {
  using T = std::array<U, size>;
  std::shared_ptr<T> operator()() {
    auto res = std::make_shared<T>();
    for (size_t i = 0; i < size; ++i) res->at(i) = 2 * i + !even;
    return res;
  }
};

template <typename U, size_t size, bool even>
struct create_array_<shad::array<U, size>, even> {
  using T = shad::array<U, size>;
  std::shared_ptr<T> operator()() {
    auto res = std::make_shared<T>();
    for (size_t i = 0; i < size; ++i) res->at(i) = 2 * i + !even;
    return res;
  }
};

template <typename U, bool even>
struct create_set_<std::unordered_set<U>, even> {
  using T = std::unordered_set<U>;
  std::shared_ptr<T> operator()(size_t size) {
    auto res = std::make_shared<T>();
    for (size_t i = 0; i < size; ++i) res->insert(2 * i + !even);
    return res;
  }
};

template <typename U, bool even>
struct create_set_<shad::unordered_set<U>, even> {
  using T = shad::unordered_set<U>;
  std::shared_ptr<T> operator()(size_t size) {
    auto res = std::make_shared<T>(size);
    shad::buffered_insert_iterator<T> ins(*res, res->end());
    for (size_t i = 0; i < size; ++i) ins = (2 * i + !even);
    ins.flush();
    return res;
  }
};

template <typename U, typename V, bool even>
struct create_map_<std::unordered_map<U, V>, even> {
  using T = std::unordered_map<U, V>;
  std::shared_ptr<T> operator()(size_t size) {
    auto res = std::make_shared<T>();
    for (size_t i = 0; i < size; ++i) (*res)[i] = 2 * i + !even;
    return res;
  }
};

template <typename U, typename V, bool even>
struct create_map_<shad::unordered_map<U, V>, even> {
  using T = shad::unordered_map<U, V>;
  std::shared_ptr<T> operator()(size_t size) {
    auto res = std::make_shared<T>(size);
    shad::buffered_insert_iterator<T> ins(*res, res->begin());
    for (size_t i = 0; i < size; i++) ins = std::make_pair(i, 2 * i + !even);
    ins.flush();
    return res;
  }
};

template <bool even>
int64_t expected_checksum_(size_t size) {
  int64_t res = 0;
  for (size_t i = 0; i < size; ++i) res += 2 * i + !even;
  return res;
}

// sub-sequencing from dynamic-size containers
template <typename T>
typename T::iterator it_seek_(std::shared_ptr<T> in, size_t start_idx) {
  auto first = in->begin();
  size_t i = 0;
  while (i++ < start_idx && first != in->end()) {
    assert(first != in->end());
    ++first;
  }
  return first;
}

template <typename T>
struct subseq_from_;

template <typename U>
struct subseq_from_<std::vector<U>> {
  using T = std::vector<U>;
  std::shared_ptr<T> operator()(std::shared_ptr<T> in, size_t start_idx,
                                size_t len) {
    assert(start_idx < in->size());
    auto first = it_seek_(in, start_idx);
    auto res = std::make_shared<T>();
    for (size_t i = 0; i < len; ++i) {
      assert(first != in->end());
      res->push_back(*first++);
    }
    return res;
  }
};

template <typename U>
struct subseq_from_<std::unordered_set<U>> {
  using T = std::unordered_set<U>;
  std::shared_ptr<T> operator()(std::shared_ptr<T> in, size_t start_idx,
                                size_t len) {
    assert(start_idx < in->size());
    auto first = it_seek_(in, start_idx);
    auto res = std::make_shared<T>();
    for (size_t i = 0; i < len; ++i) {
      assert(first != in->end());
      res->insert(*first++);
    }
    return res;
  }
};

template <typename U>
struct subseq_from_<shad::unordered_set<U>> {
  using T = shad::unordered_set<U>;
  std::shared_ptr<T> operator()(std::shared_ptr<T> in, size_t start_idx,
                                size_t len) {
    assert(start_idx < in->size());
    auto first = it_seek_(in, start_idx);
    auto res = std::make_shared<T>(len);
    shad::buffered_insert_iterator<T> ins(*res, res->end());
    for (size_t i = 0; i < len; ++i) {
      assert(first != in->end());
      ins = *first;
      ++first;
    }
    ins.flush();
    return res;
  }
};

template <typename U, typename V>
struct subseq_from_<std::unordered_map<U, V>> {
  using T = std::unordered_map<U, V>;
  std::shared_ptr<T> operator()(std::shared_ptr<T> in, size_t start_idx,
                                size_t len) {
    assert(start_idx < in->size());
    auto first = it_seek_(in, start_idx);
    auto res = std::make_shared<T>();
    std::unordered_map<U, V> x;
    for (size_t i = 0; i < len; ++i) {
      assert(first != in->end());
      res->operator[]((*first).first) = (*first).second;
      ++first;
    }
    return res;
  }
};

template <typename U, typename V>
struct subseq_from_<shad::unordered_map<U, V>> {
  using T = shad::unordered_map<U, V>;
  std::shared_ptr<T> operator()(std::shared_ptr<T> in, size_t start_idx,
                                size_t len) {
    assert(start_idx < (*in).size());
    auto first = it_seek_(in, start_idx);
    auto res = std::make_shared<T>(len);
    std::unordered_map<U, V> x;
    shad::buffered_insert_iterator<T> ins(*res, res->end());
    for (size_t i = 0; i < len; ++i) {
      assert(first != in->end());
      ins = std::make_pair((*first).first, (*first).second);
      ++first;
    }
    ins.flush();
    return res;
  }
};

// sub-sequencing from static-size containers
template <typename T, size_t size_>
struct static_subseq_from_;

template <typename U, size_t size, size_t size_>
struct static_subseq_from_<std::array<U, size>, size_> {
  using T = std::array<U, size>;
  using T_ = std::array<U, size_>;
  std::shared_ptr<T_> operator()(std::shared_ptr<T> in, size_t start_idx) {
    assert(start_idx < size);
    auto first = it_seek_(in, start_idx);
    auto res = std::make_shared<T_>();
    for (size_t i = 0; i < size_; ++i) {
      assert(first != in->end());
      res->at(i) = *first++;
    }
    return res;
  }
};

template <typename U, size_t size, size_t size_>
struct static_subseq_from_<shad::array<U, size>, size_> {
  using T = shad::array<U, size>;
  using T_ = shad::array<U, size_>;
  std::shared_ptr<T_> operator()(std::shared_ptr<T> in, size_t start_idx) {
    assert(start_idx < size);
    auto first = it_seek_(in, start_idx);
    auto res = std::make_shared<T_>();
    for (size_t i = 0; i < size_; ++i) {
      assert(first != in->end());
      res->at(i) = *first++;
    }
    return res;
  }
};

// evenness/oddness test
template <typename T>
struct is_even {
  bool operator()(const T &x) { return !(x % 2); }
};

template <typename T, typename U>
struct is_even<std::pair<T, U>> {
  bool operator()(const std::pair<T, U> &x) { return is_even<U>{}(x.second); }
};

template <typename T>
struct is_odd {
  bool operator()(const T &x) { return !(is_even<T>{}(x)); }
};

template <typename T>
std::function<bool(const T &)> is_even_wrapper =
    [](const T &x) -> bool { return is_even<T>{}(x); };

template <typename T>
std::function<bool(const T &)> is_odd_wrapper =
    [](const T &x) -> bool { return !is_even<T>{}(x); };

// convert to a int64_t
template <typename T>
struct to_int64 {
  int64_t operator()(const T &x) { return x; }
};

template <typename T1, typename T2>
struct to_int64<std::pair<T1, T2>> {
  int64_t operator()(const std::pair<T1, T2> &x) { return x.first + x.second; }
};

// checksums
template <typename It>
int64_t checksum(It first, It last) {
  using val_t = typename std::iterator_traits<It>::value_type;
  int64_t res = 0;
  for (auto it = first; it != last; ++it) res += to_int64<val_t>{}(*it);
  return res;
}

template <typename It>
int64_t ordered_checksum(It first, It last) {
  using val_t = typename std::iterator_traits<It>::value_type;
  int64_t res = 0, pos = 1;
  for (auto it = first; it != last; ++it) res += pos++ * to_int64<val_t>{}(*it);
  return res;
}

// test fixtures
template <typename T>
class TestFixture : public ::testing::Test {
 public:
  //
  // w/o execution policy
  //
  // single-range
  // check: return values
  template <typename F, typename... args_>
  void test(F &&sub_f, F &&obj_f, args_... args) {
    auto obs = sub_f(in->begin(), in->end(), args...);
    auto exp = obj_f(in->begin(), in->end(), args...);
    ASSERT_EQ(obs, exp);
  }

  // single-range, in-place "write-only"
  // check: container values
  template <typename F, typename checksum_t, typename... args_>
  void test_void(F &&sub_f, F &&obj_f, checksum_t checksum_f, args_... args) {
    int64_t obs, exp;
    sub_f(in->begin(), in->end(), args...);
    obs = checksum_f(in->begin(), in->end());
    obj_f(in->begin(), in->end(), args...);
    exp = checksum_f(in->begin(), in->end());
    ASSERT_EQ(obs, exp);
  }

  // multi-range input-output, STL insert iterators
  // check: output-container values
  template <typename F, typename checksum_t, typename... args_>
  void test_io_inserters(F &&sub_f, F &&obj_f, checksum_t checksum_f,
                         args_... args) {
    using out_it_t = std::insert_iterator<T>;
    auto out1 = create_output_container(0);
    auto out2 = create_output_container(0);
    out_it_t out1_it(*out1, out1->begin()), out2_it(*out2, out2->begin());
    sub_f(in->begin(), in->end(), out1_it, args...);
    obj_f(in->begin(), in->end(), out2_it, args...);
    auto obs = checksum(out1->begin(), out1->end());
    auto exp = checksum(out2->begin(), out2->end());
    ASSERT_EQ(obs, exp);
  }

  // multi-range input-output, assign-based output
  // check: output-container values
  template <typename F, typename checksum_t, typename... args_>
  void test_io_assignment(F &&sub_f, F &&obj_f, checksum_t checksum_f,
                          args_... args) {
    auto out1 = create_output_container(in->size());
    auto out2 = create_output_container(in->size());
    sub_f(in->begin(), in->end(), out1->begin(), args...);
    obj_f(in->begin(), in->end(), out2->begin(), args...);
    auto obs = checksum(out1->begin(), out1->end());
    auto exp = checksum(out2->begin(), out2->end());
    ASSERT_EQ(obs, exp);
  }

  //
  // w/ execution policy
  //
  // single-range
  // check: return values
  template <typename ExecutionPolicy, typename FS, typename FO,
            typename... args_>
  void test_with_policy(ExecutionPolicy &&policy, FS &&sub_f, FO &&obj_f,
                        args_... args) {
    auto obs = sub_f(std::forward<ExecutionPolicy>(policy), in->begin(),
                     in->end(), args...);
    auto exp = obj_f(in->begin(), in->end(), args...);
    ASSERT_EQ(obs, exp);
  }

  // single-range, in-place "write-only"
  // check: container values
  template <typename ExecutionPolicy, typename FS, typename FO,
            typename checksum_t, typename... args_>
  void test_void_with_policy(ExecutionPolicy &&policy, FS &&sub_f, FO &&obj_f,
                             checksum_t checksum_f, args_... args) {
    int64_t obs, exp;
    sub_f(std::forward<ExecutionPolicy>(policy), in->begin(), in->end(),
          args...);
    obs = checksum_f(in->begin(), in->end());
    obj_f(in->begin(), in->end(), args...);
    exp = checksum_f(in->begin(), in->end());
    ASSERT_EQ(obs, exp);
  }

  // multi-range input-output, SHAD/STL insert iterators
  // check: output-container values
  template <typename shad_inserter_t, typename ExecutionPolicy, typename FS,
            typename FO, typename checksum_t, typename... args_>
  void test_io_inserters_with_policy(ExecutionPolicy &&policy, FS &&sub_f,
                                     FO &&obj_f, checksum_t checksum_f,
                                     args_... args) {
    auto out1 = create_output_container(0);
    auto out2 = create_output_container(0);
    shad_inserter_t out1_it(*out1, out1->begin());
    std::insert_iterator<T> out2_it(*out2, out2->begin());
    sub_f(std::forward<ExecutionPolicy>(policy), in->begin(), in->end(),
          out1_it, args...);
    obj_f(in->begin(), in->end(), out2_it, args...);
    auto obs = checksum(out1->begin(), out1->end());
    auto exp = checksum(out2->begin(), out2->end());
    ASSERT_EQ(obs, exp);
  }

  // multi-range input-output, assign-based output
  // check: output-container values
  template <typename ExecutionPolicy, typename FS, typename FO,
            typename checksum_t, typename... args_>
  void test_io_assignment_with_policy(ExecutionPolicy &&policy, FS &&sub_f,
                                      FO &&obj_f, checksum_t checksum_f,
                                      args_... args) {
    auto out1 = create_output_container(in->size());
    auto out2 = create_output_container(in->size());
    sub_f(std::forward<ExecutionPolicy>(policy), in->begin(), in->end(),
          out1->begin(), args...);
    obj_f(in->begin(), in->end(), out2->begin(), args...);
    auto obs = checksum(out1->begin(), out1->end());
    auto exp = checksum(out2->begin(), out2->end());
    ASSERT_EQ(obs, exp);
  }

 protected:
  virtual ~TestFixture() {}
  std::shared_ptr<T> in;

  int64_t expected_checksum() { return expected_checksum_<true>(kNumElements); }
  virtual std::shared_ptr<T> create_output_container(size_t) = 0;
};

template <typename T>
class VectorTestFixture : public TestFixture<T> {
  void SetUp() { this->in = create_vector_<T, true>{}(kNumElements); }
  std::shared_ptr<T> create_output_container(size_t size) {
    return create_vector_<T, false>{}(size);
  }
};

template <typename T>
class ArrayTestFixture : public TestFixture<T> {
  void SetUp() { this->in = create_array_<T, true>{}(); }
  std::shared_ptr<T> create_output_container(size_t) {
    return create_array_<T, false>{}();
  }
};

template <typename T>
class SetTestFixture : public TestFixture<T> {
  void SetUp() { this->in = create_set_<T, true>{}(kNumElements); }
  std::shared_ptr<T> create_output_container(size_t size) {
    return create_set_<T, false>{}(size);
  }
};

template <typename T>
class MapTestFixture : public TestFixture<T> {
  void SetUp() { this->in = create_map_<T, true>{}(kNumElements); }
  std::shared_ptr<T> create_output_container(size_t size) {
    return create_map_<T, false>{}(size);
  }
};
}  // namespace shad_test_stl

#endif
