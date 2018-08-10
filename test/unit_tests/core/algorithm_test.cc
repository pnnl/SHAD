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

#include "gtest/gtest.h"

#include "shad/core/algorithm.h"
#include "shad/data_structures/array.h"

class AlgorithmsTest : public ::testing::Test {
 public:
  AlgorithmsTest() { array_ = shad::array<size_t, 10001>::Create(); }

  void SetUp() { array_->fill(size_t(1)); }
  void TearDown() {}

 protected:
  std::shared_ptr<shad::array<size_t, 10001>> array_;
};

TEST_F(AlgorithmsTest, find) {
  auto res = shad::find(shad::distributed_parallel_tag{}, array_->begin(),
                        array_->end(), 0);

  ASSERT_EQ(res, array_->end());

  res = shad::find(shad::distributed_sequential_tag{}, array_->begin(),
                   array_->end(), 0);
  ASSERT_EQ(res, array_->end());

  array_->at(array_->size() - 1) = 2;

  res = shad::find(shad::distributed_sequential_tag{}, array_->begin(),
                   array_->end(), 2);
  ASSERT_EQ(res, (array_->end() - 1));

  res = shad::find(shad::distributed_parallel_tag{}, array_->begin(),
                   array_->end(), 2);
  ASSERT_EQ(res, (array_->end() - 1));
}

TEST_F(AlgorithmsTest, find_if) {
  using value_type = typename std::array<size_t, 10001>::value_type;
  auto equal_to_zero = [](const value_type& v) -> bool { return v == 0; };
  auto res = shad::find_if(shad::distributed_parallel_tag{}, array_->begin(),
                           array_->end(), equal_to_zero);

  ASSERT_EQ(res, array_->end());

  res = shad::find_if(shad::distributed_sequential_tag{}, array_->begin(),
                      array_->end(), equal_to_zero);
  ASSERT_EQ(res, array_->end());

  array_->at(array_->size() - 1) = 2;

  auto equal_to_two = [](const value_type& v) -> bool { return v == 2; };
  res = shad::find_if(shad::distributed_sequential_tag{}, array_->begin(),
                      array_->end(), equal_to_two);
  ASSERT_EQ(res, (array_->end() - 1));

  res = shad::find_if(shad::distributed_parallel_tag{}, array_->begin(),
                      array_->end(),
                      std::bind(std::equal_to<value_type>(), value_type(2),
                                std::placeholders::_1));
  ASSERT_EQ(res, (array_->end() - 1));
}

TEST_F(AlgorithmsTest, for_each) {
  using value_type = typename std::array<size_t, 10001>::value_type;
  using reference = typename std::array<size_t, 10001>::reference;

  shad::for_each(shad::distributed_sequential_tag{}, array_->begin(),
                 array_->end(), [](reference& v) { v += 1; });

  auto res = shad::find_if(shad::distributed_parallel_tag{}, array_->begin(),
                           array_->end(),
                           std::bind(std::equal_to<value_type>(), value_type(1),
                                     std::placeholders::_1));
  ASSERT_EQ(res, array_->end());

  shad::for_each(shad::distributed_parallel_tag{}, array_->begin(),
                 array_->end(), [](reference& v) { v += 1; });

  res = shad::find_if(shad::distributed_parallel_tag{}, array_->begin(),
                      array_->end(),
                      std::bind(std::equal_to<value_type>(), value_type(2),
                                std::placeholders::_1));
  ASSERT_EQ(res, array_->end());
}

TEST_F(AlgorithmsTest, all_of) {
  using value_type = typename std::array<size_t, 10001>::value_type;
  using reference = typename std::array<size_t, 10001>::reference;

  bool res = shad::all_of(shad::distributed_sequential_tag{}, array_->begin(),
                          array_->end(),
                          std::bind(std::equal_to<value_type>(), value_type(1),
                                    std::placeholders::_1));
  ASSERT_TRUE(res);

  res = shad::all_of(shad::distributed_parallel_tag{}, array_->begin(),
                     array_->end(),
                     std::bind(std::equal_to<value_type>(), value_type(1),
                               std::placeholders::_1));
  ASSERT_TRUE(res);

  array_->at(array_->size() - 1) = 0;

  res = shad::all_of(shad::distributed_sequential_tag{}, array_->begin(),
                     array_->end(),
                     std::bind(std::equal_to<value_type>(), value_type(1),
                               std::placeholders::_1));
  ASSERT_FALSE(res);

  res = shad::all_of(shad::distributed_parallel_tag{}, array_->begin(),
                     array_->end(),
                     std::bind(std::equal_to<value_type>(), value_type(1),
                               std::placeholders::_1));
  ASSERT_FALSE(res);
}


TEST_F(AlgorithmsTest, any_of) {
  using value_type = typename std::array<size_t, 10001>::value_type;
  using reference = typename std::array<size_t, 10001>::reference;

  bool res = shad::any_of(shad::distributed_sequential_tag{}, array_->begin(),
                          array_->end(),
                          std::bind(std::equal_to<value_type>(), value_type(0),
                                    std::placeholders::_1));
  ASSERT_FALSE(res);

  res = shad::any_of(shad::distributed_parallel_tag{}, array_->begin(),
                     array_->end(),
                     std::bind(std::equal_to<value_type>(), value_type(0),
                               std::placeholders::_1));
  ASSERT_FALSE(res);

  array_->at(array_->size() - 1) = 0;

  res = shad::any_of(shad::distributed_sequential_tag{}, array_->begin(),
                     array_->end(),
                     std::bind(std::equal_to<value_type>(), value_type(0),
                               std::placeholders::_1));
  ASSERT_TRUE(res);

  res = shad::any_of(shad::distributed_parallel_tag{}, array_->begin(),
                     array_->end(),
                     std::bind(std::equal_to<value_type>(), value_type(0),
                               std::placeholders::_1));
  ASSERT_TRUE(res);
}
