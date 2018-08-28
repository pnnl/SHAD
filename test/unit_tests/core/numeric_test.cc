//===------------------------------------------------------------*- C++ -*-===//
//
//                                     SHAD
//
//      The Scalable High-performance Numeric and Data Structure Library
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

#include <array>
#include <numeric>
#include "shad/core/numeric.h"
#include "shad/data_structures/array.h"

static size_t kInitValue = 2;
constexpr static size_t kArraySize = 42;

class NumericTest : public ::testing::Test {
 public:
  NumericTest() {
    array_ = shad::array<size_t, kArraySize>::Create();
    array2_ = shad::array<size_t, kArraySize>::Create();
  }

  void SetUp() {
    array_->fill(kInitValue);
    stl_array_.fill(kInitValue);;
    for (size_t i = 0; i < kArraySize; i++) {
      array2_->at(i) = i*3;
      stl_array2_[i] = i*3;
    }
  }
  void TearDown() {}

 protected:
  std::shared_ptr<shad::array<size_t, kArraySize>> array_;
  std::shared_ptr<shad::array<size_t, kArraySize>> array2_;
  std::array<size_t, kArraySize> stl_array_;
  std::array<size_t, kArraySize> stl_array2_;
  
};

TEST_F(NumericTest, iota) {
  shad::iota(array_->begin(), array_->end(), kInitValue);
  size_t cnt = 0;
  for (auto it = array_->begin(); it != array_->end(); ++it, ++cnt) {
    size_t val = *it;
    ASSERT_EQ(val, stl_array_[cnt]);
  }
}

TEST_F(NumericTest, accumulate) {
  size_t value = shad::accumulate(array_->begin(), array_->end(), kInitValue);
  size_t exp_value = std::accumulate(stl_array_.begin(), stl_array_.end(),
                                     kInitValue);
  ASSERT_EQ(value, exp_value);
  value = shad::accumulate(array_->begin(), array_->end(), kInitValue,
                           std::multiplies<size_t>());
  exp_value = std::accumulate(stl_array_.begin(), stl_array_.end(),
                                     kInitValue, std::multiplies<size_t>());
  ASSERT_EQ(value, exp_value);
  
}

TEST_F(NumericTest, inner_product) {
  size_t value = shad::inner_product(array_->begin(), array_->end(),
                                     array2_->begin(), kInitValue);
  size_t exp_value = std::inner_product(stl_array_.begin(), stl_array_.end(),
                                        stl_array2_.begin(), kInitValue);
  ASSERT_EQ(value, exp_value);
  value = shad::inner_product(array_->begin(), array_->end(),
                              array2_->begin(), kInitValue,
                              std::plus<size_t>(), std::multiplies<size_t>());
  exp_value = std::inner_product(stl_array_.begin(), stl_array_.end(),
                                 stl_array2_.begin(), kInitValue,
                                 std::plus<size_t>(), std::multiplies<size_t>());
  ASSERT_EQ(value, exp_value);
}

TEST_F(NumericTest, adjacent_difference) {
  auto outArray = shad::array<size_t, kArraySize>::Create();
  std::array<size_t, kArraySize> outStlArray;
  auto value = shad::adjacent_difference(array2_->begin(), array2_->end(),
                                         outArray->begin());
  auto exp_value = std::adjacent_difference(stl_array2_.begin(), stl_array2_.end(),
                                            outStlArray.begin());
  size_t i = 0;
  for (auto it = outArray->begin();
       it != outArray->end(); ++it, ++i) {
    ASSERT_EQ(*it, outStlArray[i]);
  }
  value = shad::adjacent_difference(shad::distributed_parallel_tag{},
                                    array2_->begin(), array2_->end(),
                                    outArray->begin());
  i = 0;
  for (auto it = outArray->begin();
       it != outArray->end(); ++it, ++i) {
    ASSERT_EQ(*it, outStlArray[i]);
  }
  
  value = shad::adjacent_difference(array2_->begin(), array2_->end(),
                                    outArray->begin(), std::minus<size_t>());
  i = 0;
  for (auto it = outArray->begin();
       it != outArray->end(); ++it, ++i) {
    ASSERT_EQ(*it, outStlArray[i]);
  }
}

TEST_F(NumericTest, partial_sum) {
  auto outArray = shad::array<size_t, kArraySize>::Create();
  std::array<size_t, kArraySize> outStlArray;
  auto value = shad::partial_sum(array2_->begin(), array2_->end(),
                                 outArray->begin());
  auto exp_value = std::partial_sum(stl_array2_.begin(), stl_array2_.end(),
                                    outStlArray.begin());
  size_t i = 0;
  for (auto it = outArray->begin();
       it != outArray->end(); ++it, ++i) {
    ASSERT_EQ(*it, outStlArray[i]);
  }
  value = shad::partial_sum(array2_->begin(), array2_->end(),
                            outArray->begin(), std::multiplies<size_t>());
  exp_value = std::partial_sum(stl_array2_.begin(), stl_array2_.end(),
                               outStlArray.begin(), std::multiplies<size_t>());
  i = 0;
  for (auto it = outArray->begin();
       it != outArray->end(); ++it, ++i) {
    ASSERT_EQ(*it, outStlArray[i]);
  }
}


TEST_F(NumericTest, reduce) {
  size_t value = shad::reduce(array_->begin(), array_->end(), kInitValue);
  size_t exp_value = std::accumulate(stl_array_.begin(), stl_array_.end(),
                                 kInitValue);
  ASSERT_EQ(value, exp_value);
  value = shad::reduce(shad::distributed_parallel_tag{},
                       array_->begin(), array_->end(), kInitValue,
                       std::multiplies<size_t>());
  exp_value = std::accumulate(stl_array_.begin(), stl_array_.end(),
                          kInitValue, std::multiplies<size_t>());
  ASSERT_EQ(value, exp_value);
}

TEST_F(NumericTest, inclusive_scan) {
  auto outArray = shad::array<size_t, kArraySize>::Create();
  outArray->fill(42lu);
  std::array<size_t, kArraySize> outStlArray;
  auto value = shad::inclusive_scan(shad::distributed_parallel_tag{},
                                    array2_->begin(), array2_->end(),
                                    outArray->begin(), std::plus<size_t>(), 0lu);
  auto exp_value = std::partial_sum(stl_array2_.begin(), stl_array2_.end(),
                                    outStlArray.begin());
  size_t i = 0;
  for (auto it = outArray->begin();
       it != outArray->end(); ++it, ++i) {
    ASSERT_EQ(*it, outStlArray[i]);
  }
  value = shad::inclusive_scan(shad::distributed_sequential_tag{},
                               array2_->begin(), array2_->end(),
                               outArray->begin(), std::plus<size_t>(), 0lu);
  i = 0;
  for (auto it = outArray->begin();
       it != outArray->end(); ++it, ++i) {
    ASSERT_EQ(*it, outStlArray[i]);
  }
  
  value = shad::inclusive_scan(array2_->begin(), array2_->end(),
                                     outArray->begin());

  i = 0;
  for (auto it = outArray->begin();
       it != outArray->end(); ++it, ++i) {
//     std::cout << "[" << i << "] *out = " << *it << ", *in = " << array2_->at(i)
//     << ", *exp = " << outStlArray[i] << std::endl;
    ASSERT_EQ(*it, outStlArray[i]);
  }
  return;
  value = shad::inclusive_scan(shad::distributed_parallel_tag{},
                                array2_->begin(), array2_->end(),
                                outArray->begin());
  i = 0;
  for (auto it = outArray->begin();
       it != outArray->end(); ++it, ++i) {
    ASSERT_EQ(*it, outStlArray[i]);
  }
}

TEST_F(NumericTest, exclusive_scan) {
  auto outArray = shad::array<size_t, kArraySize>::Create();
  outArray->fill(42lu);
  std::array<size_t, kArraySize> outStlArray;
  auto value = shad::exclusive_scan(array2_->begin(), array2_->end(),
                                    outArray->begin(), kInitValue,
                                    std::plus<size_t>());
  auto exp_value = std::partial_sum(stl_array2_.begin(), stl_array2_.end(),
                                    outStlArray.begin());
  size_t i = 0;
  for (auto it = outArray->begin();
       it != outArray->end(); ++it, ++i) {
    auto val = outStlArray[i];
    if (i > 0) val = val - stl_array2_[i] + kInitValue;
    else val = kInitValue;
//     std::cout << "[" << i << "] *out = " << *it << ", *in = " << array2_->at(i)
//     << ", *exp = " << val << std::endl;
    ASSERT_EQ(*it, val);
  }
  value = shad::exclusive_scan(shad::distributed_parallel_tag{},
                               array2_->begin(), array2_->end(),
                               outArray->begin(), kInitValue);
  i = 0;
  for (auto it = outArray->begin();
       it != outArray->end(); ++it, ++i) {
    auto val = outStlArray[i];
    if (i > 0) val = val - stl_array2_[i] + kInitValue;
    else val = kInitValue;
/*    std::cout << "[" << i << "] *out = " << *it << ", *in = " << array2_->at(i)
    << ", *exp = " << val << std::endl;  */  
//     ASSERT_EQ(*it, val);
  }
}

TEST_F(NumericTest, transform_reduce) {
  size_t value = 0;
  size_t exp_value = std::inner_product(stl_array_.begin(), stl_array_.end(),
                                        stl_array2_.begin(), kInitValue);
  size_t i = 0;
  for (auto it = array2_->begin();
       it != array2_->end(); ++it, ++i) {
    std::cout << "[" << i << "] *it = " << *it << std::endl;
  }
  value = shad::transform_reduce(array_->begin(), array_->end(),
                                 array2_->begin(), kInitValue);
  ASSERT_EQ(value, exp_value);
  value = shad::transform_reduce(shad::distributed_parallel_tag{},
                                 array_->begin(), array_->end(),
                                 array2_->begin(), kInitValue);
  ASSERT_EQ(value, exp_value);
}