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

#include <type_traits>
#include <utility>

#include "gtest/gtest.h"

#include "shad/core/array.h"

template <typename Type, std::size_t Size>
struct ArrayTestPair {
  using value_type = Type;
  constexpr static std::size_t size = Size;
};

template <typename PairType>
class ArrayTest : public ::testing::Test {
 public:
  using ElementType = typename PairType::value_type;
  using ArrayType = shad::array<ElementType, PairType::size>;
  using size_type = typename ArrayType::size_type;

  void SetUp() {
    for (size_type i = 0; i < array_.size(); ++i) array_.at(i) = i;
  }

 protected:
  ArrayType array_;
};

TYPED_TEST_CASE_P(ArrayTest);

TYPED_TEST_P(ArrayTest, HasTypeInterface) {
  using ArrayType = typename ArrayTest<TypeParam>::ArrayType;
  ASSERT_TRUE((std::is_same<typename ArrayType::value_type,
                            typename TypeParam::value_type>::value));
  ASSERT_TRUE((std::is_integral<typename ArrayType::size_type>::value));
  ASSERT_TRUE((std::is_integral<typename ArrayType::difference_type>::value));

  ASSERT_TRUE((std::is_convertible<typename ArrayType::reference,
                                   typename TypeParam::value_type>::value));
  ASSERT_TRUE((std::is_convertible<typename ArrayType::const_reference,
                                   typename TypeParam::value_type>::value));
}

TYPED_TEST_P(ArrayTest, Size) {
  ASSERT_EQ(this->array_.size(), TypeParam::size);
  ASSERT_EQ(this->array_.max_size(), TypeParam::size);
}

TYPED_TEST_P(ArrayTest, AccessMethods) {
  using size_type = typename ArrayTest<TypeParam>::size_type;

  for (size_type i = 0; i < this->array_.size(); ++i) {
    ASSERT_EQ(this->array_[i], this->array_.at(i));
  }

  try {
    this->array_.at(this->array_.size());
    FAIL();
  } catch (std::out_of_range &) {
    SUCCEED();
  } catch (...) {
    FAIL();
  }

  ASSERT_EQ(this->array_[0], this->array_.front());
  ASSERT_EQ(this->array_[this->array_.size() - 1], this->array_.back());
}

REGISTER_TYPED_TEST_CASE_P(ArrayTest, HasTypeInterface, Size, AccessMethods);

using ArrayTestTypes =
    ::testing::Types<ArrayTestPair<char, 256>, ArrayTestPair<size_t, 125>>;
INSTANTIATE_TYPED_TEST_CASE_P(ShadArray, ArrayTest, ArrayTestTypes);
