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
#include "shad/runtime/runtime.h"

template <typename Type, std::size_t Size>
struct ArrayTestPair {
  using value_type = Type;
  constexpr static std::size_t size = Size;
};

template <typename Type, std::size_t Size>
constexpr std::size_t ArrayTestPair<Type, Size>::size;

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
  using ArrayType = typename ArrayTest<TypeParam>::ArrayType;

  for (size_type i = 0; i < this->array_.size(); ++i) {
    ASSERT_EQ(this->array_[i], i);
    ASSERT_EQ(this->array_.at(i), i);
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

  // const_reference portion
  using const_reference = typename ArrayType::const_reference;
  const ArrayType &ref = this->array_;
  for (size_type i = 0; i < this->array_.size(); ++i) {
    const_reference first = ref[i];
    const_reference second = ref.at(i);
    ASSERT_EQ(first, i);
    ASSERT_EQ(second, i);
    ASSERT_EQ(first, second);
  }

  try {
    const_reference _ = ref.at(this->array_.size());
    FAIL();
  } catch (std::out_of_range &) {
    SUCCEED();
  } catch (...) {
    FAIL();
  }

  ASSERT_EQ(ref[0], ref.front());
  ASSERT_EQ(ref[ref.size() - 1], ref.back());
}

TYPED_TEST_P(ArrayTest, IteratorMovements) {
  using ArrayType = typename ArrayTest<TypeParam>::ArrayType;

  using iterator = typename ArrayType::iterator;

  ssize_t i = 0;
  for (iterator itr = this->array_.begin(), end = this->array_.end();
       itr != end; ++itr, ++i) {
    ASSERT_EQ(*itr, this->array_[i]);
  }

  i = this->array_.size() - 1;
  for (iterator itr = this->array_.end() - 1, end = this->array_.begin();
       itr >= end; --itr, --i) {
    ASSERT_EQ(*itr, this->array_[i]);
  }

  iterator b = this->array_.begin();
  for (i = 0; i < this->array_.size();
       i += shad::rt::numLocalities(), b += shad::rt::numLocalities()) {
    ASSERT_EQ(*b, this->array_[i]);
    ASSERT_EQ(*(this->array_.begin() + i), this->array_[i]);
  }

  iterator e = this->array_.end() - 1;
  for (i = 0; i < this->array_.size();
       i -= shad::rt::numLocalities(), e -= shad::rt::numLocalities()) {
    ASSERT_EQ(*e, this->array_[this->array_.size() - (i + 1)]);
    ASSERT_EQ(*(this->array_.end() - (i + 1)),
              this->array_[this->array_.size() - (i + 1)]);
  }

  size_t pivot = this->array_.size() % shad::rt::numLocalities();
  size_t block = this->array_.size() / shad::rt::numLocalities();
  size_t offset = 0;
  for (size_t i = 0; i < shad::rt::numLocalities();
       ++i, offset += pivot != 0 && i >= pivot ? (block - 1) : block) {
    if (offset < this->array_.size())
      ASSERT_EQ(*(this->array_.begin() + offset), this->array_[offset]);
    if (offset == this->array_.size())
      ASSERT_EQ((this->array_.begin() + offset), this->array_.end());
    ASSERT_EQ(*(this->array_.begin() + offset), offset);
  }
}

REGISTER_TYPED_TEST_CASE_P(ArrayTest, HasTypeInterface, Size, AccessMethods,
                           IteratorMovements);

using ArrayTestTypes =
    ::testing::Types<ArrayTestPair<size_t, 900>, ArrayTestPair<size_t, 901>,
                     ArrayTestPair<size_t, 902>, ArrayTestPair<size_t, 42>>;
INSTANTIATE_TYPED_TEST_CASE_P(ShadArray, ArrayTest, ArrayTestTypes);
