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

#include "shad/core/vector.h"
#include "shad/runtime/runtime.h"

template <typename Type, std::size_t Size>
struct VectorTestPair {
  using value_type = Type;
  constexpr static std::size_t size = Size;
};

template <typename Type, std::size_t Size>
constexpr std::size_t VectorTestPair<Type, Size>::size;

template <typename PairType>
class VectorTest : public ::testing::Test {
 public:
  using ElementType = typename PairType::value_type;
  using VectorType = shad::vector<ElementType>;
  using size_type = typename VectorType::size_type;

  VectorTest() : vector_(PairType::size) {}
  
  void SetUp() {
    for (size_type i = 0; i < vector_.size(); ++i) vector_.at(i) = i;
  }

 protected:
  VectorType vector_;
};

TYPED_TEST_CASE_P(VectorTest);

TYPED_TEST_P(VectorTest, HasTypeInterface) {
  using VectorType = typename VectorTest<TypeParam>::VectorType;
  ASSERT_TRUE((std::is_same<typename VectorType::value_type,
                            typename TypeParam::value_type>::value));
  ASSERT_TRUE((std::is_integral<typename VectorType::size_type>::value));
  ASSERT_TRUE((std::is_integral<typename VectorType::difference_type>::value));

  ASSERT_TRUE((std::is_convertible<typename VectorType::reference,
                                   typename TypeParam::value_type>::value));
  ASSERT_TRUE((std::is_convertible<typename VectorType::const_reference,
                                   typename TypeParam::value_type>::value));
}

TYPED_TEST_P(VectorTest, Size) {
  ASSERT_EQ(this->vector_.size(), TypeParam::size);
  ASSERT_EQ(this->vector_.max_size(), TypeParam::size);
}

TYPED_TEST_P(VectorTest, AccessMethods) {
  using size_type = typename VectorTest<TypeParam>::size_type;
  using VectorType = typename VectorTest<TypeParam>::VectorType;

  for (size_type i = 0; i < this->vector_.size(); ++i) {
    ASSERT_EQ(this->vector_[i], i);
    ASSERT_EQ(this->vector_.at(i), i);
    ASSERT_EQ(this->vector_[i], this->vector_.at(i));
  }

  try {
    this->vector_.at(this->vector_.size());
    FAIL();
  } catch (std::out_of_range &) {
    SUCCEED();
  } catch (...) {
    FAIL();
  }

  ASSERT_EQ(this->vector_[0], this->vector_.front());
  ASSERT_EQ(this->vector_[this->vector_.size() - 1], this->vector_.back());

  // const_reference portion
  using const_reference = typename VectorType::const_reference;
  const VectorType &ref = this->vector_;
  for (size_type i = 0; i < this->vector_.size(); ++i) {
    const_reference first = ref[i];
    const_reference second = ref.at(i);
    ASSERT_EQ(first, i);
    ASSERT_EQ(second, i);
    ASSERT_EQ(first, second);
  }

  try {
    const_reference _ = ref.at(this->vector_.size());
    FAIL();
  } catch (std::out_of_range &) {
    SUCCEED();
  } catch (...) {
    FAIL();
  }

  ASSERT_EQ(ref[0], ref.front());
  ASSERT_EQ(ref[ref.size() - 1], ref.back());
}

TYPED_TEST_P(VectorTest, IteratorMovements) {
  using VectorType = typename VectorTest<TypeParam>::VectorType;

  using iterator = typename VectorType::iterator;

  ssize_t i = 0;
  for (iterator itr = this->vector_.begin(), end = this->vector_.end();
       itr != end; ++itr, ++i) {
    ASSERT_EQ(*itr, this->vector_[i]);
  }

  i = this->vector_.size() - 1;
  for (iterator itr = this->vector_.end() - 1, end = this->vector_.begin();
       itr >= end; --itr, --i) {
    ASSERT_EQ(*itr, this->vector_[i]);
  }

  iterator b = this->vector_.begin();
  for (i = 0; i < this->vector_.size();
       i += shad::rt::numLocalities(), b += shad::rt::numLocalities()) {
    ASSERT_EQ(*b, this->vector_[i]);
    ASSERT_EQ(*(this->vector_.begin() + i), this->vector_[i]);
  }

  iterator e = this->vector_.end() - 1;
  for (i = 0; i < this->vector_.size();
       i -= shad::rt::numLocalities(), e -= shad::rt::numLocalities()) {
    ASSERT_EQ(*e, this->vector_[this->vector_.size() - (i + 1)]);
    ASSERT_EQ(*(this->vector_.end() - (i + 1)),
              this->vector_[this->vector_.size() - (i + 1)]);
  }

  size_t pivot = this->vector_.size() % shad::rt::numLocalities();
  size_t block = this->vector_.size() / shad::rt::numLocalities();
  size_t offset = 0;
  for (size_t i = 0; i < shad::rt::numLocalities();
       ++i, offset += pivot != 0 && i >= pivot ? (block - 1) : block) {
    if (offset < this->vector_.size())
      ASSERT_EQ(*(this->vector_.begin() + offset), this->vector_[offset]);
    if (offset == this->vector_.size())
      ASSERT_EQ((this->vector_.begin() + offset), this->vector_.end());
    ASSERT_EQ(*(this->vector_.begin() + offset), offset);
  }
}

REGISTER_TYPED_TEST_CASE_P(VectorTest, HasTypeInterface, Size, AccessMethods,
                           IteratorMovements);

using VectorTestTypes =
    ::testing::Types<VectorTestPair<size_t, 900>, VectorTestPair<size_t, 901>,
                     VectorTestPair<size_t, 902>, VectorTestPair<size_t, 42>>;
INSTANTIATE_TYPED_TEST_CASE_P(ShadVector, VectorTest, VectorTestTypes);
