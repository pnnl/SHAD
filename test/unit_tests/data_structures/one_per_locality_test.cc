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

#include "gtest/gtest.h"

#include "shad/data_structures/one_per_locality.h"
#include "shad/runtime/runtime.h"

class OnePerLocalityTest : public ::testing::Test {};

class AClass {
 public:
  AClass(int a, float b) : a_(a), b_(b) {}
  int a_;
  float b_;
};

TEST_F(OnePerLocalityTest, CreationDestruction) {
  auto anInt = shad::OnePerLocality<int>::Create();

  shad::rt::executeOnAll(
      [](const shad::OnePerLocality<int>::ObjectID& oid) {
        ASSERT_TRUE(shad::OnePerLocality<int>::GetPtr(oid) != nullptr);
      },
      anInt->GetGlobalID());

  ASSERT_TRUE(anInt != nullptr);

  int a = static_cast<int>(*anInt);
  ASSERT_EQ(a, 0);

  auto anotherInt = shad::OnePerLocality<int>::Create(10);
  ASSERT_EQ(static_cast<int>(*anotherInt), 10);

  shad::OnePerLocality<int>::Destroy(anInt->GetGlobalID());
  shad::OnePerLocality<int>::Destroy(anotherInt->GetGlobalID());

  auto anObject = shad::OnePerLocality<AClass>::Create(10, 1.0f);

  shad::rt::executeOnAll(
      [](const shad::OnePerLocality<AClass>::ObjectID& oid) {
        auto ptr = shad::OnePerLocality<AClass>::GetPtr(oid);
        ASSERT_TRUE(ptr != nullptr);
        ASSERT_EQ((*ptr)->a_, 10);
        ASSERT_EQ((*ptr)->b_, 1.0f);
      },
      anObject->GetGlobalID());

  shad::OnePerLocality<AClass>::Destroy(anObject->GetGlobalID());
}

TEST_F(OnePerLocalityTest, AccessOnAllLocalities) {
  auto anInt = shad::OnePerLocality<int>::Create();

  shad::rt::executeOnAll(
      [](const shad::OnePerLocality<int>::ObjectID& oid) {
        auto localPtr = shad::OnePerLocality<int>::GetPtr(oid);
        ASSERT_TRUE(localPtr != nullptr);
        (*localPtr) = 10 + static_cast<uint32_t>(shad::rt::thisLocality());
      },
      anInt->GetGlobalID());

  shad::rt::executeOnAll(
      [](const shad::OnePerLocality<int>::ObjectID& oid) {
        auto localPtr = shad::OnePerLocality<int>::GetPtr(oid);
        ASSERT_TRUE(localPtr != nullptr);
        uint32_t expectedValue =
            10 + static_cast<uint32_t>(shad::rt::thisLocality());
        ASSERT_EQ(static_cast<int>(*localPtr), expectedValue);
      },
      anInt->GetGlobalID());

  shad::OnePerLocality<int>::Destroy(anInt->GetGlobalID());
}
