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

#include <string>

#include "gtest/gtest.h"

#include "shad/extensions/data_types/data_types.h"

static const unsigned int kUintValue = 4242;
static const int kIntValue = -4242;
static const float kFloatValue = 42.42;
static const double kDoubleValue = 42.4242;

static std::string kUintString = "4242";
static std::string kIntString = "-4242";
static std::string kFloatString = "42.42";
static std::string kDoubleString = "42.4242";
static std::string kString = "Mamba_24";


class DataTypesTest : public ::testing::Test {
 public:
  DataTypesTest() {}
  void SetUp() {}
  void TearDown() {}
};

TEST_F(DataTypesTest, UintEncodeDecodeTest) {
  using ENC_t = uint64_t;
  using IN_t = std::string;
  using DEC_t = std::string;

  auto encUint = shad::data_types::encode<ENC_t, IN_t,
                                          shad::data_types::UINT>(kUintString);
  auto encInt = shad::data_types::encode<ENC_t, IN_t,
                                         shad::data_types::INT>(kIntString);
  auto encFloat = shad::data_types::encode<ENC_t, IN_t,
                                           shad::data_types::FLOAT>(kFloatString);
  auto encDouble = shad::data_types::encode<ENC_t, IN_t,
                                            shad::data_types::DOUBLE>(kDoubleString);
  auto encChars = shad::data_types::encode<ENC_t, IN_t,
                                           shad::data_types::CHARS>(kString);

  auto decUint = shad::data_types::decode<ENC_t, DEC_t, shad::data_types::UINT>(encUint);
  auto decInt = shad::data_types::decode<ENC_t, DEC_t, shad::data_types::INT>(encInt);
  auto decFloat = shad::data_types::decode<ENC_t, DEC_t, shad::data_types::FLOAT>(encFloat);
  auto decDouble = shad::data_types::decode<ENC_t, DEC_t, shad::data_types::DOUBLE>(encDouble);
  ASSERT_EQ(decUint, kUintString);
}