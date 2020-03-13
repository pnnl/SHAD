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

#ifndef INCLUDE_SHAD_EXTENSIONS_DATA_TYPES_DATA_TYPES_H_
#define INCLUDE_SHAD_EXTENSIONS_DATA_TYPES_DATA_TYPES_H_

#include <ctime>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>


namespace shad {


namespace data_types {
enum data_t {
  STRING = 0,  // string support is currenlty limited
  CHARS,       // sequence of characters
  UINT,        // unsigned
  INT,         // int
  FLOAT,       // float
  DOUBLE,      // double
  BOOL,        // bool
  DATE,        // date in "%y-%m-%d" format
  USDATE,      // date in "%m/%d/%y" format
  DATE_TIME,   // date in "%y-%m-%dT%H:%M:%S" format
  IP_ADDRESS,  // IPv4
  LIST_UINT,   // Sequence of unsigneds, support currently limited
  LIST_INT,    // Sequence of integers, support currently limited
  LIST_DOUBLE, // Sequence of doubles, support currently limited
  NONE
};

  template <typename ENC_t>
  constexpr ENC_t kNullValue = ENC_t();
  
  template <>
  constexpr uint64_t kNullValue<uint64_t> = std::numeric_limits<int64_t>::max();
  
  using schema_t = std::vector<std::pair<std::string, data_t>>;  

  template <typename ENC_t, data_t ST>
  ENC_t encode(std::string &str);

  template <typename ENC_t, size_t MAX_s, data_t ST>
  std::array<ENC_t, MAX_s> encode(std::string &str) {
    std::array<ENC_t, MAX_s> res;
    if (str.size() > 0) {
      memcpy(res.data(), str.data(), sizeof(ENC_t)*MAX_s);
    } else {
      res.fill('\0');
    }
    return res;
  }

  template <typename ENC_t, data_t ST>
  std::string decode(ENC_t value);

  template <typename ENC_t, size_t MAX_s, data_t ST>
  std::string decode(std::array<ENC_t, MAX_s> &val) {
    return std::string(reinterpret_cast<const char*>(val.data()));
  }
}  // namespace data_types


// METHODS SPECIALIZATION FOR UINT64 ENC_t
template<>
uint64_t data_types::encode<uint64_t, data_types::UINT>(std::string &str) {
  uint64_t value;
  try { value = std::stoull(str); }
  catch(...) { value = kNullValue<uint64_t>; }
  return value;
}

template<>
uint64_t data_types::encode<uint64_t, data_types::INT>(std::string &str) {
  uint64_t encval;
  int64_t value;
  try { value = stoll(str); }
  catch(...) { return kNullValue<uint64_t>; }
  memcpy(&encval, &value, sizeof(value));
  return encval;
}

template<>
uint64_t data_types::encode<uint64_t, data_types::FLOAT>(std::string &str) {
  uint64_t encval;
  float value;
  try { value = stof(str); }
  catch(...) { return kNullValue<uint64_t>; }
  memcpy(&encval, &value, sizeof(value));
  return encval;
}

template<>
uint64_t data_types::encode<uint64_t, data_types::DOUBLE>(std::string &str) {
  uint64_t encval;
  double value;
  try { value = stod(str); }
  catch(...) { return kNullValue<uint64_t>; }
  memcpy(&encval, &value, sizeof(value));
  return encval;
}

template<>
uint64_t data_types::encode<uint64_t, data_types::BOOL>(std::string &str) {
  if (str.size() == 0) return kNullValue<uint64_t>;
  uint64_t encval = 1;
  if ((str == "F") || (str == "f") || (str == "FALSE") 
                   || (str == "false") || (str == "0")) encval = 0;
  return encval;
}


template<>
uint64_t data_types::encode<uint64_t, data_types::CHARS>(std::string &str) {
  uint64_t encval = 0;
  memset(&encval, '\0', sizeof(encval));
  memcpy(&encval, str.c_str(), sizeof(encval)-1);
  return encval;
}

template<>
uint64_t data_types::encode<uint64_t,
                            data_types::IP_ADDRESS>(std::string &str) {
  uint64_t val, value = 0;
  std::string::iterator start = str.begin();
  for (unsigned i = 0; i < 4; i ++) {
    std::string::iterator end = std::find(start, str.end(), '.');
    try {
      val = std::stoull(std::string(start, end));
    } catch(...) {
      return kNullValue<uint64_t>;
    }
    if (val < 256) {
      value = (value << 8) + val; start = end + 1;
    } else {
      return kNullValue<uint64_t>;
    }
  }
  return value;
}

template<>
uint64_t data_types::encode<uint64_t, data_types::DATE>(std::string &str) {
  uint64_t value = 0;
  struct tm date{};
  date.tm_isdst = -1;
  strptime(str.c_str(), "%Y-%m-%d", &date);
  time_t t;
  try {
    t = mktime(&date);
  }
  catch(...) {
    return kNullValue<uint64_t>;
  }
  memcpy(&value, &t, sizeof(value));
  return value;
}

template<>
uint64_t data_types::encode<uint64_t, data_types::USDATE>(std::string &str) {
  uint64_t value = 0;
  struct tm date{};
  date.tm_isdst = -1;
  strptime(str.c_str(), "%m/%d/%y", &date);
  time_t t;
  try {
    t = mktime(&date);
  }
  catch(...) {
    return kNullValue<uint64_t>;
  }
  memcpy(&value, &t, sizeof(value));
  return value;
}

template<>
uint64_t data_types::encode<uint64_t,
                            data_types::DATE_TIME>(std::string &str) {
  uint64_t value = 0;
  struct tm date{};
  date.tm_isdst = -1;
  strptime(str.c_str(), "%Y-%m-%dT%H:%M:%S", &date);
  time_t t;
  try {
    t = mktime(&date);
  }
  catch(...) {
    return kNullValue<uint64_t>;
  }
  memcpy(&value, &t, sizeof(value));
  return value;
}

template<>
std::string data_types::decode<uint64_t, data_types::UINT>(uint64_t value) {
  if (value == kNullValue<uint64_t>) return "";
  return std::to_string(value);
}

template<>
std::string data_types::decode<uint64_t, data_types::INT>(uint64_t value) {
  if (value == kNullValue<uint64_t>) return "";
  int64_t v;
  memcpy(&v, &value, sizeof(v));
  return std::to_string(v);
}

template<>
std::string data_types::decode<uint64_t, data_types::FLOAT>(uint64_t value) {
  if (value == kNullValue<uint64_t>) return "";
  float v;
  memcpy(&v, &value, sizeof(v));
  return std::to_string(v);
}

template<>
std::string data_types::decode<uint64_t, data_types::DOUBLE>(uint64_t value) {
  if (value == kNullValue<uint64_t>) return "";
  double v;
  memcpy(&v, &value, sizeof(v));
  return std::to_string(v);
}

template<>
std::string data_types::decode<uint64_t, data_types::BOOL>(uint64_t value) {
  if (value == kNullValue<uint64_t>) return "";
  return std::to_string(value);
}

template<>
std::string data_types::decode<uint64_t, data_types::CHARS>(uint64_t value) {
  const char* c = reinterpret_cast<const char*>(&value);
  return std::string(c);
}

}  // namespace shad

#endif  // INCLUDE_SHAD_EXTENSIONS_DATA_TYPES_DATA_TYPES_H_
