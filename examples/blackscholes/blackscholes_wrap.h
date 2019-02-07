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

#include "blackscholes.h"

typedef fptype price_t;

// stock-option descriptor
struct option_t {
  char name[8];
  fptype s;       // spot price
  fptype strike;  // strike price
  fptype r;       // risk-free interest rate
  fptype divq;    // dividend rate
  fptype v;       // volatility
  fptype t;  // time to maturity or option expiration in years (1yr = 1.0, 6mos
             // = 0.5, 3mos = 0.25, ..., etc)
  int OptionType;   // Option type.  "P"=PUT, "C"=CALL
  fptype divs;      // dividend vals (not used in this test)
  fptype DGrefval;  // DerivaGem Reference Value
};

// parses a stock-options descriptor from a string of space-separated fields
option_t parse_option(const std::string &opt_line) {
  option_t opt;
  char otype;
  std::stringstream ins(opt_line);

  ins >> opt.name;
  ins >> opt.s >> opt.strike >> opt.r >> opt.divq;
  ins >> opt.v >> opt.t >> otype >> opt.divs >> opt.DGrefval;
  opt.OptionType = (otype == 'P');

  return opt;
}

// computes the price from a stock-option descriptor
fptype black_scholes(const option_t &opt) {
  return BlkSchlsEqEuroNoDiv(opt.s, opt.strike, opt.r, opt.v, opt.t,
                             opt.OptionType, 0);
}
