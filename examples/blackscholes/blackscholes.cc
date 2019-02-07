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
//===----------------------------------------------------------------------===/

// This example shows the expressiveness of the shad core API through a code
// that computes the price of stock options, on the very same line as the
// well-known black-scholes benchmark, from the PARSEC suite.
//
// Some input datasets for this example can be found in the PARSEC distribution.

#include <algorithm>
#include <cassert>
#include <fstream>
#include <iostream>
#include <limits>

#include "shad/core/algorithm.h"
#include "shad/core/array.h"
#include "shad/util/measure.h"

#include "blackscholes_wrap.h"

// The array size must be equal to the number of lines in the input file.
constexpr size_t n_options = 1 << 16;

shad::array<option_t, n_options> read_options(const std::string &fname) {
  shad::array<option_t, n_options> options;
  std::ifstream in_file(fname);
  assert(in_file.is_open());
  size_t i = 0;
  while (in_file.good()) {
    std::string opt_line;
    std::getline(in_file, opt_line, '\n');
    options[i] = parse_option(opt_line);
  }
  return options;
}

price_t reference(const shad::array<option_t, n_options> &in) {
  price_t max_price = std::numeric_limits<price_t>::min();
  for (auto opt : in) max_price = std::max(max_price, black_scholes(opt));
  return max_price;
}

price_t std_algorithms(const shad::array<option_t, n_options> &in) {
  price_t max_price;
  shad::array<price_t, n_options> prices;
  std::transform(in.begin(), in.end(), prices.begin(), black_scholes);
  auto max_price_it = std::max_element(prices.begin(), prices.end());
  return *max_price_it;
}

price_t shad_algorithms(const shad::array<option_t, n_options> &in) {
  price_t max_price;
  shad::array<price_t, n_options> prices;
  shad::transform(shad::distributed_parallel_tag{}, in.begin(), in.end(),
                  prices.begin(), black_scholes);
  auto max_price_it = shad::max_element(shad::distributed_parallel_tag{},
                                        prices.begin(), prices.end());
  return *max_price_it;
}

namespace shad {

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cout << "usage: " << argv[0] << " file" << std::endl;
    return -1;
  }

  // read input data
  auto in = read_options(argv[1]);

  // sequential reference
  price_t max_price;
  auto exec_time = shad::measure<std::chrono::nanoseconds>::duration(
      [&]() { max_price = reference(in); });
  std::cout << "> reference took " << exec_time.count()
            << " ns (res = " << max_price << ")" << std::endl;

  // STL algorithms
  exec_time = shad::measure<std::chrono::nanoseconds>::duration(
      [&]() { max_price = std_algorithms(in); });
  std::cout << "> reference took " << exec_time.count()
            << " ns (res = " << max_price << ")" << std::endl;

  // shad algorithms
  exec_time = shad::measure<std::chrono::nanoseconds>::duration(
      [&]() { max_price = shad_algorithms(in); });
  std::cout << "> reference took " << exec_time.count()
            << " ns (res = " << max_price << ")" << std::endl;

  return 0;
}

}  // namespace shad
