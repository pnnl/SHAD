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

#ifndef TEST_UNIT_TESTS_CORE_STL_EMULATION_NUMERIC_H_
#define TEST_UNIT_TESTS_CORE_STL_EMULATION_NUMERIC_H_

namespace shad_test_stl {

template <class ForwardIterator, class T>
void iota_(ForwardIterator first, ForwardIterator last, T value) {
  while (first != last) {
    *first++ = value;
    ++value;
  }
}

template <class InputIt, class T, class BinaryOperation>
T accumulate_(InputIt first, InputIt last, T init, BinaryOperation op) {
  for (; first != last; ++first) {
    init = op(std::move(init), *first);  // std::move since C++20
  }
  return init;
}

template <class InputIt1, class InputIt2, class T, class BinaryOperation1,
          class BinaryOperation2>
T inner_product_(InputIt1 first1, InputIt1 last1, InputIt2 first2, T init,
                 BinaryOperation1 op1, BinaryOperation2 op2) {
  while (first1 != last1) {
    init =
        op1(std::move(init), op2(*first1, *first2));  // std::move since C++20
    ++first1;
    ++first2;
  }
  return init;
}

template <class InputIt, class OutputIt, class BinaryOperation>
OutputIt adjacent_difference_(InputIt first, InputIt last, OutputIt d_first,
                              BinaryOperation op) {
  if (first == last) return d_first;

  typedef typename std::iterator_traits<InputIt>::value_type value_t;
  value_t acc = *first;
  *d_first = acc;
  while (++first != last) {
    value_t val = *first;
    *++d_first = op(val, std::move(acc));  // std::move since C++20
    acc = std::move(val);
  }
  return ++d_first;
}

template <class InputIt, class OutputIt, class BinaryOperation>
OutputIt partial_sum_(InputIt first, InputIt last, OutputIt d_first,
                      BinaryOperation op) {
  if (first == last) return d_first;

  typename std::iterator_traits<InputIt>::value_type sum = *first;
  *d_first = sum;

  while (++first != last) {
    sum = op(std::move(sum), *first);  // std::move since C++20
    *++d_first = sum;
  }
  return ++d_first;
}

template <class InputIt, class OutputIt, class BinaryOperation, class T>
OutputIt inclusive_scan_(InputIt first, InputIt last, OutputIt d_first,
                         BinaryOperation binary_op, T init) {
  if (first == last) return d_first;

  T sum = binary_op(std::move(init), *first);
  *d_first = sum;

  while (++first != last) {
    sum = binary_op(std::move(sum), *first);  // std::move since C++20
    *++d_first = sum;
  }
  return ++d_first;
}

template <class InputIt, class OutputIt, class T, class BinaryOperation>
OutputIt exclusive_scan_(InputIt first, InputIt last, OutputIt d_first, T init,
                         BinaryOperation binary_op) {
  if (first == last) return d_first;

  T sum = std::move(init);

  do {
    *d_first = sum;
    sum = binary_op(std::move(sum), *first);  // std::move since C++20
    ++d_first;
  } while (++first != last);
  return ++d_first;
}

template <class InputIt1, class InputIt2, class T, class BinaryOp1,
          class BinaryOp2>
T transform_reduce_(InputIt1 first1, InputIt1 last1, InputIt2 first2, T init,
                    BinaryOp1 binary_op1, BinaryOp2 binary_op2) {
  return inner_product_(first1, last1, first2, init, binary_op1, binary_op2);
}

template <class InputIt, class T, class BinaryOp, class UnaryOp>
T transform_reduce_(InputIt first, InputIt last, T init, BinaryOp binop,
                    UnaryOp unary_op) {
  using val_t = typename std::iterator_traits<InputIt>::value_type;
  auto combine_f = [&](const val_t &v1, const val_t &v2) {
    return unary_op(v1);
  };
  return inner_product_(first, last, first, init, binop, combine_f);
}

template <class InputIt, class OutputIt, class BinaryOperation,
          class UnaryOperation, class T>
OutputIt transform_inclusive_scan_(InputIt first, InputIt last,
                                   OutputIt d_first, BinaryOperation binary_op,
                                   UnaryOperation unary_op, T init) {
  if (first == last) return d_first;

  T sum = binary_op(std::move(init), unary_op(*first));
  *d_first = sum;

  while (++first != last) {
    sum = binary_op(std::move(sum), unary_op(*first));  // std::move since C++20
    *++d_first = sum;
  }
  return ++d_first;
}

template <class InputIt, class OutputIt, class T, class BinaryOperation,
          class UnaryOperation>
OutputIt transform_exclusive_scan_(InputIt first, InputIt last,
                                   OutputIt d_first, T init,
                                   BinaryOperation binary_op,
                                   UnaryOperation unary_op) {
  if (first == last) return d_first;

  T sum = unary_op(std::move(init));

  do {
    *d_first = sum;
    sum = binary_op(std::move(sum), unary_op(*first));  // std::move since C++20
    ++d_first;
  } while (++first != last);
  return ++d_first;
}

template <class InputIt, class T, class BinaryOp>
T reduce_(InputIt first, InputIt last, T init, BinaryOp binary_op) {
  assert(first != last);
  return accumulate_(first, last, init, binary_op);
}

}  // namespace shad_test_stl

#endif
