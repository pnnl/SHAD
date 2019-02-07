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

#ifndef INCLUDE_SHAD_CORE_NUMERIC_H
#define INCLUDE_SHAD_CORE_NUMERIC_H

#include <functional>
#include <iterator>
#include <tuple>
#include <utility>
#include <vector>

#include "shad/core/execution.h"
#include "shad/core/impl/numeric_ops.h"
#include "shad/distributed_iterator_traits.h"
#include "shad/runtime/runtime.h"

namespace shad {

template <class ForwardIterator, class T>
void iota(ForwardIterator first, ForwardIterator last, T value) {
  return impl::iota(first, last, value);
}

////////////////////////////////////////////////////////////////////////////////
//
// accumulate
//
////////////////////////////////////////////////////////////////////////////////
template <class InputIt, class T, class BinaryOperation>
T accumulate(InputIt first, InputIt last, T init, BinaryOperation op) {
  return impl::accumulate(first, last, init, op);
}

template <class InputIt, class T>
T accumulate(InputIt first, InputIt last, T init) {
  return impl::accumulate(first, last, init, std::plus<T>());
}

////////////////////////////////////////////////////////////////////////////////
//
// inner product
//
////////////////////////////////////////////////////////////////////////////////
template <class InputIt1, class InputIt2, class T>
T inner_product(InputIt1 first1, InputIt1 last1, InputIt2 first2, T init) {
  return impl::inner_product(first1, last1, first2, init);
}

template <class InputIt1, class InputIt2, class T, class BinaryOperation1,
          class BinaryOperation2>
T inner_product(InputIt1 first1, InputIt1 last1, InputIt2 first2, T init,
                BinaryOperation1 op1, BinaryOperation2 op2) {
  return impl::inner_product(first1, last1, first2, init, op1, op2);
}

////////////////////////////////////////////////////////////////////////////////
//
// adjacent_difference
//
////////////////////////////////////////////////////////////////////////////////
template <class InputIt, class OutputIt>
OutputIt adjacent_difference(InputIt first, InputIt last, OutputIt d_first) {
  using value_t = typename distributed_iterator_traits<InputIt>::value_type;
  return impl::adjacent_difference(distributed_sequential_tag{}, first, last,
                                   d_first, std::minus<value_t>());
}

template <typename ExecutionPolicy, class InputIt, class OutputIt>
std::enable_if_t<shad::is_execution_policy<ExecutionPolicy>::value, OutputIt>
adjacent_difference(ExecutionPolicy&& policy, InputIt first, InputIt last,
                    OutputIt d_first) {
  using value_t = typename distributed_iterator_traits<InputIt>::value_type;
  return impl::adjacent_difference(std::forward<ExecutionPolicy>(policy), first,
                                   last, d_first, std::minus<value_t>());
}

template <class InputIt, class OutputIt, class BinaryOperation>
std::enable_if_t<!shad::is_execution_policy<InputIt>::value, OutputIt>
adjacent_difference(InputIt first, InputIt last, OutputIt d_first,
                    BinaryOperation op) {
  return impl::adjacent_difference(distributed_sequential_tag{}, first, last,
                                   d_first, op);
}

template <typename ExecutionPolicy, class InputIt, class OutputIt,
          class BinaryOperation>
OutputIt adjacent_difference(ExecutionPolicy&& policy, InputIt first,
                             InputIt last, OutputIt d_first,
                             BinaryOperation op) {
  return impl::adjacent_difference(std::forward<ExecutionPolicy>(policy), first,
                                   last, d_first, op);
}

////////////////////////////////////////////////////////////////////////////////
//
// partial sum
//
////////////////////////////////////////////////////////////////////////////////
template <class InputIt, class OutputIt>
OutputIt partial_sum(InputIt first, InputIt last, OutputIt d_first) {
  using value_t = typename distributed_iterator_traits<InputIt>::value_type;
  return impl::partial_sum(first, last, d_first, std::plus<value_t>());
}

template <class InputIt, class OutputIt, class BinaryOperation>
OutputIt partial_sum(InputIt first, InputIt last, OutputIt d_first,
                     BinaryOperation op) {
  return impl::partial_sum(first, last, d_first, op);
}

////////////////////////////////////////////////////////////////////////////////
//
// reduce
//
////////////////////////////////////////////////////////////////////////////////
template <class ExecutionPolicy, class ForwardIt, class T, class BinaryOp>
T reduce(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last, T init,
         BinaryOp binary_op) {
  return impl::reduce(std::forward<ExecutionPolicy>(policy), first, last, init,
                      binary_op);
}

template <class InputIt>
typename std::iterator_traits<InputIt>::value_type reduce(InputIt first,
                                                          InputIt last) {
  using val_t = typename std::iterator_traits<InputIt>::value_type;
  return reduce(distributed_sequential_tag{}, first, last, val_t{},
                std::plus<val_t>());
}

template <class ExecutionPolicy, class ForwardIt>
std::enable_if_t<shad::is_execution_policy<ExecutionPolicy>::value,
                 typename std::iterator_traits<ForwardIt>::value_type>
reduce(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last) {
  using val_t = typename std::iterator_traits<ForwardIt>::value_type;
  return reduce(std::forward<ExecutionPolicy>(policy), first, last, val_t{},
                std::plus<val_t>());
}

template <class InputIt, class T>
std::enable_if_t<!shad::is_execution_policy<InputIt>::value, T> reduce(
    InputIt first, InputIt last, T init) {
  return reduce(distributed_sequential_tag{}, first, last, init,
                std::plus<T>());
}

template <class ExecutionPolicy, class ForwardIt, class T>
std::enable_if_t<shad::is_execution_policy<ExecutionPolicy>::value, T> reduce(
    ExecutionPolicy&& policy, ForwardIt first, ForwardIt last, T init) {
  return reduce(std::forward<ExecutionPolicy>(policy), first, last, init,
                std::plus<T>());
}

template <class InputIt, class T, class BinaryOp>
std::enable_if_t<!shad::is_execution_policy<InputIt>::value, T> reduce(
    InputIt first, InputIt last, T init, BinaryOp binary_op) {
  return reduce(distributed_sequential_tag{}, first, last, init, binary_op);
}

////////////////////////////////////////////////////////////////////////////////
//
// exclusive_scan
//
////////////////////////////////////////////////////////////////////////////////
template <class InputIt, class OutputIt, class T>
OutputIt exclusive_scan(InputIt first, InputIt last, OutputIt d_first, T init) {
  return impl::exclusive_scan(shad::distributed_sequential_tag{}, first, last,
                              d_first, std::plus<T>(), init);
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2, class T>
std::enable_if_t<shad::is_execution_policy<ExecutionPolicy>::value, ForwardIt2>
exclusive_scan(ExecutionPolicy&& policy, ForwardIt1 first, ForwardIt1 last,
               ForwardIt2 d_first, T init) {
  return impl::exclusive_scan(std::forward<ExecutionPolicy>(policy), first,
                              last, d_first, std::plus<T>(), init);
}

template <class InputIt, class OutputIt, class T, class BinaryOperation>
std::enable_if_t<!shad::is_execution_policy<InputIt>::value, OutputIt>
exclusive_scan(InputIt first, InputIt last, OutputIt d_first, T init,
               BinaryOperation binary_op) {
  return impl::exclusive_scan(shad::distributed_sequential_tag{}, first, last,
                              d_first, binary_op, init);
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2, class T,
          class BinaryOperation>
ForwardIt2 exclusive_scan(ExecutionPolicy&& policy, ForwardIt1 first,
                          ForwardIt1 last, ForwardIt2 d_first, T init,
                          BinaryOperation binary_op) {
  return impl::exclusive_scan(std::forward<ExecutionPolicy>(policy), first,
                              last, d_first, binary_op, init);
}

////////////////////////////////////////////////////////////////////////////////
//
// inclusive_scan
//
////////////////////////////////////////////////////////////////////////////////
template <class InputIt, class OutputIt>
OutputIt inclusive_scan(InputIt first, InputIt last, OutputIt d_first) {
  return impl::inclusive_scan(shad::distributed_sequential_tag{}, first, last,
                              d_first, std::plus<>());
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2>
std::enable_if_t<shad::is_execution_policy<ExecutionPolicy>::value, ForwardIt2>
inclusive_scan(ExecutionPolicy&& policy, ForwardIt1 first, ForwardIt1 last,
               ForwardIt2 d_first) {
  return impl::inclusive_scan(std::forward<ExecutionPolicy>(policy), first,
                              last, d_first, std::plus<>());
}

template <class InputIt, class OutputIt, class BinaryOperation>
std::enable_if_t<!shad::is_execution_policy<InputIt>::value, OutputIt>
inclusive_scan(InputIt first, InputIt last, OutputIt d_first,
               BinaryOperation binary_op) {
  return impl::inclusive_scan(shad::distributed_sequential_tag{}, first, last,
                              d_first, binary_op);
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2,
          class BinaryOperation>
std::enable_if_t<shad::is_execution_policy<ExecutionPolicy>::value, ForwardIt2>
inclusive_scan(ExecutionPolicy&& policy, ForwardIt1 first, ForwardIt1 last,
               ForwardIt2 d_first, BinaryOperation binary_op) {
  return impl::inclusive_scan(std::forward<ExecutionPolicy>(policy), first,
                              last, d_first, binary_op);
}

template <class InputIt, class OutputIt, class BinaryOperation, class T>
std::enable_if_t<!shad::is_execution_policy<InputIt>::value, OutputIt>
inclusive_scan(InputIt first, InputIt last, OutputIt d_first,
               BinaryOperation binary_op, T init) {
  return impl::inclusive_scan(shad::distributed_sequential_tag{}, first, last,
                              d_first, binary_op, init);
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2,
          class BinaryOperation, class T>
ForwardIt2 inclusive_scan(ExecutionPolicy&& policy, ForwardIt1 first,
                          ForwardIt1 last, ForwardIt2 d_first,
                          BinaryOperation binary_op, T init) {
  return impl::inclusive_scan(std::forward<ExecutionPolicy>(policy), first,
                              last, d_first, binary_op, init);
}

////////////////////////////////////////////////////////////////////////////////
//
// transform_reduce
//
////////////////////////////////////////////////////////////////////////////////
// single range
template <class ExecutionPolicy, class ForwardIt, class T, class BinaryOp,
          class UnaryOp>
std::enable_if_t<shad::is_execution_policy<ExecutionPolicy>::value, T>
transform_reduce(ExecutionPolicy&& policy, ForwardIt first, ForwardIt last,
                 T init, BinaryOp binary_op, UnaryOp unary_op) {
  return impl::transform_reduce(std::forward<ExecutionPolicy>(policy), first,
                                last, init, binary_op, unary_op);
}

// two ranges
template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2, class T,
          class BinaryOp1, class BinaryOp2>
T transform_reduce(ExecutionPolicy&& policy, ForwardIt1 first1,
                   ForwardIt1 last1, ForwardIt2 first2, T init,
                   BinaryOp1 binary_op1, BinaryOp2 binary_op2) {
  return impl::transform_reduce(std::forward<ExecutionPolicy>(policy), first1,
                                last1, first2, init, binary_op1, binary_op2);
}

template <class InputIt1, class InputIt2, class T>
T transform_reduce(InputIt1 first1, InputIt1 last1, InputIt2 first2, T init) {
  return transform_reduce(distributed_sequential_tag{}, first1, last1, first2,
                          init, std::plus<>(), std::multiplies<>());
}

template <class InputIt1, class InputIt2, class T, class BinaryOp1,
          class BinaryOp2>
std::enable_if_t<!shad::is_execution_policy<InputIt1>::value, T>
transform_reduce(InputIt1 first1, InputIt1 last1, InputIt2 first2, T init,
                 BinaryOp1 binary_op1, BinaryOp2 binary_op2) {
  return transform_reduce(distributed_sequential_tag{}, first1, last1, first2,
                          init, binary_op1, binary_op2);
}

template <class InputIt, class T, class BinaryOp, class UnaryOp>
std::enable_if_t<!shad::is_execution_policy<InputIt>::value, T>
transform_reduce(InputIt first, InputIt last, T init, BinaryOp binop,
                 UnaryOp unary_op) {
  return transform_reduce(distributed_sequential_tag{}, first, last, init,
                          binop, unary_op);
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2, class T>
std::enable_if_t<shad::is_execution_policy<ExecutionPolicy>::value, T>
transform_reduce(ExecutionPolicy&& policy, ForwardIt1 first1, ForwardIt1 last1,
                 ForwardIt2 first2, T init) {
  return transform_reduce(std::forward<ExecutionPolicy>(policy), first1, last1,
                          first2, init, std::plus<>(), std::multiplies<>());
}

////////////////////////////////////////////////////////////////////////////////
//
// transform_exclusive_scan
//
////////////////////////////////////////////////////////////////////////////////
template <class InputIt, class OutputIt, class T, class BinaryOperation,
          class UnaryOperation>
OutputIt transform_exclusive_scan(InputIt first, InputIt last, OutputIt d_first,
                                  T init, BinaryOperation binary_op,
                                  UnaryOperation unary_op) {
  return impl::transform_exclusive_scan(distributed_sequential_tag{}, first,
                                        last, d_first, init, binary_op,
                                        unary_op);
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2, class T,
          class BinaryOperation, class UnaryOperation>
ForwardIt2 transform_exclusive_scan(ExecutionPolicy&& policy, ForwardIt1 first,
                                    ForwardIt1 last, ForwardIt2 d_first, T init,
                                    BinaryOperation binary_op,
                                    UnaryOperation unary_op) {
  return impl::transform_exclusive_scan(std::forward<ExecutionPolicy>(policy),
                                        first, last, d_first, init, binary_op,
                                        unary_op);
}

////////////////////////////////////////////////////////////////////////////////
//
// transform_inclusive_scan
//
////////////////////////////////////////////////////////////////////////////////
template <class InputIt, class OutputIt, class BinaryOperation,
          class UnaryOperation>
OutputIt transform_inclusive_scan(InputIt first, InputIt last, OutputIt d_first,
                                  BinaryOperation binary_op,
                                  UnaryOperation unary_op) {
  return impl::transform_inclusive_scan(distributed_sequential_tag{}, first,
                                        last, d_first, binary_op, unary_op);
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2,
          class BinaryOperation, class UnaryOperation>
std::enable_if_t<shad::is_execution_policy<ExecutionPolicy>::value, ForwardIt2>
transform_inclusive_scan(ExecutionPolicy&& policy, ForwardIt1 first,
                         ForwardIt1 last, ForwardIt2 d_first,
                         BinaryOperation binary_op, UnaryOperation unary_op) {
  return impl::transform_inclusive_scan(std::forward<ExecutionPolicy>(policy),
                                        first, last, d_first, binary_op,
                                        unary_op);
}

template <class InputIt, class OutputIt, class BinaryOperation,
          class UnaryOperation, class T>
std::enable_if_t<!shad::is_execution_policy<InputIt>::value, OutputIt>
transform_inclusive_scan(InputIt first, InputIt last, OutputIt d_first,
                         BinaryOperation binary_op, UnaryOperation unary_op,
                         T init) {
  return impl::transform_inclusive_scan(distributed_sequential_tag{}, first,
                                        last, d_first, binary_op, unary_op,
                                        init);
}

template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2,
          class BinaryOperation, class UnaryOperation, class T>
ForwardIt2 transform_inclusive_scan(ExecutionPolicy&& policy, ForwardIt1 first,
                                    ForwardIt1 last, ForwardIt2 d_first,
                                    BinaryOperation binary_op,
                                    UnaryOperation unary_op, T init) {
  return impl::transform_inclusive_scan(std::forward<ExecutionPolicy>(policy),
                                        first, last, d_first, binary_op,
                                        unary_op, init);
}

}  // namespace shad

#endif /* INCLUDE_SHAD_CORE_NUMERIC_H */
