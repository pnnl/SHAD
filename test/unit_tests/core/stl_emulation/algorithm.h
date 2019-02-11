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

#ifndef TEST_UNIT_TESTS_CORE_STL_EMULATION_ALGORITHM_H_
#define TEST_UNIT_TESTS_CORE_STL_EMULATION_ALGORITHM_H_

#include <algorithm>
#include <functional>

namespace shad_test_stl {

template <class InputIt, class UnaryPredicate>
bool all_of_(InputIt first, InputIt last, UnaryPredicate p) {
  return std::find_if_not(first, last, p) == last;
}

template <class InputIt, class UnaryPredicate>
bool any_of_(InputIt first, InputIt last, UnaryPredicate p) {
  return std::find_if(first, last, p) != last;
}

template <class InputIt, class UnaryPredicate>
bool none_of_(InputIt first, InputIt last, UnaryPredicate p) {
  return std::find_if(first, last, p) == last;
}

// todo for_each

template <class InputIt, class T_>
typename std::iterator_traits<InputIt>::difference_type count_(
    InputIt first, InputIt last, const T_ &value) {
  typename std::iterator_traits<InputIt>::difference_type ret = 0;
  for (; first != last; ++first) {
    if (*first == value) {
      ret++;
    }
  }
  return ret;
}

template <class InputIt, class UnaryPredicate>
typename std::iterator_traits<InputIt>::difference_type count_if_(
    InputIt first, InputIt last, UnaryPredicate p) {
  typename std::iterator_traits<InputIt>::difference_type ret = 0;
  for (; first != last; ++first) {
    if (p(*first)) {
      ret++;
    }
  }
  return ret;
}

template <class InputIt1, class InputIt2>
std::pair<InputIt1, InputIt2> mismatch_(InputIt1 first1, InputIt1 last1,
                                        InputIt2 first2) {
  while (first1 != last1 && *first1 == *first2) {
    ++first1, ++first2;
  }
  return std::make_pair(first1, first2);
}

template <class InputIt, class T_>
InputIt find_(InputIt first, InputIt last, const T_ &value) {
  for (; first != last; ++first) {
    if (*first == value) {
      return first;
    }
  }
  return last;
}

template <class InputIt, class UnaryPredicate>
InputIt find_if_(InputIt first, InputIt last, UnaryPredicate p) {
  for (; first != last; ++first) {
    if (p(*first)) {
      return first;
    }
  }
  return last;
}

template <class InputIt, class UnaryPredicate>
InputIt find_if_not_(InputIt first, InputIt last, UnaryPredicate q) {
  for (; first != last; ++first) {
    if (!q(*first)) {
      return first;
    }
  }
  return last;
}

template <class ForwardIt1, class ForwardIt2>
ForwardIt1 find_end_(ForwardIt1 first, ForwardIt1 last, ForwardIt2 s_first,
                     ForwardIt2 s_last) {
  if (s_first == s_last) return last;
  ForwardIt1 result = last;
  while (true) {
    ForwardIt1 new_result = std::search(first, last, s_first, s_last);
    if (new_result == last) {
      break;
    } else {
      result = new_result;
      first = result;
      ++first;
    }
  }
  return result;
}

template <class InputIt, class ForwardIt>
InputIt find_first_of_(InputIt first, InputIt last, ForwardIt s_first,
                       ForwardIt s_last) {
  for (; first != last; ++first) {
    for (ForwardIt it = s_first; it != s_last; ++it) {
      if (*first == *it) {
        return first;
      }
    }
  }
  return last;
}

template <class ForwardIt>
ForwardIt adjacent_find_(ForwardIt first, ForwardIt last) {
  if (first == last) {
    return last;
  }
  ForwardIt next = first;
  ++next;
  for (; next != last; ++next, ++first) {
    if (*first == *next) {
      return first;
    }
  }
  return last;
}
template <class ForwardIt1, class ForwardIt2>
ForwardIt1 search_(ForwardIt1 first, ForwardIt1 last, ForwardIt2 s_first,
                   ForwardIt2 s_last) {
  for (;; ++first) {
    ForwardIt1 it = first;
    for (ForwardIt2 s_it = s_first;; ++it, ++s_it) {
      if (s_it == s_last) {
        return first;
      }
      if (it == last) {
        return last;
      }
      if (!(*it == *s_it)) {
        break;
      }
    }
  }
}

template <class ForwardIt, class Size, class T_>
ForwardIt search_n_(ForwardIt first, ForwardIt last, Size count,
                    const T_ &value) {
  for (; first != last; ++first) {
    if (!(*first == value)) {
      continue;
    }

    ForwardIt candidate = first;
    Size cur_count = 0;

    while (true) {
      ++cur_count;
      if (cur_count == count) {
        // success
        return candidate;
      }
      ++first;
      if (first == last) {
        // exhausted the list
        return last;
      }
      if (!(*first == value)) {
        // too few in a row
        break;
      }
    }
  }
  return last;
}

template <class ForwardIt>
ForwardIt min_element_(ForwardIt first, ForwardIt last) {
  if (first == last) return last;

  ForwardIt smallest = first;
  ++first;
  for (; first != last; ++first) {
    if (*first < *smallest) {
      smallest = first;
    }
  }
  return smallest;
}

template <class ForwardIt>
ForwardIt max_element_(ForwardIt first, ForwardIt last) {
  if (first == last) return last;

  ForwardIt largest = first;
  ++first;
  for (; first != last; ++first) {
    if (*largest < *first) {
      largest = first;
    }
  }
  return largest;
}

template <class ForwardIt>
std::pair<ForwardIt, ForwardIt> minmax_element_(ForwardIt first,
                                                ForwardIt last) {
  return std::minmax_element(first, last, std::less<>());
}

template <class ForwardIt, class T>
void fill_(ForwardIt first, ForwardIt last, const T &value) {
  while (first != last) {
    *first++ = value;
  }
}

template <class InputIt, class OutputIt, class UnaryOperation>
OutputIt transform_(InputIt first1, InputIt last1, OutputIt d_first,
                   UnaryOperation unary_op) {
  while (first1 != last1) {
    *d_first++ = unary_op(*first1++);
  }
  return d_first;
}

template <class ForwardIt, class Generator>
void generate_(ForwardIt first, ForwardIt last, Generator g) {
  while (first != last) {
    *first++ = g();
  }
}

template <class ForwardIt, class T>
void replace_(ForwardIt first, ForwardIt last, const T &old_value,
              const T &new_value) {
  for (; first != last; ++first) {
    if (*first == old_value) {
      *first = new_value;
    }
  }
}

template <class ForwardIt, class UnaryPredicate, class T>
void replace_if_(ForwardIt first, ForwardIt last, UnaryPredicate p,
                 const T &new_value) {
  for (; first != last; ++first) {
    if (p(*first)) {
      *first = new_value;
    }
  }
}

}  // namespace shad_test_stl

#endif
