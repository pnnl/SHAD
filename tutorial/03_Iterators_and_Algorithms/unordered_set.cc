#include <iostream>

#include "shad/core/algorithm.h"
#include "shad/core/unordered_set.h"
#include "shad/util/measure.h"

constexpr static size_t kSize = 128;
using iterator = shad::Set<int>::iterator;
using value_type = shad::Set<int>::value_type;
using shad_inserter_t = shad::insert_iterator<shad::unordered_set<int>>;
using shad_buffered_inserter_t =
    shad::buffered_insert_iterator<shad::unordered_set<int>>;


template <typename shad_inserter>
void shad_transform_algorithm(shad::unordered_set<int> &in) {
  shad::unordered_set<int> out;
  shad::transform(
      shad::distributed_parallel_tag{}, in.begin(), in.end(),
      shad_inserter(out, out.begin()), [](const value_type &i) { return i; });
}

namespace shad {

int main(int argc, char *argv[]) {
  // unordered_set
  shad::unordered_set<int> set_;

  // create set
  shad_buffered_inserter_t ins(set_, set_.begin());
  for (size_t i = 0; i < kSize; ++i) {
    ins = 2 * (i + 1);
  }
  ins.wait();
  ins.flush();

  // shad minmax algorithm
  std::pair<iterator, iterator> min_max = shad::minmax_element(
      shad::distributed_parallel_tag{}, set_.begin(), set_.end());
  std::cout << "==> After using shad::count, min = " << *min_max.first
            << ", max = " << *min_max.second << std::endl;

  
  // shad find algorithm
  iterator iter =
      shad::find(shad::distributed_parallel_tag{}, set_.begin(), set_.end(), 6);
  std::cout << "==> After using shad::find, ";
  (iter != set_.end())
      ? std::cout << "this unordered set contains 6"
                  << std::endl
      : std::cout << "this unordered set does not contain 6 "
                  << std::endl;

  
  // shad find_if algorithm
  iter =
      shad::find_if(shad::distributed_parallel_tag{}, set_.begin(), set_.end(),
                    [](const value_type &i) { return i % 2 == 0; });
  std::cout << "==> After using shad::find_if, ";
  (iter != set_.end())
      ? std::cout << "this unordered set contains an even number"
                  << std::endl
      : std::cout << "this unordered set does not contain even numbers "
                  << std::endl;

  
  // shad any_of algorithm
  bool res = shad::any_of(shad::distributed_parallel_tag{}, set_.begin(), set_.end(),
                   [](const value_type &i) { return i % 7 == 0; });
  std::cout << "==> After using shad::any_of, ";
  (res == true)
      ? std::cout << "this unordered set contains at least one number that "
                     "is divisible by 7 "
                  << std::endl
      : std::cout << "this unordered set does not contain any number that "
                     "is divisible by 7"
                  << std::endl;


  // shad transform algorithm
  auto execute_time = shad::measure<std::chrono::seconds>::duration(
      [&]() { shad_transform_algorithm<shad_inserter_t>(set_); });
  std::cout << "==> Using shad::transform with insert_iterator, took " << execute_time.count()
            << " seconds" << std::endl;

  auto execute_time_ = shad::measure<std::chrono::seconds>::duration(
      [&]() { shad_transform_algorithm<shad_buffered_inserter_t>(set_); });
  std::cout << "==> Using shad::transform with buffered_insert_iterator, took " 
            << execute_time_.count() << " seconds" << std::endl;

  // Exercise 2
  // Create an unorder_set containing int value 2, 3, 4, 5,
  // Double each value in the unorder_set and store in another unorder_set
  // #HINT: shad::transform

  return 0;
}

}  // namespace shad
