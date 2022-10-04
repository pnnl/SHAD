#include <iostream>

#include "shad/core/algorithm.h"
#include "shad/core/unordered_set.h"


namespace shad {

int main(int argc, char *argv[]) {

  // Exercise 2
  // Create an unorder_set containing int value 2, 3, 4, 5,
  // Double each value in the unorder_set and store in another unorder_set
  // #HINT: shad::transform

  using value_type = shad::Set<int>::value_type;
  using insert_iterator = shad::insert_iterator<shad::unordered_set<int>>;

  // unordered_set
  shad::unordered_set<int> set_;

  // create set
  for (size_t i = 0; i < 4; ++i) {
    set_.insert(i + 2);
  }


  std::cout << "==> Create the unodered_set: \n";
  for (auto v: set_) std::cout << v << std::endl;

  // shad transform algorithm
  shad::unordered_set<int> out;

  shad::transform(
     shad::distributed_parallel_tag{}, set_.begin(), set_.end(),
     insert_iterator(out, out.begin()), [](const value_type &i) { return i * 2; });

  std::cout << "==> After using shad::transform, another unodered_set is: \n";
  for (auto v: out) std::cout << v << std::endl;
  
  return 0;
}

}  // namespace shad