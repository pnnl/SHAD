#include <stdlib.h>
#include <iostream>
#include <random>

#include "shad/core/algorithm.h"
#include "shad/core/array.h"


namespace shad {

int main(int argc, char *argv[]) {
  // array
  shad::array<int, 4> array_;

  // shad fill algorithm
  shad::fill(shad::distributed_parallel_tag{}, array_.begin(), array_.end(), 42);
  std::cout << "==> After using shad::fill, array is " << std::endl;
  for (auto v: array_) std::cout << v << "\n";


  // shad generate algorithm
  shad::generate(shad::distributed_parallel_tag{}, array_.begin(), array_.end(), [=]() {
    std::random_device rd;
    std::default_random_engine G(rd());
    std::uniform_int_distribution<int> dist(1, 10);
    return dist(G);
  });
  std::cout << "==> After using shad::generate, array is " << std::endl;
  for (auto v: array_) std::cout << v << "\n";


  // shad count algorithm
  size_t counter =
      shad::count(shad::distributed_parallel_tag{}, array_.begin(), array_.end(), 5);
  std::cout << "==> After using shad::count, the counter of 5 is: " << counter << std::endl;


  // shad find_if algorithm
  using iterator = shad::impl::array<int, 4>::iterator;
  iterator iter = shad::find_if(shad::distributed_parallel_tag{}, array_.begin(),
                            array_.end(), [](int i) { return i % 2 == 0; });
  std::cout << "==> After using shad::find_if, ";
  (iter != array_.end())
      ? std::cout << "array contains an even number" << std::endl
      : std::cout << "array does not contain even numbers" << std::endl;


  // shad minmax algorithm
  std::pair<iterator, iterator> min_max = shad::minmax_element(
      shad::distributed_parallel_tag{}, array_.begin(), array_.end());
  std::cout << "==> After using shad::minmax, min = " << *min_max.first 
            << ", max= " << *min_max.second << std::endl;
  

  // shad transform algorithm
  shad::transform(shad::distributed_parallel_tag{}, array_.begin(), array_.end(),
                  array_.begin(), [](int i) { return i + 100; });
  std::cout << "==> After using shad::transform, array is " << std::endl;
  for (auto v: array_) std::cout << v << "\n";

  // Exercise 1
  // Create an array containing 1, 3, 5, 7
  // replace 5 to 42
  // # HINT: use shad::replace 

  return 0;
}

}  // namespace shad