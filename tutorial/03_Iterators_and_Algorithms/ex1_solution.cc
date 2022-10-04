#include <iostream>

#include "shad/core/algorithm.h"
#include "shad/core/array.h"


namespace shad {

int main(int argc, char *argv[]) {
  // Exercise 1
  // Create an array containing 1, 3, 5, 7
  // replace 5 to 42
  // # HINT: use shad::replace 


  // create array
  shad::array<int, 4> array_;

  for (size_t i = 0; i < 4; ++i) {
    array_[i] = 2 * (i+1) - 1;
  }
  
  std::cout << "==> Create the array: \n"; 
  for(auto v: array_) std::cout << v << std::endl;

  // use shad::replace
  shad::replace(shad::distributed_parallel_tag{}, array_.begin(), array_.end(), 5, 42);
  std::cout << "==> After using shad::replace, the array is: \n";
  for(auto v: array_) std::cout << v << std::endl;


  return 0;
}

}  // namespace shad