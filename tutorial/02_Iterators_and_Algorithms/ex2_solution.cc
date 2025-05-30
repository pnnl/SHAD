#include <iostream>

#include "shad/core/algorithm.h"
#include "shad/data_structures/array.h"

namespace shad {

int main(int argc, char *argv[]) {
  
  // Exercise 2
  // Create an array containing 1, 3, 5, 7
  // Square each of the elements of the array
  // #HINT: shad::transform


  // create array
  auto shadArray = shad::Array<int>::Create(4, 0);
  int array_[4];
  
  int pos = 0;
  for (size_t i = 0; i < 4; ++i) {
    array_[i] = 2 * (i+1) - 1;
  }
  shadArray->InsertAt(pos, array_, 4);
  std::cout << "==> After using shad::InsertAt, array is " << std::endl;
  for (int i = 0; i < shadArray->Size(); i++) std::cout << shadArray->At(i) << "\n";


  // use shad::transform
  shad::transform(shad::distributed_parallel_tag{}, shadArray->begin(), shadArray->end(),
  shadArray->begin(), [](int i) { return i*i; });
  std::cout << "==> After using shad::transform, array is " << std::endl;
  for (int i = 0; i < shadArray->Size(); i++) std::cout << shadArray->At(i) << "\n";


  return 0;
}

}  // namespace shad
