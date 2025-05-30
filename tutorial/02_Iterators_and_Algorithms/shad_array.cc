#include <stdlib.h>
#include <iostream>
#include <random>
#include <assert.h>

#include "shad/core/algorithm.h"
#include "shad/data_structures/array.h"

const int arraySize = 10;

void applyFun(size_t i, int &elem) { elem = i * 20; };
void asyncApplyFun(shad::rt::Handle& h, size_t i, int &elem) { elem = i * 30; };

namespace shad {

int main(int argc, char *argv[]) {
  
  // array
  auto shadArray = shad::Array<int>::Create(arraySize, 0);

  // shad fill algorithm
  shad::fill(shad::distributed_parallel_tag{}, shadArray->begin(), shadArray->end(), 42);
  std::cout << "==> After using shad::fill, array is " << std::endl;
  for (int i = 0; i < shadArray->Size(); i++) std::cout << shadArray->At(i) << "\n";


  // shad generate algorithm
  shad::generate(shad::distributed_parallel_tag{}, shadArray->begin(), shadArray->end(), [=]() {
    std::random_device rd;
    std::default_random_engine G(rd());
    std::uniform_int_distribution<int> dist(1, 10);
    return dist(G);
  });
  std::cout << "==> After using shad::generate, array is " << std::endl;
  for (int i = 0; i < shadArray->Size(); i++) std::cout << shadArray->At(i) << "\n";

  // shad insert 
  int pos = 5;
  int values[3] = {37, 38, 39};
  shadArray->InsertAt(pos, values, 3);
  std::cout << "==> After using shad::InsertAt, array is " << std::endl;
  for (int i = 0; i < shadArray->Size(); i++) std::cout << shadArray->At(i) << "\n";

  pos = 2;
  shadArray->BufferedInsertAt(pos, 23);
  shadArray->WaitForBufferedInsert();
  std::cout << "==> After using shad::InsertAt, array is " << std::endl;
  for (int i = 0; i < shadArray->Size(); i++) std::cout << shadArray->At(i) << "\n";

  
  // shad apply 
  assert(pos < shadArray->Size()/2);
  for (size_t i = pos; i < shadArray->Size()/2; i++) {
    shadArray->Apply(i, applyFun);
  }
  std::cout << "==> After using shad::apply, array is " << std::endl;
  for (int i = 0; i < shadArray->Size(); i++) std::cout << shadArray->At(i) << "\n";

  
  // shad async apply
  shad::rt::Handle h;
  for (size_t i = 0; i < shadArray->Size(); i++) {
    shadArray->AsyncApply(h, i, asyncApplyFun);
  }
  shad::rt::waitForCompletion(h);
  std::cout << "==> After using shad::asyncApply, array is " << std::endl;
  for (int i = 0; i < shadArray->Size(); i++) std::cout << shadArray->At(i) << "\n";

  // Exercise 2
  // Create an array containing 1, 3, 5, 7
  // Square each of the elements of the array
  // #HINT: shad::transform

  shad::Array<int>::Destroy(shadArray->GetGlobalID());

  return 0;
}

}  // namespace shad