#include <iostream>

#include "shad/runtime/runtime.h"
#include "./sol1_new_data_structure.h"

namespace shad {

int main(int argc, char **argv) {  
  
  auto anInt = shad::MyNewDS<int>::Create(10);
  std::vector<int> localData(rt::numLocalities());
  for (size_t i = 0; i < localData.size(); ++i) {
    localData[i] = i;
  }
  anInt->scatter(localData);
  
  std::vector<int> gathered = anInt->gather();

  std::cout << "\nLet's check the gathered values\n" << std::endl;
  for (size_t i = 0; i < gathered.size(); ++i) {
    std::cout << "gathered[" << i << "] = " << gathered[i] <<std::endl;
  }
  shad::MyNewDS<int>::Destroy(anInt->GetGlobalID());


  return 0;
}

}  // namespace shad