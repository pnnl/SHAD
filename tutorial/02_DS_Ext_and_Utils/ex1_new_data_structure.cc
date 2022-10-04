#include <iostream>

#include "shad/runtime/runtime.h"
#include "./ex1_new_data_structure.h"

namespace shad {

int main(int argc, char **argv) {  
  
  auto anInt = shad::MyNewDS<int>::Create(10);
  rt::executeOnAll(
      [](const shad::MyNewDS<int>::ObjectID& oid) {
        auto ptr = shad::MyNewDS<int>::GetPtr(oid);
        if(ptr != nullptr) {
          std::cout << "Pointer looks good on "
                    << rt::thisLocality() << ", value is "
                    << static_cast<int>(*ptr) << std::endl;
        }
      },
      anInt->GetGlobalID());
  std::cout << "\nLet's update the values\n" << std::endl;

  rt::executeOnAll(
      [](const shad::MyNewDS<int>::ObjectID& oid) {
        auto ptr = shad::MyNewDS<int>::GetPtr(oid);
        (*ptr) = 10 + static_cast<uint32_t>(rt::thisLocality());
      },
    anInt->GetGlobalID());

  std::cout << "\nLet's check all the values are correct\n" << std::endl;

  rt::executeOnAll(
      [](const shad::MyNewDS<int>::ObjectID& oid) {
        auto ptr = shad::MyNewDS<int>::GetPtr(oid);
        if(ptr != nullptr) {
          std::cout << "Pointer looks good on "
                    << rt::thisLocality() << ", value is "
                    << static_cast<int>(*ptr) << std::endl;
        }
      },
      anInt->GetGlobalID());
  
  std::cout << "\nLet's try the scatter operation\n" << std::endl;

  std::vector<int> localData(rt::numLocalities());
  for (size_t i = 0; i < localData.size(); ++i) {
    localData[i] = i;
  }
  
  anInt->scatter(localData);

std::cout << "\nLet's check all the values again\n" << std::endl;

  rt::executeOnAll(
      [](const shad::MyNewDS<int>::ObjectID& oid) {
        auto ptr = shad::MyNewDS<int>::GetPtr(oid);
        if(ptr != nullptr) {
          std::cout << "Pointer looks good on "
                    << rt::thisLocality() << ", value is "
                    << static_cast<int>(*ptr) << std::endl;
        }
      },
      anInt->GetGlobalID());

  shad::MyNewDS<int>::Destroy(anInt->GetGlobalID());


  return 0;
}

}  // namespace shad