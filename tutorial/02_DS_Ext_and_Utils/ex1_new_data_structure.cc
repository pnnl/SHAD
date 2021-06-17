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

  shad::MyNewDS<int>::Destroy(anInt->GetGlobalID());


  return 0;
}

}  // namespace shad