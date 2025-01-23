#include <iostream>

#include "shad/runtime/runtime.h"

namespace shad {

int main(int argc, char **argv) {
  std::cout << "Num of localities: " << shad::rt::numLocalities()
            << std::endl;
  std::cout << "This is loc: " << shad::rt::thisLocality()
            << std::endl;

  std::cout << "\n\n******Execute At*****\n" << std::endl;
  auto thisLoc = shad::rt::thisLocality();  
  for (auto loc : shad::rt::allLocalities()) {
    rt::executeAt(loc,
      [](const rt::Locality &caller_loc) {
        std::cout << "Hello " << rt::thisLocality() << " from " << caller_loc << std::endl;
      },
      thisLoc
    );
  }
  std::cout << "\n\n******Execute On All*****\n" << std::endl;

  rt::executeOnAll(
    [](const rt::Locality &caller_loc) {
        std::cout << "Hello " << rt::thisLocality() << " from " << caller_loc << std::endl;
      },
      thisLoc
  );
  std::cout << "\n\n******Async Execute On All*****\n" << std::endl;

  rt::Handle handle;
  rt::asyncExecuteOnAll(handle,
    [](rt::Handle&, const rt::Locality &caller_loc) {
        std::cout << "Hello " << rt::thisLocality() << " from " << caller_loc << std::endl;
      },
      thisLoc
  );
  rt::waitForCompletion(handle);
  std::cout << " ------Completed? " << std::endl;
  

  // Exercise 1
  // Current locality says (asynchronously) Hello to all localities,
  // which nicely say (asynchronously) Hello back.
  // # HINT: you only need one handle 


  return 0;
}

}  // namespace shad