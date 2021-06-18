#include <iostream>

#include "shad/runtime/runtime.h"

namespace shad {

int main(int argc, char **argv) {
  // Exercise 1
  // Current locality says (asynchronously) Hello to all localities,
  // which nicely say (asynchronously) Hello back.
  // # HINT: you only need one handle 

  auto thisLoc = shad::rt::thisLocality();  

  rt::Handle handle;
  rt::asyncExecuteOnAll(handle,
    [](rt::Handle& handle, const rt::Locality &caller_loc) {
        auto myLoc = shad::rt::thisLocality();
        std::cout << "Hello " << myLoc << " from " << caller_loc << std::endl;
        rt::asyncExecuteAt(handle, caller_loc,
          [](rt::Handle&, const rt::Locality &myLoc) {
              std::cout << "<- Hello from" << myLoc << std::endl;
          },
          myLoc
        );
      },
      thisLoc
  );
  std::cout << " ------Completed? " << std::endl;
  rt::waitForCompletion(handle);


  return 0;
}

}  // namespace shad