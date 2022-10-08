#include <iostream>

#include "shad/data_structures/hashmap.h"
#include "shad/runtime/runtime.h"

static const size_t kNumValues = 4;
static const float kMagicNumber = 42*3.14;

namespace shad {

int main(int argc, char **argv) {  
  auto mapPtr = shad::Hashmap<uint64_t, float>::Create(kNumValues* rt::numLocalities());
  // mapPtr is technically a shared ptr to the "local" instance of the map.
  // However, the class API provides a "global", shared memory abstraction.

  mapPtr->Insert(42lu, kMagicNumber);
  float value = 0;
  bool found = mapPtr->Lookup(42lu, &value);
  if (found) {
    std::cout << "Found key with value: " << value << std::endl;
  } else {
    std::cout << "Key not found! :(" << std::endl;
  }

  found = mapPtr->Lookup(43lu, &value);
  if (found) {
    std::cout << "Surprisingly found key with value: " << value << std::endl;
  } else {
    std::cout << "Key not found! :)" << std::endl;
  }

  // these can be Async
  rt::Handle h;
  mapPtr->AsyncInsert(h, 43lu, kMagicNumber+1);
  rt::waitForCompletion(h);

  shad::Hashmap<uint64_t, float>::LookupResult lres;
  mapPtr->AsyncLookup(h, 43lu, &lres);
  rt::waitForCompletion(h);
  if (lres.found) {
    std::cout << "Not much surprisingly found key with value: " << lres.value << std::endl;
  } else {
    std::cout << "Key not found! :(" << std::endl;
  }

// apply and foreach
  auto ApplyLambda = [](const uint64_t &key,
                        float &value, uint64_t &randomNum) {
    value *= randomNum;
  };
  uint64_t rndNum = 8lu;
  mapPtr->Apply(42lu, ApplyLambda, rndNum);
  found = mapPtr->Lookup(42lu, &value);
  if (found) {
    std::cout << "Found key with value: " << value << std::endl;
  } else {
    std::cout << "Key not found! :(" << std::endl;
  }

  auto VisitLambda = [](const uint64_t &key, float &value,
                            char &arg) {
    std::cout << rt::thisLocality() << " - key is " << key
              << ", value is " << value
              << ", arg is " << arg << std::endl;
  };
  char arg = 'w';
  mapPtr->ForEachEntry(VisitLambda, arg);
  // These operations can be performed anywhere in the system.
  // The shared memory view is preserved.

  // First things first, to reference access the data structures on any locality we need their Object ID.
  // Reminder: passing pointers around is usually a BAD idea. Don't do it. Never. Well.. Unless you have to :D  

  auto mapOID = mapPtr->GetGlobalID();

  rt::asyncExecuteOnAll(h,
      [](rt::Handle &h, const shad::Hashmap<uint64_t, float>::ObjectID& oid) {
        auto hmptr = shad::Hashmap<uint64_t, float>::GetPtr(oid);
        size_t locn = (uint32_t)(rt::thisLocality());
        for (size_t i = locn; i < kNumValues + locn; ++i) {
          hmptr->AsyncInsert(h, i, i + kMagicNumber);
        }
      },
      mapOID);
  rt::waitForCompletion(h);
  auto size = mapPtr->Size();
  std::cout << "map Size: " << size << std::endl;

  // ex: iterate over all entries and insert values gt magicNumber in a shad::Set
  // hint: Create the set first!
  // hint 2: No pointers!

  shad::Hashmap<uint64_t, float>::Destroy(mapOID);


  return 0;
}

}  // namespace shad