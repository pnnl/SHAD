#include <iostream>

#include "shad/core/algorithm.h"
#include "shad/data_structures/hashmap.h"

const int mapSize = 4;

namespace shad {

int main(int argc, char *argv[]) {

    // unordered_map with int keys and int values
    auto map = shad::Hashmap<int,int>::Create(mapSize);
    map->Insert(0, -10);
    map->Insert(1, 7);
    map->Insert(2, 3);
    map->Insert(3, 1);

    std::cout << "==> Input map" << std::endl;
    for (int i = 0; i < mapSize; i++){
      int val;
      map->Lookup(i, &val);
      std::cout << "Key: " << i << " , value: " << val << std::endl;
    }

    // shad::any_of to check for any negative number 
    bool hasNegative = shad::any_of(distributed_parallel_tag{}, map->begin(), map->end(), [](const auto& pair){
      return pair.second < 0;
    });

    std::cout << std::endl;
    std::cout << "==> shad::any_of returned " << ((hasNegative) ? "true" : "false") << std::endl;

    return 0;
}
}