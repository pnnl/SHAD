#include <iostream>

#include "shad/core/algorithm.h"
#include "shad/data_structures/hashmap.h"

const int mapSize = 6;

namespace shad {

int main(int argc, char *argv[]) {
    
    // unordered_map with int keys and int values
    auto map = shad::Hashmap<int,int>::Create(mapSize);
    for (int i = 0; i < mapSize; i++){
      map->Insert(i, (i + 1) * 15);
    }

    // retrieve values corresponding to keys = 1, 3
    int key, val;
    key = 1;
    map->Lookup(key, &val);
    std::cout << "==> The value corresponding to key " << key << " is " << val << std::endl;
    key = 3;
    map->Lookup(key, &val);
    std::cout << "==> The value corresponding to key " << key << " is " << val << std::endl;
    std::cout << std::endl;

    // for_each to compute the sum of values
    std::cout << "==> Using shad::forEach, elements of even value are: " << std::endl;
    shad::for_each(distributed_parallel_tag{}, map->begin(), map->end(), [](const auto& pair) {
      if (pair.second % 2 == 0) {
          std::cout << "Key: " << pair.first << ", value: " << pair.second << std::endl;
      }
    });
    std::cout << std::endl;
    
    // count_if to count how many values are >= 50
    int count = shad::count_if(distributed_parallel_tag{}, map->begin(), map->end(), [](const auto& pair) {
      return pair.second >= 50;
    });
    std::cout << "==> Using shad::count_if, the number of entries with value >= 50: " << count << std::endl;

    // Exercise 3
    // Check if the map contains negative values
    // #HINT: shad::any_of

    shad::Hashmap<int, int>::Destroy(map->GetGlobalID());

    return 0;
}
}
