#include <atomic>
#include <algorithm>
#include <bitset>
#include <cmath>
#include <fstream>
#include <iostream>
#include <numeric>
#include <utility>

#include "shad/data_structures/set.h"
#include "shad/extensions/data_types/data_types.h"
#include "shad/extensions/graph_library/edge_index.h"
#include "shad/runtime/runtime.h"

namespace shad {

constexpr size_t n_records = 100;
constexpr size_t n_cols = 4;
using entry_t = std::array<uint64_t, n_cols>;
static const shad::data_types::schema_t entry_schema = {
  std::make_pair("domain", shad::data_types::UINT),
  std::make_pair("server", shad::data_types::IP_ADDRESS),
  std::make_pair("count",  shad::data_types::UINT),
  std::make_pair("hours",  shad::data_types::UINT)
};
static entry_t parse_entry(std::string str,
                           const shad::data_types::schema_t& schema) {
  entry_t ret;
  std::string::iterator start = str.begin();
  std::string::iterator endit = str.end();
  for (size_t i = 0; i < n_cols; ++i) {
    std::string::iterator end = std::find(start, endit, ',');
    std::string field = std::string(start, end);
    ret[i] = shad::data_types::encode<uint64_t, std::string>(field, schema[i].second);
    start = end + 1;
   }
   return ret;
}


int main(int argc, char **argv) {
  std::string filename = "./SHAD/tutorial/02_DS_Ext_and_Utils/tinyfile.csv";
  std::string line;
  std::ifstream file(filename);
  std::vector<std::string> records;
  if(!file.is_open()) {
    exit(-1);
  }
  while(getline(file, line)) {
    if(line[0] != '#') {
      records.push_back(line);
    }
  }
  file.close();

  // Let's create a graph with edges between domains and ips
  auto eiGraph = shad::EdgeIndex<uint64_t, uint64_t>::Create(n_records);
  rt::Handle handle;
  for (auto row : records) {
    entry_t encoded = parse_entry(row, entry_schema);
    eiGraph->AsyncInsert(handle, encoded[0], encoded[1]);
  }
  rt::waitForCompletion(handle);

  // let's compute the number of unique destination vertices
  auto setPtr = shad::Set<uint64_t>::Create(n_records);
  shad::Set<uint64_t>::ObjectID oid = setPtr->GetGlobalID();
  auto ForEachELambda = [](shad::rt::Handle &h, const uint64_t &src,
                           const uint64_t &dest, shad::Set<uint64_t>::ObjectID& oid) {
    auto setPtr = shad::Set<uint64_t>::GetPtr(oid);
    setPtr->AsyncInsert(h, dest);
  };
  eiGraph->AsyncForEachEdge(handle, ForEachELambda, oid);
  rt::waitForCompletion(handle);
  size_t size = setPtr->Size();
  std::cout << "The Graph has " << size << " unique destinations" << std::endl;

  return 0;
}

}  // namespace shad