#include <atomic>
#include <algorithm>
#include <bitset>
#include <cmath>
#include <fstream>
#include <iostream>
#include <numeric>
#include <utility>

#include "shad/core/algorithm.h"
#include "shad/core/array.h"
#include "shad/core/unordered_set.h"
#include "shad/extensions/data_types/data_types.h"
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
  if (argc != 2) {
    printf("Usage: <input file>\n"); return 1;
  }
  std::string filename = std::string(argv[1]);
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

  shad::array<entry_t, n_records> data;
  size_t i = 0;
  using insert_args_t = std::tuple<shad::array<entry_t, n_records>*, size_t, std::string*>;
  auto insert_lambda = [] (rt::Handle&, const insert_args_t& args) {
    std::get<0>(args)->at(std::get<1>(args)) = parse_entry(*std::get<2>(args), entry_schema);
  };
  rt::Handle h;
  for (; i < records.size(); ++i) {
    insert_args_t args(&data, i, &records[i]);
    rt::asyncExecuteAt(h, rt::thisLocality(), insert_lambda, args);
    // data.at(i) = parse_entry(records[i], entry_schema);
  }
  rt::waitForCompletion(h);
  
  auto compare = [] (const entry_t& a, const entry_t& b)->bool {
    return a[3] < b[3];
  };
  auto max = shad::max_element(shad::distributed_parallel_tag{}, data.begin(), data.end(), compare);
  entry_t max_entry = *max;
  std::cout << "max el is " << max_entry[3] << std::endl;
  std::string decoded = shad::data_types::decode<uint64_t, std::string, shad::data_types::IP_ADDRESS>(max_entry[1]);
  std::cout << "decoded " << decoded << std::endl;


  shad::unordered_set<uint64_t> myset(n_records/8);
  shad::buffered_insert_iterator<shad::unordered_set<uint64_t>> ins_it(myset, myset.end());
  auto transf_lambda = [](entry_t e)->uint64_t {return e[1];};
  shad::transform(shad::distributed_parallel_tag{}, data.begin(), data.end(), ins_it, transf_lambda);

  std::cout << "set size " << myset.size() << std::endl;


  return 0;
}

}  // namespace shad