#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <string>
#include <vector>
#include <climits>
#include <fstream>
#include <sstream>
#include <iostream>
#include "shad/runtime/runtime.h"
#include "shad/extensions/hypergraph_library/hypergraph.h"
#include "shad/extensions/table_library/local_table.h"

#include <sys/time.h>
#include <time.h>

double my_timer() {
  struct timeval tv;
  struct timezone tz;
  gettimeofday(&tv, &tz);
  return ((double)tv.tv_sec + (double)0.000001 * (double)tv.tv_usec);
}

using schema_t = shad::data_types::schema_t;
using table_t = shad::LocalTable<uint64_t>;
using hypergraph_t = shad::Hypergraph<table_t>;
using collapse_set_t = shad::LocalSet<std::pair <uint64_t, uint64_t> >;
using index_t = shad::LocalIndex<uint64_t, uint64_t>;

