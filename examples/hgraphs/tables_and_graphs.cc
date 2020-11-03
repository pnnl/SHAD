#include "tables_and_graphs.h"

namespace shad {
int main ( int argc, char ** argv) {

// READ FILE NAME FROM COMMAND LINE
  if (argc != 2) {printf("Usage: <input file>\n"); return 1;}

  double time1 = my_timer();

/**********  DATA TABLE **********/
  schema_t dataTableSchema(4);
  dataTableSchema[0] = std::make_pair("domain", shad::data_types::UINT);
  dataTableSchema[1] = std::make_pair("server", shad::data_types::IP_ADDRESS);
  dataTableSchema[2] = std::make_pair("count",  shad::data_types::UINT);
  dataTableSchema[3] = std::make_pair("hours",  shad::data_types::UINT);
  table_t Data(std::string(argv[1]), dataTableSchema);
  printf("   Number of data record = %lu\n", Data.num_rows());

  printf("Time for table construction is %lf\n", my_timer() - time1);


/**********  VERTEX TABLES **********/
// servers are the vertices
  schema_t vertexSchema(1);
  vertexSchema[0] = std::make_pair("server", shad::data_types::IP_ADDRESS);
  std::vector<uint64_t> columnsToCheck = {1};     // "server"
  std::vector<uint64_t> columnsToMove = { };      // no additional attributes
  table_t  V(columnsToCheck, columnsToMove, vertexSchema, Data, true);
  printf("   Number of vertices = %lu\n", V.num_rows());

// domains are the hyperedges
  schema_t hyperEdgeSchema(1);
  hyperEdgeSchema[0] = std::make_pair("domain", shad::data_types::UINT);
  columnsToCheck = {0};     // "domain"
  columnsToMove = { };      // nothing to move
  table_t HE(columnsToCheck, columnsToMove, hyperEdgeSchema, Data, true);
  printf("   Number of hyperedges = %lu\n", HE.num_rows());

/**********  EDGE TABLES **********/
// Hyperedges to vertices
  table_t & HEV = Data;  // same as the initial data
  printf("   Number of hyperedge to vertex edges %lu\n", HEV.num_rows());

// Vertices to hyperedges
  schema_t vheSchema(4);
  vheSchema[0] = std::make_pair("server", shad::data_types::IP_ADDRESS);
  vheSchema[1] = std::make_pair("domain", shad::data_types::UINT);
  vheSchema[2] = std::make_pair("count",  shad::data_types::UINT);
  vheSchema[3] = std::make_pair("hours",  shad::data_types::UINT);
  columnsToCheck = {1};          // "server"
  columnsToMove = {0, 2, 3};     // edge attributes
  shad::LocalTable<uint64_t> VHE(columnsToCheck, columnsToMove, vheSchema, Data, false);
  printf("   Number of vertex to hyperedge edges %lu\n", VHE.num_rows());

/**********  CREATE GRAPH **********/
  hypergraph_t hgraph(&HE, &V, &HEV, &VHE);

  time1 = my_timer();

  collapse_set_t collapse(HE.num_rows()/16);
  hgraph.Collapse(collapse);
  printf("\n   Number of collapsed items = %lu\n", collapse.Size());
  printf("Time for hgraph Collapse is %lf\n", my_timer() - time1);
  
  time1 = my_timer();
  
  index_t e2v(HEV.num_rows());
  table_t::CreateLocalIndex(HEV, e2v, 0, 1);

  printf("Time for v2e2v is %lf\n", my_timer() - time1);
  printf("e2v Index size is %lu\n", e2v.Size());

  time1 = my_timer();
  shad::LocalIndex<uint64_t, uint64_t> overlaps(HE.num_rows());
  hgraph.S_LineGraph(1, e2v, overlaps);
  printf("Time for overlpas %lf\n", my_timer() - time1);
  printf("overlaps size is %lu\n", overlaps.Size());

// purposely introduce at least a path in the hgraph
  overlaps.Insert(0lu, 2lu);
  overlaps.Insert(0lu, 20lu);
  overlaps.Insert(2lu, 3lu);
  overlaps.Insert(3lu, 1lu);
  
  auto dist = decltype(hgraph)::S_Distance(overlaps, 0lu, 1lu);
  std::cout << "dist: " << dist << std::endl;
  auto path = decltype(hgraph)::S_ShortestPath(overlaps, 0lu, 1lu);
  std::cout << "shortest path: ";
  for (auto v : path) {
    std::cout << v << " ";
  }
  std::cout << std::endl;
  return 0;
}
}
