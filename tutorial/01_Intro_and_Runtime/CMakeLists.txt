set(program ex1_hello_SHAD ex2_rdma sol1_hello_SHAD sol2_rdma)

foreach(p ${program})
  add_executable(${p} ${p}.cc)
  target_link_libraries(${p} ${SHAD_RUNTIME_LIB} runtime)
endforeach(p)
