set(program array shad_array shad_map ex1_solution ex2_solution ex3_solution)

foreach(p ${program})
  add_executable(${p} ${p}.cc)
  target_link_libraries(${p} ${SHAD_RUNTIME_LIB} runtime)
endforeach(p)
