set(program ex1_legacy_data_structures ex1_new_data_structure ex2_utils_and_ext sol1_new_data_structure sol1_legacy_data_structures)

foreach(p ${program})
  add_executable(${p} ${p}.cc)
  target_link_libraries(${p} ${SHAD_RUNTIME_LIB} runtime)
endforeach(p)
