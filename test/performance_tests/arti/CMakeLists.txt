set(tests execute asyncexecute foreach asyncforeach)

foreach(t ${tests})
  add_executable(${t} ${t}.cc)
  target_link_libraries(${t} ${SHAD_RUNTIME_LIB} runtime)
endforeach(t)
