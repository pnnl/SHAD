set(tests array_perf
    vector_perf
    hashmap_perf
    set_perf
    local_hashmap_perf)

foreach(t ${tests})
  add_executable(${t} ${t}.cc)
  target_link_libraries(${t} ${SHAD_RUNTIME_LIB} runtime ${benchmark_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
endforeach(t)
