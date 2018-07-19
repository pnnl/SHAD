set(tests
  L0_std_min_max_perf
  L0_std_non_modifying_perf
  L0_std_modifying_perf
  L0_std_numeric_perf
)

foreach(t ${tests})
  add_executable(${t} ${t}.cc)
  if (SHAD_ENABLE_STD_REDUCE_TEST)
    set_target_properties(
      ${t} PROPERTIES
      COMPILE_FLAGS "-DSTD_REDUCE_TEST")
  endif()
  if (CLANG_TIDY_EXE)
    set_target_properties(
      ${t} PROPERTIES
      CXX_CLANG_TIDY "${DO_CLANG_TIDY}")
  endif()
  target_link_libraries(${t} ${SHAD_RUNTIME_LIB} runtime shadperf_main ${benchmark_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
endforeach(t)
