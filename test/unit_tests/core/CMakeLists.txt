set(tests
  iterator_test
  for_test
  shad_array_test
  unordered_set_test
  unordered_map_test
  shad_algorithm_test
  shad_numeric_test
)

foreach(t ${tests})
  add_executable(${t} ${t}.cc)
  if (CLANG_TIDY_EXE)
    set_target_properties(
      ${t} PROPERTIES
      CXX_CLANG_TIDY "${DO_CLANG_TIDY}")
  endif()
  target_link_libraries(${t} ${SHAD_RUNTIME_LIB} runtime shadtest_main)
  add_test(NAME ${t} COMMAND ${SHAD_TEST_COMMAND} $<TARGET_FILE:${t}>)
endforeach(t)
