set(tests
  array_test
  hashmap_test
  local_hashmap_test
  one_per_locality_test
  set_test
  local_set_test
  vector_test
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
