set(tests
  for_ut
  L0_std_min_max_ut
  L0_std_non_modifying_ut
  L0_std_numeric_ut
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
  target_link_libraries(${t} ${SHAD_RUNTIME_LIB} runtime shadtest_main)
  add_test(NAME ${t} COMMAND ${SHAD_TEST_COMMAND} $<TARGET_FILE:${t}>)
endforeach(t)
