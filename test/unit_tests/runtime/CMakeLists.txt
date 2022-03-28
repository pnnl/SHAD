set(tests execute_at_test execute_on_all_test for_each_test rdma_test)

foreach(t ${tests})
  add_executable(${t} ${t}.cc)
  if (CLANG_TIDY_EXE)
    set_target_properties(
      ${t} PROPERTIES
      CXX_CLANG_TIDY "${DO_CLANG_TIDY}")
  endif()
  if (HAVE_HPX)
    target_link_libraries(${t} HPX::hpx HPX::iostreams_component runtime shadtest_main)
  else()
    target_link_libraries(${t} ${SHAD_RUNTIME_LIB} runtime shadtest_main)
  endif()
  add_test(NAME ${t} COMMAND ${SHAD_TEST_COMMAND} $<TARGET_FILE:${t}>)
endforeach(t)
