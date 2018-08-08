add_library(shadtest_main STATIC gtest_main.cc)
include_directories(${GTEST_INCLUDE_DIRS})

if (CLANG_TIDY_EXE)
  set_target_properties(
    shadtest_main PROPERTIES
    CXX_CLANG_TIDY "${DO_CLANG_TIDY}")
endif()
target_link_libraries(shadtest_main  ${GTEST_LIBRARIES} ${SHAD_RUNTIME_LIB} runtime)

add_executable(test_gtest_remote_failure gtest_test.cc)
if (CLANG_TIDY_EXE)
  set_target_properties(
    test_gtest_remote_failure PROPERTIES
    CXX_CLANG_TIDY "${DO_CLANG_TIDY}")
endif()
target_link_libraries(test_gtest_remote_failure ${SHAD_RUNTIME_LIB} runtime shadtest_main)

add_test(NAME test_gtest_remote_failure
  COMMAND ${SHAD_TEST_COMMAND} $<TARGET_FILE:test_gtest_remote_failure>)

add_subdirectory(runtime)
add_subdirectory(data_structures)
add_subdirectory(core)
add_subdirectory(extensions/graph_library)
