add_library(shadperf_main STATIC benchmark_main.cc)
include_directories(${benchmark_INCLUDE_DIRS})

if (CLANG_TIDY_EXE)
  set_target_properties(
    shadperf_main PROPERTIES
    CXX_CLANG_TIDY "${DO_CLANG_TIDY}")
endif()
target_link_libraries(shadperf_main ${benchmark_LIBRARIES} ${SHAD_RUNTIME_LIB} runtime)

add_subdirectory(arti)
add_subdirectory(data_structures)
add_subdirectory(stl)