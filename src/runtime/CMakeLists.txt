if (HAVE_TBB)
  set(sources
    tbb_mapping/tbb_main.cc)
endif()

add_library(runtime STATIC ${sources})
if (CLANG_TIDY_EXE)
  set_target_properties(runtime PROPERTIES CXX_CLANG_TIDY "${DO_CLANG_TIDY}")
endif()

if (GPERFTOOLS_FOUND)
  target_include_directories(runtime PUBLIC ${GPERFTOOLS_INCLUDE_DIRS})
  target_link_libraries(runtime PUBLIC ${GPERFTOOLS_LIBRARIES})
endif()
if (HAVE_TBB)
  target_include_directories(runtime PUBLIC ${TBB_INCLUDE_DIRS})
  target_link_libraries(runtime PUBLIC ${TBB_LIBRARIES})
endif()
install(TARGETS runtime ARCHIVE DESTINATION lib)
