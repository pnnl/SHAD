#cpp_simple always built
set(cpp_simple_sources cpp_simple/cpp_simple_main.cc)
set(runtime_prefixes cpp_simple)

if (TBB_ROOT)
  set(runtime_prefixes ${runtime_prefixes} tbb)
  set(tbb_sources tbb_mapping/tbb_main.cc)
endif ()
if (GMT_ROOT)
  set(runtime_prefixes ${runtime_prefixes} gmt)
  set(gmt_sources gmt_mapping/gmt_main.cc)
endif()

if (HAVE_CPP_SIMPLE)
  set(sources
    cpp_simple/cpp_simple_main.cc)
elseif (HAVE_TBB)
  set(sources
    tbb_mapping/tbb_main.cc)
elseif (HAVE_GMT)
  set(sources
    gmt_mapping/gmt_main.cc)
endif()


add_library(runtime STATIC ${sources})
if (CLANG_TIDY_EXE)
  set_target_properties(runtime PROPERTIES CXX_CLANG_TIDY "${DO_CLANG_TIDY}")
endif()


foreach(rp ${runtime_prefixes})
  add_library(${rp}_runtime STATIC ${${rp}_sources})
  if (CLANG_TIDY_EXE)
    set_target_properties(${rp}_runtime PROPERTIES CXX_CLANG_TIDY "${DO_CLANG_TIDY}")
  endif()
  if (GPERFTOOLS_FOUND)
    target_include_directories(${rp}_runtime PUBLIC ${GPERFTOOLS_INCLUDE_DIRS})
    target_link_libraries(${rp}_runtime PUBLIC ${GPERFTOOLS_LIBRARIES})
  endif()
  string(TOUPPER "${rp}" uprp)
  
  target_include_directories(${rp}_runtime
                             PUBLIC 
                             ${${uprp}_INCLUDE_DIRS}
                             $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                             $<INSTALL_INTERFACE:include>)
  target_link_libraries(${rp}_runtime PUBLIC ${${uprp}_LIBRARIES})
  target_compile_definitions(${rp}_runtime PUBLIC HAVE_${uprp}=1)
  install(TARGETS ${rp}_runtime
          EXPORT ${rp}_runtime_export
          ARCHIVE DESTINATION lib)
  install(EXPORT ${rp}_runtime_export
          FILE shad_${rp}_runtime_export.cmake
          DESTINATION cmake)
endforeach(rp)


if (GPERFTOOLS_FOUND)
  target_include_directories(runtime PUBLIC ${GPERFTOOLS_INCLUDE_DIRS})
  target_link_libraries(runtime PUBLIC ${GPERFTOOLS_LIBRARIES})
endif()
if (HAVE_CPP_SIMPLE)
  target_compile_definitions(runtime PUBLIC HAVE_CPP_SIMPLE=1)
elseif (HAVE_TBB)
  target_include_directories(runtime PUBLIC ${TBB_INCLUDE_DIRS})
  target_compile_definitions(runtime PUBLIC HAVE_TBB=1)
  target_link_libraries(runtime PUBLIC ${TBB_LIBRARIES})
elseif (HAVE_GMT)
  target_include_directories(runtime PUBLIC ${GMT_INCLUDE_DIRS})
  target_compile_definitions(runtime PUBLIC HAVE_GMT=1)
  target_link_libraries(runtime PUBLIC ${GMT_LIBRARIES})
endif()
install(TARGETS runtime ARCHIVE DESTINATION lib)
