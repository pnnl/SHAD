set(sources
  shad.cc
)
set(headers
)

add_library(util STATIC ${sources})

if (CLANG_TIDY_EXE)
  set_target_properties(
    util PROPERTIES
    CXX_CLANG_TIDY "${DO_CLANG_TIDY}")
endif()

target_link_libraries(util runtime)
install(TARGETS util ARCHIVE DESTINATION lib)
