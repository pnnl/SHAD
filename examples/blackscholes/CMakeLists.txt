add_executable(blackscholes blackscholes.cc)
target_link_libraries(blackscholes ${SHAD_RUNTIME_LIB} runtime)
