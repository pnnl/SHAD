# See README.md for instruction on how to build SHAD with CMake

cmake_minimum_required(VERSION 3.1)

if (NOT DEFINED SHAD_VERSION_MAJOR)
  set(SHAD_VERSION_MAJOR 0)
endif()
if (NOT DEFINED SHAD_VERSION_MINOR)
  set(SHAD_VERSION_MINOR 0)
endif()
if (NOT DEFINED SHAD_VERSION_PATCH)
  set(SHAD_VERSION_PATCH 1)
endif()

if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
  set(cmake_3_0_PROJ_VERSION
    VERSION "${SHAD_VERSION_MAJOR}.${SHAD_VERSION_MINOR}.${SHAD_VERSION_PATCH}" )
  set(cmake_3_0_LANGUAGES LANGUAGES)
endif()

if (POLICY CMP0054)
  cmake_policy(SET CMP0054 NEW)
endif()

# Add path for custom CMake modules
set(CMAKE_MODULE_PATH
  ${CMAKE_MODULE_PATH}
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake"
  )

include(GenerateVersionFromGit)
add_version_from_git(GIT_VERSION)

if (NOT PACKAGE_VERSION)
  set(PACKAGE_VERSION
    "${SHAD_VERSION_MAJOR}.${SHAD_VERSION_MINOR}.${SHAD_VERSION_PATCH} (${GIT_VERSION})")
endif()

if (CMAKE_C_COMPILER STREQUAL "")
  set(CMAKE_C_COMPILER gcc)
endif()
if (CMAKE_CXX_COMPILER STREQUAL "")
  set(CMAKE_CXX_COMPILER g++)
endif()

project(SHAD ${cmake_3_0_PROJ_VERSION} ${cmake_3_0_LANGUAGES} C CXX)

if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "No build type selected, default to Debug")
  set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Build type (default Debug)" FORCE)
endif()


# No in-tree build allowed.
if (CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
  message(FATAL_ERROR
    "In-source build are not allowed.
Please create a directory directory and run cmake from there, passing the path
to this source directory as the last argumente.
This process created the file `CMakeCache.txt' and the directory `CMakeFiles'.
Please delete them.")
endif()


string(TOUPPER "${CMAKE_BUILD_TYPE}" uppercase_CMAKE_BUILD_TYPE)

if (CMAKE_BUILD_TYPE AND
    NOT uppercase_CMAKE_BUILD_TYPE MATCHES "^(DEBUG|RELEASE|RELWITHDEBINFO|MINSIZEREL)$")
  message(FATAL_ERROR "Invalid value for CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
endif()

# Build options:
option(SHAD_ENABLE_CPPLINT "Enable the linting of source code" ON)
option(SHAD_ENABLE_DOXYGEN "Use doxygen to generate the shad API documentation" OFF)
option(SHAD_ENABLE_UNIT_TEST "Enable the compilation of Unit Tests" ON)
option(SHAD_ENABLE_PERFORMANCE_TEST "Enable the compilation of the Performance Tests" OFF)

set(
  SHAD_RUNTIME_SYSTEM "CPP_SIMPLE" CACHE STRING
  "Runtime system to be used as backend of the Abstract Runtime API (Default=CPP_SIMPLE, Supported=CPP_SIMPLE | TBB | GMT)")


include(config)

set(SHAD_MAIN_SRC_DIR     ${CMAKE_CURRENT_SOURCE_DIR})
set(SHAD_MAIN_INCLUDE_DIR ${SHAD_MAIN_SRC_DIR}/include)
set(SHAD_BINARY_DIR       ${CMAKE_CURRENT_BINARY_DIR})
set(SHAD_INCLUDE_DIR      ${SHAD_BINARY_DIR}/include)


# Verify that we can find a Python interpreter.  It is needed for the build.
include(FindPythonInterp)
if (NOT PYTHONINTERP_FOUND)
  message(FATAL_ERROR
    "Unable to find Python interpreter.  It is required for builds."
    "Please install Python or specify the PYTHON_EXECUTABLE CMake variable")
endif()

configure_file(
  ${SHAD_MAIN_INCLUDE_DIR}/shad/config/config.h.in
  ${SHAD_INCLUDE_DIR}/shad/config/config.h)
install(FILES ${SHAD_INCLUDE_DIR}/shad/config/config.h
  DESTINATION include/shad/config)

configure_file(
  ${SHAD_MAIN_INCLUDE_DIR}/shad/config/test-config.h.in
  ${SHAD_INCLUDE_DIR}/shad/config/test-config.h)

include_directories(${SHAD_MAIN_INCLUDE_DIR} ${SHAD_INCLUDE_DIR})

configure_file(
  ${SHAD_MAIN_SRC_DIR}/pkgconfig/shad.pc.in
  ${SHAD_BINARY_DIR}/pkgconfig/shad.pc @ONLY)
install(FILES ${SHAD_BINARY_DIR}/pkgconfig/shad.pc DESTINATION pkgconfig)
install(FILES ${SHAD_MAIN_SRC_DIR}/cmake/FindSHAD.cmake DESTINATION cmake)

# Test commands
set(SHAD_TEST_COMMAND "")
if (SHAD_RUNTIME_SYSTEM STREQUAL "TBB")
  set(SHAD_TEST_NODES 1)
endif()
if (SHAD_RUNTIME_SYSTEM STREQUAL "CPP_SIMPLE")
  set(SHAD_TEST_NODES 1)
endif()
if (SLURM_FOUND)
  if (NOT DEFINED SHAD_TEST_NODES)
    set(SHAD_TEST_NODES 2)
  endif()
  set(SLURM_SALLOC_FLAGS -p all -N ${SHAD_TEST_NODES}
    CACHE STRING "Flags used running srun.")
  set(SHAD_TEST_COMMAND ${SLURM_SALLOC_COMMAND} ${SLURM_SALLOC_FLAGS})
endif()
if (MPI_CXX_FOUND)
  set(MPIEXEC_PREFLAGS -pernode)
  set(SHAD_TEST_COMMAND
    ${SHAD_TEST_COMMAND} ${MPIEXEC} ${MPIEXEC_NUMPROC_FLAG} ${SHAD_TEST_NODES} ${MPIEXEC_PREFLAGS})
endif()

add_subdirectory(src)
if (SHAD_ENABLE_UNIT_TEST)
  enable_testing()
  include(CTest)
  add_subdirectory(test/unit_tests)
endif()
if (SHAD_ENABLE_PERFORMANCE_TEST)
  add_subdirectory(test/performance_tests)
endif()
if (SHAD_ENABLE_DOXYGEN)
  add_subdirectory(docs)
endif()
add_subdirectory(examples)

install(DIRECTORY ${SHAD_MAIN_INCLUDE_DIR}/shad
  DESTINATION include
  PATTERN "*.h"
  PATTERN ".*" EXCLUDE
  PATTERN "*.in" EXCLUDE
  PATTERN "*~" EXCLUDE
  PATTERN "CMakeLists.txt"
)

# Adding clang-format target if executable is found
if(CLANG_FORMAT_EXE)
  # Additional targets to perform clang-format/clang-tidy
  # Get all project files
  file(GLOB_RECURSE
    ALL_CXX_SOURCE_FILES
    *.[chi]pp *.[chi]xx *.cc *.hh *.ii *.[CHI] *.[ch]
    )

  add_custom_target(
    clang-format
    COMMAND ${CLANG_FORMAT_EXE}
    -i
    ${ALL_CXX_SOURCE_FILES}
    )
endif()
