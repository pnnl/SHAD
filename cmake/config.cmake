# Configure checks

include(CheckCCompilerFlag)
include(CheckCXXCompilerFlag)
include(CheckCompilerVersion)
include(CheckIncludeFile)
include(CheckIncludeFileCXX)
include(CheckLibraryExists)

set(CMAKE_CXX_STANDARD 14)

## Workaround for the intel compiler
if (Intel)
  if (${CMAKE_VERSION} VERSION_LESS "3.6")
    if (CMAKE_CXX_STANDARD STREQUAL 14)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
    else()
      message(FATAL_ERROR "Unsupported C++ Standard requested")
    endif()
  endif()
endif()

# C compiler flags:

## C Release Build flags:
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3")
check_c_compiler_flag("-march=native" C_SUPPORT_MARCH_NATIVE)
if (C_SUPPORT_MARCH_NATIVE)
  set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -march=native")
endif()

## C Debug Build flags:
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g")

## C Release+Debug
set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELEASE} ${CMAKE_C_FLAGS_DEBUG}")

# CXX compiler flags:

## CXX Release Build flags:
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
check_cxx_compiler_flag("-march=native" CXX_SUPPORT_MARCH_NATIVE)
if (CXX_SUPPORT_MARCH_NATIVE)
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native")
endif()

## CXX Debug Build flags:
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_GLIBCXX_DEBUG -D_GLIBCXX_DEBUG_PEDANTIC")

## CXX Release+Debug
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_CXX_FLAGS_DEBUG}")

# include files checks

# library checks:
if (SHAD_RUNTIME_SYSTEM STREQUAL "CPP_SIMPLE")
  message(STATUS "Using the default C++ implementation of the Abstract Runtime API.")
  find_package(Threads REQUIRED)
  include_directories(${THREADS_PTHREADS_INCLUDE_DIR})
  set(HAVE_CPP_SIMPLE 1)
  set(SHAD_RUNTIME_LIB ${CMAKE_THREAD_LIBS_INIT})
elseif (SHAD_RUNTIME_SYSTEM STREQUAL "TBB")
  message(STATUS "Using Intel Threading Building Blocks (TBB) as backend of the Abstract Runtime API.")

  find_package(Threads REQUIRED)
  include_directories(${THREADS_PTHREADS_INCLUDE_DIR})

  find_package(TBB REQUIRED)
  set(HAVE_TBB 1)
  set(SHAD_RUNTIME_LIB ${CMAKE_THREAD_LIBS_INIT} ${TBB_LIBRARIES})
elseif (SHAD_RUNTIME_SYSTEM STREQUAL "GMT")
  message(STATUS "Using Global Threading and Memory (GMT) as backend of the Abstract Runtime API.")
  find_package(GMT REQUIRED)
  include_directories(${GMT_INCLUDE_DIR})
  set(HAVE_GMT 1)
  set(SHAD_RUNTIME_LIB ${GMT_LIBRARIES})
else()
  message(FATAL_ERROR "${SHAD_RUNTIME_SYSTEM} is not a supported runtime system.")
endif()

find_package(Gperftools COMPONENTS tcmalloc)
if (GPERFTOOLS_FOUND)
  check_c_compiler_flag("-fno-builtin-malloc" C_SUPPORT_NO_BUILTIN_MALLOC)
  if (C_SUPPORT_NO_BUILTIN_MALLOC)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-builtin-malloc")
  endif()

  check_c_compiler_flag("-fno-builtin-calloc" C_SUPPORT_NO_BUILTIN_CALLOC)
  if (C_SUPPORT_NO_BUILTIN_CALLOC)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-builtin-calloc")
  endif()

  check_c_compiler_flag("-fno-builtin-realloc" C_SUPPORT_NO_BUILTIN_REALLOC)
  if (C_SUPPORT_NO_BUILTIN_REALLOC)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-builtin-realloc")
  endif()

  check_c_compiler_flag("-fno-builtin-free" C_SUPPORT_NO_BUILTIN_FREE)
  if (C_SUPPORT_NO_BUILTIN_FREE)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-builtin-free")
  endif()
  check_cxx_compiler_flag("-fno-builtin-malloc" CXX_SUPPORT_NO_BUILTIN_MALLOC)
  if (CXX_SUPPORT_NO_BUILTIN_MALLOC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc")
  endif()

  check_cxx_compiler_flag("-fno-builtin-malloc" CXX_SUPPORT_NO_BUILTIN_CALLOC)
  if (CXX_SUPPORT_NO_BUILTIN_CALLOC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-calloc")
  endif()

  check_cxx_compiler_flag("-fno-builtin-realloc" CXX_SUPPORT_NO_BUILTIN_REALLOC)
  if (CXX_SUPPORT_NO_BUILTIN_REALLOC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-realloc")
  endif()

  check_cxx_compiler_flag("-fno-builtin-free" CXX_SUPPORT_NO_BUILTIN_FREE)
  if (CXX_SUPPORT_NO_BUILTIN_FREE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-free")
  endif()
endif()

if (SHAD_ENABLE_UNIT_TEST)
  find_package(GTest REQUIRED)
endif()

if (SHAD_ENABLE_PERFORMANCE_TEST)
  find_package(benchmark REQUIRED)
  find_package(Threads REQUIRED)
endif()

# tools:
include(CheckClangTools)
if (SHAD_ENABLE_UNIT_TEST OR SHAD_ENABLE_PERFORMANCE_TEST)
  find_package(SLURM)
endif()

# Documentation:
if (SHAD_ENABLE_DOXYGEN)
  message(STATUS "Doxygen enabled.")
  find_package(Doxygen REQUIRED)

  find_program(SHAD_PATH_DOT NAMES dot)
  if (SHAD_PATH_DOT)
    set(HAVE_DOT 1 CACHE INTERNAL "Dot ?")
  else()
    set(HAVE_DOT "" CACHE INTERNAL "Dot ?")
  endif()

  add_custom_target(doxygen ALL)
else()
  message(STATUS "Doxygen disabled.")
endif()

