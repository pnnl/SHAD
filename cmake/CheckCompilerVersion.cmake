# Check that the compiler is new enough.

include(CheckCXXSourceCompiles)

set("${CMAKE_CXX_COMPILER_ID}" TRUE CACHE STRING "C++ Compiler")
if (NOT DEFINED SHAD_COMPILER_CHECKED)
  set(DEFINED SHAD_COMPILER_CHECKED ON)

  if (GNU)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
      message(FATAL_ERROR "Host GCC version must be at least 4.8!")
    endif()
  elseif(Clang)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.1)
      message(FATAL_ERROR "Host Clang version must be at least 3.1!")
    endif()
  elseif(Intel)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 12.0)
      message(FATAL_ERROR "Host Intel Compiler version must be at least 12.0!")
    endif()
  else()
    message(WARNING "Your host compiler may not be supported")
  endif()
endif()
