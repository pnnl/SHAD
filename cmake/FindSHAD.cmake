# - Try to find the SHAD Library.
# Input variables:
#   SHAD_ROOT          - The SHAD install diretory
#   SHAD_INCLUDE       - The SHAD include directory
#   SHAD_LIBRARY       - The SHAD library directory
# Output variables:
#   SHAD_FOUND         - System has SHAD
#   SHAD_INCLUDE_DIRS  - The SHAD include directory
#   SHAD_LIBRARIES     - The libraries needed to use SHAD

include(FindPackageHandleStandardArgs)

if (NOT DEFINED SHAD_FOUND)
  if (SHAD_ROOT)
    set(SHAD_INCLUDE ${SHAD_ROOT}/include CACHE PATH "The include directory for SHAD")
    set(SHAD_LIBRARY ${SHAD_ROOT}/lib CACHE PATH "The library directory for SHAD")
  endif()

  find_path(SHAD_INCLUDE_DIRS NAMES shad/shad.h HINTS ${SHAD_INCLUDE})

  find_library(SHAD_UTIL_LIBRARY util HINTS ${SHAD_LIBRARY})
  if (SHAD_UTIL_LIBRARY)
    list(APPEND SHAD_LIBRARIES "${SHAD_UTIL_LIBRARY}")
  endif()

  find_library(SHAD_RUNTIME_LIBRARY runtime HINTS ${SHAD_LIBRARY})
  if (SHAD_RUNTIME_LIBRARY)
    list(APPEND SHAD_LIBRARIES "${SHAD_RUNTIME_LIBRARY}")
  endif()

  find_package_handle_standard_args(SHAD
      FOUND_VAR SHAD_FOUND
      REQUIRED_VARS SHAD_LIBRARIES SHAD_INCLUDE_DIRS
      HANDLE_COMPONENTS)

  mark_as_advanced(SHAD_INCLUDE SHAD_LIBRARY SHAD_INCLUDE_DIRS SHAD_LIBRARIES)
endif()
