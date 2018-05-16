# Try to find the GMT runtime system
# Input variables:
#   GMT_ROOT   - The GMT install directory
# Output variables:
#   GMT_FOUND          - System has GMT
#   GMT_INCLUDE_DIRS   - The GMT include directories
#   GMT_LIBRARIES      - The GMT libraries needed to use gperftools

include(FindPackageHandleStandardArgs)

if (NOT DEFINED GMT_FOUND)
  # Check that MPI is available
  find_package(MPI REQUIRED)
  find_package(Threads REQUIRED)

  # Set default search paths
  if (GMT_ROOT)
    set(GMT_INCLUDE_DIR ${GMT_ROOT}/include CACHE PATH "The include directory for GMT")
    set(GMT_LIBRARY_DIR ${GMT_ROOT}/lib CACHE PATH "The library directory for GMT")
  endif()

  find_path(GMT_INCLUDE_DIRS NAMES gmt/gmt.h HINTS ${GMT_INCLUDE_DIR})

  # Search for the GMT library
  find_library(GMT_LIBRARY gmt HINTS ${GMT_LIBRARY_DIR})
  if (GMT_LIBRARY)
    list(APPEND GMT_INCLUED_DIR ${CMAKE_PTHREADS_INCLUDE_DIR})
    list(APPEND GMT_LIBRARIES rt)
    list(APPEND GMT_LIBRARIES ${GMT_LIBRARY})
    list(APPEND GMT_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
    list(APPEND GMT_LIBRARIES ${MPI_LIBRARIES})
  endif()

  find_package_handle_standard_args(GMT
    FOUND_VAR GMT_FOUND
    REQUIRED_VARS GMT_LIBRARIES GMT_INCLUDE_DIRS
    HANDLE_COMPONENTS)
  mark_as_advanced(GMT_INCLUDE_DIR GMT_LIBRARY GMT_INCLUDE_DIRS GMT_LIBRARIES)
endif()
