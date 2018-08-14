# Try to find the SPDLOG header only library
# Created by Methun
# Input variables:
#   SPDLOG_ROOT   - The SPDLOG root folder directory
# Output variables:
#   SPDLOG_FOUND          - System has SPDLOG
#   SPDLOG_INCLUDE_DIRS   - The SPDLOG include directories

include(FindPackageHandleStandardArgs)

if (NOT DEFINED SPDLOG_FOUND)

    # Set default search paths
    if (SPDLOG_ROOT)
        set(SPDLOG_INCLUDE_DIR ${SPDLOG_ROOT}/include CACHE PATH "The include directory for SPDLOG")
    endif()

    find_path(SPDLOG_INCLUDE_DIRS NAMES spdlog/spdlog.h HINTS ${SPDLOG_INCLUDE_DIR})

    find_package_handle_standard_args(SPDLOG
        FOUND_VAR SPDLOG_FOUND
        REQUIRED_VARS SPDLOG_INCLUDE_DIRS
        HANDLE_COMPONENTS)
    mark_as_advanced(SPDLOG_INCLUDE_DIR SPDLOG_INCLUDE_DIRS)
endif()
