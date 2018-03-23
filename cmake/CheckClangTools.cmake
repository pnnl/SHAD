find_program(
  CLANG_TIDY_EXE
  NAMES "clang-tidy"
  DOC "Path to clang-tidy executable")

if(CLANG_TIDY_EXE)
  set(
    DO_CLANG_TIDY "${CLANG_TIDY_EXE}" "-p=${SHAD_BINARY_DIR} -format-style=file")
  message(STATUS "ClangTidy found: ${CLANG_TIDY_EXE}")
  if (NOT CMAKE_EXPORT_COMPILE_COMMANDS)
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
  endif()
else()
  message(STATUS "ClangTidy not found.")
endif()

find_program(
  CLANG_FORMAT_EXE
  NAMES "clang-format"
  DOC "Path to clang-format executable")

if (CLANG_FORMAT_EXE)
  message(STATUS "ClangFormat found: ${CLANG_FORMAT_EXE}")
  if (NOT CMAKE_EXPORT_COMPILE_COMMANDS)
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
  endif()
else()
  message(STATUS "ClangFormat not found.")
endif()
