# Try to find the sphinx-build executable
#

find_program(SPHINX_EXECUTABLE NAMES sphinx-build
  HINTS
  ${SPHINX_ROOT}
  PATH_SUFFIXES bin
  DOC "Sphinx documentation generator")

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(Sphinx DEFAULT_MSG SPHINX_EXECUTABLE)

mark_as_advanced(SPHINX_EXECUTABLE)
