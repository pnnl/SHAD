if (SHAD_ENABLE_DOXYGEN)
  set(abs_top_srcdir   ${SHAD_MAIN_SRC_DIR})
  set(abs_top_builddir ${SHAD_BINARY_DIR})

  # if (HAVE_DOT)
  #   set(DOT ${SHAD_PATH_DOT})
  # endif()

  # configure_file(${CMAKE_CURRENT_SOURCE_DIR}/doxygen.cfg.in
  #   ${CMAKE_CURRENT_BINARY_DIR}/doxygen.cfg @ONLY)

  # add_custom_target(doxygen-shad
  #   COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/doxygen.cfg
  #   WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  #   COMMENT "Generating SHAD doxygen documentation." VERBATIM)

  # add_dependencies(doxygen doxygen-shad)

  add_custom_target(sphinx-docs
    ${SPHINX_EXECUTABLE}
      -q -b html
      -c ${CMAKE_CURRENT_SOURCE_DIR}
      -d ${CMAKE_CURRENT_BINARY_DIR}/doctrees
      ${CMAKE_CURRENT_SOURCE_DIR}
      ${CMAKE_CURRENT_BINARY_DIR}/html)
  add_dependencies(docs sphinx-docs)

  install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/html/.
    COMPONENT docs
    DESTINATION docs)
endif()
