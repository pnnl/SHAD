if (SHAD_ENABLE_DOXYGEN)
  set(abs_top_srcdir   ${SHAD_MAIN_SRC_DIR})
  set(abs_top_builddir ${SHAD_BINARY_DIR})

  if (HAVE_DOT)
    set(DOT ${SHAD_PATH_DOT})
  endif()

  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/doxygen.cfg.in
    ${CMAKE_CURRENT_BINARY_DIR}/doxygen.cfg @ONLY)

  add_custom_target(doxygen-shad
    COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/doxygen.cfg
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generating SHAD doxygen documentation." VERBATIM)

  install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/doxygen/html/.
    COMPONENT doxygen-html
    DESTINATION doc)

  add_dependencies(doxygen doxygen-shad)
endif()
