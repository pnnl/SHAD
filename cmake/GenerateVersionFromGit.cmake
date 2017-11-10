# Add Git revision information to the variable VERS.
# The revinios information is determined using git describe.
function(add_version_from_git VERS)
  set(SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})

  if (EXISTS "${SOURCE_DIR}/.git")
    find_program(git_executable NAMES git git.exe git.cmd)
    if (git_executable)
      execute_process(COMMAND
        ${git_executable} describe --all --dirty --long
        WORKING_DIRECTORY ${SOURCE_DIR}
        TIMEOUT 5
        RESULT_VARIABLE git_result
        OUTPUT_VARIABLE git_output)

      if (git_result EQUAL 0)
        string(STRIP "${git_output}" git_ref_id)
        set(${VERS} ${git_ref_id} PARENT_SCOPE)
      endif()
    endif()
  endif()
endfunction()
