file(GLOB SIMPLE_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*/*.cc")

include(GoogleTest)

find_program(VALGRIND_BIN valgrind)
if ("${VALGRIND_BIN}" STREQUAL "VALGRIND_BIN-NOTFOUND")
    message(WARNING " simpledb/test couldn't find valgrind.")
else()
    message(STATUS " simpledb/test found valgrind at ${VALGRIND_BIN}")
endif()

set(VALGRIND_OPTIONS
        --error-exitcode=1                      # if leaks are detected, return nonzero value
        # --gen-suppressions=all  # uncomment for leak suppression syntax
        --leak-check=full                       # detailed leak information
        --soname-synonyms=somalloc=*jemalloc*   # also intercept jemalloc
        --trace-children=yes                    # trace child processes
        --track-origins=yes                     # track origin of uninitialized values
        )

# "make unittests"
##########################################
add_custom_target(show-tests COMMAND ${CMAKE_CTEST_COMMAND} --show-only)
add_custom_target(unittests COMMAND ${CMAKE_CTEST_COMMAND} --verbose)

##########################################
# "make XYZ_test"
##########################################
foreach (simple_test_source ${SIMPLE_TEST_SOURCES})
    # Create a human readable name.
    get_filename_component(simple_test_filename ${simple_test_source} NAME)
    string(REPLACE ".cc" "" simple_test_name ${simple_test_filename})

    add_executable(${simple_test_name} ${simple_test_source})

    target_link_libraries(${simple_test_name} gtest_main ${CMAKE_PROJECT_NAME}_lib)

    # Set test target properties and dependencies.
    # Specify the output directory. So we can integrate it into ctest directly
    # using path
    set_target_properties(${simple_test_name}
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
        COMMAND ${simple_test_name}
    )

    # Add the test under CTest.
    add_test(${simple_test_name} ${VALGRIND_BIN} ${VALGRIND_OPTIONS}
    --suppressions=${VALGRIND_SUPPRESSIONS_FILE} ${CMAKE_BINARY_DIR}/test/${simple_test_name}
    --gtest_color=yes --gtest_output=xml:${CMAKE_BINARY_DIR}/test/unit_${test_name}.xml)
    add_test(${simple_test_name} ${CMAKE_BINARY_DIR}/test/${simple_test_name} --gtest_color=yes
            --gtest_output=xml:${CMAKE_BINARY_DIR}/test/${simple_test_name}.xml)
    gtest_discover_tests(${simple_test_name})

endforeach(simple_test_source ${simple_TEST_SOURCES})