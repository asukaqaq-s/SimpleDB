# CMake generated Testfile for 
# Source directory: /home/asuka/workbench/project/SimpleDB/test
# Build directory: /home/asuka/workbench/project/SimpleDB/build/test
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
include("/home/asuka/workbench/project/SimpleDB/build/test/main[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/file_manager_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/file_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/page_test[1]_include.cmake")
add_test(main "/home/asuka/workbench/project/SimpleDB/build/test/main" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/main.xml")
set_tests_properties(main PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(file_manager_test "/home/asuka/workbench/project/SimpleDB/build/test/file_manager_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/file_manager_test.xml")
set_tests_properties(file_manager_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(file_test "/home/asuka/workbench/project/SimpleDB/build/test/file_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/file_test.xml")
set_tests_properties(file_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(page_test "/home/asuka/workbench/project/SimpleDB/build/test/page_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/page_test.xml")
set_tests_properties(page_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")