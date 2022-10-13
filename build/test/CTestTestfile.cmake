# CMake generated Testfile for 
# Source directory: /home/asuka/workbench/project/SimpleDB/test
# Build directory: /home/asuka/workbench/project/SimpleDB/build/test
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
include("/home/asuka/workbench/project/SimpleDB/build/test/main[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/buffer_manager_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/buffer_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/lru_replacer_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/concurrency_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/transaction_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/file_manager_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/file_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/page_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/log_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/metadata_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/tokenizer_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/product_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/scan_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/table_page_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/table_scan_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/log_record_test[1]_include.cmake")
include("/home/asuka/workbench/project/SimpleDB/build/test/recovery_manager_test[1]_include.cmake")
add_test(main "/home/asuka/workbench/project/SimpleDB/build/test/main" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/main.xml")
set_tests_properties(main PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(buffer_manager_test "/home/asuka/workbench/project/SimpleDB/build/test/buffer_manager_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/buffer_manager_test.xml")
set_tests_properties(buffer_manager_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(buffer_test "/home/asuka/workbench/project/SimpleDB/build/test/buffer_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/buffer_test.xml")
set_tests_properties(buffer_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(lru_replacer_test "/home/asuka/workbench/project/SimpleDB/build/test/lru_replacer_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/lru_replacer_test.xml")
set_tests_properties(lru_replacer_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(concurrency_test "/home/asuka/workbench/project/SimpleDB/build/test/concurrency_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/concurrency_test.xml")
set_tests_properties(concurrency_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(transaction_test "/home/asuka/workbench/project/SimpleDB/build/test/transaction_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/transaction_test.xml")
set_tests_properties(transaction_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(file_manager_test "/home/asuka/workbench/project/SimpleDB/build/test/file_manager_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/file_manager_test.xml")
set_tests_properties(file_manager_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(file_test "/home/asuka/workbench/project/SimpleDB/build/test/file_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/file_test.xml")
set_tests_properties(file_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(page_test "/home/asuka/workbench/project/SimpleDB/build/test/page_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/page_test.xml")
set_tests_properties(page_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(log_test "/home/asuka/workbench/project/SimpleDB/build/test/log_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/log_test.xml")
set_tests_properties(log_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(metadata_test "/home/asuka/workbench/project/SimpleDB/build/test/metadata_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/metadata_test.xml")
set_tests_properties(metadata_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(tokenizer_test "/home/asuka/workbench/project/SimpleDB/build/test/tokenizer_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/tokenizer_test.xml")
set_tests_properties(tokenizer_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(product_test "/home/asuka/workbench/project/SimpleDB/build/test/product_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/product_test.xml")
set_tests_properties(product_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(scan_test "/home/asuka/workbench/project/SimpleDB/build/test/scan_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/scan_test.xml")
set_tests_properties(scan_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(table_page_test "/home/asuka/workbench/project/SimpleDB/build/test/table_page_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/table_page_test.xml")
set_tests_properties(table_page_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(table_scan_test "/home/asuka/workbench/project/SimpleDB/build/test/table_scan_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/table_scan_test.xml")
set_tests_properties(table_scan_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(log_record_test "/home/asuka/workbench/project/SimpleDB/build/test/log_record_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/log_record_test.xml")
set_tests_properties(log_record_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
add_test(recovery_manager_test "/home/asuka/workbench/project/SimpleDB/build/test/recovery_manager_test" "--gtest_color=yes" "--gtest_output=xml:/home/asuka/workbench/project/SimpleDB/build/test/recovery_manager_test.xml")
set_tests_properties(recovery_manager_test PROPERTIES  _BACKTRACE_TRIPLES "/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;48;add_test;/home/asuka/workbench/project/SimpleDB/test/CMakeLists.txt;0;")
