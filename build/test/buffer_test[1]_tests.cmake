add_test( BufferTest.Simpletest1 /home/asuka/workbench/project/SimpleDB/build/test/buffer_test [==[--gtest_filter=BufferTest.Simpletest1]==] --gtest_also_run_disabled_tests)
set_tests_properties( BufferTest.Simpletest1 PROPERTIES WORKING_DIRECTORY /home/asuka/workbench/project/SimpleDB/build/test)
add_test( BufferPoolManagerTest.BinaryDataTest /home/asuka/workbench/project/SimpleDB/build/test/buffer_test [==[--gtest_filter=BufferPoolManagerTest.BinaryDataTest]==] --gtest_also_run_disabled_tests)
set_tests_properties( BufferPoolManagerTest.BinaryDataTest PROPERTIES WORKING_DIRECTORY /home/asuka/workbench/project/SimpleDB/build/test)
add_test( BufferPoolManagerTest.ConcurrencyWriteTest /home/asuka/workbench/project/SimpleDB/build/test/buffer_test [==[--gtest_filter=BufferPoolManagerTest.ConcurrencyWriteTest]==] --gtest_also_run_disabled_tests)
set_tests_properties( BufferPoolManagerTest.ConcurrencyWriteTest PROPERTIES WORKING_DIRECTORY /home/asuka/workbench/project/SimpleDB/build/test)
add_test( BufferPoolManagerTest.ConcurrencyTest /home/asuka/workbench/project/SimpleDB/build/test/buffer_test [==[--gtest_filter=BufferPoolManagerTest.ConcurrencyTest]==] --gtest_also_run_disabled_tests)
set_tests_properties( BufferPoolManagerTest.ConcurrencyTest PROPERTIES WORKING_DIRECTORY /home/asuka/workbench/project/SimpleDB/build/test)
add_test( BufferPoolManagerTest.ConcurrencyPinTest /home/asuka/workbench/project/SimpleDB/build/test/buffer_test [==[--gtest_filter=BufferPoolManagerTest.ConcurrencyPinTest]==] --gtest_also_run_disabled_tests)
set_tests_properties( BufferPoolManagerTest.ConcurrencyPinTest PROPERTIES WORKING_DIRECTORY /home/asuka/workbench/project/SimpleDB/build/test)
add_test( BufferPoolManagerTest.OneProcessVictimTest /home/asuka/workbench/project/SimpleDB/build/test/buffer_test [==[--gtest_filter=BufferPoolManagerTest.OneProcessVictimTest]==] --gtest_also_run_disabled_tests)
set_tests_properties( BufferPoolManagerTest.OneProcessVictimTest PROPERTIES WORKING_DIRECTORY /home/asuka/workbench/project/SimpleDB/build/test)
add_test( BufferPoolManagerTest.ConcurrencyVictimTest /home/asuka/workbench/project/SimpleDB/build/test/buffer_test [==[--gtest_filter=BufferPoolManagerTest.ConcurrencyVictimTest]==] --gtest_also_run_disabled_tests)
set_tests_properties( BufferPoolManagerTest.ConcurrencyVictimTest PROPERTIES WORKING_DIRECTORY /home/asuka/workbench/project/SimpleDB/build/test)
set( buffer_test_TESTS BufferTest.Simpletest1 BufferPoolManagerTest.BinaryDataTest BufferPoolManagerTest.ConcurrencyWriteTest BufferPoolManagerTest.ConcurrencyTest BufferPoolManagerTest.ConcurrencyPinTest BufferPoolManagerTest.OneProcessVictimTest BufferPoolManagerTest.ConcurrencyVictimTest)
