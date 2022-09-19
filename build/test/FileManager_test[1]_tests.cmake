add_test( FileManagerTest.SimpleTest2 /home/asuka/workbench/project/SimpleDB/build/test/FileManager_test [==[--gtest_filter=FileManagerTest.SimpleTest2]==] --gtest_also_run_disabled_tests)
set_tests_properties( FileManagerTest.SimpleTest2 PROPERTIES WORKING_DIRECTORY /home/asuka/workbench/project/SimpleDB/build/test)
set( FileManager_test_TESTS FileManagerTest.SimpleTest2)
