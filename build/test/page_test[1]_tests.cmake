add_test( PageTest.simpletest1 /home/asuka/workbench/project/SimpleDB/build/test/page_test [==[--gtest_filter=PageTest.simpletest1]==] --gtest_also_run_disabled_tests)
set_tests_properties( PageTest.simpletest1 PROPERTIES WORKING_DIRECTORY /home/asuka/workbench/project/SimpleDB/build/test)
set( page_test_TESTS PageTest.simpletest1)
