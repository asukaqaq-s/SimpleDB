if(EXISTS "/home/asuka/workbench/project/SimpleDB/build/test/file_manager_test[1]_tests.cmake")
  include("/home/asuka/workbench/project/SimpleDB/build/test/file_manager_test[1]_tests.cmake")
else()
  add_test(file_manager_test_NOT_BUILT file_manager_test_NOT_BUILT)
endif()
