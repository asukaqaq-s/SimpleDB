# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.16

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/asuka/workbench/project/SimpleDB

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/asuka/workbench/project/SimpleDB/build

# Include any dependencies generated for this target.
include test/CMakeFiles/page_test.dir/depend.make

# Include the progress variables for this target.
include test/CMakeFiles/page_test.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/page_test.dir/flags.make

test/CMakeFiles/page_test.dir/file/page_test.cc.o: test/CMakeFiles/page_test.dir/flags.make
test/CMakeFiles/page_test.dir/file/page_test.cc.o: ../test/file/page_test.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/asuka/workbench/project/SimpleDB/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/CMakeFiles/page_test.dir/file/page_test.cc.o"
	cd /home/asuka/workbench/project/SimpleDB/build/test && /usr/bin/g++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/page_test.dir/file/page_test.cc.o -c /home/asuka/workbench/project/SimpleDB/test/file/page_test.cc

test/CMakeFiles/page_test.dir/file/page_test.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/page_test.dir/file/page_test.cc.i"
	cd /home/asuka/workbench/project/SimpleDB/build/test && /usr/bin/g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/asuka/workbench/project/SimpleDB/test/file/page_test.cc > CMakeFiles/page_test.dir/file/page_test.cc.i

test/CMakeFiles/page_test.dir/file/page_test.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/page_test.dir/file/page_test.cc.s"
	cd /home/asuka/workbench/project/SimpleDB/build/test && /usr/bin/g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/asuka/workbench/project/SimpleDB/test/file/page_test.cc -o CMakeFiles/page_test.dir/file/page_test.cc.s

# Object files for target page_test
page_test_OBJECTS = \
"CMakeFiles/page_test.dir/file/page_test.cc.o"

# External object files for target page_test
page_test_EXTERNAL_OBJECTS =

test/page_test: test/CMakeFiles/page_test.dir/file/page_test.cc.o
test/page_test: test/CMakeFiles/page_test.dir/build.make
test/page_test: lib/libgtest_maind.a
test/page_test: src/libSimpleDB_lib.a
test/page_test: lib/libgtestd.a
test/page_test: test/CMakeFiles/page_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/asuka/workbench/project/SimpleDB/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable page_test"
	cd /home/asuka/workbench/project/SimpleDB/build/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/page_test.dir/link.txt --verbose=$(VERBOSE)
	cd /home/asuka/workbench/project/SimpleDB/build/test && /usr/bin/cmake -D TEST_TARGET=page_test -D TEST_EXECUTABLE=/home/asuka/workbench/project/SimpleDB/build/test/page_test -D TEST_EXECUTOR= -D TEST_WORKING_DIR=/home/asuka/workbench/project/SimpleDB/build/test -D TEST_EXTRA_ARGS= -D TEST_PROPERTIES= -D TEST_PREFIX= -D TEST_SUFFIX= -D NO_PRETTY_TYPES=FALSE -D NO_PRETTY_VALUES=FALSE -D TEST_LIST=page_test_TESTS -D CTEST_FILE=/home/asuka/workbench/project/SimpleDB/build/test/page_test[1]_tests.cmake -D TEST_DISCOVERY_TIMEOUT=5 -P /usr/share/cmake-3.16/Modules/GoogleTestAddTests.cmake

# Rule to build all files generated by this target.
test/CMakeFiles/page_test.dir/build: test/page_test

.PHONY : test/CMakeFiles/page_test.dir/build

test/CMakeFiles/page_test.dir/clean:
	cd /home/asuka/workbench/project/SimpleDB/build/test && $(CMAKE_COMMAND) -P CMakeFiles/page_test.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/page_test.dir/clean

test/CMakeFiles/page_test.dir/depend:
	cd /home/asuka/workbench/project/SimpleDB/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/asuka/workbench/project/SimpleDB /home/asuka/workbench/project/SimpleDB/test /home/asuka/workbench/project/SimpleDB/build /home/asuka/workbench/project/SimpleDB/build/test /home/asuka/workbench/project/SimpleDB/build/test/CMakeFiles/page_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/page_test.dir/depend

