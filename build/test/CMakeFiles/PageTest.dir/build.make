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
include test/CMakeFiles/PageTest.dir/depend.make

# Include the progress variables for this target.
include test/CMakeFiles/PageTest.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/PageTest.dir/flags.make

test/CMakeFiles/PageTest.dir/file/PageTest.cc.o: test/CMakeFiles/PageTest.dir/flags.make
test/CMakeFiles/PageTest.dir/file/PageTest.cc.o: ../test/file/PageTest.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/asuka/workbench/project/SimpleDB/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/CMakeFiles/PageTest.dir/file/PageTest.cc.o"
	cd /home/asuka/workbench/project/SimpleDB/build/test && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/PageTest.dir/file/PageTest.cc.o -c /home/asuka/workbench/project/SimpleDB/test/file/PageTest.cc

test/CMakeFiles/PageTest.dir/file/PageTest.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/PageTest.dir/file/PageTest.cc.i"
	cd /home/asuka/workbench/project/SimpleDB/build/test && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/asuka/workbench/project/SimpleDB/test/file/PageTest.cc > CMakeFiles/PageTest.dir/file/PageTest.cc.i

test/CMakeFiles/PageTest.dir/file/PageTest.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/PageTest.dir/file/PageTest.cc.s"
	cd /home/asuka/workbench/project/SimpleDB/build/test && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/asuka/workbench/project/SimpleDB/test/file/PageTest.cc -o CMakeFiles/PageTest.dir/file/PageTest.cc.s

# Object files for target PageTest
PageTest_OBJECTS = \
"CMakeFiles/PageTest.dir/file/PageTest.cc.o"

# External object files for target PageTest
PageTest_EXTERNAL_OBJECTS =

test/PageTest: test/CMakeFiles/PageTest.dir/file/PageTest.cc.o
test/PageTest: test/CMakeFiles/PageTest.dir/build.make
test/PageTest: lib/libgtest_main.a
test/PageTest: src/libSimpleDB_lib.a
test/PageTest: lib/libgtest.a
test/PageTest: test/CMakeFiles/PageTest.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/asuka/workbench/project/SimpleDB/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable PageTest"
	cd /home/asuka/workbench/project/SimpleDB/build/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/PageTest.dir/link.txt --verbose=$(VERBOSE)
	cd /home/asuka/workbench/project/SimpleDB/build/test && /usr/bin/cmake -D TEST_TARGET=PageTest -D TEST_EXECUTABLE=/home/asuka/workbench/project/SimpleDB/build/test/PageTest -D TEST_EXECUTOR= -D TEST_WORKING_DIR=/home/asuka/workbench/project/SimpleDB/build/test -D TEST_EXTRA_ARGS= -D TEST_PROPERTIES= -D TEST_PREFIX= -D TEST_SUFFIX= -D NO_PRETTY_TYPES=FALSE -D NO_PRETTY_VALUES=FALSE -D TEST_LIST=PageTest_TESTS -D CTEST_FILE=/home/asuka/workbench/project/SimpleDB/build/test/PageTest[1]_tests.cmake -D TEST_DISCOVERY_TIMEOUT=5 -P /usr/share/cmake-3.16/Modules/GoogleTestAddTests.cmake

# Rule to build all files generated by this target.
test/CMakeFiles/PageTest.dir/build: test/PageTest

.PHONY : test/CMakeFiles/PageTest.dir/build

test/CMakeFiles/PageTest.dir/clean:
	cd /home/asuka/workbench/project/SimpleDB/build/test && $(CMAKE_COMMAND) -P CMakeFiles/PageTest.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/PageTest.dir/clean

test/CMakeFiles/PageTest.dir/depend:
	cd /home/asuka/workbench/project/SimpleDB/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/asuka/workbench/project/SimpleDB /home/asuka/workbench/project/SimpleDB/test /home/asuka/workbench/project/SimpleDB/build /home/asuka/workbench/project/SimpleDB/build/test /home/asuka/workbench/project/SimpleDB/build/test/CMakeFiles/PageTest.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/PageTest.dir/depend

