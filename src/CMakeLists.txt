set(binary ${CMAKE_PROJECT_NAME})

file(GLOB_RECURSE sources ${PROJECT_SOURCE_DIR}/src/*/*.cc ${PROJECT_SOURCE_DIR}/src/*/*/*.cc ${PROJECT_SOURCE_DIR}/src/*.cc)

set(sources ${sources})

# add_executable(${binary} ${sources})

add_library(${binary}_lib STATIC ${sources})