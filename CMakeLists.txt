cmake_minimum_required(VERSION 4.1)
project(TSDB)

set(CMAKE_CXX_STANDARD 20)

add_executable(TSDB main.cpp src/Storage.cpp)

enable_testing()

add_executable(TSDB_tests
        tests/TestStorage.cpp
        src/Storage.cpp
)

find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})
target_link_libraries(TSDB_tests ${GTEST_LIBRARIES} ${GTEST_MAIN_LIBRARIES} pthread)

add_test(NAME StorageTests COMMAND TSDB_tests)