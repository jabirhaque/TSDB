cmake_minimum_required(VERSION 4.1)
project(TSDB)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(TSDB
        main.cpp
        src/Storage.cpp
)

add_executable(TSDB_tests
        tests/TestStorage.cpp
        src/Storage.cpp
)

find_package(ZLIB REQUIRED)
find_package(GTest REQUIRED)

target_link_libraries(TSDB
        PRIVATE
        ZLIB::ZLIB
)

target_link_libraries(TSDB_tests
        PRIVATE
        ZLIB::ZLIB
        GTest::gtest
        GTest::gtest_main
        pthread
)

enable_testing()
add_test(NAME StorageTests COMMAND TSDB_tests)
