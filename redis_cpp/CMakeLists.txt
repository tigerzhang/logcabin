cmake_minimum_required (VERSION 2.8.11)
project(Redis_cpp_client)

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
CHECK_CXX_COMPILER_FLAG("-std=c++0x" COMPILER_SUPPORTS_CXX0X)
if(COMPILER_SUPPORTS_CXX11)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
elseif(COMPILER_SUPPORTS_CXX0X)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
else()
    message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no
    C++11 support. Please use a different C++ compiler.")
endif()



# -------------------------------------------
# Include directories
# -------------------------------------------
include_directories(include)

# -------------------------------------------
# Source files
# -------------------------------------------
file(GLOB SOURCES "src/*.cpp" "src/redis_protocol/*.cpp")

# -------------------------------------------
# Dependencies
# -------------------------------------------
set(DEPENDENCIES boost_system pthread)

# -------------------------------------------
# Exec + lib
# -------------------------------------------
add_library(rediscpp SHARED  src/redis_interface.cpp ${SOURCES})

target_compile_definitions(rediscpp PRIVATE DEBUG=1)
target_link_libraries(rediscpp ${DEPENDENCIES})

# EXAMPLES
add_executable(simple examples/basic_functions.cpp)
target_link_libraries(simple rediscpp)

add_executable(task_queue examples/task_queue.cpp)
target_link_libraries(task_queue rediscpp)

# -------------------------------------------
# tests
# -------------------------------------------
add_executable(tests tests/protocol_tests.cpp)
target_link_libraries(tests rediscpp boost_unit_test_framework boost_system)
