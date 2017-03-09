cmake_minimum_required(VERSION 3.5)
project(logcabin)

# add_definitions(-DROCKSDB_STATEMACHINE)
add_definitions(-DREDIS_STATEMACHINE)
enable_testing()

# install dependecies
set(DEPENDENCIES lz4 jemalloc crypto++ protobuf snappy bz2 pthread-stubs0 boost-all msgpack)
foreach(Dep ${DEPENDENCIES})
    string(CONCAT Dep2 lib ${Dep} -dev)
    message("Installing Dep:  " ${Dep2})
    execute_process(COMMAND "dpkg" -l ${Dep2} RESULT_VARIABLE res_var OUTPUT_QUIET)
    if(NOT "${res_var}" STREQUAL "0")
        execute_process(COMMAND "sudo" apt-get -qq install ${Dep2})
    endif()
endforeach(Dep)

# install protobuf compiler
set(PROTOBUF_COMPILER protobuf-compiler)
message("Installing protobuf-compiler ...")
execute_process(COMMAND "dpkg" -l ${PROTOBUF_COMPILER} RESULT_VARIABLE res_var OUTPUT_QUIET)
if(NOT "${res_var}" STREQUAL "0")
    execute_process(COMMAND "sudo" apt-get -qq install ${PROTOBUF_COMPILER})
endif()

# build submodule
message(STATUS "building redis...")
set(REDIS_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/redis")
execute_process(COMMAND "make"
                WORKING_DIRECTORY ${REDIS_SOURCE_DIR})

message(STATUS "building rocksdb...")
set(ROCKSDB_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rocksdb")
execute_process(COMMAND "make" static_lib
                WORKING_DIRECTORY ${ROCKSDB_SOURCE_DIR})

message(STATUS "building hiredis..")
set(HIREDIS_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/hiredis")
execute_process(COMMAND "make"
                WORKING_DIRECTORY ${HIREDIS_SOURCE_DIR})

message(STATUS "building gtest...")
add_subdirectory(gtest)

message(STATUS "building redis3m...")
add_subdirectory(redis3m)

# Download libevent-2.1.8
set(LIBEVENT_URL "https://github.com/libevent/libevent/releases/download/release-2.1.8-stable/libevent-2.1.8-stable.tar.gz")
set(LIBEVENT_DOWNLOAD_PATH "${CMAKE_SOURCE_DIR}/libevent-2.1.8-stable.tar.gz")
set(LIBEVENT_EXTRACT_PATH "${CMAKE_SOURCE_DIR}/libevent-2.1.8-stable")

if (NOT EXISTS "${LIBEVENT_EXTRACT_PATH}")
    message("Download ${LIBEVENT_DOWNLOAD_PATH} ...")
    execute_process(
        COMMAND wget ${LIBEVENT_URL} -O ${LIBEVENT_DOWNLOAD_PATH}
        TIMEOUT 100000 # it takes very long time
    )
    message("Download ${LIBEVENT_DOWNLOAD_PATH} finished.")
    execute_process(
        COMMAND ${CMAKE_COMMAND} -E tar xzf ${LIBEVENT_DOWNLOAD_PATH}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
	    RESULT_VARIABLE res_var
    )
    if (${res_var} STREQUAL "0")
        execute_process(
                COMMAND "sh" -c ./autogen.sh
                WORKING_DIRECTORY ${LIBEVENT_EXTRACT_PATH}
                RESULT_VARIABLE res_var
        )
        execute_process(
                COMMAND "sh" -c ./configure
                WORKING_DIRECTORY ${LIBEVENT_EXTRACT_PATH}
                RESULT_VARIABLE res_var
        )
        execute_process(
                COMMAND "sh" -c make -j4
                WORKING_DIRECTORY ${LIBEVENT_EXTRACT_PATH}
                RESULT_VARIABLE res_var
        )
    endif()
        
endif()

SET(SUBMODULE_LIBS gtest redis3m
                   ${CMAKE_SOURCE_DIR}/rocksdb/librocksdb.a)

find_package(Protobuf REQUIRED)
#find_package(JeMalloc REQUIRED)
set(Protobuf_IMPORT_DIRS ${PROJECT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/Protocol)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -ggdb3 -fpermissive")
set(SERVER_FILES "Server/ClientService.cc"
    "Server/ControlService.cc"
    "Server/Globals.cc"
    "Server/RaftConsensus.cc"
    "Server/RaftConsensusInvariants.cc"
    "Server/RaftService.cc"
    "Server/ServerStats.cc"
    "Server/StateMachine.cc"
    "Server/StateMachineRocksdb.cc"
    "Server/StateMachineBase.cc"
    "Server/StateMachineRedis.cc"
)
file(GLOB SERVER_PROTO_FILES "Server/*.proto")
set(STORAGE_FILES "Storage/FilesystemUtil.cc"
    "Storage/Layout.cc"
    "Storage/Log.cc"
    "Storage/LogFactory.cc"
    "Storage/MemoryLog.cc"
    "Storage/SegmentedLog.cc"
    "Storage/SimpleFileLog.cc"
    "Storage/SnapshotFile.cc"
)
file(GLOB STORAGE_PROTO_FILES "Storage/*.proto")
set(TREE_FILES "Tree/ProtoBuf.cc"
    "Tree/Tree.cc"
)
file(GLOB TREE_PROTO_FILES "Tree/*.proto")
set(CLIENT_FILES "Client/Backoff.cc"
    "Client/Client.cc"
    "Client/ClientImpl.cc"
    "Client/LeaderRPC.cc"
    "Client/MockClientImpl.cc"
    "Client/SessionManager.cc"
    "Client/Util.cc"
)
file(GLOB PROTOCOL_FILES "Protocol/*.proto")
set(RPC_FILES "RPC/Address.cc"
    "RPC/ClientRPC.cc"
    "RPC/ClientSession.cc"
    "RPC/MessageSocket.cc"
    "RPC/OpaqueClientRPC.cc"
    "RPC/OpaqueServer.cc"
    "RPC/OpaqueServerRPC.cc"
    "RPC/Protocol.cc"
    "RPC/Server.cc"
    "RPC/ServerRPC.cc"
    "RPC/ThreadDispatchService.cc"
)
set(EVENT_FILES "Event/File.cc"
    "Event/Loop.cc"
    "Event/Signal.cc"
    "Event/Timer.cc"
)
file(GLOB CORE_PROTO_FILES "Core/*.proto")
set(CORE_FILES "Core/Buffer.cc"
    "Core/Checksum.cc"
    "Core/ConditionVariable.cc"
    "Core/Config.cc"
    "Core/Debug.cc"
    "Core/ProtoBuf.cc"
    "Core/Random.cc"
    "Core/RollingStat.cc"
    "Core/ThreadId.cc"
    "Core/Time.cc"
    "Core/StringUtil.cc"
    "Core/Util.cc"
)

set(REDISSERVER_FILES
        "RedisServer/RedisServerLib.cc"
        "RedisServer/sds.cc"
        )

PROTOBUF_GENERATE_CPP(ProtoSources ProtoHeaders ${PROTOCOL_FILES})
PROTOBUF_GENERATE_CPP(ServerProtoSources ServerProtoHeaders ${SERVER_PROTO_FILES})
PROTOBUF_GENERATE_CPP(StorageProtoSources StorageProtoHeaders ${STORAGE_PROTO_FILES})
PROTOBUF_GENERATE_CPP(TreeProtoSources TreeProtoHeaders ${TREE_PROTO_FILES})
PROTOBUF_GENERATE_CPP(CoreProtoSources CoreProtoHeaders ${CORE_PROTO_FILES})

set(REDIS_CPP_FILES
        "redis_cpp/src/helpers.cpp"
        "redis_cpp/src/redis_interface.cpp"
        "redis_cpp/src/redis_protocol/resp_protocol.cpp")

set(SOURCE_FILES
    "Server/Main.cc"
    ${SERVER_FILES}
    ${ServerProtoSources}
    ${StorageProtoSources}
    ${TreeProtoSources}
    ${STORAGE_FILES}
    ${TREE_FILES}
    ${CLIENT_FILES}
    ${ProtoSources}
    ${RPC_FILES}
    ${EVENT_FILES}
    ${CORE_FILES}
    Server/utils.cc Server/utils.h
    ${REDIS_CPP_FILES})

set(HIREDIS_FILES
        "hiredis/async.c"
        "hiredis/hiredis.c"
        "hiredis/read.c"
        "hiredis/dict.c"
        "hiredis/net.c"
        "hiredis/sds.c")

set(HIREDIS_LIB_STATIC ${CMAKE_SOURCE_DIR}/hiredis/libhiredis.a)

find_library(LZ4_LIB_STATIC NAMES liblz4.a)
find_library(PROTOBUF_LIB_STATIC NAMES libprotobuf.a)
find_library(CRYPTOPP_LIB_STATIC NAMES libcrypto++.a)
find_library(SNAPPY_LIB_STATIC NAMES libsnappy.a)
find_library(BZ2_LIB_STATIC NAMES libbz2.a)
find_library(JEMALLOC_LIB_STATIC NAMES libjemalloc.a)
file(GLOB EVENT_LIB_STATIC "${CMAKE_SOURCE_DIR}/libevent-2.1.8-stable/.libs/*.a")
find_library(BOOST_SYSTEM_LIB_STATIC NAMES libboost_system.a)

set(LIBS ${SUBMODULE_LIBS} ${LZ4_LIB_STATIC} ${PROTOBUF_LIB_STATIC} ${CRYPTOPP_LIB_STATIC} ${HIREDIS_LIB_STATIC}
         pthread rt ${SNAPPY_LIB_STATIC} z ${BZ2_LIB_STATIC} ${JEMALLOC_LIB_STATIC} ${EVENT_LIB_STATIC} ${BOOST_SYSTEM_LIB_STATIC})
add_executable(LogCabin ${SOURCE_FILES} ${HIREDIS_FILES} "RedisServer/sds.cc")
target_link_libraries(LogCabin ${LIBS})
include_directories(${CMAKE_SOURCE_DIR}/libevent-2.1.8-stable/include ${CMAKE_CURRENT_BINARY_DIR} . ./include gtest/include redis3m/include rocksdb/include)

add_library(logcabin STATIC
        ${CLIENT_FILES}
        ${TREE_FILES}
        ${ProtoSources}
        ${RPC_FILES}
        ${EVENT_FILES}
        ${CORE_FILES}
        ${ProtoSources}
        ${ServerProtoSources}
        ${StorageProtoSources}
        ${TreeProtoSources})

add_executable(Benchmark ${CMAKE_SOURCE_DIR}/Examples/Benchmark.cc Examples/RedisProxy.h)
target_link_libraries(Benchmark logcabin ${LIBS})
add_executable(FailoverTest ${CMAKE_SOURCE_DIR}/Examples/FailoverTest.cc)
target_link_libraries(FailoverTest logcabin ${LIBS})
add_executable(HelloWorld ${CMAKE_SOURCE_DIR}/Examples/HelloWorld.cc)
target_link_libraries(HelloWorld logcabin ${LIBS})
add_executable(Reconfigure ${CMAKE_SOURCE_DIR}/Examples/Reconfigure.cc)
target_link_libraries(Reconfigure logcabin ${LIBS})
add_executable(ReconfigureTest ${CMAKE_SOURCE_DIR}/Examples/ReconfigureTest.cc)
target_link_libraries(ReconfigureTest logcabin ${LIBS})
add_executable(SmokeTest ${CMAKE_SOURCE_DIR}/Examples/SmokeTest.cc)
target_link_libraries(SmokeTest logcabin ${LIBS})
add_executable(TreeOps ${CMAKE_SOURCE_DIR}/Examples/TreeOps.cc)
target_link_libraries(TreeOps logcabin ${LIBS})
add_executable(ServerControl ${CMAKE_SOURCE_DIR}/Client/ServerControl.cc ${HIREDIS_FILES})
target_link_libraries(ServerControl logcabin ${LIBS})
add_executable(Tool ${CMAKE_SOURCE_DIR}/Storage/Tool.cc ${STORAGE_FILES} ${ServerProtoSources})
target_link_libraries(Tool logcabin ${LIBS} pthread)
add_executable(RedisProxy ${CMAKE_SOURCE_DIR}/Examples/RedisProxy.cc ${REDISSERVER_FILES} ${HIREDIS_FILES})
target_link_libraries(RedisProxy logcabin ${LIBS})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-access-control")

file(GLOB CLIENT_TEST_FILES "Client/*Test.cc" "Client/*Mock.cc")
file(GLOB CORE_TEST_FILES "Core/*Test.cc" "Core/*Mock.cc")
file(GLOB EVENT_TEST_FILES "Event/*Test.cc" "Event/*Mock.cc")
file(GLOB RPC_TEST_FILES "RPC/*Test.cc" "RPC/*Mock.cc")
file(GLOB SERVER_TEST_FILES "Server/*Test.cc" "Server/*Mock.cc")
file(GLOB STORAGE_TEST_FILES "Storage/*Test.cc" "Storage/*Mock.cc")
file(GLOB TREE_TEST_FILES "Tree/*Test.cc" "Tree/*Mock.cc")

set(TEST_SOURCE_FILES test/TestRunner.cc
        ${SERVER_FILES}
        ${STORAGE_FILES}
        ${CLIENT_FILES}
        ${TREE_FILES}
        ${PROTOCOL_FILES}
        ${RPC_FILES}
        ${EVENT_FILES}
        ${CORE_FILES}
        ${REDIS_CPP_FILES}
        ${CoreProtoSources}
        ${CLIENT_TEST_FILES}
        ${CORE_TEST_FILES}
        ${EVENT_TEST_FILES}
        ${RPC_TEST_FILES}
        ${SERVER_TEST_FILES}
        ${STORAGE_TEST_FILES}
        ${TREE_TEST_FILES})
add_executable(Test ${TEST_SOURCE_FILES} ${HIREDIS_FILES} "RedisServer/sds.cc")
target_link_libraries(Test logcabin ${LIBS})

SET(INSTALL_PATH ${CMAKE_CURRENT_BINARY_DIR}/logcabin)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/ServerControl RENAME logcabinctl DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/logcabin)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/LogCabin RENAME logcabind DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/logcabin)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/TreeOps RENAME logcabin DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/logcabin)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/Benchmark RENAME logcabin-benchmark DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/logcabin)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/Reconfigure RENAME logcabin-reconfigure DESTINATION ${INSTALL_PATH})
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/SmokeTest RENAME logcabin-smoketest DESTINATION ${INSTALL_PATH})
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/Tool RENAME logcabin-storage DESTINATION ${INSTALL_PATH})
