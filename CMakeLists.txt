cmake_minimum_required(VERSION 3.5)
project(logcabin)

# add_definitions(-DROCKSDB_STATEMACHINE)
#add_definitions(-DREDIS_STATEMACHINE)
add_definitions(-DARDB_STATEMACHINE)
enable_testing()

set(THIRD_PARTY_DEPS_DIR     ${CMAKE_CURRENT_SOURCE_DIR}/deps)
set(THIRD_PARTY_LIBS_REL_DIR ${CMAKE_CURRENT_SOURCE_DIR}/build/third_party)

# Vars for building gtest
set(GTEST_SOURCE_DIR_NAME gtest-c6d1811)
set(GTEST_LIB_NAME libgtest.a)
set(GTEST_REL_DIR ${THIRD_PARTY_LIBS_REL_DIR}/${GTEST_SOURCE_DIR_NAME})
set(GTEST_SOURCE_DIR ${THIRD_PARTY_DEPS_DIR}/${GTEST_SOURCE_DIR_NAME})

# Vars for building rocksdb
set(ROCKSDB_SOURCE_DIR_NAME rocksdb-5.2.1)
set(ROCKSDB_LIB_NAME librocksdb.a)
set(ROCKSDB_REL_DIR ${THIRD_PARTY_LIBS_REL_DIR}/${ROCKSDB_SOURCE_DIR_NAME})
set(ROCKSDB_SOURCE_DIR ${THIRD_PARTY_DEPS_DIR}/${ROCKSDB_SOURCE_DIR_NAME})

# Vars for building libevent
set(LIBEVENT_SOURCE_DIR_NAME libevent-2.1.8-stable)
set(LIBEVENT_REL_DIR ${THIRD_PARTY_LIBS_REL_DIR}/${LIBEVENT_SOURCE_DIR_NAME})
set(LIBEVENT_SOURCE_DIR ${THIRD_PARTY_DEPS_DIR}/${LIBEVENT_SOURCE_DIR_NAME})

# Vars for building protobuf
set(PROTOBUF_SOURCE_DIR_NAME protobuf-2.6.1)
set(PROTOBUF_REL_DIR ${THIRD_PARTY_LIBS_REL_DIR}/${PROTOBUF_SOURCE_DIR_NAME})
set(PROTOBUF_SOURCE_DIR ${THIRD_PARTY_DEPS_DIR}/${PROTOBUF_SOURCE_DIR_NAME})
set(PROTOC ${PROTOBUF_REL_DIR}/bin/protoc)

if (APPLE)
    message(STATUS "Cannot build or install 3rd dependency on non-Linux machine")
elseif (UNIX)
    message(STATUS "Create 3rd libs release directory: " ${THIRD_PARTY_LIBS_REL_DIR})
    execute_process(COMMAND "nproc" OUTPUT_VARIABLE PROCESSOR_NUM)
    execute_process(COMMAND "mkdir" -p ${THIRD_PARTY_LIBS_REL_DIR} WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

    # install basic dependecies
    set(DEPENDENCIES lz4 jemalloc crypto++ snappy bz2 pthread-stubs0 boost-all msgpack)
    foreach(Dep ${DEPENDENCIES})
        string(CONCAT Dep2 lib ${Dep} -dev)
        message("Install basic dependency:  " ${Dep2})
        execute_process(COMMAND "dpkg" -l ${Dep2} RESULT_VARIABLE res_var OUTPUT_QUIET)
        if(NOT "${res_var}" STREQUAL "0")
            execute_process(COMMAND "sudo" apt-get -qq install ${Dep2})
        endif()
    endforeach(Dep)

    # building rocksdb
    if (EXISTS ${ROCKSDB_REL_DIR}/${ROCKSDB_LIB_NAME})
        message(STATUS "Use: " ${ROCKSDB_REL_DIR}/${ROCKSDB_LIB_NAME})
    else()
        message(STATUS "Building rocksdb => " ${ROCKSDB_SOURCE_DIR})
        execute_process(
                COMMAND "mkdir" -p ${ROCKSDB_REL_DIR}
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        )
        execute_process(
                COMMAND "make" static_lib
                WORKING_DIRECTORY ${ROCKSDB_SOURCE_DIR}
        )
        execute_process(
                COMMAND "cp" ${ROCKSDB_SOURCE_DIR}/${ROCKSDB_LIB_NAME} ${ROCKSDB_REL_DIR}
                WORKING_DIRECTORY ${ROCKSDB_SOURCE_DIR}
        )
    endif()

    # building gtest
    if (EXISTS ${GTEST_REL_DIR}/${GTEST_LIB_NAME})
        message(STATUS "Use: " ${GTEST_REL_DIR}/${GTEST_LIB_NAME})
    else()
        message(STATUS "Building gtest => " ${GTEST_SOURCE_DIR})
        execute_process(
                COMMAND "mkdir" -p ${GTEST_SOURCE_DIR}/build
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
        execute_process(
                COMMAND "cmake" ..
                WORKING_DIRECTORY ${GTEST_SOURCE_DIR}/build)
        execute_process(
                COMMAND "make"
                WORKING_DIRECTORY ${GTEST_SOURCE_DIR}/build)
        execute_process(
                COMMAND "mkdir" -p ${GTEST_REL_DIR}
                WORKING_DIRECTORY ${GTEST_SOURCE_DIR}/build)
        execute_process(
                COMMAND "cp" ${GTEST_SOURCE_DIR}/build/${GTEST_LIB_NAME} ${GTEST_REL_DIR}
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        )
    endif()

    # building libevent
    if (EXISTS ${LIBEVENT_REL_DIR})
        message(STATUS "Use: " ${LIBEVENT_REL_DIR})
    else()
        message(STATUS "Building libevent => " ${LIBEVENT_SOURCE_DIR})
        execute_process(
                COMMAND "./configure" --prefix=${LIBEVENT_REL_DIR}
                WORKING_DIRECTORY ${LIBEVENT_SOURCE_DIR}
        )
        execute_process(
                COMMAND "make" -j2
                WORKING_DIRECTORY ${LIBEVENT_SOURCE_DIR}
        )
        execute_process(
                COMMAND "make" install
                WORKING_DIRECTORY ${LIBEVENT_SOURCE_DIR}
        )
    endif()

    if (EXISTS ${PROTOBUF_REL_DIR})
        message(STATUS "Use: " ${PROTOBUF_REL_DIR})
    else()
        message(STATUS "Building protobuf => " ${PROTOBUF_SOURCE_DIR})
        execute_process(
                COMMAND "tar" "-xf" "protobuf-2.6.1.tar.gz"
                WORKING_DIRECTORY ${THIRD_PARTY_DEPS_DIR}
        )

        execute_process(
                COMMAND "./configure" --prefix=${PROTOBUF_REL_DIR}
                WORKING_DIRECTORY ${PROTOBUF_SOURCE_DIR}
        )
        execute_process(
                COMMAND "make" -j2
                WORKING_DIRECTORY ${PROTOBUF_SOURCE_DIR}
        )
        execute_process(
                COMMAND "make" install
                WORKING_DIRECTORY ${PROTOBUF_SOURCE_DIR}
        )
    endif()
else ()
    message(STATUS "Cannot build or install 3rd dependency on non-Linux machine")
endif ()

SET(SUBMODULE_LIBS ${GTEST_REL_DIR}/${GTEST_LIB_NAME}
                   ${ROCKSDB_REL_DIR}/${ROCKSDB_LIB_NAME})

add_compile_options(-std=c++11)
add_compile_options(-O0)
add_compile_options(-ggdb3)
add_compile_options(-fpermissive)
add_compile_options(-Wall)
add_compile_options(-Wextra)
add_compile_options(-Wcast-align)
add_compile_options(-Wcast-qual)
add_compile_options(-Wconversion)
add_compile_options(-Weffc++)
add_compile_options(-Wformat=2)
add_compile_options(-Wmissing-format-attribute)
add_compile_options(-Wno-non-template-friend)
add_compile_options(-Wno-unused-parameter)
add_compile_options(-Woverloaded-virtual)
add_compile_options(-Wwrite-strings)
add_compile_options(-DSWIG)
add_compile_options(-fno-access-control)

set(SERVER_FILES "Server/ClientService.cc"
    "Server/ControlService.cc"
    "Server/Globals.cc"
    "Server/RaftConsensus.cc"
    "Server/RaftConsensusInvariants.cc"
    "Server/RaftService.cc"
    "Server/ServerStats.cc"
    "Server/StateMachine.cc"
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

SET(BUILD_GEN_PROTO_SRC)
macro(COPY_PROTO_FILE_TO_BUILD SRC_FILES DIR)
    file(MAKE_DIRECTORY ${CMAKE_SOURCE_DIR}/build)
    file(MAKE_DIRECTORY ${CMAKE_SOURCE_DIR}/build/${DIR})
    SET(CURRENT_PROTO_FILES)
    SET(CMAKE_CURRENT_BINARY_DIR ${CMAKE_SOURCE_DIR}/build/${DIR})
    SET(${DIR}ProtoSources)
    foreach(proto_file ${${SRC_FILES}})
        file(RELATIVE_PATH relative_path ${CMAKE_SOURCE_DIR} ${proto_file})
        message(STATUS "Proto file: " ${relative_path})
        add_custom_command(OUTPUT ${CMAKE_SOURCE_DIR}/build/${relative_path}
            COMMAND cp ${proto_file} ${CMAKE_SOURCE_DIR}/build/${relative_path}
            DEPENDS ${proto_file}
            )
        list(APPEND CURRENT_PROTO_FILES ${CMAKE_SOURCE_DIR}/build/${relative_path})
        string(REPLACE ".proto" ".pb.cc" CURRENT_OUTPUT ${CMAKE_SOURCE_DIR}/build/${relative_path})
        add_custom_command(
            OUTPUT ${CURRENT_OUTPUT}
            COMMAND ${PROTOC} -I. --cpp_out=. --python_out=. build/${relative_path}
            DEPENDS ${CMAKE_SOURCE_DIR}/build/${relative_path}
        )
        list(APPEND ${DIR}ProtoSources ${CURRENT_OUTPUT})
        message(STATUS "current output:" ${CURRENT_OUTPUT})
        message(STATUS "current ProtoSources var name:" ${DIR}ProtoSources)
        message(STATUS "current ProtoSources:" ${${DIR}ProtoSources})
    endforeach()

    message("CURRENT PROTO FILES:" ${CURRENT_PROTO_FILES})

    list(APPEND BUILD_GEN_PROTO_SRC ${CURRENT_PROTO_FILES})
    SET(CMAKE_CURRENT_BINARY_DIR ${CMAKE_BINARY_DIR})
endmacro()

COPY_PROTO_FILE_TO_BUILD(TREE_PROTO_FILES "Tree")
COPY_PROTO_FILE_TO_BUILD(PROTOCOL_FILES "Protocol")
COPY_PROTO_FILE_TO_BUILD(STORAGE_PROTO_FILES "Storage")
COPY_PROTO_FILE_TO_BUILD(SERVER_PROTO_FILES "Server")
COPY_PROTO_FILE_TO_BUILD(CORE_PROTO_FILES "Core")

SET(ProtoSources ${TreeProtoSources} ${ProtocolProtoSources} ${StorageProtoSources} ${ServerProtoSources} ${CoreProtoSources})

message(STATUS "PROTO Sources:" ${ProtoSources})
add_custom_target(prepare DEPENDS ${BUILD_GEN_PROTO_SRC})

set(CMAKE_GENERATED_FILES CMakeCache.txt CMakeFiles cmake_install.cmake CTestTestfile.cmake cmake-build-debug)
add_custom_target(clean-cmake-files
        COMMAND rm -rf ${CMAKE_GENERATED_FILES})

SET(CMAKE_CURRENT_BINARY_DIR ${CMAKE_BINARY_DIR})
SET(CMAKE_BINARY_DIR ${ORIGINT_BIN_PATH})

set(SOURCE_FILES
    "Server/Main.cc"
    ${SERVER_FILES}
    ${ServerProtoSources}
    ${StorageProtoSources}
    ${TreeProtoSources}
    ${STORAGE_FILES}
    ${TREE_FILES}
    ${CLIENT_FILES}
    ${ProtocolProtoSources}
    ${RPC_FILES}
    ${EVENT_FILES}
    ${CORE_FILES})

if (APPLE)
    message(STATUS "Can not find library in non-Linux machine")
elseif (UNIX)
    find_library(LZ4_LIB_STATIC NAMES liblz4.a)
    find_library(PROTOBUF_LIB_STATIC NAMES "libprotobuf.a")
    find_library(CRYPTOPP_LIB_STATIC NAMES libcrypto++.a)
    find_library(SNAPPY_LIB_STATIC NAMES libsnappy.a)
    find_library(BZ2_LIB_STATIC NAMES libbz2.a)
    find_library(JEMALLOC_LIB_STATIC NAMES libjemalloc.a)
    file(GLOB EVENT_LIB_STATIC ${LIBEVENT_REL_DIR}/lib)
    find_library(BOOST_SYSTEM_LIB_STATIC NAMES libboost_system.a)
    set(LIBS ${SUBMODULE_LIBS}
            ${LZ4_LIB_STATIC}
            ${PROTOBUF_LIB_STATIC}
            ${CRYPTOPP_LIB_STATIC}
            pthread rt ${SNAPPY_LIB_STATIC}
            z ${BZ2_LIB_STATIC}
            ${JEMALLOC_LIB_STATIC}
            ${EVENT_LIB_STATIC}
            ${BOOST_SYSTEM_LIB_STATIC})
else ()
    message(STATUS "Can not find library in non-Linux machine")
endif ()

add_executable(LogCabin ${SOURCE_FILES})

target_link_libraries(LogCabin ${LIBS})

include_directories(${PROTOBUF_REL_DIR}/include)

include_directories(${LIBEVENT_REL_DIR}/include
        ${CMAKE_CURRENT_BINARY_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${GTEST_SOURCE_DIR}/include
        ${ROCKSDB_SOURCE_DIR}/include)

add_library(logcabin STATIC
        ${CLIENT_FILES}
        ${TREE_FILES}
        ${SERVER_FILES}
        ${ProtocolProtoSources}
        ${STORAGE_FILES}
        ${RPC_FILES}
        ${EVENT_FILES}
        ${CORE_FILES}
        ${ServerProtoSources}
        ${StorageProtoSources}
        ${TreeProtoSources})

add_dependencies(logcabin prepare)

add_executable(Benchmark ${CMAKE_SOURCE_DIR}/Examples/Benchmark.cc)
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
add_executable(ServerControl ${CMAKE_SOURCE_DIR}/Client/ServerControl.cc)
target_link_libraries(ServerControl logcabin ${LIBS})
add_executable(Tool ${CMAKE_SOURCE_DIR}/Storage/Tool.cc ${STORAGE_FILES} ${ServerProtoSources})
target_link_libraries(Tool logcabin ${LIBS} pthread)

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
        ${CoreProtoSources}
        ${CLIENT_TEST_FILES}
        ${CORE_TEST_FILES}
        ${EVENT_TEST_FILES}
        ${RPC_TEST_FILES}
        ${SERVER_TEST_FILES}
        ${STORAGE_TEST_FILES}
        ${TREE_TEST_FILES})
add_executable(Test ${TEST_SOURCE_FILES})
add_executable(Rocksdbtest rocksdb.cpp)
target_link_libraries(Test logcabin ${LIBS})
target_link_libraries(Rocksdbtest ${LIBS} pthread)

SET(INSTALL_PATH ${CMAKE_CURRENT_BINARY_DIR}/logcabin)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/ServerControl RENAME logcabinctl DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/logcabin)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/LogCabin RENAME logcabind DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/logcabin)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/TreeOps RENAME logcabin DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/logcabin)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/Benchmark RENAME logcabin-benchmark DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/logcabin)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/Reconfigure RENAME logcabin-reconfigure DESTINATION ${INSTALL_PATH})
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/SmokeTest RENAME logcabin-smoketest DESTINATION ${INSTALL_PATH})
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/Tool RENAME logcabin-storage DESTINATION ${INSTALL_PATH})
