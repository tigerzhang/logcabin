cmake_minimum_required(VERSION 3.5)
project(logcabin)

add_definitions(-DROCKSDB_STATEMACHINE)

find_package(Protobuf REQUIRED)
#find_package(JeMalloc REQUIRED)
set(Protobuf_IMPORT_DIRS ${PROJECT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/Protocol)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -ggdb3")
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
        "RedisServer/sds.c"
        )

PROTOBUF_GENERATE_CPP(ProtoSources ProtoHeaders ${PROTOCOL_FILES})
PROTOBUF_GENERATE_CPP(ServerProtoSources ServerProtoHeaders ${SERVER_PROTO_FILES})
PROTOBUF_GENERATE_CPP(StorageProtoSources StorageProtoHeaders ${STORAGE_PROTO_FILES})
PROTOBUF_GENERATE_CPP(TreeProtoSources TreeProtoHeaders ${TREE_PROTO_FILES})

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
    ${REDISSERVER_FILES}
        ${REDIS_CPP_FILES})

set(LIBS ${CMAKE_SOURCE_DIR}/redis3m/build/libredis3m.a ${CMAKE_SOURCE_DIR}/rocksdb/librocksdb.a
        lz4 protobuf cryptopp hiredis
        pthread rt snappy z bz2 jemalloc event boost_system)
add_executable(LogCabin ${SOURCE_FILES})
target_link_libraries(LogCabin ${LIBS})
include_directories(${CMAKE_CURRENT_BINARY_DIR} ./build . ./include gtest/include redis3m/include)

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
add_executable(TreeOps ${CMAKE_SOURCE_DIR}/Examples/TreeOps.cc ${REDISSERVER_FILES})
target_link_libraries(TreeOps logcabin ${LIBS})
add_executable(ServerControl ${CMAKE_SOURCE_DIR}/Client/ServerControl.cc)
target_link_libraries(ServerControl logcabin ${LIBS})