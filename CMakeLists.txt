cmake_minimum_required(VERSION 3.5)
project(logcabin)

find_package(Protobuf REQUIRED)
#find_package(JeMalloc REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
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
set(TREE_FILES "Tree/ProtoBuf.cc"
    "Tree/Tree.cc"
)
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

PROTOBUF_GENERATE_CPP(ProtoSources ProtoHeaders ${PROTOCOL_FILES})
PROTOBUF_GENERATE_CPP(ServerProtoSources ServerProtoHeaders ${SERVER_PROTO_FILES})

set(SOURCE_FILES
    "Server/Main.cc"
    ${SERVER_FILES}
    ${ServerProtoSources}
    ${STORAGE_FILES}
    ${TREE_FILES}
    ${CLIENT_FILES}
    ${ProtoSources}
    ${RPC_FILES}
    ${EVENT_FILES}
    ${CORE_FILES}
        Server/utils.cc Server/utils.h)

set(LIBS ${CMAKE_SOURCE_DIR}/redis3m/build/libredis3m.a lz4 protobuf cryptopp hiredis
        pthread rt snappy z bz2 jemalloc boost)
add_executable(logcabin ${SOURCE_FILES})
target_link_libraries(logcabin ${LIBS})
include_directories(. ./include gtest/include redis3m/include)
