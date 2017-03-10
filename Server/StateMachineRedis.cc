//
// Created by parallels on 8/28/16.
//

#include <exception>
#include <redis3m/redis3m.hpp>
#include "../RedisServer/sds.h"
#include "Server/RaftConsensus.h"
#include "StateMachineBase.h"
#include "StateMachineRedis.h"
#include "../redis_cpp/include/redis_protocol/resp_protocol.hpp"

#include "Core/Debug.h"
#include "utils.h"
#include "Globals.h"

//#include <boost/algorithm/string/trim.hpp>
//#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include <fcntl.h>
#include <hiredis/hiredis.h>

namespace LogCabin {
namespace Server {

//int StateMachineRedis::initKVStore() {
//    return 0;
//}

int encodeRedisReply(redisReply *reply, std::string &str);
int encodeRedisReply(redisReply *reply, std::string &str) {
    int ret = 0;
    char buf[128];
    switch (reply->type) {
        case REDIS_REPLY_STRING: {
            str += '$';

            snprintf(buf, 128, "%lu", reply->len);
            str += buf;
            str += "\r\n";

            std::string s(reply->str, reply->len);
            str += s;
            str += "\r\n";
        }
            break;
        case REDIS_REPLY_ARRAY: {
            snprintf(buf, 128, "*%lu\r\n", reply->elements);
            str += buf;
            for (auto i = 0; i < reply->elements; i++) {
                encodeRedisReply(reply->element[i], str);
            }
        }
            break;
        case REDIS_REPLY_INTEGER:
            str += ':';
            snprintf(buf, 128, "%lld", reply->integer);
            str += buf;
            str += "\r\n";
            break;
        case REDIS_REPLY_NIL:
            str += "$-1\r\n";
            break;
        case REDIS_REPLY_STATUS:
            str += '+';
            str += reply->str;
            str += "\r\n";
            break;
        case REDIS_REPLY_ERROR:
            str += '-';
            str += reply->str;
            str += "\r\n";
            break;
        default:
            str += "$-1\r\n";
            ret = -1;
    }
    return ret;
}

redisReply *StateMachineRedis::getReply(const std::string &key) const {
    redisReply *reply;
    redisContext *c = (redisContext *) kvstore;

    // TODO: make sure the request(key) is a valid redis request

    VVERBOSE("set key: %s", key.c_str());
    sds sdsnew;
    sdsnew = sdscatlen(c->obuf, key.c_str(), key.length());
    c->obuf = sdsnew;
    VVERBOSE("obuf: %s", c->obuf);
    redisGetReply(c, (void**)&reply);
    return reply;
}

int StateMachineRedis::put(const std::string &key, const std::string &value) {
    redisReply *reply = getReply(key);
    lastApplyResult.clear();
    return encodeRedisReply(reply, lastApplyResult);
}

int StateMachineRedis::get(const std::string &key, std::string *value) const {
    redisReply *reply = getReply(key);
    value->clear();
    int ret = encodeRedisReply(reply, *value);
    return ret;
}

StateMachineRedis::StateMachineRedis(std::shared_ptr<RaftConsensus> consensus, Core::Config &config,
                                     Globals &globals, void *kvstore)
        : StateMachineBase(consensus, config, globals, kvstore)
        , snapshotContext(nullptr) {
}


void StateMachineRedis::takeSnapshotWriteData(uint64_t lastIncludedIndex,
                                   Storage::SnapshotFile::Writer *writer) {
//    redis3m::connection *connection = ((redis3m::connection *) kvstore);
    assert(snapshotContext != nullptr);
    redisContext *conn = (redisContext *)snapshotContext;

//    redis3m::reply reply = connection->run(
//            redis3m::command("CONFIG") << "SET" << "dir"
//                                       << globals.raft->getStorageLayout().snapshotDir.path);
    char buf[2048];
    snprintf(buf, 2048, "CONFIG SET dir %s", globals.raft->getStorageLayout().snapshotDir.path.c_str());
    redisReply *reply = (redisReply *)redisCommand(conn, buf);
    assert(reply->type == REDIS_REPLY_STATUS);

//    connection->run(redis3m::command("CONFIG") << "REWRITE");
    redisCommand(conn, "CONFIG REWRITE");

//    reply = connection->run(redis3m::command("BGSAVE"));
    reply = (redisReply *)redisCommand(conn, "BGSAVE");
//    if (reply.type() == redis3m::reply::type_t::STRING) {
    VERBOSE("BGSAVE return: %s", reply->str);
    assert(strcmp(reply->str, "Background saving started") == 0);

    bool done = false;
    while (!done) {
        usleep(1000 * 1000);
//        reply = connection->run(redis3m::command("INFO") << "Persistence");
        reply = (redisReply *)redisCommand(conn, "INFO Persistence");
        VVERBOSE("INFO Persistence reply type: %d", reply->type);
//        if (reply.type() == redis3m::reply::type_t::STRING) {
//            auto found = reply.str().find("rdb_bgsave_in_progress:0");
//            if (found != std::string::npos) {
//                done = true;
//                break;
//            }
//        }
        if (reply->type == REDIS_REPLY_STRING) {
            VVERBOSE("%s", reply->str);
            auto found = strstr(reply->str, "rdb_bgsave_in_progress:0");
            if (found != nullptr) {
                done = true;
                break;
            }
        }
//        if (reply.type() == redis3m::reply::type_t::ARRAY) {
//            for (auto &element : reply.elements()) {
//                VVERBOSE("%s", element.str().c_str());
//                if (element.str() == "rdb_bgsave_in_progress:0") {
//                    done = true;
//                    break;
//                }
//            }
//        }
        if (reply->type == REDIS_REPLY_ARRAY) {
            for (int i = 0; i < reply->elements; i++) {
                VVERBOSE("%s", reply->element[i]->str);
                if (strcmp(reply->element[i]->str,
                           "rdb_bgsave_in_progress:0") == 0) {
                    done = true;
                    break;
                }
            }
        }
    }

//    reply = connection->run(redis3m::command("CONFIG") << "GET" << "dir");
//    assert(reply.type() == redis3m::reply::type_t::ARRAY);
//    assert(reply.elements().at(0).str() == "dir");
//    std::string dir = reply.elements().at(1);
    reply = (redisReply *)redisCommand(conn, "CONFIG GET dir");
    assert(reply->type == REDIS_REPLY_ARRAY);
    assert(strcmp(reply->element[0]->str, "dir") == 0);
    std::string dir = reply->element[0]->str;

//    reply = connection->run(redis3m::command("CONFIG") << "GET" << "dbfilename");
//    assert(reply.type() == redis3m::reply::type_t::ARRAY);
//    assert(reply.elements().at(0).str() == "dbfilename");
//    std::string dbfilename = reply.elements().at(1);
    reply = (redisReply *)redisCommand(conn, "CONFIG GET dbfilename");
    assert(reply->type == REDIS_REPLY_ARRAY);
    assert(strcmp(reply->element[0]->str, "dbfilename") == 0);
    std::string dbfilename = reply->element[0]->str;

    std::string fullname = dir + "/" + dbfilename;
    ulong size = fullname.size();
    writer->writeRaw(&size, sizeof(size));
    writer->writeRaw(fullname.c_str(), size);

//    Storage::FilesystemUtil::FileContents snapshotFile(
//            Storage::FilesystemUtil::openFile(
//                    Storage::FilesystemUtil::openDir(dir), dbfilename, O_RDONLY)
//    );
//    uint64_t numDataBytes = 0;
//    uint64_t snapshotFileOffset = 0;

//    while (true) {
//        numDataBytes = std::min(
//                snapshotFile.getFileLength() - snapshotFileOffset,
//                (uint64_t) 4096);
//
//        writer.writeRaw(snapshotFile.get<char>(snapshotFileOffset,
//                                               numDataBytes),
//                        numDataBytes);
//
//        if (snapshotFileOffset + numDataBytes == snapshotFile.getFileLength())
//            break;
//
//        snapshotFileOffset += numDataBytes;
//    }
}

void StateMachineRedis::loadSnapshotLoadData(Core::ProtoBuf::InputStream &stream) {
    ulong size;
    stream.readRaw(&size, sizeof(ulong));
    char buf[1024];
    stream.readRaw(buf, std::min(size, ulong(1024)));

//    redis3m::connection *connection = ((redis3m::connection *) kvstore);
//    connection->run(redis3m::command("DEBUG") << "RELOADRDBFROM" << std::string(buf));
    if (snapshotContext == nullptr) {
        snapshotContext = getContext();
    }
    redisContext *conn = (redisContext *)snapshotContext;
    char cmd[2048];
    snprintf(cmd, 2048, "DEBUG RELOADRDBFROM %s", buf);
    redisCommand(conn, cmd);
}

void *StateMachineRedis::createSnapshotPoint() {
    if (snapshotContext != nullptr) {
        redisFree(snapshotContext);
        snapshotContext = nullptr;
    }
    snapshotContext = getContext();
    return nullptr;
}

redisContext *StateMachineRedis::getContext() const {
    redisContext *c = NULL;
    struct timeval timeout = { 1, 500000 }; // 1.5 seconds

    std::string redisAddress =
            globals.config.read<std::string>("redisAddress", std::string(""));
    if (redisAddress == "") {
        std::string redisSock =
                globals.config.read<std::string>("redisSock", std::string(""));
        if (redisSock != "") {
            // open unix socket
            std::string sock_path = globals.raft->getStorageLayout().serverDir.path + "/redis.sock";
            NOTICE("redisSock: %s", sock_path.c_str());
            c = redisConnectUnix(sock_path.c_str());
        }
    } else {
        std::vector<std::string> splitVect;
        split(splitVect, redisAddress, boost::algorithm::is_any_of(":"));
        NOTICE("redisAddress: %s, %s", splitVect[0].c_str(), splitVect[1].c_str());
        c = redisConnectWithTimeout(splitVect[0].c_str(),
                                    std::stoi(splitVect[1].c_str()),
                                    timeout);
    }
    return c;
}

void StateMachineRedis::snapshotDone() {
    if (snapshotContext) {
        redisFree(snapshotContext);
        snapshotContext = nullptr;
    }
}

}
}