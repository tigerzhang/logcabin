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
#include <unistd.h>
#include <hiredis/hiredis.h>

namespace LogCabin {
namespace Server {

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
        : StateMachineBase(consensus, config, globals, kvstore) {
}


void StateMachineRedis::takeSnapshotWriteData(uint64_t lastIncludedIndex, Storage::SnapshotFile::Writer *writer) {

#ifdef REDIS_STATEMACHINE
    do_redis_bgsave(lastIncludedIndex, writer);
#elif ARDB_STATEMACHINE
    do_ardb_bgsave(lastIncludedIndex, writer);
#endif

}

void StateMachineRedis::loadSnapshotLoadData(Core::ProtoBuf::InputStream &stream) {
#ifdef REDIS_STATEMACHINE
    do_redis_load_snapshot(stream);
#elif ARDB_STATEMACHINE
    do_ardb_load_snapshot(stream);
#endif

}

void *StateMachineRedis::createSnapshotPoint() {
    return nullptr;
}

void StateMachineRedis::snapshotDone() {

}

void StateMachineRedis::do_redis_load_snapshot(Core::ProtoBuf::InputStream &stream) {
    ulong size;
    stream.readRaw(&size, sizeof(ulong));
    char buf[1024];
    stream.readRaw(buf, std::min(size, ulong(1024)));

//    redis3m::connection *connection = ((redis3m::connection *) kvstore);
//    connection->run(redis3m::command("DEBUG") << "RELOADRDBFROM" << std::string(buf));
    redisContext *conn = (redisContext *)kvstore;
    char cmd[2048];
    snprintf(cmd, 2048, "DEBUG RELOADRDBFROM %s", buf);
    redisCommand(conn, cmd);
}

void StateMachineRedis::do_ardb_load_snapshot(Core::ProtoBuf::InputStream &stream) {
    ulong size;
    stream.readRaw(&size, sizeof(ulong));
    char buf[1024] = {'\0'};
    stream.readRaw(buf, std::min(size, ulong(1024)));
    redisContext *conn = (redisContext *)kvstore;
    char cmd[2048] = {'\0'};
    VVERBOSE("buf: %s, len: %d", buf, strlen(buf));
    snprintf(cmd, 2048, "import %s", buf);
    VVERBOSE("size: %d, execuate command: %s", size, cmd);
    redisCommand(conn, cmd);
}

void StateMachineRedis::do_ardb_bgsave(uint64_t lastIncludedIndex, Storage::SnapshotFile::Writer *writer) {
    redisContext *conn = (redisContext *)kvstore;
    char buf[2048];
    char cwd[1024] = { '\0' };

    getcwd(cwd, sizeof(cwd));
    strcat(cwd, "/"),
    strcat(cwd, globals.raft->getStorageLayout().snapshotDir.path.c_str());
    VVERBOSE("current dir: %s", cwd);
    snprintf(buf, 2048, "CONFIG SET backup-dir %s", cwd);
    VERBOSE("backup dir is: %s", buf);

    redisReply *reply = (redisReply *)redisCommand(conn, buf);
    assert(reply->type == REDIS_REPLY_STATUS);

    redisCommand(conn, "CONFIG REWRITE");
    reply = (redisReply *)redisCommand(conn, "BGSAVE redis");
    VERBOSE("BGSAVE return: %s", reply->str);
    assert(strcmp(reply->str, "OK") == 0);

    bool done = false;
    while (!done) {
        usleep(1000 * 1000);
        reply = (redisReply *)redisCommand(conn, "INFO Persistence");
        VVERBOSE("INFO Persistence reply type: %d", reply->type);
        if (reply->type == REDIS_REPLY_STRING) {
            VVERBOSE("%s", reply->str);
            auto found = strstr(reply->str, "rdb_bgsave_in_progress:0");
            if (found != nullptr) {
                done = true;
                break;
            }
        }
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

    VERBOSE("BGSAVE is finished");
    reply = (redisReply *)redisCommand(conn, "CONFIG GET backup-dir");
    assert(reply->type == REDIS_REPLY_ARRAY);
    assert(strcmp(reply->element[0]->str, "backup-dir") == 0);
    std::string dir = reply->element[1]->str;

//    reply = (redisReply *)redisCommand(conn, "CONFIG GET dbfilename");
//    assert(reply->type == REDIS_REPLY_ARRAY);
//    assert(strcmp(reply->element[0]->str, "dbfilename") == 0);
    std::string dbfilename = "save-redis-snapshot";

    std::string fullname = dir + "/" + dbfilename;
    ulong size = fullname.size();

    VVERBOSE("fullname: %s, size: %d", fullname.c_str(), size);

    writer->writeRaw(&size, sizeof(size));
    writer->writeRaw(fullname.c_str(), size);
}

void StateMachineRedis::do_redis_bgsave(uint64_t lastIncludedIndex, Storage::SnapshotFile::Writer *writer) {
//    redis3m::connection *connection = ((redis3m::connection *) kvstore);
    redisContext *conn = (redisContext *)kvstore;

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

} // namespace LogCabin::Server
} // namespace LogCabin