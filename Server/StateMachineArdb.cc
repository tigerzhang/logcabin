//
// Created by Zhang Hu on 8/28/16.
//

#include <exception>
#include <hiredis/sds.h>
#include "Server/RaftConsensus.h"
#include "StateMachineBase.h"
#include "StateMachineArdb.h"

#include "Core/Debug.h"
#include "utils.h"
#include "Globals.h"

#include <boost/algorithm/string.hpp>
#include <fcntl.h>
#include <unistd.h>
#include <hiredis/hiredis.h>

namespace LogCabin {
namespace Server {

extern int encodeRedisReply(redisReply *reply, std::string &str);

// TODO: wrap redis request in a C++ class, then redisReply call be freed automatically

redisReply *StateMachineArdb::getReply(const std::string &key) const {
    redisReply *reply;
    redisContext *c = (redisContext *) kvstore;

    // TODO: make sure the request(key) is a valid redis request

    VVERBOSE("set key: %s", key.c_str());
    sds sdsnew;
    sdsnew = sdscatlen(c->obuf, key.c_str(), key.length());
    if (sdsnew == nullptr) {
        return nullptr;
    }
    c->obuf = sdsnew;
    VVERBOSE("obuf: %s", c->obuf);
    redisGetReply(c, (void**)&reply);
    return reply;
}

int StateMachineArdb::put(const std::string &key, const std::string &value) {
    redisReply *reply = getReply(key);
    lastApplyResult.clear();
    if (reply == nullptr) {
        return -1;
    }
    int ret = encodeRedisReply(reply, lastApplyResult);
    freeReplyObject(reply);
    return ret;
}

int StateMachineArdb::get(const std::string &key, std::string *value) const {
    redisReply *reply = getReply(key);
    value->clear();
    if (reply == nullptr) {
        return -1;
    }
    int ret = encodeRedisReply(reply, *value);
    freeReplyObject(reply);
    return ret;
}

StateMachineArdb::StateMachineArdb(std::shared_ptr<RaftConsensus> consensus, Core::Config &config,
                                     Globals &globals, void *kvstore)
        : StateMachineBase(consensus, config, globals, kvstore)
        , snapshotContext(nullptr) {
}


void StateMachineArdb::takeSnapshotWriteData(uint64_t lastIncludedIndex, Storage::SnapshotFile::Writer *writer) {
    do_ardb_bgsave(lastIncludedIndex, writer);
}

void StateMachineArdb::loadSnapshotLoadData(Core::ProtoBuf::InputStream &stream) {
    do_ardb_load_snapshot(stream);
}

void *StateMachineArdb::createSnapshotPoint() {
    if (snapshotContext != nullptr) {
        redisFree(snapshotContext);
        snapshotContext = nullptr;
    }
    snapshotContext = getContext();
    return nullptr;
}

redisContext *StateMachineArdb::getContext() const {
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

void StateMachineArdb::snapshotDone() {
    if (snapshotContext) {
        redisFree(snapshotContext);
        snapshotContext = nullptr;
    }
}

void StateMachineArdb::do_ardb_load_snapshot(Core::ProtoBuf::InputStream &stream) {
    ulong size;
    stream.readRaw(&size, sizeof(ulong));
    char buf[1024] = {'\0'};
    stream.readRaw(buf, std::min(size, ulong(1024)));

    if (snapshotContext == nullptr) {
        snapshotContext = getContext();
    }
    redisContext *conn = (redisContext *)snapshotContext;

    char cmd[2048] = {'\0'};
    VVERBOSE("buf: %s, len: %lu", buf, strlen(buf));
    snprintf(cmd, 2048, "import %s", buf);
    VVERBOSE("size: %lu, execuate command: %s", size, cmd);
    redisReply *reply = (redisReply *)redisCommand(conn, cmd);
    freeReplyObject(reply);
}

void StateMachineArdb::do_ardb_bgsave(uint64_t lastIncludedIndex, Storage::SnapshotFile::Writer *writer) {
    assert(snapshotContext != nullptr);
    redisContext *conn = (redisContext *)snapshotContext;
    redisReply *reply;
#define BUFSIZE 2048
    char buf[BUFSIZE] = { '\0' };
#define CWDSIZE 1024
    char cwd[CWDSIZE] = { '\0' };

    // save current committed index for debugging
    snprintf(buf, BUFSIZE - 1, "set __commit_index %lu", lastIncludedIndex);
    reply = (redisReply *)redisCommand(conn, buf);
    freeReplyObject(reply);

    const std::string& snapshotDir(globals.raft->getStorageLayout().snapshotDir.path);

    if (snapshotDir.compare(0, 1, "/") != 0) { // relative path
        getcwd(cwd, CWDSIZE);
        strcat(cwd, "/"),
                strcat(cwd, globals.raft->getStorageLayout().snapshotDir.path.c_str());
        NOTICE("backup-dir: %s", cwd);

        snprintf(buf, BUFSIZE - 1, "CONFIG SET backup-dir %s", cwd);
    } else { // absolute path
        snprintf(buf, BUFSIZE - 1, "CONFIG SET backup-dir %s", snapshotDir.c_str());
    }

    VERBOSE("backup dir is: %s", buf);
    reply = (redisReply *)redisCommand(conn, buf);
    assert(reply->type == REDIS_REPLY_STATUS);
    freeReplyObject(reply);

    reply = (redisReply *)redisCommand(conn, "CONFIG REWRITE");
    freeReplyObject(reply);

    // issue bgsave command
    snprintf(buf, BUFSIZE - 1, "BGSAVE REDIS %lu", lastIncludedIndex);
    reply = (redisReply *)redisCommand(conn, buf);
    VERBOSE("BGSAVE return: %s", reply->str);
    assert(strcmp(reply->str, "OK") == 0);
    freeReplyObject(reply);

    // wait bgsave done
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
                freeReplyObject(reply);
                break;
            }
        }
        if (reply->type == REDIS_REPLY_ARRAY) {
            for (auto i = 0; i < reply->elements; i++) {
                VVERBOSE("%s", reply->element[i]->str);
                if (strcmp(reply->element[i]->str,
                           "rdb_bgsave_in_progress:0") == 0) {
                    done = true;
                    freeReplyObject(reply);
                    break;
                }
            }
        }
        freeReplyObject(reply);
    }

    VERBOSE("BGSAVE is finished");

    reply = (redisReply *)redisCommand(conn, "CONFIG GET backup-dir");
    assert(reply->type == REDIS_REPLY_ARRAY);
    assert(strcmp(reply->element[0]->str, "backup-dir") == 0);
    std::string dir = reply->element[1]->str;
    freeReplyObject(reply);

    // get the filename of backup
//    reply = (redisReply *)redisCommand(conn, "INFO replication");
//    if (reply->type == REDIS_REPLY_STRING) {
//        // search the last snapshot:
//        // snapshot<n>:type=[redis|backup],create_time=<timestamp>,name=<filename>
//    }
//    freeReplyObject(reply);

    // save the snapshot filename to raft snapshot
    snprintf(buf, BUFSIZE - 1, "save-redis-snapshot.%lu", lastIncludedIndex);
    std::string dbfilename(buf);

    std::string fullname = dir + "/" + dbfilename;
    ulong size = fullname.size();

    VVERBOSE("fullname: %s, size: %lu", fullname.c_str(), size);

    // suppress the snapshot watcher
    //
    // FIXME: the snapshot watcher should check the real snapshot file
    //
    writer->writeRaw(&size, sizeof(size));
    writer->writeRaw(fullname.c_str(), size);
}

} // namespace LogCabin::Server
} // namespace LogCabin