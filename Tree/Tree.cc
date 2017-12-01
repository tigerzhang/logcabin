/* Copyright (c) 2012 Stanford University
#include "Tree/TreeStorageLayer.h"
 * Copyright (c) 2014 Diego Ongaro
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <cassert>
#include <algorithm>
#include <dirent.h>
#include <sys/stat.h>

#include "Tree/MemTree.h"
#include "build/Protocol/ServerStats.pb.h"
#include "build/Tree/Snapshot.pb.h"
#include "Core/Debug.h"
#include "Core/StringUtil.h"
#include "Tree/Tree.h"
#include "Tree/TreeStorageLayer.h"

#include "Server/RaftConsensus.h"

namespace LogCabin {
namespace Tree {

using Core::StringUtil::format;

////////// enum Status //////////

std::ostream&
operator<<(std::ostream& os, Status status)
{
    switch (status) {
        case Status::OK:
            os << "Status::OK";
            break;
        case Status::INVALID_ARGUMENT:
            os << "Status::INVALID_ARGUMENT";
            break;
        case Status::LOOKUP_ERROR:
            os << "Status::LOOKUP_ERROR";
            break;
        case Status::TYPE_ERROR:
            os << "Status::TYPE_ERROR";
            break;
        case Status::CONDITION_NOT_MET:
            os << "Status::CONDITION_NOT_MET";
            break;
        case Status::LIST_EMPTY:
            os << "Status::LIST_EMPTY";
            break;
        case Status::KEY_EXPIRED:
            os << "Status::KEY_EXPIRED";
            break;
    }
    return os;
}

std::vector<std::string>
split_args(const std::string& input)
{
    std::vector<std::string> args;
    bool token_begin = false;
    auto token_start = input.begin();

    for (auto it = input.begin(); it != input.end(); ++it) {
        if (*it == ' ') {
            if (token_begin) {
                args.emplace_back(std::string(token_start, it));
                token_begin = false;
            } else {
                continue;
            }
        } else {
            if (token_begin) {
                continue;
            } else {
                token_begin = true;
                token_start = it;
            }
        }
    }
    if (token_begin) {
        args.emplace_back(std::string(token_start, input.end()));
    }

    return args;
}

////////// struct Result //////////

Result::Result()
    : status(Status::OK)
    , error()
{
}


////////// class Tree //////////

Tree::Tree() :
    numConditionsChecked(0)
    , numConditionsFailed(0)
    , numMakeDirectoryAttempted(0)
    , numMakeDirectorySuccess(0)
    , numListDirectoryAttempted(0)
    , numListDirectorySuccess(0)
    , numRemoveDirectoryAttempted(0)
    , numRemoveDirectoryParentNotFound(0)
    , numRemoveDirectoryTargetNotFound(0)
    , numRemoveDirectoryDone(0)
    , numRemoveDirectorySuccess(0)
    , numWriteAttempted(0)
    , numWriteSuccess(0)
    , numReadAttempted(0)
    , numReadSuccess(0)
    , numRemoveFileAttempted(0)
    , numRemoveFileParentNotFound(0)
    , numRemoveFileTargetNotFound(0)
    , numRemoveFileDone(0)
    , numRemoveFileSuccess(0)
    , numRPushAttempted(0)
    , numRPushSuccess(0)
    , numLPopAttempted(0)
    , numLPopSuccess(0)
    , numLRemAttempted(0)
    , numLRemSuccess(0)
#ifdef ARDB_FSM
    , ardb()
    , worker_ctx()
    , rdb(NULL)
#endif // ARSDB_FSM
    , raft(NULL)
{

//    ardb::Server server;
//    server.Start();


#ifdef ARDB_FSM
    worker_ctx.ClearFlags();
#endif // ARDB_FSM

    storage_layer = std::make_shared<MemTree>();
}

Tree::~Tree() {
}

void Tree::setRaft(LogCabin::Server::RaftConsensus* raft) {
    this->raft = raft;
}

void Tree::Init(std::string& path) {

    storage_layer->Init(path);
#ifdef ARDB_FSM
    if (ardb.Init("ardb.conf") != 0) {
        PANIC("Open ardb failed.");
    }

    if (0 != g_repl->Init())
    {
        PANIC("Failed to init replication service.");
    }
#endif // ROCKSDB_FSM
}



void
Tree::findLatestSnapshot(Core::ProtoBuf::OutputStream* stream) const {
#if 0
    DIR *dir;
    struct dirent *ent;
    char latest_snapshot_dir[256];
    uint32_t latest_timestamp = 0;

    std::string backup_dir = g_db->GetConf().backup_dir;
    if ((dir = opendir (backup_dir.c_str())) != NULL) {
        char filename[256];
        /* print all the files and directories within directory */
        while ((ent = readdir (dir)) != NULL) {
            // skip "." and ".."
            if (strcmp(ent->d_name, ".") == 0)
                continue;
            if (strcmp(ent->d_name, "..") == 0)
                continue;

            snprintf(filename, 255, "%s/%s", backup_dir.c_str(), ent->d_name);
            // printf ("%s\n", ent->d_name);
            struct stat t_stat;
            stat(filename, &t_stat);
            // found a newer snapshot
            if (t_stat.st_mtime > latest_timestamp) {
                latest_timestamp = t_stat.st_mtime;
                strncpy(latest_snapshot_dir, ent->d_name, 255);
            }
        }
        closedir (dir);

        if (latest_timestamp > 0) {
            Snapshot::File file;
            file.set_contents(latest_snapshot_dir);
            if (stream) {
                stream->writeMessage(file);
            } else {
                printf("Latest snapshot dir: %s\n", latest_snapshot_dir);
            }
        }
    } else {
        /* could not open directory */
        perror ("findLatestSnapshot");
    }
#endif
}

void
Tree::dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const
{
    storage_layer->dumpSnapshot(stream);

#ifdef ARDB_FSM
    // findLatestSnapshot(&stream);
    const ardb::Ardb* pArdb = &ardb;
    ardb::Ardb* pArdb2 = &ardb;
    const rocksdb::Snapshot* snapshot =
            (rocksdb::Snapshot*)(pArdb->GetSnapshot((ardb::Context&)worker_ctx));
    rocksdb::ReadOptions options;
    // options.snapshot = snapshot;

    ardb::RocksDBEngine * pEngine = (ardb::RocksDBEngine *)(pArdb2->m_engine);
    if (pEngine == NULL) {
        ERROR("engine is null");
        return;
    }

    void * p = pEngine->m_db;

//    rocksdb::DB* db = (rocksdb::DB*)(pEngine->GetDB());
    rocksdb::DB* db = (rocksdb::DB*)p;
    if (db == NULL) {
        ERROR("db is null");
        return;
    }
    Snapshot::KeyValue kv;
    kv.set_key("hello");
    kv.set_value("world");
    stream.writeMessage(kv);

    int keyDumped = 0;
    if (ardb::g_rocksdb) {
//        pEngine->Put((ardb::Context &)worker_ctx, "a", "b");

//        ardb::g_rocksdb->Put(rocksdb::WriteOptions(), "a", "b");
        ardb::codec::ArgumentArray cmdArray;
        cmdArray.push_back("set");
        cmdArray.push_back("a");
        cmdArray.push_back("b");
        ardb::codec::RedisCommandFrame redisCommandFrame(cmdArray);
        pArdb2->Call((Context&)worker_ctx, redisCommandFrame);

        // rocksdb::Iterator* it= db->NewIterator(options);
//        auto it = ardb::g_rocksdb->NewIterator(rocksdb::ReadOptions());
        auto it = db->NewIterator(rocksdb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            // cout << it->key().ToString() << ": " << it->value().ToString() << endl;
            Snapshot::KeyValue kv;
            kv.set_key(it->key().ToString());
            kv.set_value(it->value().ToString());
            stream.writeMessage(kv);
            NOTICE("key %s value %s", it->key().ToString().c_str(), it->value().ToString().c_str());

            keyDumped++;
        }
        assert(it->status().ok()); // Check for any errors found during the scan
        delete it;
    }
//    pArdb->ReleaseSnapshot((ardb::Context&)worker_ctx);
    NOTICE("key dumped: %d", keyDumped);
#endif // ROCKSDB_FSM
}

/**
 * Load the tree from the given stream.
 */
void
Tree::loadSnapshot(Core::ProtoBuf::InputStream& stream)
{
#ifdef ARDB_FSM
    Snapshot::File node;
    std::string error = stream.readMessage(node);
    if (!error.empty()) {
        PANIC("Couldn't read snapshot: %s", error.c_str());
    }
    std::string latest_snapshot_dir = node.contents();
    std::string backup_dir = g_db->GetConf().backup_dir;

    Result result;
    ardb::codec::ArgumentArray cmdArray;
    cmdArray.push_back("import");
    cmdArray.push_back(backup_dir + "/" + latest_snapshot_dir);
    ardb::codec::RedisCommandFrame redisCommandFrame(cmdArray);
    ardb.Call((ardb::Context&)worker_ctx, redisCommandFrame);
//    NOTICE(worker_ctx.GetReply().GetString().c_str());
    NOTICE("import backup status: %s", worker_ctx.GetReply().Status().c_str());

#endif
    storage_layer->loadSnapshot(stream);
}

Result
Tree::checkCondition(const std::string& path,
                     const std::string& contents) const
{
    ++numConditionsChecked;
    std::string actualContents;
    Result readResult = read(path, actualContents);
    if (readResult.status == Status::OK) {
        if (contents == actualContents) {
            return Result();
        } else {
            Result result;
            result.status = Status::CONDITION_NOT_MET;
            result.error = format("Path '%s' has value '%s', not '%s' as "
                                  "required",
                                  path.c_str(),
                                  actualContents.c_str(),
                                  contents.c_str());
            ++numConditionsFailed;
            return result;
        }
    }
    if (readResult.status == Status::LOOKUP_ERROR && contents.empty()) {
        return Result();
    }
    Result result;
    result.status = Status::CONDITION_NOT_MET;
    result.error = format("Could not read value at path '%s': %s",
                          path.c_str(), readResult.error.c_str());
    ++numConditionsFailed;
    return result;
}

Result
Tree::makeDirectory(const std::string& symbolicPath)
{
    ++numMakeDirectoryAttempted;
    Result result = storage_layer->makeDirectory(symbolicPath);

    ++numMakeDirectorySuccess;
    return result;
}

Result
Tree::scard(const std::string& symbolicPath,
                    std::string& content) const
{
    ++numListSmembersAttempted;
    Result result;
    result = storage_layer->scard(symbolicPath, content);
    ++numListSmembersSuccess;
    return result;
}

Result
Tree::smembers(const std::string& symbolicPath,
                    std::vector<std::string>& children) const
{
    ++numListSmembersAttempted;
    children.clear();
    Result result;
    result = storage_layer->smembers(symbolicPath, children);
    ++numListSmembersSuccess;
    return result;
}

Result
Tree::listDirectory(const std::string& symbolicPath,
                    std::vector<std::string>& children) const
{
    ++numListDirectoryAttempted;
    children.clear();
    Result result;
    result = storage_layer->listDirectory(symbolicPath, children);
    ++numListDirectorySuccess;
    return result;
}

Result
Tree::removeDirectory(const std::string& symbolicPath)
{
    ++numRemoveDirectoryAttempted;
    Result result = storage_layer->removeDirectory(symbolicPath);
    ++numRemoveDirectoryDone;
    ++numRemoveDirectorySuccess;
    return result;
}
Result Tree::removeExpireSetting(const std::string& path)
{
    return storage_layer->removeExpireSetting(path);
}
    
Result Tree::cleanExpiredKeys(const std::string& path)
{
    return storage_layer->cleanExpiredKeys(path);
}

Result
Tree::write(const std::string& symbolicPath, const std::string& contents, int64_t requestTime)
{
    ++numWriteAttempted;
    Result result;
    result.status = Status::OK;
    if (symbolicPath == "") {
        result.status = Status::INVALID_ARGUMENT;
        return result;
    }

    if (symbolicPath == "/") {
        // write to a directory, return TYPE_ERROR
        result.status = Status::TYPE_ERROR;
        return result;
    }

    checkIsKeyExpiredForWriteRequest(symbolicPath, requestTime);
    result = storage_layer->write(symbolicPath, contents, requestTime);

#ifdef ARDB_FSM
    ardb::codec::ArgumentArray cmdArray;
    cmdArray.push_back("lpush");
    cmdArray.push_back(symbolicPath);
    cmdArray.push_back(contents);
    ardb::codec::RedisCommandFrame redisCommandFrame(cmdArray);
    ardb.Call(worker_ctx, redisCommandFrame);
#endif // ROCKSDB_FSM

    ++numWriteSuccess;
    return result;
}

Result
Tree::sadd(const std::string& symbolicPath, const std::string& contents)
{
    ++numWriteAttempted;
    Result result = storage_layer->sadd(symbolicPath, contents);

#ifdef ARDB_FSM
    Result result;
    size_t pos = contents.find('-');
    if (pos >= 0) {
        std::string start = contents.substr(0, pos-1);
        int iStart = atoi(start.c_str());
        std::string end = contents.substr(pos + 1);
        int iEnd = atoi(end.c_str());

        for (auto i = iStart; i <= iEnd; i++) {
            ardb::codec::ArgumentArray cmdArray;
            cmdArray.push_back("sadd");
            cmdArray.push_back(symbolicPath);
            std::stringstream ss;
            ss << i;
            cmdArray.push_back("uid" + ss.str());
            ardb::codec::RedisCommandFrame redisCommandFrame(cmdArray);
            ardb.Call(worker_ctx, redisCommandFrame);
        }
    } else {
        ardb::codec::ArgumentArray cmdArray;
        cmdArray.push_back("sadd");
        cmdArray.push_back(symbolicPath);
        cmdArray.push_back(contents);
        ardb::codec::RedisCommandFrame redisCommandFrame(cmdArray);
        ardb.Call(worker_ctx, redisCommandFrame);
    }
#endif // ROCKSDB_FSM

    ++numWriteSuccess;
    return result;
}

Result
Tree::srem(const std::string& symbolicPath, const std::string& contents)
{
    ++numWriteAttempted;
    Result result = storage_layer->srem(symbolicPath, contents);

#ifdef ARDB_FSM
    Result result;
    ardb::codec::ArgumentArray cmdArray;
    cmdArray.push_back("srem");
    cmdArray.push_back(symbolicPath);
    cmdArray.push_back(contents);
    ardb::codec::RedisCommandFrame redisCommandFrame(cmdArray);
    ardb.Call(worker_ctx, redisCommandFrame);
#endif // ROCKSDB_FSM

    ++numWriteSuccess;
    return result;
}

Result
Tree::pub(const std::string& symbolicPath, const std::string& contents)
{
    //TODO: should store offline msg here
    return lpush(symbolicPath, contents, 0);
}

Result
Tree::expire(const std::string &symbolicPath, const int64_t expireIn, const uint32_t op, const int64_t requestTime) {
    ++numExpireAttempted;
    VERBOSE("expire request recv for:%s", symbolicPath.c_str());
    Result result;
    int64_t expireAt = 0;
    if(Protocol::Client::CLEAN_UP_EXPIRE_KEYS == op)
    {
        //this is a expire clean up request, no need to check expire, it will be removed after all
        VERBOSE("this is a clean up request for :%s", symbolicPath.c_str());
    } 
    else
    {
        //need to check expire before setting a new expire
        VERBOSE("this is a set up request for :%s", symbolicPath.c_str());
        checkIsKeyExpiredForWriteRequest(symbolicPath, requestTime);
        expireAt = requestTime + expireIn;
    }

    storage_layer->expire(symbolicPath, expireAt, op, requestTime);

    ++numExpireSuccess;
    return result;
}

//TODO: should extract lpush and rpush
Result
Tree::lpush(const std::string &symbolicPath, const std::string &contents, int64_t requestTime) {
    ++numRPushAttempted;
    Result result;
    checkIsKeyExpiredForWriteRequest(symbolicPath, requestTime);
    result = storage_layer->lpush(symbolicPath, contents, requestTime);
    ++numRPushSuccess;
    return result;
}

Result
Tree::rpush(const std::string &symbolicPath, const std::string &contents, int64_t requestTime) {
    ++numRPushAttempted;
    Result result;
    checkIsKeyExpiredForWriteRequest(symbolicPath, requestTime);
    result = storage_layer->rpush(symbolicPath, contents, requestTime);
    ++numRPushSuccess;
    return result;
}

Result
Tree::lpop(const std::string& symbolicPath, const std::string& contents, int64_t requestTime) {
    ++numLPopAttempted;
    Result result;
    checkIsKeyExpiredForWriteRequest(symbolicPath, requestTime);
    result = storage_layer->lpop(symbolicPath, contents, requestTime);
    ++numLPopSuccess;
    return result;
}

Result
Tree::lrem(const std::string& symbolicPath, const std::string &contents, const int32_t count, int64_t requestTime) {
    ++numLRemAttempted;
    Result result;
    checkIsKeyExpiredForWriteRequest(symbolicPath, requestTime);
    result = storage_layer->lrem(symbolicPath, contents, count, requestTime);
    ++numLRemSuccess;
    return result;
}

Result
Tree::ltrim(const std::string& symbolicPath, const std::string &contents, int64_t requestTime) {
    ++numLTrimAttempted;
    Result result;
    checkIsKeyExpiredForWriteRequest(symbolicPath, requestTime);
    std::vector<std::string> args(std::move(split_args(contents)));
    result = storage_layer->ltrim(symbolicPath, args, requestTime);
    ++numLTrimSuccess;
    return result;
}

bool Tree::checkIsKeyExpiredForWriteRequest(const std::string& symbolicPath, int64_t requestTime)
{
    auto expireStatus = isKeyExpired(symbolicPath, requestTime); 
    if(Tree::KeyExpireStatusExpired == expireStatus)
    {
        cleanExpiredKeys(symbolicPath);
        return true;
    }
    else if(Tree::KeyExpireStatusNotExpired == expireStatus)
    {
        //expire should be flush before writing
        removeExpireSetting(symbolicPath);
        return false;
    }
    else
    {
        //nothing todo if there is no setting for expire
        return false;
    }
}

bool Tree::checkIsKeyExpiredForReadRequest(const std::string& symbolicPath)
{
    if(isKeyExpired(symbolicPath, 0) == Tree::KeyExpireStatusExpired)
    {
        return true;
    }
    return false;
}

void Tree::appendCleanExpireRequestLog(const std::string &path, const int64_t expireAt)
{
    if(NULL == raft ||
            raft->state != Server::RaftConsensus::State::LEADER ){
        // don't need to do anything if this node is not leader
        return;
    }
    //this function should not be retry!
    uint64_t index = this->zeroSessionIndex;
    Protocol::Client::StateMachineCommand::Request command;
    command.mutable_tree()->set_path(path);
    command.mutable_tree()->set_command(LogCabin::Protocol::Client::CLEAN_UP_EXPIRE_KEYS);
    command.mutable_tree()->set_expire_in(expireAt);
    command.mutable_tree()->mutable_exactly_once()->set_client_id(0);
    command.mutable_tree()->mutable_exactly_once()->set_rpc_number(index);
    command.mutable_tree()->mutable_exactly_once()->set_first_outstanding_rpc(index);
    this->zeroSessionIndex ++;
    Core::Buffer cmdBuffer;
    Core::ProtoBuf::serialize(command, cmdBuffer);
    //this make the Tool compile fail, but it's ok in current stage

    //also make sure you are master, but this can be put off
    if(NULL != raft )
    {
        raft->replicate(cmdBuffer);
    }
}


int64_t Tree::getKeyExpireTime(const std::string& path)
{
    //nothing found, goto rdb
    return storage_layer->getKeyExpireTime(path);
}

#define IS_REQUEST_FROM_READ (0 == requestTime)
Tree::KeyExpireStatus Tree::isKeyExpired(const std::string& path, int64_t requestTime)
{
    auto expireAt = getKeyExpireTime(path);
    if(expireAt == -1)
    {
        //no expire setting is found
        return Tree::KeyExpireStatusNotSet;
    }
    long now = requestTime;
    if(IS_REQUEST_FROM_READ)
    {
        //use current time if the request time is zero
        auto timeSpec = Core::Time::makeTimeSpec(Core::Time::SystemClock::now());
        now = timeSpec.tv_sec;
    }

    if(now > expireAt)
    {
        //don'y delete anything here, just return if the reqeust time is not zero
        if(IS_REQUEST_FROM_READ)
        {
            appendCleanExpireRequestLog(path, expireAt);
        }
        VERBOSE("key expired:%s", path.c_str());
        return Tree::KeyExpireStatusExpired;
    }
    else
    {
        //not expired, return this
        return Tree::KeyExpireStatusNotExpired;
    }
}

Result
Tree::read(const std::string& symbolicPath, std::string& contents)
{
    ++numReadAttempted;
    Result result;
    contents = "";
    if(true == checkIsKeyExpiredForReadRequest(symbolicPath))
    {

        result.status = Status::LOOKUP_ERROR;
        result.error = "Key expired";
        return result; 
    }

#ifdef ARDB_FSM
    ardb::codec::ArgumentArray cmdArray;
    cmdArray.push_back("smembers");
    cmdArray.push_back(symbolicPath);
    ardb::codec::RedisCommandFrame redisCommandFrame(cmdArray);
    ardb::Context ctx;
    ardb.Call(ctx, redisCommandFrame);
    RedisReply &r = ctx.GetReply();
    if (r.elements != NULL && !r.elements->empty())
    {
        for (uint32 i = 0; i < r.elements->size(); i++)
        {
            contents += r.elements->at(i)->GetString() + ",";
        }
    }

    cmdArray.clear();
    cmdArray.push_back("lrange");
    cmdArray.push_back(symbolicPath);
    cmdArray.push_back("0");
    cmdArray.push_back("-1");
    ardb::codec::RedisCommandFrame redisCommandFrame2(cmdArray);
    ardb.Call(ctx, redisCommandFrame2);
    r = ctx.GetReply();
    if (r.elements != NULL && !r.elements->empty())
    {
        for (uint32 i = 0; i < r.elements->size(); i++)
        {
            contents += r.elements->at(i)->GetString() + ",";
        }
    }

    cmdArray.clear();
    cmdArray.push_back("get");
    cmdArray.push_back("__lastAppliedIndex");
    ardb::codec::RedisCommandFrame redisCommandFrameGet(cmdArray);
    ardb.Call(ctx, redisCommandFrameGet);
    r = ctx.GetReply();
    contents += "\n\n" + r.GetString();

#endif // ARDB_FSM

    result = storage_layer->read(symbolicPath, contents);
    ++numReadSuccess;
    return result;
}

Result
Tree::lrange(const std::string& symbolicPath, const std::string& args, std::vector<std::string>& output)
{
    ++numLRANGEAttempted;
    Result result;
    output.clear() ;
    VERBOSE("LRANGE command: path=%s, args=%s\n", symbolicPath.c_str(), args.c_str());

    if(true == checkIsKeyExpiredForReadRequest(symbolicPath))
    {
        result.status = Status::LOOKUP_ERROR;
        result.error = "Key expired";
        return result;
    }
    std::vector<std::string> splittedArgs(std::move(split_args(args)));

    result = storage_layer->lrange(symbolicPath, splittedArgs, output);

    ++numLRANGESuccess;
    return result;
}

Result
Tree::head(const std::string& symbolicPath, std::string& contents) const
{
    ++numReadAttempted;
    contents.clear();
    Result result = storage_layer->head(symbolicPath, contents);
    ++numReadSuccess;
    return result;
}

Result
Tree::removeFile(const std::string& symbolicPath)
{
    ++numRemoveFileAttempted;

    Result result = storage_layer->removeFile(symbolicPath);
    ++numRemoveFileSuccess;
    return result;
}

void
Tree::updateServerStats(Protocol::ServerStats::Tree& tstats) const
{
    tstats.set_num_conditions_checked(
         numConditionsChecked);
    tstats.set_num_conditions_failed(
        numConditionsFailed);
    tstats.set_num_make_directory_attempted(
        numMakeDirectoryAttempted);
    tstats.set_num_make_directory_success(
        numMakeDirectorySuccess);
    tstats.set_num_list_directory_attempted(
        numListDirectoryAttempted);
    tstats.set_num_list_directory_success(
        numListDirectorySuccess);
    tstats.set_num_remove_directory_attempted(
        numRemoveDirectoryAttempted);
    tstats.set_num_remove_directory_parent_not_found(
        numRemoveDirectoryParentNotFound);
    tstats.set_num_remove_directory_target_not_found(
        numRemoveDirectoryTargetNotFound);
    tstats.set_num_remove_directory_done(
        numRemoveDirectoryDone);
    tstats.set_num_remove_directory_success(
        numRemoveDirectorySuccess);
    tstats.set_num_write_attempted(
        numWriteAttempted);
    tstats.set_num_write_success(
        numWriteSuccess);
    tstats.set_num_read_attempted(
        numReadAttempted);
    tstats.set_num_read_success(
        numReadSuccess);
    tstats.set_num_remove_file_attempted(
        numRemoveFileAttempted);
    tstats.set_num_remove_file_parent_not_found(
        numRemoveFileParentNotFound);
    tstats.set_num_remove_file_target_not_found(
        numRemoveFileTargetNotFound);
    tstats.set_num_remove_file_done(
        numRemoveFileDone);
    tstats.set_num_remove_file_success(
        numRemoveFileSuccess);
}

void Tree::cleanUpExpireKeyEvent(){
    if(NULL == raft ||
            raft->state != Server::RaftConsensus::State::LEADER)
    {
        //don't do check on follower, but the timer should keep running
        return;
    }
    //TODO:need to do more to append log
    storage_layer->cleanUpExpireKeyEvent();

}

void Tree::setUpZeroSessionIndex(uint64_t index)
{
    VERBOSE("set up zero session index:%ld", index);
    if(index > this->zeroSessionIndex){
        this->zeroSessionIndex = index;
    }
}

void Tree::startSnapshot(uint64_t lastIncludedIndex) {
    storage_layer->startSnapshot(lastIncludedIndex);
#ifdef ARDB_FSM
    int ret = ardb.Snapshot(worker_ctx);
    VERBOSE("ret = %d", ret);
#endif // ARDB_FSM
#if 0
    ardb::codec::ArgumentArray cmdArray;
    cmdArray.push_back("set");
    cmdArray.push_back("__lastAppliedIndex");
    std::stringstream ss;
    ss << lastIncludedIndex;
    cmdArray.push_back(ss.str());
    ardb::codec::RedisCommandFrame redisCommandFrameSet(cmdArray);
    ardb.Call((ardb::Context&) worker_ctx, redisCommandFrameSet);
    NOTICE("set __lastAppliedIndex: %llu", lastIncludedIndex);

    cmdArray.clear();
    cmdArray.push_back("save");
    cmdArray.push_back("backup");
    ardb::codec::RedisCommandFrame redisCommandFrame(cmdArray);
    ardb.Call((ardb::Context&)worker_ctx, redisCommandFrame);
//    NOTICE(worker_ctx.GetReply().GetString().c_str());
    NOTICE("save backup status: %s", worker_ctx.GetReply().Status().c_str());
#endif
}

} // namespace LogCabin::Tree
} // namespace LogCabin
