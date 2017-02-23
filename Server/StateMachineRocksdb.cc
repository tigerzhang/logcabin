/* Copyright (c) 2012-2014 Stanford University
 * Copyright (c) 2015 Diego Ongaro
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

#include <cstdio>
#include <string>

#include "rocksdb/db.h"

#include <RPC/ClientRPC.h>
#include <rocksdb/utilities/backupable_db.h>
#include <Client.pb.h>
#include <include/LogCabin/Client.h>
#include <RocksdbSnapshot.pb.h>
#include <sys/stat.h>

#include "Core/Debug.h"
#include "Core/Mutex.h"
#include "Core/ProtoBuf.h"
#include "Core/Random.h"
#include "Core/ThreadId.h"
#include "Core/Util.h"
#include "Server/Globals.h"
#include "Server/StateMachineRocksdb.h"

namespace LogCabin {
namespace Server {

namespace PC = LogCabin::Protocol::Client;


// for testing purposes
extern bool stateMachineSuppressThreads;
extern uint32_t stateMachineChildSleepMs;

StateMachineRocksdb::StateMachineRocksdb(
        std::shared_ptr<RaftConsensus> consensus,
        Core::Config& config,
        Globals& globals,
        std::unique_ptr<rocksdb::DB> rdb)
    : _rdb(std::move(rdb))
    , StateMachineBase(consensus, config, globals, nullptr)
{
}

StateMachineRocksdb::~StateMachineRocksdb()
{
}

////////// StateMachineRocksdb private methods //////////



void StateMachineRocksdb::takeSnapshotWriteData(uint64_t lastIncludedIndex,
                                                Storage::SnapshotFile::Writer *writer) {
    // fake writen bytes count, for supressing the snapshot watch dog
//    std::string data = "StateMachineRocksdb:takeSnapshotWriteData";
//    writer->writeRaw(data.c_str(), data.length());
//
//    rocksdb::BackupEngine* backup_engine =
//            (rocksdb::BackupEngine *)_snapshotCheckpoint;
//
//    backup_engine->CreateNewBackup(_rdb.get());

//    rocksdb::ReadOptions options;
//    options.snapshot = _rdb->GetSnapshot();
//    auto it = _rdb->NewIterator(options);

//    rocksdb::ReadOptions &options = *((rocksdb::ReadOptions *) _snapshotCheckpoint);
    auto it = _rdb->NewIterator(_options);
    VVERBOSE("iterator: %lud", (uint64_t)it);
    const uint64_t step = 200;
    uint64_t count = 0, num = 0;
    //
    // Dump the key/value pairs to the snapshot file.
    //
    RocksdbSnapshot::KeyValues keyValues;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
//        std::string item = it->key().ToString() + ":" + it->value().ToString();
//        VVERBOSE("item: %s", item.c_str());
//        writer->writeRaw(item.c_str(), item.length());
//        LogCabin::Server::RocksdbSnapshot
        RocksdbSnapshot::KeyValue *keyValue = keyValues.add_key_value();
        keyValue->set_key(it->key().ToString());
        keyValue->set_value(it->value().ToString());
        count++;
        num++;

        // Write a batch of key/values to the snapshot file
        if (count >= step) {
            writer->writeMessage(keyValues);
            keyValues.Clear();
            count = 0;
        }
    }
    // Write last batch to file, if there are any
    if (keyValues.key_value().size()) {
        writer->writeMessage(keyValues);
    }

    assert(it->status().ok()); // Check for any errors found during the scan
    delete it;

    NOTICE("%lu keys stored", num);
    // FIXME: Release snapshot in child process???
//    _rdb->ReleaseSnapshot(_options.snapshot);
}

void StateMachineRocksdb::loadSnapshotLoadData(Core::ProtoBuf::InputStream &stream) {
    RocksdbSnapshot::KeyValues keyValues;
    uint64_t num = 0;

    while(true) {
        std::string error = stream.readMessage(keyValues);
        if (!error.empty()) {
            ERROR("Read message failed. %s.", error.c_str());
            break;
        }
        for (auto it = keyValues.key_value().begin();
             it != keyValues.key_value().end();
             it++) {
            int ret = put(it->key(), it->value());
            assert(ret == 0);
            num++;
        }
    }

    NOTICE("%lu keys loaded.", num);
}

int StateMachineRocksdb::put(const std::string &key, const std::string &value) {
    rocksdb::WriteOptions writeOptions = rocksdb::WriteOptions();
    writeOptions.disableWAL = true;
    rocksdb::Status s = _rdb->Put(writeOptions, key, value);
    return s.ok() ? 0 : -1;
}

int StateMachineRocksdb::get(const std::string &key, std::string *value) const {
    rocksdb::Status s = _rdb->Get(rocksdb::ReadOptions(), key, value);
    return s.ok() ? 0 : -1;
}

static void _mkdir(const char *dir) {
    char tmp[256];
    char *p = NULL;
    size_t len;

    snprintf(tmp, sizeof(tmp),"%s",dir);
    len = strlen(tmp);
    if(tmp[len - 1] == '/')
        tmp[len - 1] = 0;
    for(p = tmp + 1; *p; p++)
        if(*p == '/') {
            *p = 0;
            mkdir(tmp, S_IRWXU);
            *p = '/';
        }
    mkdir(tmp, S_IRWXU);
}

std::unique_ptr<rocksdb::DB>
StateMachineRocksdb::openStateMachineDb(Globals& globals) {
    std::unique_ptr<rocksdb::DB> rdb;
//    if (rdb != nullptr) {
//        return;
//    }

    char buf[1024];
    snprintf(buf, 1023, "%s/server%ld/fsm-rocksdb",
             globals.config.read<std::string>("storagePath", "storage").c_str(),
             globals.serverId);
    std::string kDBPath(buf);

    // Make sure directory is existed
    _mkdir(kDBPath.c_str());

    rocksdb::DB* db;
    rocksdb::Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    // options.IncreaseParallelism();
    // options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;

	NOTICE("FSM rocksdb dir: %s", kDBPath.c_str());

    // open DB
    rocksdb::Status s = rocksdb::DB::Open(options, kDBPath, &db);
    rdb.reset(db);
    if (!s.ok()) {
        ERROR("Open rocksdb failed: %s", s.ToString().c_str());
    }
    assert(s.ok());
    return rdb;
}

void *StateMachineRocksdb::createSnapshotPoint() {
    NOTICE("Get a rocksdb snapshot");
    _options.snapshot = _rdb->GetSnapshot();
    if (_options.snapshot == nullptr) {
        return nullptr;
    }

    return (void *)&_options;
//    rocksdb::BackupEngine *backup_engine =
//            (rocksdb::BackupEngine *)malloc(sizeof(rocksdb::BackupEngine));
//    auto &layout = getConsensus()->getStorageLayout();
//    std::string backup_dir = layout.snapshotDir.path + "/rocksdb_backup";
//    auto s = rocksdb::BackupEngine::Open(rocksdb::Env::Default(),
//                                         rocksdb::BackupableDBOptions(backup_dir),
//                                         &backup_engine);
//    if (s.ok()) {
//        return (void *)backup_engine;
//    }
//
//    return nullptr;
}

void StateMachineRocksdb::snapshotDone() {
    NOTICE("Release rocksdb snapshot");
    _rdb->ReleaseSnapshot(_options.snapshot);
    _options.snapshot = nullptr;
}

} // namespace LogCabin::Server
} // namespace LogCabin
