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
                                   Core::ProtoBuf::OutputStream& writer) {

}

void StateMachineRocksdb::loadSnapshotLoadData(Core::ProtoBuf::InputStream &stream) {

}

int StateMachineRocksdb::put(const std::string &key, const std::string &value) {
    rocksdb::Status status = putRdb(key, value);
    return status.ok() ? 0 : -1;
}

int StateMachineRocksdb::get(const std::string &key, std::string *value) const {
    rocksdb::Status status = getRdb(key, value);
    return status.ok() ? 0 : -1;
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
    assert(s.ok());
    return rdb;
}

rocksdb::Status
StateMachineRocksdb::putRdb(
        const std::string& key, const std::string& value) {
    rocksdb::Status s = _rdb->Put(rocksdb::WriteOptions(), key, value);
    assert(s.ok());
    return s;
}

rocksdb::Status
StateMachineRocksdb::getRdb(
        const std::string &key, std::string *value) const {
    rocksdb::Status s = _rdb->Get(rocksdb::ReadOptions(), key, value);
//	assert(s.ok());
    return s;
}

} // namespace LogCabin::Server
} // namespace LogCabin
