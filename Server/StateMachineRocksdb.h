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
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "Client.pb.h"
#include "SnapshotStateMachine.pb.h"
#include "Core/ConditionVariable.h"
#include "Core/Config.h"
#include "Core/Mutex.h"
#include "Core/Time.h"
#include "Tree/Tree.h"

#ifndef LOGCABIN_SERVER_STATEMACHINEROCKSDB_H
#define LOGCABIN_SERVER_STATEMACHINEROCKSDB_H

namespace LogCabin {
namespace Server {

// forward declaration
class Globals;
class RaftConsensus;

/**
 * Interprets and executes operations that have been committed into the Raft
 * log.
 *
 * Version history:
 * - Version 1 of the State Machine shipped with LogCabin v1.0.0.
 * - Version 2 added the CloseSession command, which clients can use when they
 *   gracefully shut down.
 */
class StateMachineRocksdb : public StateMachineBase {
  public:


    StateMachineRocksdb(std::shared_ptr<RaftConsensus> consensus,
                 Core::Config& config,
                 Globals& globals,
                        std::unique_ptr<rocksdb::DB> rdb);
    ~StateMachineRocksdb();

  private:
    virtual void takeSnapshotWriteData(uint64_t lastIncludedIndex,
                                       Core::ProtoBuf::OutputStream& writer);

    virtual void loadSnapshotLoadData(Core::ProtoBuf::InputStream &stream);

    std::unique_ptr<rocksdb::DB> _rdb;

public:
    static std::unique_ptr<rocksdb::DB> openStateMachineDb(Globals& globals);

    rocksdb::Status putRdb(const std::string& key, const std::string& value);
    rocksdb::Status getRdb(const std::string& key, std::string* value) const;

    virtual int put(const std::string &key, const std::string &value);

    virtual int get(const std::string &key, std::string *value) const;
};

} // namespace LogCabin::Server
} // namespace LogCabin

#endif // LOGCABIN_SERVER_STATEMACHINEROCKSDB_H
