//
// Created by parallels on 8/28/16.
//

#ifndef LOGCABIN_STATEMACHINEARDB_H
#define LOGCABIN_STATEMACHINEARDB_H

#include <hiredis/hiredis.h>
#include "StateMachineBase.h"

namespace LogCabin {
namespace Server {

class StateMachineBase;

// forward declaration
class Globals;
class RaftConsensus;

class StateMachineArdb : public StateMachineBase {
public:
    StateMachineArdb(std::shared_ptr<RaftConsensus> consensus,
    Core::Config& config,
            Globals& globals, void *kvstore);
    virtual ~StateMachineArdb() {}


    virtual void takeSnapshotWriteData(uint64_t lastIncludedIndex,
                                       Storage::SnapshotFile::Writer *writer);

    int put(const std::string &key, const std::string &value);

    int get(const std::string &key, std::string *value) const;

//    int initKVStore();

    virtual void* createSnapshotPoint();
    virtual void snapshotDone();

protected:
    virtual void loadSnapshotLoadData(Core::ProtoBuf::InputStream &stream);

private:
    redisContext *snapshotContext;
    redisReply *getReply(const std::string &key) const;

    redisContext *getContext() const;

    void do_ardb_bgsave(uint64_t lastIncludedIndex, Storage::SnapshotFile::Writer *writer);
    void do_ardb_load_snapshot(Core::ProtoBuf::InputStream &stream);
};

}
}
#endif //LOGCABIN_STATEMACHINEARDB_H
