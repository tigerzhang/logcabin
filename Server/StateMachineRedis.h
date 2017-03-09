//
// Created by parallels on 8/28/16.
//

#ifndef LOGCABIN_STATEMACHINEREDIS_H
#define LOGCABIN_STATEMACHINEREDIS_H

#include "StateMachineBase.h"

namespace LogCabin {
namespace Server {

class StateMachineBase;

// forward declaration
class Globals;
class RaftConsensus;

class StateMachineRedis : public StateMachineBase {
public:
    StateMachineRedis(std::shared_ptr<RaftConsensus> consensus,
    Core::Config& config,
            Globals& globals, void *kvstore);
    virtual ~StateMachineRedis() {}


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
    redisReply *getReply(const std::string &key) const;
    void do_redis_bgsave(uint64_t lastIncludedIndex, Storage::SnapshotFile::Writer *writer);
    void do_ardb_bgsave(uint64_t lastIncludedIndex, Storage::SnapshotFile::Writer *writer);
    void do_redis_load_snapshot(Core::ProtoBuf::InputStream &stream);
    void do_ardb_load_snapshot(Core::ProtoBuf::InputStream &stream);
};

}
}
#endif //LOGCABIN_STATEMACHINEREDIS_H
