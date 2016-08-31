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


    virtual int put(const std::string &key, const std::string &value);

    virtual int get(const std::string &key, std::string *value) const;

//    int initKVStore();

};

}
}
#endif //LOGCABIN_STATEMACHINEREDIS_H
