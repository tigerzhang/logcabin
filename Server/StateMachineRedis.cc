//
// Created by parallels on 8/28/16.
//

#include <redis3m/redis3m.hpp>
#include "Server/RaftConsensus.h"
#include "StateMachineBase.h"
#include "StateMachineRedis.h"

namespace LogCabin {
namespace Server {

//int StateMachineRedis::initKVStore() {
//    return 0;
//}

int StateMachineRedis::put(const std::string &key, const std::string &value) {
    redis3m::connection *connection = ((redis3m::connection *)kvstore);
    redis3m::reply reply = connection->run(redis3m::command("SET") << key << value);
    if (reply.type() == redis3m::reply::type_t::ERROR) {
        return -1;
    }
    return 0;
}

int StateMachineRedis::get(const std::string &key, std::string *value) const {
    redis3m::connection *connection = ((redis3m::connection *)kvstore);
    redis3m::reply reply = connection->run(redis3m::command("GET") << key);
    if (reply.type() == redis3m::reply::type_t::ERROR) {
        return -1;
    }
    *value = reply.str();
    return 0;
}

StateMachineRedis::StateMachineRedis(std::shared_ptr<RaftConsensus> consensus, Core::Config &config,
                                     Globals &globals, void *kvstore)
        : StateMachineBase(consensus, config, globals, kvstore) {
}

}
}