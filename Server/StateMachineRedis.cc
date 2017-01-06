//
// Created by parallels on 8/28/16.
//

#include <exception>
#include <redis3m/redis3m.hpp>
#include "Server/RaftConsensus.h"
#include "StateMachineBase.h"
#include "StateMachineRedis.h"

#include "Core/Debug.h"
#include "utils.h"

#include <boost/algorithm/string.hpp>

namespace LogCabin {
namespace Server {

//int StateMachineRedis::initKVStore() {
//    return 0;
//}

int StateMachineRedis::put(const std::string &key, const std::string &value) {
    try {
        redis3m::connection *connection = ((redis3m::connection *)kvstore);
        if (value.compare(0, 3, "sub") == 0) {
            VVERBOSE("Sub command: %s", value.c_str());
            std::string uid = "0";
//            auto command = split(value, ' ');
            std::vector<std::string> command;
            boost::split(command, value, boost::is_any_of(" "));

            float_t score = 0;
            if (command.size() == 2) {
                uid = command[1];
            } else if (command.size() == 3) {
                uid = command[1];
                score = std::stof(command[2]);
            } else {
                ERROR("bad sub command: %s", value.c_str());
                return 0;
            }

            redis3m::reply reply = connection->run(redis3m::command("ZADD") << key << score << uid);
            if (reply.type() == redis3m::reply::type_t ::ERROR) {
                VVERBOSE("Sub failed: %s", reply.str().c_str());
                return -1;
            }
        } else if (value.compare(0, 5, "unsub") == 0) {
            VVERBOSE("Unsub command: %s", value.c_str());
            std::string uid = value.substr(6);
            redis3m::reply reply = connection->run(redis3m::command("ZREM") << key << uid);

            if (reply.type() == redis3m::reply::type_t::ERROR) {
                VVERBOSE("Unsub failed: %s", reply.str().c_str());
                return -1;
            }
        }
    } catch (std::invalid_argument& e) {
        ERROR("StateMachineRedis::put invalid_argument: %s. key[%s] value[%s]. Skipped",
              e.what(), key.c_str(), value.c_str());

        return 0;
    } catch(std::exception& e) {
        ERROR("StateMachineRedis::put exception: %s. key[%s] value[%s]",
              e.what(), key.c_str(), value.c_str());

        return -1;
    }

//    redis3m::reply reply = connection->run(redis3m::command("SET") << key << value);
//    if (reply.type() == redis3m::reply::type_t::ERROR) {
//        return -1;
//    }
    return 0;
}

int StateMachineRedis::get(const std::string &key, std::string *value) const {
    redis3m::connection *connection = ((redis3m::connection *)kvstore);
    redis3m::reply reply = connection->run(redis3m::command("ZRANGE") << key << 0 << -1 << "WITHSCORES");
    if (reply.type() == redis3m::reply::type_t::ERROR) {
        VVERBOSE("ZRANGE failed: %s", reply.str().c_str());
        return -1;
    }

    VVERBOSE("reply type: %d", reply.type());
    std::string reply_str;
    if (reply.type() == redis3m::reply::type_t::ARRAY) {
        for (auto& element : reply.elements()) {
            if (element.type() == redis3m::reply::type_t::STRING) {
                reply_str += element.str() + "\n";
                VVERBOSE("str %s", element.str().c_str());
            } else if (element.type() == redis3m::reply::type_t::INTEGER) {
                VVERBOSE("int %lld", element.integer());
            }
        }
    } else if (reply.type() == redis3m::reply::type_t::STRING) {
        reply_str = reply.str();
    }

    *value = reply_str;
    return 0;
}

StateMachineRedis::StateMachineRedis(std::shared_ptr<RaftConsensus> consensus, Core::Config &config,
                                     Globals &globals, void *kvstore)
        : StateMachineBase(consensus, config, globals, kvstore) {
}

}
}