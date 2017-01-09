//
// Created by parallels on 8/28/16.
//

#include <exception>
#include <redis3m/redis3m.hpp>
#include "Server/RaftConsensus.h"
#include "StateMachineBase.h"
#include "StateMachineRedis.h"
#include "../redis_cpp/include/redis_protocol/resp_protocol.hpp"

#include "Core/Debug.h"
#include "utils.h"

//#include <boost/algorithm/string/trim.hpp>
//#include <boost/algorithm/string/split.hpp>
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
                score = std::stof(command[1]);
                uid = command[2];
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
        } else {
            std::string realKey = key;
            if (key.substr(0, 1) == "/") {
                realKey = key.substr(1, key.size()-1);
            }
            redis3m::reply reply = connection->run(redis3m::command("SET") << realKey << value);
//            lastApplyResult = reply.str();
            VVERBOSE("redis3m reply: %d %s", reply.type(), reply.str().c_str());
            if (reply.type() == redis3m::reply::type_t::STATUS) {
                rediscpp::protocol::EncodeString(reply.str(), lastApplyResult);
            } else if (reply.type() == redis3m::reply::type_t::ERROR) {
                rediscpp::protocol::EncodeError(reply.str(), lastApplyResult);
//                return -1;
            } else if (reply.type() == redis3m::reply::type_t::INTEGER) {
                rediscpp::protocol::EncodeInteger(reply.integer(), lastApplyResult);
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

    return 0;
}

int StateMachineRedis::get(const std::string &key, std::string *value) const {
    redis3m::command cmd;
    if (key.substr(0, 1) == "/") {
//        boost::algorithm::trim_right_if(key, boost::is_any_of("/"));

        std::string cmdstr = key.substr(1, key.length()-1);

        std::vector< std::string > SplitVec;
        boost::split( SplitVec, cmdstr, boost::is_any_of(" "));

        if (SplitVec.size() > 0) {
            cmd = redis3m::command(SplitVec[0]);
            for (auto i = 1; i < SplitVec.size(); i++) {
                cmd = cmd << SplitVec[i];
            }
        } else {
            return 0;
        }
    } else {
        return 0;
    }
    redis3m::connection *connection = ((redis3m::connection *)kvstore);
//    redis3m::reply reply = connection->run(redis3m::command("ZRANGE") << key << 0 << -1 << "WITHSCORES");
    redis3m::reply reply = connection->run(cmd);
    if (reply.type() == redis3m::reply::type_t::ERROR) {
        VVERBOSE("ZRANGE failed: %s", reply.str().c_str());
        return -1;
    }

    VVERBOSE("reply type: %d", reply.type());
    std::string reply_str;
    if (reply.type() == redis3m::reply::type_t::ARRAY) {
        std::vector<std::string> reply_vec;
        for (auto& element : reply.elements()) {
            if (element.type() == redis3m::reply::type_t::STRING) {
//                reply_str += element.str() + "\n";
                reply_vec.push_back(element.str());
                VVERBOSE("str %s", element.str().c_str());
            } else if (element.type() == redis3m::reply::type_t::INTEGER) {
                char buf[64];
                snprintf(buf, 64, "%lld", element.integer());
                reply_vec.push_back(buf);
                VVERBOSE("int %lld", element.integer());
            }
        }
        rediscpp::protocol::EncodeBulkStringArray(reply_vec, reply_str);
    } else if (reply.type() == redis3m::reply::type_t::STRING) {
        VVERBOSE("reply string: %s", reply.str().c_str());
//        reply_str = reply.str();
        rediscpp::protocol::EncodeBulkString(reply.str(), reply_str);
    } else if (reply.type() == redis3m::reply::type_t::STATUS) {
//        reply_str = "+" + reply.str();
        rediscpp::protocol::EncodeString(reply.str(), reply_str);
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