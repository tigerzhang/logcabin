#include <iostream>
#include "redis_protocol/resp_protocol.hpp"
#include "redis_interface.hpp"

using namespace rediscpp;
using namespace rediscpp::protocol;

bool ParseReply(rediscpp::protocol::RedisReplyPtr& reply)
{
    switch (reply->type) {
    case RedisDataType::STRING:
        if (reply->type == RedisDataType::STRING) {
            if (reply->string_value == "quit") {
                return false;
            } else {
                // You can serialize to json for example.
                std::cout << "Will do: " << reply->string_value << std::endl;
            }
        }
        break;
    case RedisDataType::ARRAY:
        if (reply->elements.size() == 2) {
            return ParseReply(reply->elements[1]);
        }
        break;
    }
    return true;
}

int main()
{

    try {
        RedisInterface redis("localhost", "6379");

        while (true) {
            // Blocking left pop without timeout.
            auto reply = redis.Blpop("mylist");
            std::cout << "Got a reply\n";
            if (!ParseReply(reply)) {
                break;
            }
        }
    } catch (std::exception &e) {
        std::cout << e.what() << std::endl;
    }

    return 0;
}
