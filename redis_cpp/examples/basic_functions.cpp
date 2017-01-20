#include <iostream>
#include "redis_protocol/resp_protocol.hpp"
#include "redis_interface.hpp"

using namespace rediscpp;
using namespace rediscpp::protocol;

int main()
{

    try {
        RedisInterface redis("localhost", "6379");

        // Set value for a key
        {
            // reply is a RedisReplyPtr.
            auto reply = redis.Set("mykey", "value");
            if (reply->type == RedisDataType::STRING) {
                std::cout << reply->string_value << std::endl;
            }
        }

        // Push value to the left
        {
            auto reply = redis.Lpush("mylist", "value1", "value2");
            if (reply->type == RedisDataType::STRING) {
                std::cout << reply->string_value << std::endl;
            }
        }

        // Get a value
        {
            auto reply = redis.Get("mykey");
            if (reply->type == RedisDataType::STRING) {
                std::cout << "The value is : " << reply->string_value << std::endl;
            }
        }
        
    } catch (std::exception &e) {
        std::cout << e.what() << std::endl;
    }

    return 0;
}
