#ifndef REDISINTERFACE_HPP
#define REDISINTERFACE_HPP

#include <boost/asio.hpp>
#include <vector>
#include <boost/optional.hpp>
#include "redis_protocol/resp_protocol.hpp"
#include "helpers.hpp"
#include "debug.hpp"
/*
    Synchronous connection to a redis server.
    Use boost asio tcp sockets.
*/
namespace rediscpp {
class RedisInterface
{
public:
    /*
        Will create a instance and try to connect to the redis server using
        TCP.

        This will throw an exception if we cannot connect to the server.
        There is not use of this object if it's not connected..
    */
    RedisInterface(std::string host, std::string port);

    /*
        Cannot copy.
    */
    RedisInterface(const RedisInterface&) = delete;
    RedisInterface& operator=(const RedisInterface&) = delete;


    /*
        REDIS API. Most used commands are implement. A general way to write commands
        is also given.
    */

    /*
        Get value for the given key.
        Returns a RedisReplyPtr
    */
    protocol::RedisReplyPtr Get(std::string key);

    /*
        Set value for the given key.
        Should return OK
    */
    protocol::RedisReplyPtr Set(std::string key, std::string value);

    /*
        Push a value to the left of a list
        you can push many values by separating them with a comma

        ex;
        redis.lpush("mylist", "myvalue1", "myvalue2");
    */
    template <typename ... Args>
    protocol::RedisReplyPtr Lpush(std::string key, Args ... values)
    {
        return AccumulateAndSend("LPUSH", key, values ...);
    }

    /*
        Get values from a list.
        By default, get everything
    */
    protocol::RedisReplyPtr Lrange(std::string key, int begin = 0, int end = -1);

    /*
        Trim an existing list so that it will contain only the specified range
        of elements specified
    */
    protocol::RedisReplyPtr Ltrim(std::string key, int begin, int end);

    /*
        Push a value to the right of a list
        you can push many values by separating them with a comma

        ex;
        redis.lpush("mylist", "myvalue1", "myvalue2");
    */
    template <typename ... Args>
    protocol::RedisReplyPtr Rpush(std::string key, Args ... values)
    {
        return AccumulateAndSend("RPUSH", key, values ...);
    }


    /*
        Pop a value from the left of a list. If the list is empty, will return
        immediately with reply type equal to NIL
    */
    protocol::RedisReplyPtr Lpop(std::string key);

    /*
        Pop a value from the right of a list. If the list is empty, will return
        immediately with reply type equal to NIL
    */
    protocol::RedisReplyPtr Rpop(std::string key);

    /*
        Block left pop. Not timeout for this one. If you want a timeout, use
        BlpopTimeout.
        You can give many list name. The first non-empty in order will be poped.
    */
    template <typename ... Args>
    protocol::RedisReplyPtr Blpop(Args ... keys)
    {
        return AccumulateAndSend("BLPOP", keys ..., "0");
    }

    /*
        Block right pop. Not timeout for this one. If you want a timeout, use
        BrpopTimeout.
        You can give many list name. The first non-empty in order will be poped.
    */
    template <typename ... Args>
    protocol::RedisReplyPtr Brpop(Args ... keys)
    {
        return AccumulateAndSend("BRPOP", keys ..., "0");
    }

    /*
        Blocking left pop with timeout
    */
    template <typename ... Args>
    protocol::RedisReplyPtr BlpopTimeout(unsigned int timeout, Args ... keys)
    {
        return AccumulateAndSend("BLPOP", keys ..., std::to_string(timeout));
    }

    /*
        Blocking right pop with timeout
    */
    template <typename ... Args>
    protocol::RedisReplyPtr BrpopTimeout(unsigned int timeout, Args ... keys)
    {
        return AccumulateAndSend("BRPOP", keys ..., std::to_string(timeout));
    }



    /*
        Send a command to the redis instance. Argument is a vector with the command
        tokens. For example. "KEYS *" would be a vector of two elements, "KEYS"
        and "*"
    */
    protocol::RedisReplyPtr SendCommand(std::vector<std::string> tokens);

private:

    /*
        Send a packet and wait for an answer. The packet sent is already encoded
        so we do not make this function available to the user.
    */
    protocol::RedisReplyPtr SendEncodedPacket(std::string packet);

    /*
        This io service object for the boost asio objects
    */
    boost::asio::io_service io_;

    /*
        The connection TCP socket.
    */
    boost::asio::ip::tcp::socket socket_;

    /*
        ACcumulate values in vector and send result to the server.
        Used for example in lpush, rpush...
    */
    template <typename ... Args>
    protocol::RedisReplyPtr AccumulateAndSend(Args ... values)
    {
        std::vector<std::string> command;
        push_all(command, values...);

        //  Now we can encode and send.
        std::string packet;
        protocol::EncodeBulkStringArray(command, packet);

        return SendEncodedPacket(packet);
    }

};
}

#endif
