#ifndef REDIS_PROTOCOL_HPP
#define REDIS_PROTOCOL_HPP

#include <string>
#include <vector>
#include <memory>
#include "../helpers.hpp"

/*
    Contains functions to encode and decode data coming to and coming from the
    redis server.

    see http://redis.io/topics/protocol
*/
namespace rediscpp {

    namespace protocol {

        /*
            Return value from the encode / decode value
        */
        enum class EncodeDecodeResult {
            OK,
            PARSE_ERROR, // When string do not correspond to integer, ...
            NIL, // when the result does not exist.
        };

        /*
            The redis data type.
        */
        enum class RedisDataType {
            STRING,
            ERROR,
            INTEGER,
            ARRAY,
            NIL_VALUE,
        };

        /*
            Unique ptr to a RedisReply to manage the ownership of the reply
            and the free of memory
        */
        struct RedisReply;
        typedef std::unique_ptr<RedisReply> RedisReplyPtr;

        /*
            The redis server can reply with anu resp data type. It can be
            integer, string, bulk, string or array. Array can be arrays of mixed
            type, or even arrays of arrays..
        */
        struct RedisReply {

            RedisReply() {}
            RedisReply(const RedisReply&) = delete;
            RedisReply& operator=(const RedisReply&) = delete;

            /*
                The type of the reply, which will decide the type of value.
            */
            RedisDataType type = RedisDataType::ERROR;

            /*
                Integer value received by the server
            */
            int integer_value;

            /*
                string, error or bulk string
            */
            std::string string_value = "Empty reply - not received by the server";

            /*
                Add a redis reply to the array.
                Arg; A unique pointer to a redisreply. THE REDISREPLY WILL TAKE
                    OWNERSHIP.
            */
            void AddElementToArray(RedisReplyPtr& reply)
            {
                elements.push_back(std::move(reply));
            }

            /*
                Can be an array of redis data type.
            */
            std::vector<RedisReplyPtr> elements;
        };

        /*
            Will encode an integer;
            This is a CRLF terminated string prefixed by :

            The following commands will reply with an integer reply: SETNX, DEL,
            EXISTS, INCR, INCRBY, DECR, DECRBY, DBSIZE, LASTSAVE, RENAMENX, MOVE,
            LLEN, SADD, SREM, SISMEMBER, SCARD.
        */
        EncodeDecodeResult EncodeInteger(const int integer_to_encode, std::string &result);

        /*
            Will decode an integer.
        */
        EncodeDecodeResult DecodeInteger(const std::string integer_to_decode, int& result);

        /*
            Will encode a string. Simple strings are CRLF terminated strings prefixed
            by +
            Simple Strings are used to transmit non binary safe strings with minimal overhead.
        */
        EncodeDecodeResult EncodeString(const std::string string_to_encode, std::string& result);

        /*
            Will decode a string
        */
        EncodeDecodeResult DecodeString(const std::string string_to_decode, std::string& result);

        /*
            Will encode an error. Simple strings are CRLF terminated strings prefixed
            by -
        */
        EncodeDecodeResult EncodeError(const std::string error_to_encode, std::string& result);

        /*
            Will decode an error
        */
        EncodeDecodeResult DecodeError(const std::string error_to_decode, std::string& result);

        /*
            Will encode a bulk string
            Bulk Strings are encoded in the following way:
            A "$" byte followed by the number of bytes composing the string (a prefixed length), terminated by CRLF.
            The actual string data.
            A final CRLF.
        */
        EncodeDecodeResult EncodeBulkString(const std::string bulk_string_to_encode, std::string& result);

        /*
            Will decode a bulk string
        */
        EncodeDecodeResult DecodeBulkString(const std::string bulk_string_to_decode, std::string& result);

        /*
            Encode a bulk string array - Client Side ONLY sends bulk string arrays.
            Input is a vector of strings. Output if the encoded string
        */
        EncodeDecodeResult EncodeBulkStringArray(const std::vector<std::string> array_to_encode, std::string& result);


        /*
            Decode a reply coming from the redis server. It can be whatever redis
            data type. This function will return a unique ptr to a redisreply object.
        */
        RedisReplyPtr ParseReply(const std::string reply_str);

        /*
            Decode an array. Will populate a RedisReply pointer
            Will return the position where we stopped reading the array_to_decode string.
            This is used for example inside array of arrays
            if returned -1, there is an error and we should cancel the decoding
        */
        int DecodeArray(const std::string array_to_decode, RedisReply* array);

    }
}

#endif
