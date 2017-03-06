#include "../include/redis_interface.hpp"
#include "../include/debug.hpp"
#include <boost/array.hpp>
#include <iostream>

using boost::asio::ip::tcp;

namespace rediscpp {

RedisInterface::RedisInterface(std::string host, std::string port):
    socket_(io_)
{
    tcp::resolver resolver(io_);
    tcp::resolver::query query(host, port);

    tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
    tcp::resolver::iterator end;

    boost::system::error_code error = boost::asio::error::host_not_found;
    while (error && endpoint_iterator != end)
    {
        socket_.close();
        socket_.connect(*endpoint_iterator++, error);
    }
    if (error)
        throw boost::system::system_error(error);

    // Successfully connected yay
}

protocol::RedisReplyPtr RedisInterface::SendEncodedPacket(std::string packet)
{
    boost::system::error_code error;
    boost::asio::write(socket_, boost::asio::buffer(packet), boost::asio::transfer_all(), error);
    if (error) {
        debug_print("Error while writing to the socket: %s", error.message().c_str());
    }

    // Wait for an answer and read everything
    std::string received_packet;
    auto reply = make_unique<protocol::RedisReply>();
    for (;;)
    {
        boost::array<char, 128> buf;
        boost::system::error_code error;

        // This will block if connection is very slow...
        size_t len = socket_.read_some(boost::asio::buffer(buf), error);

        if (error == boost::asio::error::eof)
            break; // Connection closed cleanly by peer.
        else if (error)
            reply->type = protocol::RedisDataType::ERROR;
            reply->string_value = error.message();
            //throw boost::system::system_error(error); // Some other error.

        received_packet += std::string(buf.data(), len);

        // check each step if the reply is a valid redis reply. If not. read more
        // bytes.
        auto partial_reply = protocol::ParseReply(received_packet);
        if (partial_reply->type != protocol::RedisDataType::ERROR) {
            reply.swap( partial_reply);
            break;
        } else {
            std::cout << "Reply is not complete yet\n";
            std::cout << partial_reply->string_value << std::endl;
        }
    }

    // Now, decode and return the redis reply.
    return reply;
}


protocol::RedisReplyPtr RedisInterface::Get(std::string key)
{
    // First, encode the string to send.
    std::vector<std::string> bulk_values = {"GET", key};
    return SendCommand(bulk_values);
}

protocol::RedisReplyPtr RedisInterface::Set(std::string key, std::string value)
{
    std::vector<std::string> bulk_values = {"SET", key, value};
    return SendCommand(bulk_values);
}

protocol::RedisReplyPtr RedisInterface::Lrange(std::string key, int begin, int end)
{
    std::vector<std::string> bulk_values = {"LRANGE", key, std::to_string(begin), std::to_string(end)};
    return SendCommand(bulk_values);
}

protocol::RedisReplyPtr RedisInterface::Ltrim(std::string key, int begin, int end)
{
    std::vector<std::string> bulk_values = {"LTRIM", key, std::to_string(begin), std::to_string(end)};
    return SendCommand(bulk_values);
}

protocol::RedisReplyPtr RedisInterface::Lpop(std::string key)
{
    std::vector<std::string> bulk_values = {"LPOP", key};
    return SendCommand(bulk_values);
}

protocol::RedisReplyPtr RedisInterface::Rpop(std::string key)
{
    std::vector<std::string> bulk_values = {"RPOP", key};
    return SendCommand(bulk_values);
}

protocol::RedisReplyPtr RedisInterface::SendCommand(std::vector<std::string> tokens)
{
    std::string packet;
    protocol::EncodeBulkStringArray(tokens, packet);
    return SendEncodedPacket(packet);
}

}
