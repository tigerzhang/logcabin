/* Copyright (c) 2017 Zhang Hu
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <iostream>
#include <iterator>
#include <sstream>

#include <LogCabin/Client.h>
#include <LogCabin/Debug.h>
#include <LogCabin/Util.h>
#include <zconf.h>
#include <Client/ClientImpl.h>
#include <include/LogCabin/Client.h>

#include "../RedisServer/RedisServer.h"

#include "redisCommands.cc"
#include "RedisProxy.h"

namespace RedisProxy {

class RedisServer :public RedisServerBase
{
public:
//    RedisServer(LogCabin::Server::Globals& globals) : globals(globals) {
//
//    }
    RedisServer(OptionParser& options) : RedisServerBase(options) {}
    ~RedisServer() {

    }

public:
    bool Init()
    {
        CmdRegister();
    }

private:
//    LogCabin::Server::Globals& globals;
//    Tree tree;

    bool CmdRegister()
    {
        for(int i = 0;
            i < sizeof(redisCommandTable)/sizeof(redisCommandTable[0]);
            i++) {
            if (redisCommandTable[i].name == 0) {
                break;
            }

            if (strchr(redisCommandTable[i].flags, 'r') != NULL) {
                if (!SetCmdTable(redisCommandTable[i].name,
                                 (CmdCallback)&RedisServer::ProcessReadCommand))
                    return false;
            } else {
                if (!SetCmdTable(redisCommandTable[i].name,
                                 (CmdCallback)&RedisServer::ProcessWriteCommand))
                    return false;
            }
        }

        return true;

//        std::cout << "CmdRegister" << std::endl;
        const char *readCommands[] = {
                /* List Read Commands */
                "LINDEX", "LLEN", "LRANGE",

                /* Strings Read Commands */
                "BITCOUNT", "GET"
        };
        const char *writeCommands[] = {
                /* List Write Commands */
                "BLPOP", "BRPOP", "BRPOPLPUSH",
                "LINSERT", "LPOP", "LPUSH", "LPUSHX", "LREM", "LSET", "LTRIM",
                "RPOP", "RPOPLPUSH", "RPUSH", "RPUSHX",

                /* Strings Write Commands */
                "APPEND", "BITFIELD", "SET"
        };

        for (int i = 0; i < sizeof(readCommands)/sizeof(readCommands[0]); i++)
            if (!SetCmdTable(readCommands[i],
                             (CmdCallback)&RedisServer::ProcessReadCommand))
                return false;
        for (int i = 0; i < sizeof(writeCommands)/sizeof(writeCommands[0]); i++)
            if (!SetCmdTable(writeCommands[i],
                             (CmdCallback)&RedisServer::ProcessWriteCommand))
                return false;
        return true;
    }

    int encodeRedisRequest(int argc, sds *argv, std::string &cmd) {
        char buf[128];
        snprintf(buf, 128, "*%d\r\n", argc);
        cmd = buf;
        for (auto i = 0; i < argc; i++) {
            snprintf(buf, 128, "$%lu\r\n", sdslen(argv[i]));
            cmd += buf;
            std::string arg(argv[i], sdslen(argv[i]));
            cmd += arg + "\r\n";
        }
    }

    void ProcessReadCommand(RedisConnect *pConnector)
    {
        /*
        std::string cmd = "";
        for (auto i = 0; i < pConnector->argc; i++) {
            if (cmd != "") {
                cmd += " ";
            }
            cmd += pConnector->argv[i];
        }
         */
        std::string cmd;
        encodeRedisRequest(pConnector->argc, pConnector->argv, cmd);

        std::string value;
        LogCabin::Client::Result result = pConnector->tree->kvread(cmd, value);
        if (result.status == LogCabin::Client::Status::OK) {
            SendRawReply(pConnector, value);
        } else {
            SendRawReply(pConnector, result.error);
        }
    }

    void ProcessWriteCommand(RedisConnect *pConnector)
    {
        /*
        std::string cmd = "";
        for (auto i = 0; i < pConnector->argc; i++) {
            if (cmd != "") {
                cmd += " ";
            }
            cmd += pConnector->argv[i];
        }
         */

        std::string cmd;
        encodeRedisRequest(pConnector->argc, pConnector->argv, cmd);

        std::string value;
        LogCabin::Client::Result result = pConnector->tree->kvwriteEx(cmd, value);
//        std::cout << "ProcessWriteCommand result.error: " << result.error << std::endl;
        SendRawReply(pConnector, result.error);
    }

private:

};

using LogCabin::Client::Cluster;
using LogCabin::Client::Tree;

/**
 * Depth-first search tree traversal, dumping out contents of all files
 */
void
dumpTree(const Tree& tree, std::string path)
{
    std::cout << path << std::endl;
    std::vector<std::string> children = tree.listDirectoryEx(path);
    for (auto it = children.begin(); it != children.end(); ++it) {
        std::string child = path + *it;
        if (*child.rbegin() == '/') { // directory
            dumpTree(tree, child);
        } else { // file
            std::cout << child << ": " << std::endl;
            std::cout << "    " << tree.readEx(child) << std::endl;
        }
    }
}

std::string
readStdin()
{
    std::cin >> std::noskipws;
    std::istream_iterator<char> it(std::cin);
    std::istream_iterator<char> end;
    std::string results(it, end);
    return results;
}

} // RedisProxy namespace

int
main(int argc, char** argv)
{
    try {
        RedisProxy::OptionParser options(argc, argv);

        LogCabin::Client::Debug::setLogPolicy(
            LogCabin::Client::Debug::logPolicyFromString(
                options.logPolicy));

//        Tree tree = getTree(options);

        std::string& path = options.path;

        RedisProxy::RedisServer redis(options);
        redis.Init();
//        std::string pass = "shahe22f";
//        redis.SetPassword(pass);
        int port = 6479;
        std::cout << "Listen port " << port << std::endl;
        redis.Start("127.0.0.1", port);

        exit(0);

    } catch (const LogCabin::Client::Exception& e) {
        std::cerr << "Exiting due to LogCabin::Client::Exception: "
                  << e.what()
                  << std::endl;
        exit(1);
    }


    return 0;
}

LogCabin::Client::Tree getTree(RedisProxy::OptionParser &options) {
    LogCabin::Client::Cluster cluster(options.cluster);
    LogCabin::Client::Tree tree = cluster.getTree();

    if (options.timeout > 0) {
            tree.setTimeout(options.timeout);
        }

    if (!options.dir.empty()) {
            tree.setWorkingDirectoryEx(options.dir);
        }

    if (!options.condition.first.empty()) {
            tree.setConditionEx(options.condition.first,
                                options.condition.second);
        }
    return tree;
}
