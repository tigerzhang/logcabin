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

#include <cassert>
#include <getopt.h>
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

namespace {

class RedisServer :public RedisServerBase
{
public:
//    RedisServer(LogCabin::Server::Globals& globals) : globals(globals) {
//
//    }
    RedisServer(LogCabin::Client::Tree& tree) : tree(tree) {}
    ~RedisServer() {

    }

public:
    bool Init()
    {
        CmdRegister();
    }

private:
//    LogCabin::Server::Globals& globals;
    LogCabin::Client::Tree& tree;

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
        LogCabin::Client::Result result = tree.kvread(cmd, value);
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
        LogCabin::Client::Result result = tree.kvwriteEx(cmd, value);
//        std::cout << "ProcessWriteCommand result.error: " << result.error << std::endl;
        SendRawReply(pConnector, result.error);
    }

private:

};

using LogCabin::Client::Cluster;
using LogCabin::Client::Tree;
using LogCabin::Client::Util::parseNonNegativeDuration;

enum class Command {
    MKDIR,
    LIST,
    DUMP,
    RMDIR,
    WRITE,
    READ,
    REMOVE,
    SUBSCRIBE,
    UNSUBSCRIBE,
    KVREAD,
    KVWRITE
};

/**
 * Parses argv for the main function.
 */
class OptionParser {
  public:
    OptionParser(int& argc, char**& argv)
        : argc(argc)
        , argv(argv)
        , cluster("localhost:5254")
        , command()
        , condition()
        , dir()
        , logPolicy("")
        , path()
        , timeout(parseNonNegativeDuration("0s"))
    {
        while (true) {
            static struct option longOptions[] = {
               {"cluster",  required_argument, NULL, 'c'},
               {"dir",  required_argument, NULL, 'd'},
               {"help",  no_argument, NULL, 'h'},
               {"condition",  required_argument, NULL, 'p'},
               {"quiet",  no_argument, NULL, 'q'},
               {"timeout",  required_argument, NULL, 't'},
               {"verbose",  no_argument, NULL, 'v'},
               {"verbosity",  required_argument, NULL, 256},
               {0, 0, 0, 0}
            };
            int c = getopt_long(argc, argv, "c:d:p:t:hqv", longOptions, NULL);

            // Detect the end of the options.
            if (c == -1)
                break;

            switch (c) {
                case 'c':
                    cluster = optarg;
                    break;
                case 'd':
                    dir = optarg;
                    break;
                case 'h':
                    usage();
                    exit(0);
                case 'p': {
                    std::istringstream stream(optarg);
                    std::string path;
                    std::string value;
                    std::getline(stream, path, ':');
                    std::getline(stream, value);
                    condition = {path, value};
                    break;
                }
                case 'q':
                    logPolicy = "WARNING";
                    break;
                case 't':
                    timeout = parseNonNegativeDuration(optarg);
                    break;
                case 'v':
                    logPolicy = "VERBOSE";
                    break;
                case 256:
                    logPolicy = optarg;
                    break;
                case '?':
                default:
                    // getopt_long already printed an error message.
                    break;
            }
        }
    }

    void usage() {
        std::cout << "Run various operations on a LogCabin replicated state "
                  << "machine."
                  << std::endl
                  << std::endl
                  << "This program was released in LogCabin v1.0.0."
                  << std::endl;
        std::cout << std::endl;

        std::cout << "Usage: " << argv[0] << " [options] <command> [<args>]"
                  << std::endl;
        std::cout << std::endl;

        std::cout << "Commands:" << std::endl;
        std::cout
            << "  mkdir <path>    If no directory exists at <path>, create it."
            << std::endl
            << "  list <path>     List keys within directory at <path>. "
            << "Alias: ls."
            << std::endl
            << "  dump [<path>]   Recursively print keys and values within "
            << "directory at <path>."
            << std::endl
            << "                  Defaults to printing all keys and values "
            << "from root of tree."
            << std::endl
            << "  rmdir <path>    Recursively remove directory at <path>, if "
            << "any."
            << std::endl
            << "                  Alias: removedir."
            << std::endl
            << "  write <path>    Set/create value of file at <path> to "
            << "stdin."
            << std::endl
            << "                  Alias: create, set."
            << std::endl
            << "  read <path>     Print value of file at <path>. Alias: get."
            << std::endl
            << "  remove <path>   Remove file at <path>, if any. Alias: rm, "
            << "removefile."
            << std::endl
            << "  kvread <key>"
            << "  kvwrite <key>     Read value from stdin"
            << std::endl;

        std::cout << "Options:" << std::endl;
        std::cout
            << "  -c <addresses>, --cluster=<addresses>  "
            << "Network addresses of the LogCabin"
            << std::endl
            << "                                         "
            << "servers, comma-separated"
            << std::endl
            << "                                         "
            << "[default: localhost:5254]"
            << std::endl

            << "  -d <path>, --dir=<path>        "
            << "Set working directory [default: /]"
            << std::endl

            << "  -h, --help                     "
            << "Print this usage information"
            << std::endl

            << "  -p <pred>, --condition=<pred>  "
            << "Set predicate on the operation of the"
            << std::endl
            << "                                 "
            << "form <path>:<value>, indicating that the key"
            << std::endl
            << "                                 "
            << "at <path> must have the given value."
            << std::endl

            << "  -q, --quiet                    "
            << "Same as --verbosity=WARNING"
            << std::endl

            << "  -t <time>, --timeout=<time>    "
            << "Set timeout for the operation"
            << std::endl
            << "                                 "
            << "(0 means wait forever) [default: 0s]"
            << std::endl

            << "  -v, --verbose                  "
            << "Same as --verbosity=VERBOSE (added in v1.1.0)"
            << std::endl

            << "  --verbosity=<policy>           "
            << "Set which log messages are shown."
            << std::endl
            << "                                 "
            << "Comma-separated LEVEL or PATTERN@LEVEL rules."
            << std::endl
            << "                                 "
            << "Levels: SILENT ERROR WARNING NOTICE VERBOSE."
            << std::endl
            << "                                 "
            << "Patterns match filename prefixes or suffixes."
            << std::endl
            << "                                 "
            << "Example: Client@NOTICE,Test.cc@SILENT,VERBOSE."
            << std::endl
            << "                                 "
            << "(added in v1.1.0)"
            << std::endl;
    }

    int& argc;
    char**& argv;
    std::string cluster;
    Command command;
    std::pair<std::string, std::string> condition;
    std::string dir;
    std::string logPolicy;
    std::string path;
    uint64_t timeout;
};

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

} // anonymous namespace

int
main(int argc, char** argv)
{
    try {
        OptionParser options(argc, argv);

        LogCabin::Client::Debug::setLogPolicy(
            LogCabin::Client::Debug::logPolicyFromString(
                options.logPolicy));

        Cluster cluster(options.cluster);
        Tree tree = cluster.getTree();

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

        std::string& path = options.path;

        RedisServer redis(tree);
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
