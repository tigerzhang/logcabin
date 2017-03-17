//
// Created by parallels on 17-3-8.
//

#ifndef LOGCABIN_REDISPROXY_H
#define LOGCABIN_REDISPROXY_H

#include <cassert>
#include <getopt.h>
#include <iostream>
#include <iterator>
#include <sstream>

#include <LogCabin/Util.h>

using LogCabin::Client::Util::parseNonNegativeDuration;

namespace RedisProxy {

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
    OptionParser(int &argc, char **&argv)
            : argc(argc), argv(argv), cluster("localhost:5254"), command(), condition(), dir(), logPolicy(""), path(),
              timeout(parseNonNegativeDuration("0s")) {
        while (true) {
            static struct option longOptions[] = {
                    {"cluster",   required_argument, NULL, 'c'},
                    {"dir",       required_argument, NULL, 'd'},
                    {"help",      no_argument,       NULL, 'h'},
                    {"condition", required_argument, NULL, 'p'},
                    {"quiet",     no_argument,       NULL, 'q'},
                    {"timeout",   required_argument, NULL, 't'},
                    {"verbose",   no_argument,       NULL, 'v'},
                    {"verbosity", required_argument, NULL, 256},
                    {"fork",      required_argument, NULL, 'f'},
                    {0, 0, 0,                              0}
            };
            int c = getopt_long(argc, argv, "c:d:p:t:hqvf:", longOptions, NULL);

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
                case 'f':
                    forkNum = parseNonNegativeDuration(optarg);
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

    int &argc;
    char **&argv;
    std::string cluster;
    Command command;
    std::pair<std::string, std::string> condition;
    std::string dir;
    std::string logPolicy;
    std::string path;
    uint64_t timeout;
    uint64_t forkNum;
};

}

extern LogCabin::Client::Tree getTree(RedisProxy::OptionParser &options);

#endif //LOGCABIN_REDISPROXY_H
