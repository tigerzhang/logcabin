/*
* ----------------------------------------------------------------------------
* Copyright (c) 2015-2016, xSky <guozhw@gmail.com>
* All rights reserved.
* Distributed under GPL license.
* ----------------------------------------------------------------------------
*/

#include <unistd.h>

#include "../Server/Globals.h"

#include "RedisServerLib.h"

class RedisConnect :public RedisConnectorBase
{
public:
    RedisConnect();
    ~RedisConnect();
private:
};

std::map<std::string, std::string> kvmap;

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
        if (!SetCmdTable("get", (CmdCallback)&RedisServer::ProcessCmdGet)) return false;
        if (!SetCmdTable("set", (CmdCallback)&RedisServer::ProcessCmdSet)) return false;
        if (!SetCmdTable("zadd", (CmdCallback)&RedisServer::ProcessCmdZAdd)) return false;
        if (!SetCmdTable("zrem", (CmdCallback)&RedisServer::ProcessCmdZRem)) return false;
        return true;
    }

    void ProcessCmdGet(RedisConnect *pConnector)
    {
        if (2 != pConnector->argc) {
            SendErrReply(pConnector, "ERR", "error arg");
            return;
        }
//        auto k = kvmap.find(pConnector->argv[1]);
//        if (k == kvmap.end()) {
//            SendErrReply(pConnector, "ERR", "not found");
//            return;
//        }
//        SendBulkReply(pConnector, k->second.c_str());
        std::string cmd("get ");
        cmd += std::string(pConnector->argv[1]);
        auto value = tree.readEx(cmd);
        if (value == "") {
            SendNullReply(pConnector);
        } else {
//            SendBulkReply(pConnector, value);
            SendRawReply(pConnector, value);
        }
        return;
    }

    void ProcessCmdSet(RedisConnect *pConnector) {
        if (3 != pConnector->argc) {
            SendErrReply(pConnector, "cmd error:", "error arg");
            return;
        }
        kvmap[pConnector->argv[1]] = pConnector->argv[2];
        std::string key(pConnector->argv[1]);
        std::string value(pConnector->argv[2]);
        LogCabin::Client::Result result = tree.write(key, value);
        std::cout << "result status: " << result.status
                  << " error :" << result.error
                  << std::endl;
        SendRawReply(pConnector, result.error);
//        std::string cmd("set ");
//        cmd += std::string(pConnector->argv[1]);
//        cmd += " ";
//        cmd += std::string(pConnector->argv[2]);
//        auto value = tree.readEx(cmd);
//        SendRawReply(pConnector, value);

        return;
    }

    void ProcessCmdZAdd(RedisConnect *pConnector) {
        if (pConnector->argc < 4) {
            SendErrReply(pConnector, "ERR", "error arg");
            return;
        }

        std::string topic(pConnector->argv[1]);
        sds cmd = sdsjoinsds(pConnector->argv+2, 2, (char *)" ", 1);
        sds sub = sdsnew((char *)"sub ");
        cmd = sdscatsds(sub, cmd);
        std::cout << "cmd: " << cmd << std::endl;
        tree.writeEx(/*path*/topic, std::string(cmd));
        SendIntReply(pConnector, 1);
        return;
    }

    void ProcessCmdZRem(RedisConnect *pConnector) {
        if (pConnector->argc < 3) {
            SendErrReply(pConnector, "ERR", "error arg");
            return;
        }

        std::string topic(pConnector->argv[1]);
        sds unsub = sdsnew((char *)"unsub ");
        sds cmd = sdscatsds(unsub, pConnector->argv[2]);
        tree.writeEx(topic, std::string(cmd));
        SendIntReply(pConnector, 1);
        return;
    }

private:

};
//
//int main(int argc, char **argv)
//{
//    RedisServer redis;
//    redis.Init();
//    string pass = "shahe22f";
//    redis.SetPassword(pass);
//    redis.Start("127.0.0.1", 6479);
//
//    while (1) {
//        usleep(1000);
//    }
//
//    return 0;
//}

