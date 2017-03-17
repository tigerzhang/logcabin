
/*
* ----------------------------------------------------------------------------
* Copyright (c) 2015-2016, xSky <guozhw@gmail.com>
* All rights reserved.
* Distributed under GPL license.
* ----------------------------------------------------------------------------
*/

#ifndef _XREDIS_SERVER_BASE_H_
#define _XREDIS_SERVER_BASE_H_

#include <event.h>
#include <event2/event.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/event_struct.h>
#include <event2/event_compat.h>

extern "C" {
    #include "sds.h"
}


#include <vector>
#include <string>
#include <pthread.h>
#include <map>
#include <LogCabin/Client.h>

#include "../Examples/RedisProxy.h"

using namespace std;

#define CMD_CALLBACK_MAX 512


class RedisServerBase;
class RedisConnectorBase;
class OptionParser;

class RedisConnectorBase
{
public:
    RedisConnectorBase(std::unique_ptr<LogCabin::Client::Cluster> cluster,
                       std::unique_ptr<LogCabin::Client::Tree> tree);
    ~RedisConnectorBase();

public:
   inline int getfd() { return fd; }

private:
    bool OnTimer();
    bool FreeArg();
    void SetSocketOpt();
    friend class RedisServerBase;
public:
    int argc;
    sds *argv;
    std::unique_ptr<LogCabin::Client::Cluster> cluster;
    std::unique_ptr<LogCabin::Client::Tree> tree;

private:
    int    fd;
    struct bufferevent *bev;
    struct event evtimer;
    uint32_t sid;
    uint32_t activetime;
    RedisServerBase *xredisvr;
    std::string cmdbuffer;
    int argnum;
    int parsed;
    bool authed;
    bool quit;
};


class RedisServerBase;
typedef void (RedisServerBase::*CmdCallback)(RedisConnectorBase *pConnector);
typedef struct _CMD_FUN_ {
    _CMD_FUN_() {
        cmd = NULL;
        cb = NULL;
    }
    const char    *cmd;
    CmdCallback    cb;
}CmdFun;

class RedisServerBase
{
public:
    RedisServerBase(RedisProxy::OptionParser& options);
    ~RedisServerBase();

public:
    bool Start(const char *ip, int port, int fork_num);
    bool SetCmdTable(const char* cmd, CmdCallback fun);
    bool SetPassword(std::string &password);
    
public:
    int SendStatusReply(RedisConnectorBase *pConnector, const char* str);
    int SendNullReply(RedisConnectorBase *pConnector);
    int SendErrReply(RedisConnectorBase *pConnector, const char *errtype, const char *errmsg);
    int SendIntReply(RedisConnectorBase *pConnector, int64_t ret);
    int SendBulkReply(RedisConnectorBase *pConnector, const std::string &vResult);
    int SendMultiBulkReply(RedisConnectorBase *pConnector, const std::vector<std::string> &vResult);
    int SendRawReply(RedisConnectorBase *pConnector, std::string& rawString);

private:
    int main_loop(const char *ip, int port, bool fork_child = true, int fork_num = 4);
    bool MallocConnection(evutil_socket_t skt);
    RedisConnectorBase* FindConnection(uint32_t sid);
    bool FreeConnection(uint32_t sid);
    bool ProcessCmd(RedisConnectorBase *pConnector);
    bool SendData(RedisConnectorBase *pConnector, const char* data, int len);
    int NetPrintf(RedisConnectorBase *pConnector, const char* fmt, ...);
    bool Run();

    static void *Dispatch(void *arg);
    static void listener_cb(struct evconnlistener *listener, evutil_socket_t fd,
                            struct sockaddr *sa, int socklen, void *user_data);
    static void ReadCallback(struct bufferevent *bev, void *arg);
    static void ErrorCallback(struct bufferevent *bev, short event, void *arg);
    static void WriteCallback(struct bufferevent *bev, void *arg);
    static void TimeoutCallback(int fd, short event, void *arg);

private:
    int ParaseLength(const char* ptr, int size, int &head_count);
    int ParaseData(RedisConnectorBase *pConnector, const char* ptr, int size);
    void DoCmd(RedisConnectorBase *pConnector);
    CmdFun * GetCmdProcessFun(const char *cmd);

private:
    friend class RedisConnectorBase;
    bool CheckSession(RedisConnectorBase *pConnector);
    void ProcessCmd_auth(RedisConnectorBase *pConnector);
private:
    RedisProxy::OptionParser& options;

    struct event_base *base;
    std::map<uint32_t, RedisConnectorBase*> connectionmap;
    uint32_t    sessionbase;
    CmdFun mCmdTables[CMD_CALLBACK_MAX];
    int    mCmdCount;
    bool bAuth;
    std::string pass;

    void ProcessCmd_quit(RedisConnectorBase *pBase);

    static void conn_writecb(bufferevent *bev, void *user_data);
};

#endif


