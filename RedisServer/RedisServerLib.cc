/*
* ----------------------------------------------------------------------------
* Copyright (c) 2015-2016, xSky <guozhw@gmail.com>
* All rights reserved.
* Distributed under GPL license.
* ----------------------------------------------------------------------------
*/

#include <inttypes.h>
#include <assert.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <Core/Debug.h>
#include <zconf.h>
#include "RedisServerLib.h"
#include <event2/listener.h>
#include <csignal>

#define TIMEOUT_CLOSE      3600
#define TIMEVAL_TIME       600

RedisConnectorBase::RedisConnectorBase(std::unique_ptr<LogCabin::Client::Cluster> cluster,
                                       std::unique_ptr<LogCabin::Client::Tree> tree) :
        quit(false), cluster(std::move(cluster)), tree(std::move(tree)) {
    bev = NULL;
    activetime = time(NULL);
    xredisvr = NULL;
    argc = 0;
    argv = NULL;
    argnum = 0;
    parsed = 0;
    authed = false;
}

RedisConnectorBase::~RedisConnectorBase()
{
    FreeArg();
}

bool RedisConnectorBase::FreeArg()
{
    for (int i = 0; i < argc; ++i){
        if (NULL == argv[i]) {
            fprintf(stderr,"FreeArg error i: %d", i);
        }
        sdsfree(argv[i]);
    }
    if (argv) free(argv);
    cmdbuffer.erase(0, parsed);
    argv = NULL;
    argc = 0;
    argnum = 0;
    parsed = 0;
    return true;
}

bool RedisConnectorBase::OnTimer()
{
    if (time(NULL) - activetime > TIMEOUT_CLOSE) {
        return false;
    }
    return true;
}

void RedisConnectorBase::SetSocketOpt()
{
    int optval = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval))) {
        fprintf(stderr, "setsockopt(TCP_NODELAY) failed: %s", strerror(errno));
    }
}

RedisServerBase::RedisServerBase(RedisProxy::OptionParser& options) : options(options)
{
    sessionbase = 1000;
    mCmdCount = 0;
    bAuth = false;
}

RedisServerBase::~RedisServerBase()
{
    event_base_free(base);
}

int RedisServerBase::ParaseLength(const char* ptr, int size, int &head_count)
{
    char *lf = (char *)memchr(ptr, '\n', size);
    if (lf == NULL) {
        return 0;
    }
    char tmp[11] = { 0 };
    int i = 0;

    ptr++;
    while (ptr[i] != '\n') {
        if (ptr[i] == '\r') {
            i++;
            continue;
        }
        tmp[i] = ptr[i];
        i++;
    }
    head_count = (int)strtol(tmp, NULL, 10);
    if (0==head_count) {
        return 0;
    }
    return i + 2;
}

int RedisServerBase::ParaseData(RedisConnectorBase *pConnector, const char* ptr, int size)
{
    int parased = 0;
    if (ptr[0] != '$') {
        return 0;
    }

    int len = 0;
    int ret = ParaseLength(ptr, size, len);
    if (0 == ret){
        return 0;
    }
    parased += ret;
    ptr += parased;
    size -= parased;
    if (size < len + 2) {
        return 0;
    }
    pConnector->argv[pConnector->argc++] = sdsnewlen(ptr, len);
    pConnector->argnum--;

    return parased + len + 2;
}

bool RedisServerBase::ProcessCmd(RedisConnectorBase *pConnector)
{
    pConnector->activetime = time(NULL);
    int size = pConnector->cmdbuffer.length();
    const char *pData = pConnector->cmdbuffer.c_str();

    if (0 == size) {
        return false;
    }

    pData += pConnector->parsed;
    size -= pConnector->parsed;
    const char *ptr = pData;

    if (pConnector->argnum == 0){
        if (ptr[0] == '*') {
            int num = 0;
            int pos = ParaseLength(ptr, size, num);
            if (0 == pos) {
                return false;
            }
            ptr += pos;
            size -= pos;
            pConnector->parsed += pos;
            pConnector->argnum = num;
            if (pConnector->argv) pConnector->FreeArg();
            pConnector->argv = (sds *)malloc(sizeof(sds*)*(pConnector->argnum));
        } else {
            return false;
        }
    }

    while (pConnector->argnum > 0) {
        int  p = ParaseData(pConnector, ptr, size);
        if (p == 0){
            break;
        }
        ptr += p;
        size -= p;
        pConnector->parsed += p;
    }

    if (pConnector->argnum == 0) {
        DoCmd(pConnector);
        return true;
    }
    return false;
}

bool RedisServerBase::SetCmdTable(const char* cmd, CmdCallback fun)
{
    if ((NULL == cmd) || (NULL == fun) || (mCmdCount >= CMD_CALLBACK_MAX)) {
        assert(false);
        return false;
    }
    mCmdTables[mCmdCount].cmd = cmd;
    mCmdTables[mCmdCount].cb = fun;
    mCmdCount++;
    return true;
}

CmdFun * RedisServerBase::GetCmdProcessFun(const char *cmd)
{
    CmdFun *iter;
    for (iter = &mCmdTables[0]; (NULL != iter) && (iter->cmd); ++iter) {
        if ((NULL != iter->cmd) && (0 == strcasecmp(iter->cmd, cmd)))
            return iter;
    }
    return NULL;
}

void RedisServerBase::DoCmd(RedisConnectorBase *pConnector)
{
//    printf("Got Command: %s\n", pConnector->argv[0]);

    CmdFun *cmd = GetCmdProcessFun(pConnector->argv[0]);
    if (cmd) {
        if (CheckSession(pConnector)) {
            (this->*cmd->cb)(pConnector);
        }
    } else {
        if (0 == strncasecmp(pConnector->argv[0], "AUTH",4)) {
            ProcessCmd_auth(pConnector);
        } else if (0 == strncasecmp(pConnector->argv[0], "QUIT", 4)) {
            ProcessCmd_quit(pConnector);
        } else {
            SendErrReply(pConnector, pConnector->argv[0], "not suport");
        }
    }
    pConnector->FreeArg();
}

void RedisServerBase::listener_cb(struct evconnlistener *listener, evutil_socket_t fd,
                                  struct sockaddr *sa, int socklen, void *user_data)
{
    class RedisServerBase *pRedisvr = (class RedisServerBase *)user_data;

    struct event_base *base = pRedisvr->base;
    struct bufferevent *bev;

    bev = bufferevent_socket_new(base, fd, BEV_OPT_CLOSE_ON_FREE);
    if (!bev) {
        fprintf(stderr, "Error constructing bufferevent!");
        event_base_loopbreak(base);
        return;
    }

    printf("New Connectioin %d\n", fd);
    pRedisvr->MallocConnection(fd);
}

void RedisServerBase::ReadCallback(struct bufferevent *bev, void *arg)
{
    RedisConnectorBase *pConnector = reinterpret_cast<RedisConnectorBase*>(arg);
    RedisServerBase *pRedisvr = pConnector->xredisvr;
    struct evbuffer* input = bufferevent_get_input(bev);

    while (1) {
        size_t total_len = evbuffer_get_length(input);
        if (total_len < 2) {
            break;
        }

        unsigned char *buffer = evbuffer_pullup(input, total_len);
        if (NULL == buffer) {
            fprintf(stderr, "evbuffer_pullup msg_len failed!\r\n");
            return;
        }

        pConnector->cmdbuffer.append((char*)buffer, total_len);
        if (evbuffer_drain(input, total_len) < 0) {
            fprintf(stderr, "evbuffer_drain failed!\r\n");
            return;
        }
    }

    if (pConnector->cmdbuffer.length()) {
        while (pRedisvr->ProcessCmd(pConnector))
        {

        }
    }

}

void RedisServerBase::WriteCallback(struct bufferevent *bev, void *arg)
{
    
}

void RedisServerBase::TimeoutCallback(int fd, short event, void *arg)
{
    RedisConnectorBase *pConnector = reinterpret_cast<RedisConnectorBase*>(arg);
    RedisServerBase *pRedisvr = pConnector->xredisvr;

    if (pConnector->quit) {
        pRedisvr->FreeConnection(pConnector->sid);
    }

    if (pConnector->OnTimer()) {
        struct timeval tv;
        evutil_timerclear(&tv);
        tv.tv_sec = TIMEVAL_TIME;
        event_add(&pConnector->evtimer, &tv);
    } else {
        pRedisvr->FreeConnection(pConnector->sid);
    }
}

void RedisServerBase::ErrorCallback(struct bufferevent *bev, short event, void *arg)
{
    RedisConnectorBase *pConnector = reinterpret_cast<RedisConnectorBase*>(arg);
    RedisServerBase *pRedisvr = pConnector->xredisvr;

    evutil_socket_t fd = pConnector->getfd();
    //fprintf(stderr, "error_cb fd:%u, sid:%u bev:%p event:%d arg:%d\n", fd, pConnector->sid, bev, event, arg);
    if (event & BEV_EVENT_TIMEOUT) {
        fprintf(stderr, "Timed out fd:%d \n", fd);
    } else if (event & BEV_EVENT_EOF) {
        fprintf(stderr, "connection closed fd:%d \n", fd);
    } else if (event & BEV_EVENT_ERROR) {
        fprintf(stderr, "BEV_EVENT_ERROR error fd:%d \n", fd);
    } else if (event & BEV_EVENT_READING) {
        fprintf(stderr, "BEV_EVENT_READING error fd:%d \n", fd);
    } else if (event & BEV_EVENT_WRITING) {
        fprintf(stderr, "BEV_EVENT_WRITING error fd:%d \n", fd);
    } else {
    
    }
    pRedisvr->FreeConnection(pConnector->sid);
}

bool RedisServerBase::Start(const char* ip, int port)
{
    main_loop(ip, port);
    return false;
}

static void
signal_cb(evutil_socket_t sig, short events, void *user_data)
{
    struct event_base *base = (struct event_base *)user_data;
    struct timeval delay = { 2, 0 };

    printf("Caught an interrupt signal; exiting cleanly in two seconds.\n");

    event_base_loopexit(base, &delay);
}

int RedisServerBase::main_loop(const char *ip, int port)
{
    struct event *signal_event;

    base = event_base_new();
    assert(base != NULL);

    if (NULL==ip) {
        return 1;
    }
    struct evconnlistener *listener;

    struct sockaddr_in sin;
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = inet_addr(ip);
    sin.sin_port = htons(port);

    event_enable_debug_logging(EVENT_DBG_ALL);

    listener = evconnlistener_new_bind(base, listener_cb, (void *)this,
                                       LEV_OPT_REUSEABLE|LEV_OPT_CLOSE_ON_FREE, -1,
                                       (struct sockaddr*)&sin,
                                       sizeof(sin));
    if (!listener) {
        fprintf(stderr, "Could not create a listener!\n");
        return 1;
    }

    signal_event = evsignal_new(base, SIGINT, signal_cb, (void *)base);

    if (!signal_event || event_add(signal_event, NULL)<0) {
        fprintf(stderr, "Could not create/add a signal event!\n");
        return 1;
    }

    event_base_dispatch(base);

    evconnlistener_free(listener);
    event_free(signal_event);
    event_base_free(base);

    printf("done\n");

    return 0;
}

bool RedisServerBase::Run()
{
    pthread_t pid;
    int ret = pthread_create(&pid, NULL, Dispatch, base);
    return (0==ret);
}

void *RedisServerBase::Dispatch(void *arg){
    if (NULL == arg) {
        return NULL;
    }
    event_base_dispatch((struct event_base *) arg);

    fprintf(stderr, "Dispatch thread end\n");
    return NULL;
}

void
RedisServerBase::conn_writecb(struct bufferevent *bev, void *user_data)
{
    RedisConnectorBase *pConnector = (RedisConnectorBase*)user_data;
    RedisServerBase *pRedisvr = pConnector->xredisvr;

    struct evbuffer *output = bufferevent_get_output(bev);
    if (evbuffer_get_length(output) == 0 && pConnector->quit) {
        printf("flushed answer\n");
//        bufferevent_free(bev);
        pRedisvr->FreeConnection(pConnector->sid);
    }
}

bool RedisServerBase::MallocConnection(evutil_socket_t skt)
{

    std::unique_ptr<LogCabin::Client::Cluster> cluster(new LogCabin::Client::Cluster(options.cluster));
    std::unique_ptr<LogCabin::Client::Tree> tree(new LogCabin::Client::Tree(cluster->getTree())) ;

    if (options.timeout > 0) {
        tree->setTimeout(options.timeout);
    }

    if (!options.dir.empty()) {
        tree->setWorkingDirectoryEx(options.dir);
    }

    if (!options.condition.first.empty()) {
        tree->setConditionEx(options.condition.first,
                            options.condition.second);
    }

    RedisConnectorBase *pConnector = new RedisConnectorBase(std::move(cluster),
                                                            std::move(tree));
    if (NULL == pConnector) {
        return false;
    }

    pConnector->fd = skt;
    pConnector->xredisvr = this;
    pConnector->sid = sessionbase++;
    pConnector->bev = bufferevent_socket_new(base, skt, BEV_OPT_CLOSE_ON_FREE);
    bufferevent_setcb(pConnector->bev, ReadCallback, conn_writecb, ErrorCallback, pConnector);
    pConnector->SetSocketOpt();

    struct timeval tv;
    tv.tv_sec = TIMEVAL_TIME;
    tv.tv_usec = 0;
    evtimer_set(&pConnector->evtimer, TimeoutCallback, pConnector);
    event_base_set(base, &pConnector->evtimer);
    evtimer_add(&pConnector->evtimer, &tv);

    bufferevent_enable(pConnector->bev, EV_READ | EV_WRITE | EV_PERSIST);
    connectionmap.insert(pair<uint32_t, RedisConnectorBase*>(pConnector->sid, pConnector));

    return true;
}

RedisConnectorBase* RedisServerBase::FindConnection(uint32_t sid)
{
    std::map<uint32_t, RedisConnectorBase*>::iterator iter = connectionmap.find(sid);
    if (iter == connectionmap.end()) {
        return NULL;
    } else {
        return iter->second;
    }
}

bool RedisServerBase::FreeConnection(uint32_t sid)
{
    printf("Free Connection %u\n", sid);

    std::map<uint32_t, RedisConnectorBase*>::iterator iter = connectionmap.find(sid);
    if (iter == connectionmap.end()) {
        return false;
    } else {
        iter->second->FreeArg();
        event_del(&iter->second->evtimer);
        bufferevent_free(iter->second->bev);
        delete iter->second;
        connectionmap.erase(iter);
    }
    return true;
}

bool RedisServerBase::SendData(RedisConnectorBase *pConnector, const char* data, int len)
{
    int ret = bufferevent_write(pConnector->bev, data, len);
    return (0 == ret);
}

int RedisServerBase::NetPrintf(RedisConnectorBase *pConnector, const char* fmt, ...)
{
    char szBuf[256] = { 0 };
    int len = 0;
    va_list va;
    va_start(va, fmt);
    len = vsnprintf(szBuf, sizeof(szBuf), fmt, va);
    va_end(va);
    bool bRet = SendData(pConnector, szBuf, len);
    return (bRet) ? len : 0;
}

int RedisServerBase::SendStatusReply(RedisConnectorBase *pConnector, const char* str)
{
    return NetPrintf(pConnector, "+%s\r\n", str);
}

int RedisServerBase::SendNullReply(RedisConnectorBase *pConnector)
{
    return NetPrintf(pConnector, "$-1\r\n");
}

int RedisServerBase::SendErrReply(RedisConnectorBase *pConnector, const char *errtype, const char *errmsg)
{
    return NetPrintf(pConnector, "-%s %s\r\n", errtype, errmsg);
}

int RedisServerBase::SendIntReply(RedisConnectorBase *pConnector, int64_t ret)
{
    return NetPrintf(pConnector, ":%" PRId64 "\r\n", ret);
}

int RedisServerBase::SendBulkReply(RedisConnectorBase *pConnector, const std::string &strResult)
{
    NetPrintf(pConnector, "$%zu\r\n", strResult.size());
    SendData(pConnector, strResult.c_str(), strResult.size());
    SendData(pConnector, "\r\n", 2);
    return 0;
}

int RedisServerBase::SendMultiBulkReply(RedisConnectorBase *pConnector, const std::vector<std::string> &vResult)
{
    NetPrintf(pConnector, "*%zu\r\n", vResult.size());
    for (size_t i = 0; i < vResult.size(); ++i) {
        SendBulkReply(pConnector, vResult[i]);
    }
    return 0;
}

int RedisServerBase::SendRawReply(RedisConnectorBase *pConnector, std::string& rawString) {
    SendData(pConnector, rawString.c_str(), rawString.size());
    return 0;
}

bool RedisServerBase::CheckSession(RedisConnectorBase *pConnector)
{
    bool bRet = (!bAuth) ? true : (pConnector->authed);
    if (!bRet){
        SendErrReply(pConnector, "ERR", "operation not permitted");
    }
    return bRet;
}

bool RedisServerBase::SetPassword(std::string &password)
{
    pass = password;
    bAuth = (password.length() > 0) ? true : false;
    return bAuth;
}

void RedisServerBase::ProcessCmd_auth(RedisConnectorBase *pConnector)
{
    if (2 != pConnector->argc) {
        SendErrReply(pConnector, "arg error", "argc error");
        return;
    }

    if (bAuth) {
        if (0 == strcmp(pConnector->argv[1], pass.c_str())) {
            pConnector->authed = true;
            SendStatusReply(pConnector, "OK");
        } else {
            SendErrReply(pConnector, "ERR", "auth failed");
        }
        return;
    }

    SendStatusReply(pConnector, "OK");
    return;
}

void RedisServerBase::ProcessCmd_quit(RedisConnectorBase *pConnector) {
    if (1 != pConnector->argc) {
        SendErrReply(pConnector, "arg error", "argc error");
        return;
    }

    SendStatusReply(pConnector, "OK");

    pConnector->quit = true;
}

