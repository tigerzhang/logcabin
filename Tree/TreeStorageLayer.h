#pragma once

#ifndef TREESTORAGELAYER_H
#define TREESTORAGELAYER_H

#include <map>
#include <string>
#include <vector>
#include <list>
#include <set>
#include "Core/ProtoBuf.h"
#include "Tree.h"

namespace LogCabin {
namespace Tree{
class TreeStorageLayer
{
protected:
    std::map<std::string, uint64_t> listIndexes;
    std::map<std::string, uint64_t> listRevertIndexes;
    std::map<std::string, int64_t> expireCache;

public:
    TreeStorageLayer(){};
    virtual void Init(const std::string& path) = 0;
    virtual void dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const = 0;
    virtual void loadSnapshot(Core::ProtoBuf::InputStream& stream) = 0;
    virtual Result
    makeDirectory(const std::string& symbolicPath) = 0;

    virtual Result
    listDirectory(const std::string& symbolicPath, std::vector<std::string>& children) const = 0;

    virtual Result
    smembers(const std::string& symbolicPath, std::vector<std::string>& children) const = 0;

    virtual Result
    removeDirectory(const std::string& symbolicPath) = 0;

    virtual Result
    write(const std::string& path, const std::string& contents,int64_t requestTime) = 0;
    
    virtual Result
    sadd(const std::string& path, const std::string& contents) = 0;

    virtual Result
    srem(const std::string& path, const std::string& contents) = 0;

    virtual Result
    rpush(const std::string& path, const std::string& contents,int64_t request_time) = 0;

    virtual Result
    lpush(const std::string& path, const std::string& contents,int64_t request_time) = 0;

    virtual Result
    lpop(const std::string& path, std::string& contents, int64_t requestTime) = 0;

    virtual Result
    lrem(const std::string& path, const std::string& contents, const int32_t count, int64_t requestTime) = 0;

    virtual Result
    ltrim(const std::string& path, const std::vector<std::string>& contents, int64_t requestTime) = 0;

    virtual Result
    expire(const std::string& path, const int64_t expire, const uint32_t op, int64_t request_time) = 0;

    virtual Result
    read(const std::string& path, std::string& contents) = 0;

    virtual Result
    remove(const std::string& path) = 0;

    virtual Result
    lrange(const std::string& path, const std::vector<std::string>& args, std::vector<std::string>& output) = 0;

    virtual Result
    head(const std::string& path, std::string& contents) const = 0;

    virtual int64_t getKeyExpireTime(const std::string& path) = 0;

    virtual void startSnapshot(uint64_t lastIncludedIndex) = 0;

    virtual Result removeExpireSetting(const std::string& path) = 0;

    virtual void cleanUpExpireKeyEvent() = 0;
     
    virtual Result removeFile(const std::string& symbolicPath) = 0;
    virtual Result cleanExpiredKeys(const std::string& path) = 0;

    virtual ~TreeStorageLayer(){};
};
} //namespace Tree
} //namespace LogCabin

#endif /* TREESTORAGELAYER_H */
