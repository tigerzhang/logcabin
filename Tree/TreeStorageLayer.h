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
    sadd(const std::string& path, const std::vector<std::string>& contents) = 0;

    virtual Result
    srem(const std::string& path, const std::string& contents) = 0;

    virtual Result
    rpush(const std::string& path, const std::string& contents,int64_t request_time) = 0;

    virtual Result
    lpush(const std::string& path, const std::string& contents,int64_t request_time) = 0;

    virtual Result
    lpop(const std::string& path, const std::string& contents, int64_t requestTime) = 0;

    virtual Result
    lrem(const std::string& path, const std::string& contents, const int32_t count, int64_t requestTime) = 0;

    virtual Result
    ltrim(const std::string& path, const std::vector<std::string>& contents, int64_t requestTime) = 0;

    virtual Result
    scard(const std::string& path,
                  std::string& content) const = 0;

    virtual Result
    read(const std::string& path, std::string& contents) = 0;

    virtual Result
    remove(const std::string& path) = 0;

    virtual Result
    lrange(const std::string& path, const std::vector<std::string>& args, std::vector<std::string>& output) = 0;

    virtual Result
    head(const std::string& path, std::string& contents) const = 0;

    virtual void startSnapshot(uint64_t lastIncludedIndex) = 0;

    virtual Result removeFile(const std::string& symbolicPath) = 0;

    virtual ~TreeStorageLayer(){};
};
} //namespace Tree
} //namespace LogCabin

#endif /* TREESTORAGELAYER_H */
