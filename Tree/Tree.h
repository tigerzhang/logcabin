/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2014 Diego Ongaro
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

#include <map>
#include <string>
#include <vector>
#include <list>
#include <set>

#include "Core/ProtoBuf.h"

#include <Server/Globals.h>

#ifndef LOGCABIN_TREE_TREE_H
#define LOGCABIN_TREE_TREE_H

//#define ARDB_FSM
#define ROCKSDB_FSM

namespace LogCabin {

// forward declaration
namespace Protocol {
class ServerStats_Tree;
}

namespace Tree {

    class TreeStorageLayer;
/**
 * Status codes returned by Tree operations.
 */
enum class Status {

    /**
     * The operation completed successfully.
     */
    OK = 0,

    /**
     * If an argument is malformed (for example, a path that does not start
     * with a slash).
     */
    INVALID_ARGUMENT = 1,

    /**
     * If a file or directory that is required for the operation does not
     * exist.
     */
    LOOKUP_ERROR = 2,

    /**
     * If a directory exists where a file is required or a file exists where
     * a directory is required. 
     */
    TYPE_ERROR = 3,

    /**
     * A predicate on an operation was not satisfied.
     */
    CONDITION_NOT_MET = 4,

    /**
     * This List is Empty
     */
    LIST_EMPTY = 5,

    /**
     * The Key is Expired
     */
    KEY_EXPIRED = 6,
};

/**
 * Print a status code to a stream.
 */
std::ostream&
operator<<(std::ostream& os, Status status);

/**
 * Returned by Tree operations; contain a status code and an error message.
 */
struct Result {
    /**
     * Default constructor. Sets status to OK and error to the empty string.
     */
    Result();
    /**
     * A code for whether an operation succeeded or why it did not. This is
     * meant to be used programmatically.
     */
    Status status;
    /**
     * If status is not OK, this is a human-readable message describing what
     * went wrong.
     */
    std::string error;
};


/**
 * This is an in-memory, hierarchical key-value store.
 * TODO(ongaro): Document how this fits into the rest of the system.
 */
class Tree {
  public:
    /**
     * Constructor.
     */
    Tree();

    ~Tree();

    void Init(std::string& path);

    void setUpZeroSessionIndex(uint64_t index);

    void findLatestSnapshot(Core::ProtoBuf::OutputStream* stream) const;
    /**
     * Write the tree to the given stream.
     */
    void dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const;

    void startSnapshot(uint64_t lastIncludedIndex);

    /**
     * Load the tree from the given stream.
     * \warning
     *      This will blow away any existing files and directories.
     */
    void loadSnapshot(Core::ProtoBuf::InputStream& stream);

    /**
     * Verify that the file at path has the given contents.
     * \param path
     *      The path to the file that must have the contents specified in
     *      'contents'.
     * \param contents
     *      The contents that the file specified by 'path' should have for an
     *      OK response. An OK response is also returned if 'contents' is the
     *      empty string and the file specified by 'path' does not exist.
     * \return
     *      Status and error message. Possible errors are:
     *       - CONDITION_NOT_MET upon any error.
     */
    Result
    checkCondition(const std::string& path,
                   const std::string& contents) const;

    /**
     * Make sure a directory exists at the given path.
     * Create parent directories listed in path as necessary.
     * \param path
     *      The path where there should be a directory after this call.
     * \return
     *      Status and error message. Possible errors are:
     *       - INVALID_ARGUMENT if path is malformed.
     *       - TYPE_ERROR if a parent of path is a file.
     *       - TYPE_ERROR if path exists but is a file.
     */
    Result
    makeDirectory(const std::string& path);

    /**
     * List the contents of a directory.
     * \param path
     *      The directory whose direct children to list.
     * \param[out] children
     *      This will be replaced by a listing of the names of the directories
     *      and files that the directory at 'path' immediately contains. The
     *      names of directories in this listing will have a trailing slash.
     *      The order is first directories (sorted lexicographically), then
     *      files (sorted lexicographically).
     * \return
     *      Status and error message. Possible errors are:
     *       - INVALID_ARGUMENT if path is malformed.
     *       - LOOKUP_ERROR if a parent of path does not exist.
     *       - LOOKUP_ERROR if path does not exist.
     *       - TYPE_ERROR if a parent of path is a file.
     *       - TYPE_ERROR if path exists but is a file.
     */
    Result
    listDirectory(const std::string& path,
                  std::vector<std::string>& children) const;

    /**
     * Make sure a directory does not exist.
     * Also removes all direct and indirect children of the directory.
     *
     * If called with the root directory, this will remove all descendants but
     * not actually remove the root directory; it will still return status OK.
     *
     * \param path
     *      The path where there should not be a directory after this call.
     * \return
     *      Status and error message. Possible errors are:
     *       - INVALID_ARGUMENT if path is malformed.
     *       - TYPE_ERROR if a parent of path is a file.
     *       - TYPE_ERROR if path exists but is a file.
     */
    Result
    removeDirectory(const std::string& path);

    /**
     * Set the value of a file.
     * \param path
     *      The path where there should be a file with the given contents after
     *      this call.
     * \param contents
     *      The new value associated with the file.
     * \return
     *      Status and error message. Possible errors are:
     *       - INVALID_ARGUMENT if path is malformed.
     *       - INVALID_ARGUMENT if contents are too large to fit in a file.
     *       - LOOKUP_ERROR if a parent of path does not exist.
     *       - TYPE_ERROR if a parent of path is a file.
     *       - TYPE_ERROR if path exists but is a directory.
     */
    Result
    write(const std::string& path, const std::string& contents,int64_t requestTime);

    Result
    sadd(const std::string& path, const std::string& contents);

    Result
    srem(const std::string& path, const std::string& contents);

    Result
    pub(const std::string& path, const std::string& contents);

    Result
    rpush(const std::string& path, const std::string& contents,int64_t request_time);

    Result
    lpush(const std::string& path, const std::string& contents,int64_t request_time);

    Result
    lpop(const std::string& path, std::string& contents, int64_t requestTime);

    Result
    lrem(const std::string& path, const std::string& contents, const int32_t count, int64_t requestTime);

    Result
    ltrim(const std::string& path, const std::string& contents, int64_t requestTime);

    /**
     * Do expire, if the op is CLEAN_UP_EXPIRE_KEYS, this api will remove the expire setting on the path, otherwise it will set up an expire timer on the key, the key should be expired in 'expire' seconds, 'expire' must be convertable to int
     */
    Result
    expire(const std::string& path, const int64_t expire, const uint32_t op, int64_t request_time);

    /**
     * Get the value of a file.
     * \param path
     *      The path of the file whose contents to read.
     * \param contents
     *      The current value associated with the file.
     * \return
     *      Status and error message. Possible errors are:
     *       - INVALID_ARGUMENT if path is malformed.
     *       - LOOKUP_ERROR if a parent of path does not exist.
     *       - LOOKUP_ERROR if path does not exist.
     *       - TYPE_ERROR if a parent of path is a file.
     *       - TYPE_ERROR if path is a directory.
     */
    Result
    read(const std::string& path, std::string& contents);

    Result
    lrange(const std::string& path, const std::string& args, std::vector<std::string>& output);

    Result
    head(const std::string& path, std::string& contents) const;

    /**
     * Make sure a file does not exist.
     * \param path
     *      The path where there should not be a file after this call.
     * \return
     *      Status and error message. Possible errors are:
     *       - INVALID_ARGUMENT if path is malformed.
     *       - TYPE_ERROR if a parent of path is a file.
     *       - TYPE_ERROR if path exists but is a directory.
     */
    Result
    removeFile(const std::string& path);

    /**
     * Add metrics about the tree to the given structure.
     */
    void
    updateServerStats(Protocol::ServerStats_Tree& tstats) const;

    void setRaft(LogCabin::Server::RaftConsensus* raft);

    /**
      scan through meta info of expire key, clean up the expired keys
      */
    void cleanUpExpireKeyEvent();

private:

    enum KeyExpireStatus{
        KeyExpireStatusNotExpired,
        KeyExpireStatusExpired,
        KeyExpireStatusNotSet,
    };


    /**
     * check if the key is expire or not , also clean up the key if it's expired, this should be called at the begin of all reading reqeust
     * 
     * \param[in] path
     *      The key of which you wanna check if it's expired or not.
     * \return KeyExpireStatus
     */
    KeyExpireStatus isKeyExpired(const std::string& path, int64_t request_time);

    /**
      construct the meta key of the path's expire setting
     */
    const std::string getMetaKeyOfExpireSetting(const std::string& path);

    /**
      do check expire fo writing request, if expired, 
      the expire setting and the old value of this path will be removed,
      return true if the key is expired
      reutrn false if the key is not expired or no expire setting on the key
     */
    bool checkIsKeyExpiredForWriteRequest(const std::string& path, int64_t request_time);

    /**
      do check expire fo read request, if expired, 
      a log with clean up expire setting will be appended to raft service.
      reutrn false if the key is not expired or no expire setting on the key
     */
    bool checkIsKeyExpiredForReadRequest(const std::string& path);


    /*
       a session whose client id is zero means it's started inside server, 
       this api is used to set up the exactly_once index, so the requested log won't be dropped because of index
    */
    uint64_t zeroSessionIndex;
    void appendCleanExpireRequestLog(const std::string& path, const int64_t expireIn);

    /*
       return -1 if key is not in the expire list, reutrn the timestamp (unit: second) if the key exists in expire list
    */
    int64_t getKeyExpireTime(const std::string& path);


    /**
     * clean the expired keys, this should be call expired key is detected. 
        This api should call removeExpireSetting at the end of the call
     * 
     * \param[in] path
     *      The key of which you wanna check if it's expired or not.
     */
    Result cleanExpiredKeys(const std::string& path);

    /**
     * clean the meta info of the key's expire info
     * 
     * \param[in] path
     *      The key of which you wanna check if it's expired or not.
     */
    Result removeExpireSetting(const std::string& path);



    // Server stats collected in updateServerStats.
    // Note that when a condition fails, the operation is not invoked,
    // so operations whose conditions fail are not counted as 'Attempted'.
    mutable uint64_t numConditionsChecked;
    mutable uint64_t numConditionsFailed;
    uint64_t numMakeDirectoryAttempted;
    uint64_t numMakeDirectorySuccess;
    mutable uint64_t numListDirectoryAttempted;
    mutable uint64_t numListDirectorySuccess;
    uint64_t numRemoveDirectoryAttempted;
    uint64_t numRemoveDirectoryParentNotFound;
    uint64_t numRemoveDirectoryTargetNotFound;
    uint64_t numRemoveDirectoryDone;
    uint64_t numRemoveDirectorySuccess;
    uint64_t numWriteAttempted;
    uint64_t numWriteSuccess;
    mutable uint64_t numReadAttempted;
    mutable uint64_t numReadSuccess;
    mutable uint64_t numLRANGEAttempted;
    mutable uint64_t numLRANGESuccess;
    uint64_t numRemoveFileAttempted;
    uint64_t numRemoveFileParentNotFound;
    uint64_t numRemoveFileTargetNotFound;
    uint64_t numRemoveFileDone;
    uint64_t numRemoveFileSuccess;

    uint64_t numRPushAttempted;
    uint64_t numRPushSuccess;
    uint64_t numExpireAttempted;
    uint64_t numExpireSuccess;
    uint64_t numLPopAttempted;
    uint64_t numLPopSuccess;
    uint64_t numLRemAttempted;
    uint64_t numLRemSuccess;
    uint64_t numLTrimAttempted;
    uint64_t numLTrimSuccess;

    LogCabin::Server::RaftConsensus* raft;

    std::shared_ptr<TreeStorageLayer> storage_layer; 

#ifdef ARDB_FSM
    ardb::Ardb ardb;
    ardb::Context worker_ctx;
#endif //ARDB_FSM 

}; //class Locabin::Tree::Tree


} // namespace LogCabin::Tree
} // namespace LogCabin

#endif // LOGCABIN_TREE_TREE_H
