#pragma once

#include "TreeStorageLayer.h"

#include <rocksdb/db.h>
#include <rocksdb/utilities/checkpoint.h>

namespace LogCabin {
namespace Tree {
    class RocksdbTree: public TreeStorageLayer{
        public:
        RocksdbTree();
        virtual ~RocksdbTree();

        virtual void Init(const std::string& path);

        virtual Result
            makeDirectory(const std::string& symbolicPath);

        virtual Result
            listDirectory(const std::string& symbolicPath, std::vector<std::string>& children) const;

        virtual int64_t getKeyExpireTime(const std::string& path);
        virtual Result
            removeDirectory(const std::string& symbolicPath);
        virtual void dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const ;
        virtual void loadSnapshot(Core::ProtoBuf::InputStream& stream) ;

        virtual Result
            removeFile(const std::string& symbolicPath);

        virtual Result
            write(const std::string& path, const std::string& contents,int64_t requestTime) ;

        virtual Result
            sadd(const std::string& path, const std::string& contents) ;

        virtual Result
            srem(const std::string& path, const std::string& contents) ;

        virtual Result
            rpush(const std::string& path, const std::string& contents,int64_t request_time) ;

        virtual Result
            lpush(const std::string& path, const std::string& contents,int64_t request_time) ;

        virtual Result
            lpop(const std::string& path, std::string& contents, int64_t requestTime) ;

        virtual Result
            lrem(const std::string& path, const std::string& contents, const int32_t count, int64_t requestTime) ;

        virtual Result
            ltrim(const std::string& path, const std::vector<std::string>& contents, int64_t requestTime) ;

        virtual Result
            expire(const std::string& path, const int64_t expire, const uint32_t op, int64_t request_time) ;

        virtual Result
            read(const std::string& path, std::string& contents) ;

        virtual Result
            lrange(const std::string& path, const std::vector<std::string>& args, std::vector<std::string>& output) ;

        virtual Result
            head(const std::string& path, std::string& contents) const ;

        virtual Result removeExpireSetting(const std::string& path);

        virtual Result remove(const std::string& path);

        virtual Result cleanExpiredKeys(const std::string& path);

        virtual void startSnapshot(uint64_t lastIncludedIndex);

        virtual void cleanUpExpireKeyEvent() ;
        private:

        std::string serverDir;
        std::string fsmDir;
        typedef std::shared_ptr<rocksdb::ColumnFamilyHandle> ColumnFamilyHandlePtr;
        typedef std::map<std::string, ColumnFamilyHandlePtr> ColumnFamilyHandleTable;

        rocksdb::DB* rdb;
        rocksdb::Checkpoint* checkpoint;
        rocksdb::Snapshot* snapshot;
        bool disableWAL;
        rocksdb::WriteOptions writeOptions;
        rocksdb::ReadOptions readOptions;
        ColumnFamilyHandleTable handlers;
        ColumnFamilyHandlePtr getColumnFamilyHandle(std::string cfName, bool create_if_noexist) const;
        void Reopen();
    };

} // namespace LogCabin::Tree
} // namespace LogCabin
