#include "RocksdbTree.h"

#include <rocksdb/db.h>
#include <rocksdb/statistics.h>
#include <rocksdb/slice.h>
#include <rocksdb/snapshot.h>
#include <rocksdb/options.h>
#include <rocksdb/iterator.h>
#include "Core/StringUtil.h"
#include "build/Tree/Snapshot.pb.h"
#include "build/Protocol/Client.pb.h"


namespace LogCabin {
namespace Tree {
using Core::StringUtil::format;

RocksdbTree::RocksdbTree()
    : checkpoint(NULL)
        , snapshot(NULL)
        , disableWAL(true)
        , writeOptions(std::move(rocksdb::WriteOptions())){
    writeOptions.disableWAL = this->disableWAL;
    writeOptions.sync = false;
}

RocksdbTree::~RocksdbTree(){}

void RocksdbTree::Init(const std::string& path)
{
    serverDir = path;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.max_open_files = 1024;
    options.statistics = rocksdb::CreateDBStatistics();
    options.stats_dump_period_sec = 1;
    options.dump_malloc_stats = true;

    fsmDir = serverDir + "/rocksdb-fsm";

    rocksdb::Status status;
    std::vector<std::string> column_families;
    status = rocksdb::DB::ListColumnFamilies(options, fsmDir, &column_families);
    if (column_families.empty()) {
        status = rocksdb::DB::Open(options, fsmDir, &rdb);
    } else {
        std::vector<rocksdb::ColumnFamilyDescriptor> column_families_descs(column_families.size());
        for (size_t i = 0; i < column_families.size(); i++) {
            column_families_descs[i] = rocksdb::ColumnFamilyDescriptor(column_families[i],
            rocksdb::ColumnFamilyOptions(options));
        }

        std::vector<rocksdb::ColumnFamilyHandle*> handler;
        status = rocksdb::DB::Open(
                        options,
                        fsmDir,
                        column_families_descs,
                        &handler,
                        &rdb);

        for (size_t i = 0; i < handler.size(); i++)
        {
            rocksdb::ColumnFamilyHandle* h = handler[i];
            auto name = column_families_descs[i].name;
            handlers[name].reset(h);
            NOTICE("Open column family:%s success.", column_families_descs[i].name.c_str());
        }
    }

    if (!status.ok()) {
        PANIC("Failed to open db:%s. %s", fsmDir.c_str(), status.ToString().c_str());
    }

    if (!status.ok()) {
        PANIC("Open rocksdb failed %s", status.ToString().c_str());
    }
}

RocksdbTree::ColumnFamilyHandlePtr
RocksdbTree::getColumnFamilyHandle(std::string cfName, bool create_if_noexist) const {
//    VERBOSE("cf name: %s", cfName.c_str());
//    for (auto i : handlers) {
//        VERBOSE("handlers: %s %lu", i.first.c_str(), i.second.get());
//    }
    auto found = handlers.find(cfName);
    if (found != handlers.end()) {
//        VERBOSE("Found cf %s:%lu", cfName.c_str(), handlers[cfName].get());
        return handlers[cfName];
    }
    if (!create_if_noexist) {
        return NULL;
    }

    // Create Column Family
//    rocksdb::Options options;
//    options.create_if_missing = true;
    rocksdb::ColumnFamilyOptions cf_options;
    rocksdb::ColumnFamilyHandle* cfh = NULL;
    rocksdb::Status s = rdb->CreateColumnFamily(cf_options, cfName, &cfh);
    if (s.ok()) {
//        s = rdb->Put(rocksdb::WriteOptions(), cfh, "test1", "value1");
//        VERBOSE("Put after create column family: %s", s.ToString().c_str());
//        assert(s.ok());

        handlers[cfName].reset(cfh);


        NOTICE("Create Column Family Handle with name:%s succeed %lu",
            cfName.c_str(), handlers[cfName].get());
        return handlers[cfName];
    }

    ERROR("Create Column Family Handle:%s failed:%s", cfName.c_str(), s.ToString().c_str());
    return NULL;
}

void RocksdbTree::dumpSnapshot(Core::ProtoBuf::OutputStream& stream) const{
    try {
        ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);

        rocksdb::ColumnFamilyHandle* pcf = cfp.get();
        if (NULL == pcf) {
            PANIC("Get cf failed");
        }

        int keyDumped = 0;
        rocksdb::ReadOptions readOptions = rocksdb::ReadOptions();
        readOptions.snapshot = snapshot;
        auto it = rdb->NewIterator(readOptions, pcf);
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            VERBOSE("iter: key %s value %s , dumped %d",
                    it->key().ToString().c_str(),
                    it->value().ToString().c_str(),
                    keyDumped);
//        std::cout << "key: " << it->key().ToString() << " value: " << it->value().ToString() << std::endl;
            Snapshot::KeyValue kv;
            kv.set_key(it->key().ToString());
            kv.set_value(it->value().ToString());
            stream.writeMessage(kv);

            keyDumped++;
        }
        delete it;
        NOTICE("key dumped: %d", keyDumped);
    } catch (std::exception e) {
        ERROR("Tree dump snapshot failed: %s", e.what());
    };
}

void
RocksdbTree::loadSnapshot(Core::ProtoBuf::InputStream& stream){

    {
        //drop column
        ColumnFamilyHandlePtr columnFamilyPtr = getColumnFamilyHandle("cf0", true);
        rocksdb::ColumnFamilyHandle* pcf = columnFamilyPtr.get();
        if (NULL == pcf) {
            PANIC("Get cf failed");
        }
        rocksdb::Status status = rdb->DropColumnFamily(pcf);
        if (!status.ok()) {
            PANIC("Drop default column family failed: %s", status.ToString().c_str());
            pcf = NULL;
        }
        Reopen();
    }

    //re init column family, and then insert value into it
    ColumnFamilyHandlePtr columnFamilyPtr = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = columnFamilyPtr.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }
    Snapshot::KeyValue kv;
    std::string error = stream.readMessage(kv);
    while(error.empty()) {
        VERBOSE("load key %s value %s", kv.key().c_str(), kv.value().c_str());
        auto writeResult = rdb->Put(writeOptions, pcf, kv.key(), kv.value());
        if(writeResult.ok())
        {
            error = stream.readMessage(kv);
        }else{
            PANIC("Can not load snapshot : %s", writeResult.ToString().c_str());
        }
    }
    NOTICE("Load snapshot succeed.");
}

void RocksdbTree::Reopen() {
    for (auto handle = handlers.begin();
            handle != handlers.end();
            handle++
        ) {
        handlers.erase(handle);
    }
    delete rdb;
    rdb = NULL;

    Init(serverDir);
}

Result
RocksdbTree::makeDirectory(const std::string& symbolicPath)
{
    Result result;
    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }

    std::string key = symbolicPath ;
    if("" == symbolicPath || *symbolicPath.end() != '/')
    {
        key += '/';
    }
    rdb->Put(writeOptions, pcf, key, "dir");
    return result;
}

Result
RocksdbTree::listDirectory(const std::string& symbolicPath,
                    std::vector<std::string>& children) const
{

    Result result;
    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }

    std::string prefix = symbolicPath;
    auto iter = rdb->NewIterator(rocksdb::ReadOptions(), pcf);
    for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        VERBOSE("iter: %s", iter->key().ToString().c_str());
        children.emplace_back(iter->key().ToString().substr(1));
    }
    delete iter;
    return result;
}

Result
RocksdbTree::removeDirectory(const std::string& symbolicPath)
{
    Result result;
    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }

    std::string prefix = symbolicPath;
    std::string key = symbolicPath;
    if("" == symbolicPath || *symbolicPath.end() != '/')
    {
        key += '/';
    }
    rdb->Delete(writeOptions, pcf, key);
    return result;
}

const std::string getMetaKeyOfExpireSetting(const std::string& path){
    return ":meta:e:" + path;
}

Result RocksdbTree::removeExpireSetting(const std::string& path)
{
    Result s;
    auto cacheIt = this->expireCache.find(path);
    if(this->expireCache.end() != cacheIt)
    {
        this->expireCache.erase(cacheIt);
    }
    std::string expireKeyMeta = getMetaKeyOfExpireSetting(path);

    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }
    VERBOSE("now delete expried key from map");

    auto deleteResult = rdb->Delete(writeOptions, pcf, expireKeyMeta);
    if(!deleteResult.ok())
    {
        PANIC("delete expire meta failed");
    }
    return s;
}

Result RocksdbTree::cleanExpiredKeys(const std::string& path){
    Result s;
    remove(path);
    removeExpireSetting(path);
    return s;
}

Result
RocksdbTree::remove(const std::string& path){
    //TODO: fix it to really do remove
    Result s;
    return s;
}

Result
RocksdbTree::write(const std::string& symbolicPath, const std::string& contents, int64_t requestTime){

    Result result;
    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }

    rocksdb::Status s;
    std::string meta;
    std::string key = symbolicPath;
    if("" != symbolicPath && *symbolicPath.end() != '/')
    {
        key += "/";
    }
    s = rdb->Get(rocksdb::ReadOptions(), pcf, key, &meta);
    if (s.ok() && meta == "dir") {
        result.status = Status::TYPE_ERROR;
        result.error = symbolicPath + " is a directory";
        return result;
    }
    s = rdb->Put(writeOptions, pcf, symbolicPath, contents);
    if (!s.ok()) {
        PANIC("rocksdb put failed: %s", s.ToString().c_str());
    }
    return result;
}

Result
RocksdbTree::sadd(const std::string& symbolicPath, const std::string& contents){
    Result result;
    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }

    std::string key = symbolicPath + ":meta";
    rdb->Put(writeOptions, pcf, key, "s");

    key = symbolicPath + ":s:" + contents;
    rdb->Put(writeOptions, pcf, key, "s");
    return result;
}

Result
RocksdbTree::srem(const std::string& symbolicPath, const std::string& contents){

    Result result;
    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }
//    std::string key = symbolicPath + ":meta";
//    rdb->Put(writeOptions, key, "set");

    std::string key = symbolicPath + ":s:" + contents;
    rdb->Delete(writeOptions, pcf, key);
    return result;
}

Result
RocksdbTree::expire(const std::string &symbolicPath, const int64_t expireAt, const uint32_t op, const int64_t requestTime) {
    Result result;
    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }

    rocksdb::Status s;
    std::string keyMeta = getMetaKeyOfExpireSetting(symbolicPath);
    //but the basic routine is samesame
    //no need to check key exists, this is a complicate key, so it must be exists
    if(Protocol::Client::CLEAN_UP_EXPIRE_KEYS == op)
    {
        std::string content = "";
        auto getOldExpireResult = rdb->Get(readOptions, pcf, keyMeta, &content);
        if(getOldExpireResult.ok())
        {
            const int64_t oldExpireTime = *((const int64_t*)content.c_str());
            if(oldExpireTime == expireAt)
            {
                cleanExpiredKeys(symbolicPath);
            }
            //not same, might be triggered by read request when doing write request 
        }
        //nothing found, do nothing, can be duplicate request 
    }else{
        rdb->Put(writeOptions, pcf, keyMeta, rocksdb::Slice((const char*)&expireAt, sizeof(int64_t)));
        //should maintain a list to make sure it can hold in memory
        //but currently it's ok to stop here, let's make it work first
        expireCache[symbolicPath] = expireAt;
    }
    return result;

} 


Result
RocksdbTree::lpush(const std::string &symbolicPath, const std::string &contents, int64_t requestTime) {
    Result result;
    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }

    std::string meta;
    rocksdb::Status s;
    uint64_t index = 99999;
    std::string keyMeta = symbolicPath + ":meta";

    auto it = listRevertIndexes.find(symbolicPath);
    if (it != listRevertIndexes.end()) {
        index = it->second;
    } else {
        s = rdb->Get(readOptions, pcf, keyMeta, &meta);
        if (s.ok()) {
            assert(meta.substr(0, 1) == "l");
            std::string indexStored = meta.substr(1);
            index = atoll(indexStored.c_str());
            listRevertIndexes[symbolicPath] = index;
        }
    }

    char indexStr[8];
    snprintf(indexStr, 8, "%07lu", index);
    std::string keyElement = symbolicPath + ":l:" + std::string(indexStr);
    listRevertIndexes[symbolicPath] = index - 1;

    rdb->Put(writeOptions, pcf, keyElement, contents);

    snprintf(indexStr, 8, "%lu", index+1);
    rdb->Put(writeOptions, pcf, keyMeta, "l" + std::string(indexStr));

    //TODO: need to be abstract as function
    std::string listLengthStr;
    int listLength;
    std::string keyStoreListLength(symbolicPath + ":c");
    s = rdb->Get(readOptions, pcf, keyStoreListLength, &listLengthStr);
    if (s.ok()) {
        listLength = atoi(listLengthStr.c_str());
        rdb->Put(writeOptions, pcf, keyStoreListLength, std::to_string(++listLength));
        VERBOSE("key %s , current length %d\n", keyStoreListLength.c_str(), listLength);
    } else if (s.IsNotFound()) {
        VERBOSE("list length initialize to 1\n");
        rdb->Put(writeOptions, pcf, keyStoreListLength, "1");
    }
    return result;
}

Result
RocksdbTree::rpush(const std::string &symbolicPath, const std::string &contents, int64_t requestTime) {
    Result result;
    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }

    std::string meta;
    rocksdb::Status s;
    //if nothing is found, should start from 100000
    uint64_t index = 100000;
    std::string keyMeta = symbolicPath + ":meta";

    auto it = listIndexes.find(symbolicPath);
    if (it != listIndexes.end()) {
        index = it->second;
    } else {
        s = rdb->Get(readOptions, pcf, keyMeta, &meta);
        if (s.ok()) {
            assert(meta.substr(0, 1) == "l");
            std::string indexStored = meta.substr(1);
            index = atoll(indexStored.c_str());
            listIndexes[symbolicPath] = index;
        }
    }

    char indexStr[8];
    snprintf(indexStr, 8, "%07lu", index);
    std::string keyElement = symbolicPath + ":l:" + std::string(indexStr);
    listIndexes[symbolicPath] = index + 1;

    rdb->Put(writeOptions, pcf, keyElement, contents);

    snprintf(indexStr, 8, "%lu", index+1);
    rdb->Put(writeOptions, pcf, keyMeta, "l" + std::string(indexStr));

    //TODO: need to be abstract as function
    std::string listLengthStr;
    int listLength;
    std::string keyStoreListLength(symbolicPath + ":c");
    s = rdb->Get(readOptions, pcf, keyStoreListLength, &listLengthStr);
    if (s.ok()) {
        listLength = atoi(listLengthStr.c_str());
        rdb->Put(writeOptions, pcf, keyStoreListLength, std::to_string(++listLength));
        VERBOSE("key %s , current length %d\n", keyStoreListLength.c_str(), listLength);
    } else if (s.IsNotFound()) {
        VERBOSE("list length initialize to 1\n");
        rdb->Put(writeOptions, pcf, keyStoreListLength, "1");
    }
    return result;
}

Result
RocksdbTree::lpop(const std::string& symbolicPath, const std::string& contents, int64_t requestTime) {
    Result result;
    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }

    result.status = Status::LIST_EMPTY;
    std::string prefix = symbolicPath + ":l:";
    auto iter = rdb->NewIterator(rocksdb::ReadOptions(), pcf);
    iter->Seek(prefix);
    if (iter->Valid() && iter->key().starts_with(prefix)) {
        result.status = Status::OK;
        contents = iter->value().ToString();
        rdb->Delete(writeOptions, pcf, iter->key());
    }
    delete iter;
    return result;
}
Result
RocksdbTree::lrem(const std::string& symbolicPath, const std::string &contents, const int32_t count, int64_t requestTime) {
    Result result;
    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }

    int removed = 0;

    result.status = Status::CONDITION_NOT_MET;
    std::string prefix = symbolicPath + ":l:";
    auto iter = rdb->NewIterator(rocksdb::ReadOptions(), pcf);
    iter->Seek(prefix);
    //if count < 0, reverse the searching
    if(count < 0){
        iter->SeekToLast();
    }
    while(iter->Valid() && iter->key().starts_with(prefix) &&
            (count == 0 || std::abs(count) > removed))
    {

        if (iter->value() == contents) {
            rdb->Delete(writeOptions, pcf, iter->key());
            result.status = Status::OK;
            removed++;
        }
        if(count < 0){
            iter->Prev();
        }else{
            iter->Next();
        }
    }
    delete iter;

    //TODO: need to be abstract as function
    std::string listLengthStr;
    int listLength;
    rocksdb::Status s;
    std::string keyStoreListLength(symbolicPath + ":c");
    s = rdb->Get(readOptions, pcf, keyStoreListLength, &listLengthStr);
    if (s.ok()) {
        listLength = atoi(listLengthStr.c_str());
        listLength -= removed;
        if (listLength <= 0) {
            rdb->Delete(writeOptions, pcf, keyStoreListLength);
        } else {
            rdb->Put(writeOptions, pcf, keyStoreListLength, std::to_string(listLength));
        }
        VERBOSE("key %s , current length %d\n", keyStoreListLength.c_str(), listLength);
    }
    return result;
}

Result
RocksdbTree::ltrim(const std::string& symbolicPath, const std::vector<std::string> &args, int64_t requestTime) {

    Result result;
    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }

    if (args.size() != 2) {
        ERROR("Err wrong number of arguments for ltrim command");
        result.status = Status::INVALID_ARGUMENT;
        return result;
    } else {
        int start = atoi(args[0].c_str());
        int stop = atoi(args[1].c_str());

        VERBOSE("LTRIM input args: start: %d, stop: %d\n", start, stop);

        std::string listLengthStr;
        int listLength;
        rocksdb::Status s;
        std::string keyStoreListLength(symbolicPath + ":c");
        s = rdb->Get(readOptions, pcf, keyStoreListLength, &listLengthStr);
        if (s.ok()) {
            listLength = atoi(listLengthStr.c_str());
            VERBOSE("key %s , current length %d\n", keyStoreListLength.c_str(), listLength);

            if (start < 0) {
                start = start + listLength < 0 ? -1 : start + listLength;
            }

            if (stop < 0) {
                stop = stop + listLength < 0 ? -1 : stop + listLength;
            } else if (stop > listLength) {
                stop = listLength - 1;
            }

            if (start < stop) {
                start = start == -1 ? 0 : start;
            }

            std::string prefix = symbolicPath + ":l:";
            auto iter = rdb->NewIterator(rocksdb::ReadOptions(), pcf);
            int index = 0;
            for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next(), index++) {
                if (start > stop) {
                    rdb->Delete(writeOptions, pcf, iter->key()); // delete all the keys
                    listLength--;
                } else {                                         // keep [start, stop] of list
                    if (index < start) {
                        rdb->Delete(writeOptions, pcf, iter->key());
                        listLength--;
                    } else if (index > stop) {
                        rdb->Delete(writeOptions, pcf, iter->key());
                        listLength--;
                    } else {
                        continue;
                    }
                }
            }
            if (listLength <= 0) {
                rdb->Delete(writeOptions, pcf, keyStoreListLength);
            } else {
                rdb->Put(writeOptions, pcf, keyStoreListLength, std::to_string(listLength));
            }
            delete iter;
        } else {
            result.status = Status::LOOKUP_ERROR;
            return result;
        }
    }
    result.status = Status::OK;
    return result;
}

int64_t RocksdbTree::getKeyExpireTime(const std::string& path)
{
    auto it = expireCache.find(path);
    if(expireCache.end() != it)
    {
        return it->second;
    }
    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }

    std::string key = getMetaKeyOfExpireSetting(path);
    std::string contents = "";
    rocksdb::Status s = rdb->Get(rocksdb::ReadOptions(), pcf, key, &contents);
    if(s.ok())
    {
        return *((const int64_t*)contents.c_str());
    }else{
        return -1;
    }
}

Result
RocksdbTree::read(const std::string& symbolicPath, std::string& contents)
{
    Result result;
    result.status = Status::LOOKUP_ERROR;
    result.error = symbolicPath + " does not exist";

    if (symbolicPath == "") {
        result.status = Status::INVALID_ARGUMENT;
        return result;
    }

    if (symbolicPath == "/") {
        // write to a directory, return TYPE_ERROR
        result.status = Status::TYPE_ERROR;
        return result;
    }

    /*
TODO: should check path legal before reading
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
        */

    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }

    /*
    for (auto p : path.parents) {
        std::cout << "path: " << p << std::endl;
    }
     */

    rocksdb::Status s = rdb->Get(rocksdb::ReadOptions(), pcf, symbolicPath, &contents);
//    VERBOSE("rocksdb get %s", s.ToString().c_str());
    if (s.ok()) {
        result.status = Status::OK;
    }

    return result;
}

Result
RocksdbTree::lrange(const std::string& symbolicPath, const std::vector<std::string>& args, std::vector<std::string>& output)
{
    Result result;
    result.status = Status::LOOKUP_ERROR;
    result.error = symbolicPath + " does not exist";

    if (args.size() != 2) {
        ERROR("Err wrong number of arguments for ltrim command");
        result.status = Status::INVALID_ARGUMENT;
        return result;
    } else {
        //TODO: start and stop can be negative (following redis implementation)
        int start = atoi(args[0].c_str());
        int stop = atoi(args[1].c_str());

        VERBOSE("LRANGE input args: start: %d, stop: %d\n", start, stop);

        if (symbolicPath == "") {
            result.status = Status::INVALID_ARGUMENT;
            return result;
        }

        if (symbolicPath == "/") {
            // write to a directory, return TYPE_ERROR
            result.status = Status::TYPE_ERROR;
            return result;
        }

        /*
TODO:shoudl check path before reading
        Path path(symbolicPath);
        if (path.result.status != Status::OK)
            return path.result;
            */

        ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
        rocksdb::ColumnFamilyHandle* pcf = cfp.get();
        if (NULL == pcf) {
            PANIC("Get cf failed");
        }

        std::string listLengthStr;
        int listLength;
        rocksdb::Status s;
        std::string keyStoreListLength(symbolicPath + ":c");
        s = rdb->Get(readOptions, pcf, keyStoreListLength, &listLengthStr);
        if (s.ok()) {
            listLength = atoi(listLengthStr.c_str());
            VERBOSE("key %s , current length %d\n", keyStoreListLength.c_str(), listLength);

            if (start < 0) {
                start = start + listLength < 0 ? -1 : start + listLength;
            }

            if (stop < 0) {
                stop = stop + listLength < 0 ? -1 : stop + listLength;
            } else if (stop > listLength) {
                stop = listLength - 1;
            }

            if (start > stop) {
                result.status = Status::OK;
            } else {
                start = start == -1 ? 0 : start;
                VERBOSE("get list range: [%d,%d]\n", start, stop);

                std::string content;
                s = rdb->Get(rocksdb::ReadOptions(), pcf, symbolicPath, &content);
                if (s.ok()) {
                    result.status = Status::OK;
                }

                std::string prefix = symbolicPath + ":l:";
                int index = 0;
                auto iter = rdb->NewIterator(rocksdb::ReadOptions(), pcf);
                for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next(), index++) {
                    if (index >= start && index <= stop) {
                        std::string currentContent = iter->value().ToString();
                        output.push_back(currentContent);
                        result.status = Status::OK;
                        if (index == stop) {
                            break;
                        }
                    }
                }
                delete iter;
            }
        } else {
            result.status = Status::LOOKUP_ERROR;
            return result;
        }
    }
    return result;
}

Result
RocksdbTree::head(const std::string& symbolicPath, std::string& contents) const
{

    Result result ;
    return result;
}

Result
RocksdbTree::removeFile(const std::string& symbolicPath)
{
    Result result;
    result.status = Status::OK;

    if (symbolicPath == "") {
        result.status = Status::INVALID_ARGUMENT;
        return result;
    }

    if (symbolicPath == "/") {
        // write to a directory, return TYPE_ERROR
        result.status = Status::TYPE_ERROR;
        return result;
    }

    /*
TODO: shoudl check path legal before remove
    Path path(symbolicPath);
    if (path.result.status != Status::OK)
        return path.result;
*/

    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }

    rdb->Delete(writeOptions, pcf, symbolicPath);
    return result;
}

void RocksdbTree::cleanUpExpireKeyEvent()
{
    auto timeSpec = Core::Time::makeTimeSpec(Core::Time::SystemClock::now());
    long now = timeSpec.tv_sec;
    static std::string lastCheckKey = ":meta:e:";
    
    
    ColumnFamilyHandlePtr cfp = getColumnFamilyHandle("cf0", true);
    rocksdb::ColumnFamilyHandle* pcf = cfp.get();
    if (NULL == pcf) {
        PANIC("Get cf failed");
    }

    std::string contents = "";
    //check 1000 keys in one expire test
    int counter = 1000;

    auto it = rdb->NewIterator(readOptions, pcf);
    for(it->Seek(lastCheckKey);counter > 0 && it->Valid() ; counter-- ,it->Next()){
        if(!it->key().starts_with(":meta:e"))
        {
            //loop back to start if reach end
            lastCheckKey = ":meta:e:";
            break;
        }
        std::string content = it->value().ToString();
        int64_t expireSecond = *((const int64_t*)content.c_str());
        if(expireSecond < now)
        {
            std::string key = it->key().ToString();
            //8 = strlen of ":meta:e:"
            std::string path = key.substr(8);
            VERBOSE("the path :%s, should be expired at :%ld", path.c_str(), expireSecond);
            //TODO: what should I do to make it expired by upper layer
            //or, should i just use the cache to do expie?
//            appendCleanExpireRequestLog(path, expireSecond);
        }
        lastCheckKey = it->key().ToString();
    }
    delete it;
}


void RocksdbTree::startSnapshot(uint64_t lastIncludedIndex) {
    snapshot = (rocksdb::Snapshot*)(rdb->GetSnapshot());
    if (snapshot == NULL) {
        PANIC("Get Snapshot failed");
    }
}

} //Tree
} //Logcabin
