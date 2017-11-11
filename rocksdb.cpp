#include <rocksdb/db.h>
#include <rocksdb/statistics.h>
#include <rocksdb/snapshot.h>
#include <rocksdb/options.h>
#include <rocksdb/iterator.h>
#include <unistd.h>
#include <string>

int main(int argc, char *argv[])
{
    std::string fsmDir = "./rocksdb-fsm";
    std::string cfName = "cf0";
    rocksdb::Options options;
    rocksdb::DB* rdb;
    options.create_if_missing = true;
    options.max_open_files = 1024;
    options.statistics = rocksdb::CreateDBStatistics();

    rocksdb::Status status;
    std::vector<std::string> column_families;
    status = rocksdb::DB::ListColumnFamilies(options, fsmDir, &column_families);
    status = rocksdb::DB::Open(options, fsmDir, &rdb);
    rocksdb::ColumnFamilyOptions cf_options;
    rocksdb::ColumnFamilyHandle* cfh = NULL;
    rocksdb::Status s = rdb->CreateColumnFamily(cf_options, cfName, &cfh);
    auto writeOptions = rocksdb::WriteOptions();
    while(true){
        for(long long i = 0 ; i < 100000; i++){
            auto writeResult = rdb->Put(writeOptions, cfh, "abc" + std::to_string(i), "def");
        }
    }
    return 0;
}
