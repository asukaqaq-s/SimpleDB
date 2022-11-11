#ifndef STATIC_HASH_TABLE_H
#define STATIC_HASH_TABLE_H

#include "type/value.h"
#include "index/search_key.h"
#include "recovery/recovery_manager.h"
#include "record/table_heap.h"


namespace SimpleDB {
    
/**
* @brief static hashtable use tableheap to implement read and write data.
* every buckets will be stored as a file.and a bucket can unlimited growth
* maybe we can store all buckets into one file.But if think about it, it's
* easy to make phantom reads.
*/
class StaticHashTable {

    static constexpr int BUCKET_NUM = 100;

public:

    StaticHashTable(const std::string &index_name, 
                    Schema* kv_schema, 
                    RecoveryManager *rm);

    bool Read(Transaction *txn, const SearchKey& key, std::vector<RID> *result);

    void Insert(Transaction *txn, const SearchKey& key, const RID& rid);

    bool Remove(Transaction *txn, const SearchKey &key, const RID& rid);
    

private:

    std::string GenerateBucketFileName(const SearchKey &key);

    TableHeap* GenerateBucketFile(Transaction *txn, const std::string &bucket_name);

    Tuple GenerateInsertTuple(const SearchKey& key, RID rid);



private:


    std::string index_name_;

    // key schema, format:
    // [key, block, slot]
    Schema *kv_schema_;

    // i think index should have their own logrecord to ensure datastructure correctly
    // TODO:
    RecoveryManager *rm_;


    // maybe we can cache tableheap to avoid multipy access
    // since there will be 100 buckets at most, i think it's useful.
    std::map<std::string, std::unique_ptr<TableHeap>> buckets_;
};

} // namespace SimpleDB



#endif