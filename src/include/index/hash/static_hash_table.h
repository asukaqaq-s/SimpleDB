#ifndef STATIC_HASH_TABLE_H
#define STATIC_HASH_TABLE_H

#include "type/value.h"
#include "index/search_key.h"
#include "recovery/recovery_manager.h"
#include "record/table_heap.h"


namespace SimpleDB {
    
/**
* @brief static hashtable use tableheap to implement read and write op.
* every buckets will be stored as a file and a bucket can unlimited growth.
* 
* Maybe we can store all buckets into one file. But if think about it, it's
* easy to make phantom reads.
*/
class StaticHashTable {

    static constexpr int BUCKET_NUM = 100;

public:

    
    StaticHashTable(const std::string &index_name, 
                    Schema* kv_schema, 
                    RecoveryManager *rm);

    /**
    * @brief read op
    * 
    * @param txn execution context
    * @param key insert key
    * @param result rid array
    */
    bool Read(Transaction *txn, const Value& key, std::vector<RID> *result);


    /**
    * @brief insert a key-value pair into bucket
    */
    void Insert(Transaction *txn, const Value& key, const RID& rid);


    /**
    * @brief remove a key-value pair which satisfy request 
    */
    bool Remove(Transaction *txn, const Value &key, const RID& rid);
    

private:

    /**
    * @brief get a bucket number by hash 
    */
    std::string GenerateBucketFileName(const Value &key);

    /**
    * @brief create a tableheap and cache it
    */
    TableHeap* GenerateBucketFile(Transaction *txn, const std::string &bucket_name);

    /**
    * @brief helper function in insert
    */
    Tuple GenerateInsertTuple(const Value& key, RID rid);



private:


    std::string index_name_;

    // key schema, format:
    // [key, block, slot]
    // key is a string which includes many values 
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