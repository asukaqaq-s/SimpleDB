#ifndef EXTENDIBLE_HASH_TABLE_H
#define EXTENDIBLE_HASH_TABLE_H

#include "index/hash/hash_table_bucket_page.h"
#include "index/hash/hash_table_directory_page.h"
#include "index/search_key.h"
#include "concurrency/transaction.h"

namespace SimpleDB {


class ExtendibleHashTable {

    using KVPair = std::pair<Value, RID>;


public:

    ExtendibleHashTable(Transaction *txn,
                        const std::string &index_name, 
                        RecoveryManager *rm,
                        BufferManager *bfm, 
                        int directory_block_num);

    ~ExtendibleHashTable();

    /**
    * @brief init this extendible hash table
    */
    void InitExtendibleHashTable(Transaction *txn);


    bool GetValue(const SearchKey &key, 
                  std::vector<RID> *result, 
                  Transaction *txn);

    /**
    * @brief 
    * 
    * @return true if insert is successful, false if hash table or bucket full.
    */
    bool Insert(const SearchKey &key, 
                const RID &rid, 
                Transaction *txn);


    bool Remove(Transaction *txn, const SearchKey &key, const RID &rid);


private:

    
    HashTableBucketPage* CreateBucket(Transaction *txn, int *new_bucket_block_num);

    /**
    * Performs insertion with an optional bucket splitting.
    *
    * @param transaction a pointer to the current transaction
    * @param key the key to insert
    * @param value the value to insert
    * @return whether or not the insertion was successful
    */
    bool SplitInsert(Transaction *transaction, const SearchKey &key, const RID &value);

    /**
    * Optionally merges an empty bucket into it's pair.  This is called by Remove,
    * if Remove makes a bucket empty.
    *
    * There are three conditions under which we skip the merge:
    * 1. The bucket is no longer empty.
    * 2. The bucket has local depth 0.
    * 3. The bucket's local depth doesn't match its split image's local depth.
    *
    * @param transaction a pointer to the current transaction
    * @param key the key that was removed
    * @param value the value that was removed
    */
    void Merge(Transaction *transaction, const SearchKey &key, const RID &value);


    uint32_t GetIndexByHash(const Value &key);
    



private:

    std::string index_file_name_;

    int key_size_{0};

    Schema *key_schema_;
    
    RecoveryManager *rm_;

    BufferManager *bfm_;

    int directory_block_num{0};


    // cache directory page is useful when multiply access in the same time.
    HashTableDirectoryPage *dir_page_;

    // Readers includes inserts and removes, 
    // writers are splits and merges
    ReaderWriterLatch table_latch_;


};


}


#endif