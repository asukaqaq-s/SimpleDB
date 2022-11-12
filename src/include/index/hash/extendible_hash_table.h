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
                        Schema *key_schema,
                        RecoveryManager *rm,
                        BufferManager *bfm, 
                        int directory_block_num);

    ~ExtendibleHashTable();

    
    /**
    * @brief init this extendible hash table
    * delete a bucket and set their bucket_block_num to INVALID
    */
    void InitExtendibleHashTable(Transaction *txn);

    
    /**
    * Performs a point query on the hash table.
    *
    * @param transaction the current transaction
    * @param key the key to look up
    * @param[out] result the value(s) associated with a given key
    * @return the value(s) associated with the given key
    */
    bool GetValue(const Value &key, 
                  std::vector<RID> *result, 
                  Transaction *txn);



    /**
    * Inserts a key-value pair into the hash table.
    *
    * @param transaction the current transaction
    * @param key the key to create
    * @param value the value to be associated with the key
    * @return true if insert succeeded, false otherwise
    */
    bool Insert(const Value &key, 
                const RID &rid, 
                Transaction *txn);


    
    bool Remove(const Value &key, 
                const RID &rid, 
                Transaction *txn);


    int GetDectorySize();

    /**
    * @brief debugging helper function
    */
    bool VerifyHashTable();


    /**
    * @brief debugging helper function
    */
    void PrintHashTable();

private:

    /**
    * @brief First scan the index file to find a deleted page
    * If can't find it, create a new one via NewBlock.
    */
    HashTableBucketPage* CreateBucketPage(Transaction *txn, 
                                          int *new_bucket_block_num);


    /**
    * @brief this function will be used to Merge function.
    */
    void DeleteBucketPage(HashTableBucketPage *bucket);


    /**
    * Performs insertion with an optional bucket splitting.
    *
    * @param transaction a pointer to the current transaction
    * @param key the key to insert
    * @param value the value to insert
    * @return whether or not the insertion was successful
    */
    bool SplitInsert(Transaction *transaction, const Value &key, const RID &value);


    /**
    * Optionally merges an empty bucket into it's pair.  This is called by Remove,
    * if Remove makes a bucket empty.
    *
    * There are three conditions under which we skip the merge:
    * 1. The bucket is no longer empty.
    * 2. The bucket has local depth 0.
    * 3. The bucket's local depth doesn't match its split image's local depth.
    * 
    *
    * @param transaction a pointer to the current transaction
    * @param key the key that was removed
    * @param value the value that was removed
    */
    bool Merge(Transaction *txn, 
               const Value &key, 
               const RID &value);



    int GetIndexByHash(const Value &key);
    
    
    std::string IntToBinary(int x);


    int DeduceKeySize();

private:

    std::string index_file_name_;

    // cache key_size_
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