#ifndef STATIC_HASH_TABLE_CC
#define STATIC_HASH_TABLE_CC

#include "index/hash/static_hash_table.h"


#include <functional>

namespace SimpleDB {

StaticHashTable::StaticHashTable(const std::string &index_name, 
                                 Schema *kv_schema,
                                 RecoveryManager *rm) 
            : index_name_(index_name), 
              kv_schema_(kv_schema), 
              rm_(rm) {}


bool StaticHashTable::Read(Transaction *txn, 
                           const Value& key, 
                           std::vector<RID> *result) {
    auto bucket_file = GenerateBucketFileName(key);
    auto table_heap = GenerateBucketFile(txn, bucket_file);
    auto table_iter = table_heap->Begin(txn);
    bool has_found = false;

    while (!table_iter.IsEnd()) {
        Tuple tuple = *table_iter;
        table_iter++;

        if (tuple.GetValue("key", *kv_schema_) == key) {
            int block = tuple.GetValue("block", *kv_schema_).AsInt();
            int slot = tuple.GetValue("slot", *kv_schema_).AsInt();
            result->emplace_back(RID(block, slot));
            has_found = true;
        }
    }

    return has_found;
}



void StaticHashTable::Insert(Transaction *txn, 
                             const Value& key, 
                             const RID& rid) {
    auto bucket_file = GenerateBucketFileName(key);
    auto table_heap = GenerateBucketFile(txn, bucket_file);
    Tuple tuple = GenerateInsertTuple(key, rid);

    // insert tuple to this table
    table_heap->Insert(txn, tuple, nullptr);    
}


bool StaticHashTable::Remove(Transaction *txn, 
                             const Value &key, 
                             const RID& rid) {
    auto bucket_file = GenerateBucketFileName(key);
    auto table_heap = GenerateBucketFile(txn, bucket_file);
    auto table_iter = table_heap->Begin(txn);                      

    // assume that one k-v pair only stored once
    while (!table_iter.IsEnd()) {
        Tuple tuple = *table_iter;
        table_iter++;

        if (tuple.GetValue("key", *kv_schema_) == key) {
            int block = tuple.GetValue("block", *kv_schema_).AsInt();
            int slot = tuple.GetValue("slot", *kv_schema_).AsInt();
            if (block == rid.GetBlockNum() && slot == rid.GetSlot()) {
                // use tuple's rid instead param rid
                table_heap->Delete(txn, tuple.GetRID());
                return true;
            }
        }
    }

    return false;
}



std::string StaticHashTable::GenerateBucketFileName(const Value &key) {
    std::hash<std::string> hs;
    int hash_code = hs(key.to_string()) % BUCKET_NUM;
    
    return index_name_ + ".bucket_" + std::to_string(hash_code);
}


TableHeap* StaticHashTable::GenerateBucketFile
(Transaction*txn, const std::string &bucket_name) {
    if (buckets_.find(bucket_name) == buckets_.end()) {
        assert(txn);
        auto file_manager = txn->GetFileManager();
        auto buffer_manager = txn->GetBufferManager();
        buckets_[bucket_name] =  std::make_unique<TableHeap>
                                 (txn, bucket_name, file_manager, rm_, buffer_manager);
    }

    return buckets_[bucket_name].get();
}


Tuple StaticHashTable::GenerateInsertTuple(const Value &key, RID rid) {
    std::vector<Value> values 
    {
        // key
        key,
        // value
        Value(rid.GetBlockNum()),
        Value(rid.GetSlot())
    };

    return Tuple(values, *kv_schema_);
}

}


#endif