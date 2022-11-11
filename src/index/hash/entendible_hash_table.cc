#ifndef EXTENDIBLE_HASH_TABLE_CC
#define EXTENDIBLE_HASH_TABLE_CC


#include "index/hash/extendible_hash_table.h"

namespace SimpleDB {


ExtendibleHashTable::ExtendibleHashTable(Transaction *txn,
                                         const std::string &index_name,  
                                         RecoveryManager *rm,
                                         BufferManager *bfm, 
                                         int directory_block_num) 
        : index_file_name_(index_name), rm_(rm), bfm_(bfm) {
    table_latch_.WLock();

    // invalid, need to init this table
    if (directory_block_num == -1) {
        dir_page_ = reinterpret_cast<HashTableDirectoryPage*> 
                    (bfm_->NewBlock(index_file_name_, &directory_block_num)->contents()->GetRawDataPtr());
        InitExtendibleHashTable(txn);
    }
    else {
        dir_page_ = reinterpret_cast<HashTableDirectoryPage*> 
                (bfm_->PinBlock(BlockId(index_file_name_, directory_block_num))->contents()->GetRawDataPtr());
    }

    table_latch_.WUnlock();
}


void ExtendibleHashTable::InitExtendibleHashTable(Transaction *txn) {
    // init this global depth
    dir_page_->SetGlobalDepth(0);
    int new_block_num;
    CreateBucket(txn, &new_block_num);

    // only one bucket exist in newly hash table, 
    // The following code is same to SetBucketBlockNum(0, ...);
    int size = dir_page_->Size();
    for (int i = 0;i < size; i++) {
        dir_page_->SetBucketBlockNum(i, new_block_num);
        dir_page_->SetLocalDepth(i, 0);
    }

    // unpin this table page to avoid error
    // remember set dirty flag
    bfm_->UnpinBlock(BlockId(index_file_name_, new_block_num), true);
}




bool ExtendibleHashTable::GetValue(const SearchKey &key, 
                                   std::vector<RID> *result,
                                   Transaction *txn) {
    table_latch_.RLock();

    // we ensure this index points to correct blocknum
    uint32_t index = GetIndexByHash(key.GetValue());
    int bucket_block_num = dir_page_->GetBucketBlockNum(index);
    
    // unlock immediately to avoid other txns wait too long
    table_latch_.RUnlock();

    // acquire resource
    assert(bucket_block_num >= 0);
    BlockId block(index_file_name_, bucket_block_num);
    txn->LockShared(block);
    auto *bucket = static_cast<HashTableBucketPage*>
                        (bfm_->PinBlock(BlockId(index_file_name_, bucket_block_num)));


    // read data from bucket
    bucket->RLock();
    bool res = bucket->GetValue(key.GetValue(), result);
    bucket->RUnlock();


    // release resource
    bfm_->UnpinBlock(block);
    txn->UnLockWhenRUC(block);

    return res;
}


bool ExtendibleHashTable::Insert(const SearchKey &key, 
                                 const RID &rid, 
                                 Transaction *txn) {
    // just need to acquire reader lock unless we should split
    table_latch_.RLock();
    
    // generate a bucket page and insert k-v pair into it
    uint32_t index = GetIndexByHash(key.GetValue());
    int bucket_block_num = dir_page_->GetBucketBlockNum(index);

    table_latch_.RUnlock();


    // require resource
    BlockId block(index_file_name_, bucket_block_num);
    txn->LockExclusive(block);
    auto *bucket = static_cast<HashTableBucketPage*> (bfm_->PinBlock(block));
    

    // insert into it
    bucket->WLock();
    bool res = bucket->Insert(key.GetValue(), rid);
    bool is_full = bucket->IsFull();
    
    // release resource
    bucket->WUnlock();
    bfm_->UnpinBlock(block, false);
    txn->UnLockWhenRUC(block);


    // insert error which has duplicate k-v pair
    if (!res && !is_full) {
        return false;
    }


    // if insert failed, we should split this bucket until insert is successful
    while (!res) {

        // if we can't split any more, return false
        if (dir_page_->Size() == DIRECTORY_ARRAY_SIZE) {
            return false;
        }

        res = SplitInsert(txn, key, rid);
    }
    
    return true;
}


bool ExtendibleHashTable::SplitInsert(Transaction *txn, 
                                      const SearchKey &key, 
                                      const RID &value) {
    table_latch_.WLock();
    
    // generate a bucket page and insert k-v pair into it
    uint32_t index = GetIndexByHash(key.GetValue());
    int bucket_block_num = dir_page_->GetBucketBlockNum(index);
    
    // require resource
    BlockId block(index_file_name_, bucket_block_num);
    txn->LockExclusive(block);
    auto *bucket = static_cast<HashTableBucketPage*> (bfm_->PinBlock(block));

    
    int old_hash_bits = dir_page_->GetLocalDepth(index);
    int old_hash_mask = dir_page_->GetLocalDepthMask(index);
    int old_local_depth = dir_page_->GetLocalDepth(index);
    int split_image_index = dir_page_->GetSplitImageIndex(index);
    int new_local_depth = old_local_depth + 1;
    int new_hash_mask = (1 << (new_local_depth)) - 1;
    int max_tuple_count = bucket->GetSize();

    
    // change the local depth of all direcorty_ids which point to this bucket
    int size = dir_page_->Size();
    for (int i = 0;i < size; i++) {
        if (dir_page_->GetBucketBlockNum(i) == bucket_block_num) {
            SIMPLEDB_ASSERT(i & old_hash_mask == index & old_hash_mask, 
                            "logic error");
            dir_page_->SetLocalDepth(i, new_local_depth);
        }
    }


    if (new_local_depth > dir_page_->GetGlobalDepth()) {
        SIMPLEDB_ASSERT(new_local_depth == dir_page_->GetGlobalDepth() + 1,
                        "split insert: unexpectable error");
        dir_page_->IncrGlobalDepth();
    }


    // tmp array  
    std::vector<KVPair> tmp_array;
    
    // get tmp value which hash_index & old_mask != hash_index & new_mask 
    for (int i = 0;i < max_tuple_count; i++) {
        auto value_i = bucket->KeyAt(i);
        uint32_t index_i = GetIndexByHash(value_i);

        if (index_i & old_hash_mask != index_i & new_hash_mask) {
            SIMPLEDB_ASSERT(index & new_hash_mask == split_image_index, 
                            "unexpectable split image index ");
            
            KVPair p = {value_i, bucket->ValueAt(i)};
            tmp_array.emplace_back(p);
            bucket->RemoveAt(i);
        }
    }

    
    // create a new bucket 
    int new_bucket_block_num;
    int new_size = dir_page_->Size();
    auto *new_bucket = CreateBucket(txn, &new_bucket_block_num);

    
    // update block number of direcotry 
    for (int i = 0;i < new_size; i++) {
        if (i & new_hash_mask == split_image_index) {
            // point to new bucket
            dir_page_->SetBucketBlockNum(i, new_bucket_block_num);    
        }
    }
    

    new_bucket->WLock();
    // insert data into new bucket
    for (auto kv_tmp : tmp_array) {
        new_bucket->Insert(kv_tmp.first, kv_tmp.second);    
    }
    new_bucket->WUnlock();
    

    // release source
    bfm_->UnpinBlock(block, true);
    bfm_->UnpinBlock(BlockId(index_file_name_, new_bucket_block_num), true);


    table_latch_.WUnlock();
}




uint32_t ExtendibleHashTable::GetIndexByHash(const Value &key) {
    std::hash<std::string> hsh;
    int size = dir_page_->Size();
    return hsh(key.to_string()) % size;
}


HashTableBucketPage* ExtendibleHashTable::CreateBucket
(Transaction *txn, int *new_bucket_block_num) {

    auto bucket = static_cast<HashTableBucketPage*>
                  (bfm_->NewBlock(index_file_name_, new_bucket_block_num));
    txn->LockExclusive(BlockId(index_file_name_, *new_bucket_block_num));
    auto type = key_schema_->GetColumn(0).GetType();
    int tuple_size = key_size_ + 8;

    bucket->InitHashBucketPage(tuple_size, type);   
}


} // namespace SimpleDB


#endif