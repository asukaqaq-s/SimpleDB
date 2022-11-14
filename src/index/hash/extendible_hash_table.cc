#ifndef EXTENDIBLE_HASH_TABLE_CC
#define EXTENDIBLE_HASH_TABLE_CC


#include "index/hash/extendible_hash_table.h"

namespace SimpleDB {


ExtendibleHashTable::ExtendibleHashTable(Transaction *txn,
                                         const std::string &index_name,  
                                         Schema *key_schema,
                                         RecoveryManager *rm,
                                         BufferManager *bfm, 
                                         int directory_block_num) 
        : index_file_name_(index_name),key_schema_(key_schema), 
          rm_(rm), bfm_(bfm) {
    table_latch_.WLock();

    key_size_ = DeduceKeySize();

    // invalid, need to init this table
    if (directory_block_num == INVALID_BLOCK_NUM) {
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


ExtendibleHashTable::~ExtendibleHashTable() {
    bfm_->UnpinBlock({index_file_name_, directory_block_num}, true);
}



void ExtendibleHashTable::InitExtendibleHashTable(Transaction *txn) {
    // init this global depth
    dir_page_->SetGlobalDepth(0);
    int new_block_num;
    CreateBucketPage(txn, &new_block_num);

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




bool ExtendibleHashTable::GetValue(const Value &key, 
                                   std::vector<RID> *result,
                                   Transaction *txn) {
    table_latch_.RLock();

    // we ensure this index points to correct blocknum
    int index = GetIndexByHash(key);
    int bucket_block_num = dir_page_->GetBucketBlockNum(index);

    // acquire resource
    assert(bucket_block_num >= 0);
    BlockId block(index_file_name_, bucket_block_num);
    txn->LockShared(block);
    auto *bucket = static_cast<HashTableBucketPage*>
                   (bfm_->PinBlock(BlockId(index_file_name_, bucket_block_num)));


    // read data from bucket
    bucket->RLock();
    bool res = bucket->GetValue(key, result);
    bucket->RUnlock();


    // release resource
    bfm_->UnpinBlock(block);
    txn->UnLockWhenRUC(block);
    table_latch_.RUnlock();

    return res;
}


bool ExtendibleHashTable::Insert(const Value &key, 
                                 const RID &rid, 
                                 Transaction *txn) {
    // just need to acquire reader lock unless we should split
    table_latch_.RLock();
    
    // generate a bucket page and insert k-v pair into it
    int index = GetIndexByHash(key);
    int bucket_block_num = dir_page_->GetBucketBlockNum(index);

    // require resource
    BlockId block(index_file_name_, bucket_block_num);
    txn->LockExclusive(block);
    auto *bucket = static_cast<HashTableBucketPage*> (bfm_->PinBlock(block));
    

    // insert into it
    bucket->WLock();
    bool res = bucket->Insert(key, rid);
    bool is_full = false;

    
    // only check if full when insert error
    if (!res) {
        is_full = bucket->IsFull();
    }
    

    // release resource
    bucket->WUnlock();
    bfm_->UnpinBlock(block, true);
    table_latch_.RUnlock();

    // insert error which has duplicate k-v pair
    if (!res && !is_full) {
        return false;
    }



    // if insert failed, we should split this bucket until insert is successful
    while (!res) {
        table_latch_.RLock();
        bool is_cannot_split = (dir_page_->GetLocalDepth(index) == 9);
        table_latch_.RUnlock();

        // if we can't split any more, return false
        if (is_cannot_split) {
            return false;
        }
        
        table_latch_.WLock();
        res = SplitInsert(txn, key, rid);
        table_latch_.WUnlock();
    }


    return true;
}


bool ExtendibleHashTable::SplitInsert(Transaction *txn, 
                                      const Value &key, 
                                      const RID &value) {

    

    // generate a bucket page and insert k-v pair into it
    int index = GetIndexByHash(key);
    int bucket_block_num = dir_page_->GetBucketBlockNum(index);
    
    
    // can't insert
    if (dir_page_->GetLocalDepth(index) == 9 ) {
        return false;
    }

    
    // require resource
    BlockId block(index_file_name_, bucket_block_num);
    txn->LockExclusive(block);
    auto *bucket = static_cast<HashTableBucketPage*> (bfm_->PinBlock(block));


    
    int old_local_depth = dir_page_->GetLocalDepth(index);
    int old_hash_mask = dir_page_->GetLocalDepthMask(index);
    int old_hash_index = index & old_hash_mask;
    int old_global_hash_mask = dir_page_->GetGlobalDepthMask();
    int new_local_depth = old_local_depth + 1;
    int new_hash_mask = (1 << (new_local_depth)) - 1;
    int split_image_index = dir_page_->GetSplitImageIndex(old_hash_index);
    int max_tuple_count = bucket->GetSize();


    // update the local depth of all direcorty_ids which point to this bucket
    int old_size = dir_page_->Size();
    for (int i = 0;i < old_size; i++) {
        if (dir_page_->GetBucketBlockNum(i) == bucket_block_num) {
            SIMPLEDB_ASSERT((i & old_hash_mask) == old_hash_index, 
                            "These indexes should have the same hash_index value");
            SIMPLEDB_ASSERT(dir_page_->GetLocalDepth(i) == old_local_depth,
                            "local depth incorrect");
            dir_page_->SetLocalDepth(i, new_local_depth);
        }
    }


    if (new_local_depth > dir_page_->GetGlobalDepth()) {
        SIMPLEDB_ASSERT(new_local_depth == dir_page_->GetGlobalDepth() + 1,
                        "split insert: unexpectable error");
        dir_page_->IncrGlobalDepth();
        SIMPLEDB_ASSERT(dir_page_->Size() <= DIRECTORY_ARRAY_SIZE, 
                        "directory array can't overflow!");


        // if we want to expand this directory, half of the new index 's 
        // information need be updated, such as their local depth and bucket block num
        int new_size = dir_page_->Size();
        for (int i = old_size;i < new_size; i++) {
            // new index points to old index's block_num
            int old_index_i = i & old_global_hash_mask;
            int bucket_block_num_i = dir_page_->GetBucketBlockNum(old_index_i);
            int local_depth_i = dir_page_->GetLocalDepth(old_index_i);

            dir_page_->SetBucketBlockNum(i, bucket_block_num_i);
            dir_page_->SetLocalDepth(i, local_depth_i);
        }
    }


    // tmp array  
    std::vector<KVPair> tmp_array;
    

    // get tmp value which hash_index & old_mask != hash_index & new_mask 
    for (int i = 0;i < max_tuple_count; i++) {
        if (!bucket->IsOccupied(i)) {
            break;
        }

        if (!bucket->IsReadable(i)) {
            continue;
        }


        auto value_i = bucket->KeyAt(i);
        int index_i = GetIndexByHash(value_i); // since global_depth may change

        if ((index_i & old_hash_mask) != (index_i & new_hash_mask)) {
            SIMPLEDB_ASSERT((index_i & new_hash_mask) == split_image_index, 
                            "unexpectable hash index ");
            
            KVPair p = {value_i, bucket->ValueAt(i)};
            tmp_array.emplace_back(p);
            bucket->RemoveAt(i);
        }
    }


    
    // create a new bucket 
    int new_bucket_block_num;
    int new_size = dir_page_->Size();
    auto *new_bucket = CreateBucketPage(txn, &new_bucket_block_num);

    
    // update block number of direcotry 
    for (int i = 0;i < new_size; i++) {
        if ((i & new_hash_mask) == split_image_index) {
            SIMPLEDB_ASSERT((i & old_hash_mask) == old_hash_index, "logic error");
            SIMPLEDB_ASSERT(dir_page_->GetLocalDepth(i) == new_local_depth, "logic error");

            // point to new bucket
            dir_page_->SetBucketBlockNum(i, new_bucket_block_num);    
        }
    }
    


    // insert data into new bucket
    for (auto kv_tmp : tmp_array) {
        new_bucket->Insert(kv_tmp.first, kv_tmp.second);    
    }

    

    bool is_success = false;

    // try to insert into this block
    // recacluate index because global depth may be changed
    index = GetIndexByHash(key);
    int new_hash_index = index  & new_hash_mask;
    if (new_hash_index == old_hash_index) {
        is_success = bucket->Insert(key, value);
    }
    else if (new_hash_index == split_image_index) {
        is_success = new_bucket->Insert(key, value);
    }
    else { assert(false); }


    // release source
    bfm_->UnpinBlock(block, true);
    bfm_->UnpinBlock(BlockId(index_file_name_, new_bucket_block_num), true);



    return is_success;
}



bool ExtendibleHashTable::Remove(const Value &key, 
                                 const RID &rid, 
                                 Transaction *txn) {
    // just need to acquire reader lock unless we should merge
    table_latch_.RLock();
    
    // generate a bucket page and try removing something in this bucket
    int index = GetIndexByHash(key);
    int bucket_block_num = dir_page_->GetBucketBlockNum(index);

    

    // require resource
    BlockId block(index_file_name_, bucket_block_num);
    txn->LockExclusive(block);
    auto *bucket = static_cast<HashTableBucketPage*> (bfm_->PinBlock(block));
    

    // try removing
    bucket->WLock();
    bool res = bucket->Remove(key, rid);
    bool is_empty = bucket->IsEmpty();
    bucket->WUnlock();


    // release buffer heretable_latch_
    bfm_->UnpinBlock(block, true);
    table_latch_.RUnlock();


    // whether remove is successful or not, 
    // try to merge as long as it is empty
    while (is_empty) {
        table_latch_.WLock();
        is_empty = Merge(txn, key, rid);
        table_latch_.WUnlock();
    }

    return res;
}


bool ExtendibleHashTable::Merge(Transaction *txn, 
                                const Value &key, 
                                const RID &value) {

    int index = GetIndexByHash(key);
    int bucket_block_num = dir_page_->GetBucketBlockNum(index);
    

    // require resource
    BlockId block(index_file_name_, bucket_block_num);
    txn->LockExclusive(block);
    auto *bucket = static_cast<HashTableBucketPage*> (bfm_->PinBlock(block));
    
    
    if (dir_page_->GetLocalDepth(bucket_block_num) == 0 || 
        !bucket->IsEmpty()) {
        bfm_->UnpinBlock(block, false);
        return false;
    }

    
    int old_local_depth = dir_page_->GetLocalDepth(index);
    int old_hash_mask = dir_page_->GetLocalDepthMask(index);
    int old_hash_index = index & old_hash_mask;
    int merge_image_index = dir_page_->GetMergeImageIndex(old_hash_index);
    int merge_image_bucket_num = dir_page_->GetBucketBlockNum(merge_image_index);



    if (dir_page_->GetLocalDepth(merge_image_index) != old_local_depth) {
        bfm_->UnpinBlock(block, false);
        return false;
    }



    // currently, we can merge two buckets!
    // acquire bucket page of merge image 
    BlockId merge_image_block(index_file_name_, merge_image_bucket_num);
    txn->LockExclusive(block);
    auto *new_bucket = static_cast<HashTableBucketPage*> 
                      (bfm_->PinBlock(merge_image_block));
    

    // new_bucket_block_num indicates which block should these 
    // hash_index point to after the merge 
    int old_index_high_bit = (old_hash_index >> (old_local_depth - 1)) & 1;
    int new_bucket_block_num = 0;
    int be_deleted_block_num = 0;


    // move k-v pair
    if (old_index_high_bit) {
        // don't need to move, since this bucket is empty
        new_bucket_block_num = merge_image_bucket_num;
        be_deleted_block_num = bucket_block_num;
    }
    else {
        
        std::vector<KVPair> tmp_array;
        int max_tuple_count = new_bucket->GetSize();
        for (int i = 0;i < max_tuple_count; i++) {
            if (!new_bucket->IsOccupied(i)) {
                break;
            }

            if (!new_bucket->IsReadable(i)) {
                continue;
            }

            KVPair p = {new_bucket->KeyAt(i), new_bucket->ValueAt(i)};
            tmp_array.emplace_back(p);
            new_bucket->RemoveAt(i);
        }


        // insert merge_image's data into bucket
        for (auto t:tmp_array) {
            assert(bucket->Insert(t.first, t.second));
        } 

        new_bucket_block_num = bucket_block_num;
        be_deleted_block_num = merge_image_bucket_num;
    }


    
    // update directory's local_depth table and block_num table
    int directory_size = dir_page_->Size();
    for (int i = 0;i < directory_size; i++) {
        if (dir_page_->GetBucketBlockNum(i) == be_deleted_block_num) {
            dir_page_->SetBucketBlockNum(i, new_bucket_block_num);
            dir_page_->DecrLocalDepth(i);
        }
        else if (dir_page_->GetBucketBlockNum(i) == new_bucket_block_num) {
            dir_page_->DecrLocalDepth(i);
        }
    }


    // check if need to shrink the directory
    if (old_local_depth == dir_page_->GetGlobalDepth() &&
        dir_page_->CanShrink()) {

        // shrink the directory and clear local_depth table and block_num table
        dir_page_->DecrGlobalDepth();
        int directory_size = dir_page_->Size();
        for (int i = directory_size; i < 2 * directory_size; i++) {
            // clear
            dir_page_->SetLocalDepth(i, 0);
            dir_page_->SetBucketBlockNum(i, 0);
        }
    }


    if (be_deleted_block_num == merge_image_bucket_num) {
        DeleteBucketPage(new_bucket);
    }
    else {
        DeleteBucketPage(bucket);
    }

    
    bfm_->UnpinBlock(block, true);
    bfm_->UnpinBlock(merge_image_block, true);
    return true;
}



int ExtendibleHashTable::GetDectorySize() {
    return dir_page_->Size();
}



void ExtendibleHashTable::PrintHashTable() {
    dir_page_->PrintDirectory();
    // std::unordered_map<int,int> mp;
    int dir_size =  dir_page_->Size();

    for (int i = 0;i < dir_size; i++) {
        //mp.find(dir_page_->GetBucketBlockNum(i)) == mp.end()
        // if (true) {
        //     // mp[dir_page_->GetBucketBlockNum(i)] ++;
            int block_num = dir_page_->GetBucketBlockNum(i);
            auto *bucket = static_cast<HashTableBucketPage*> 
                 (bfm_->PinBlock(BlockId(index_file_name_, block_num)));
            std::cout << "bucket block num = " << block_num << "  " << std::flush;
            bucket->PrintBucket();
            bfm_->UnpinBlock(bucket);
        // }
    }
    std::cout << "\n\n" << std::endl;
}



int ExtendibleHashTable::GetIndexByHash(const Value &key) {
    std::hash<std::string> hsh;
    int size = dir_page_->Size();
    return hsh(key.to_string()) % size;
}


HashTableBucketPage* ExtendibleHashTable::CreateBucketPage
(Transaction *txn, int *new_bucket_block_num) {
    // assume that you have granted writer latch in directory before entering this func

    // a map is used to indicate how many blocks of the file 
    // are not being used
    std::unordered_map<int, int> index_block_nums;

    
    // first of all, add all blocks of this file into map
    auto *file_manager = txn->GetFileManager();
    int file_block_nums = file_manager->GetFileBlockNum(index_file_name_);
    for (int i = 0;i < file_block_nums; i++) {
        index_block_nums[i]++;
    }


    // reduce io cost by scaning directory, if a block is used 
    // as a bucket in direcotry, it will not be made to a new bucket
    index_block_nums.erase(directory_block_num);
    int directory_size = dir_page_->Size();
    for (int i = 0;i < directory_size; i++) {
        int be_used_block_num = dir_page_->GetBucketBlockNum(i);
        index_block_nums.erase(be_used_block_num);
    }


    int found_block_num = INVALID_BLOCK_NUM;
    
    // only scan these blocks, check if their block_num is INVALID_BLOCK_NUM
    for (auto t:index_block_nums) {
        auto *bucket = static_cast<HashTableBucketPage*> 
                       (bfm_->PinBlock(BlockId(index_file_name_, t.first)));
        bool is_invalid = (bucket->GetPageType() == PageType::DEFAULT_PAGE_TYPE);
        bfm_->UnpinBlock(BlockId(index_file_name_, t.first), false);


        if (is_invalid) {
            found_block_num = t.first;
            break;
        }
    }

    HashTableBucketPage *bucket = nullptr;


    // check if find is successful
    if (found_block_num != INVALID_BLOCK_NUM) {
        bucket = static_cast<HashTableBucketPage*>
                  (bfm_->PinBlock(BlockId(index_file_name_, found_block_num)));
        *new_bucket_block_num = found_block_num;
    }
    else {
        // otherwise, cate a new block
        bucket = static_cast<HashTableBucketPage*>
                (bfm_->NewBlock(index_file_name_, new_bucket_block_num));
    }


    txn->LockExclusive(BlockId(index_file_name_, *new_bucket_block_num));
    auto type = key_schema_->GetColumn(0).GetType();
    int tuple_size = key_size_ + 8;

    
    bucket->InitHashBucketPage(tuple_size, type);   
    return bucket;
}


std::string ExtendibleHashTable::IntToBinary(int x) {
    std::string s;
    while (x) {
        s.push_back((x & 1) + '0');
        x >>= 1;
    }
    if (s.empty())
        s.push_back('0');
    std::reverse(s.begin(), s.end());
    return s;
}



void ExtendibleHashTable::DeleteBucketPage(HashTableBucketPage *bucket) {
    bucket->SetPageType(PageType::DEFAULT_PAGE_TYPE);
}



bool ExtendibleHashTable::VerifyHashTable() {
    int global_depth = dir_page_->GetGlobalDepth();
    int size = dir_page_->Size();

    for (int i = 0;i < size; i++) {
        if (dir_page_->GetLocalDepth(i) > global_depth) {
            return false;
        }
    }


    for (int i = 0;i < size; i++) {
        int hash_index = i & dir_page_->GetLocalDepthMask(i);
        if (dir_page_->GetBucketBlockNum(hash_index) != dir_page_->GetBucketBlockNum(i)) {
            return false;
        }
    }



    return true;
}


int ExtendibleHashTable::DeduceKeySize() {
    return key_schema_->GetColumn(0).GetLength();
}




} // namespace SimpleDB


#endif