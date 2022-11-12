#ifndef HASH_TABLE_DIRECTORY_PAGE_CC
#define HASH_TABLE_DIRECTORY_PAGE_CC

#include "index/hash/hash_table_directory_page.h"

#include <unordered_map>
#include <iostream>

namespace SimpleDB {

int HashTableDirectoryPage::GetBlockNum() const {
    return block_num_;
}


void HashTableDirectoryPage::SetBlockNum(int block_num) {
    block_num_ = block_num;
}


lsn_t HashTableDirectoryPage::GetLSN() const{
    return lsn_;
}


void HashTableDirectoryPage::SetLSN(lsn_t lsn) {
    lsn_ = lsn;
}


int HashTableDirectoryPage::GetBucketBlockNum(int bucket_idx) {
    SIMPLEDB_ASSERT(bucket_idx < 512, "overflow!");
    return bucket_block_nums_[bucket_idx];
}


void HashTableDirectoryPage::SetBucketBlockNum
(int bucket_idx, int bucket_block_num) {
    SIMPLEDB_ASSERT(bucket_idx < 512, "overflow!");
    bucket_block_nums_[bucket_idx] = bucket_block_num;
}


int HashTableDirectoryPage::GetSplitImageIndex(int bucket_idx) {
    // in split phase, assume that local_depth = k, we only use the 
    // last k bits under index binary to get the block number to 
    // which the key is to be inserted
    // @see extendible hash index to see more

    return bucket_idx ^ (1 << GetLocalDepth(bucket_idx));
}



int HashTableDirectoryPage::GetMergeImageIndex(int bucket_idx) {
    SIMPLEDB_ASSERT(GetLocalDepth(bucket_idx) > 0, 
                    "we can't merge a bucket which local depth is 0");
    return bucket_idx ^ (1 << (GetLocalDepth(bucket_idx) - 1));
}




int HashTableDirectoryPage::GetGlobalDepthMask() {
    return (1 << (global_depth_)) - 1;
}


int HashTableDirectoryPage::GetLocalDepthMask(int bucket_idx) {
    return (1 << (GetLocalDepth(bucket_idx))) - 1;
}


void HashTableDirectoryPage::SetGlobalDepth(int global_depth) {
    global_depth_ = global_depth;
}


int HashTableDirectoryPage::GetGlobalDepth() {
    return global_depth_;
}


void HashTableDirectoryPage::IncrGlobalDepth() {
    global_depth_ ++;
}


void HashTableDirectoryPage::DecrGlobalDepth() {
    global_depth_ --;
}


bool HashTableDirectoryPage::CanShrink() {
    // if all local_depth is less than global depth, 
    // means we can shrink this directory

    int bucket_num = Size();
    for (int i = 0;i < bucket_num; i++) {
        if (GetLocalDepth(i) >= global_depth_) {
            return false;
        }
    }

    return true;
}


int HashTableDirectoryPage::Size() {
    return 1 << global_depth_;
}


int HashTableDirectoryPage::GetLocalDepth(int bucket_idx) {
    return local_depths_[bucket_idx];
}


void HashTableDirectoryPage::SetLocalDepth
(int bucket_idx, uint8_t local_depth) {
    local_depths_[bucket_idx] = local_depth;
}


void HashTableDirectoryPage::IncrLocalDepth(int bucket_idx) {
    local_depths_[bucket_idx] ++;
}


void HashTableDirectoryPage::DecrLocalDepth(int bucket_idx) {
    local_depths_[bucket_idx]--;
    SIMPLEDB_ASSERT(local_depths_[bucket_idx] >= 0, "can't less than 0");
}


int HashTableDirectoryPage::GetLocalHighBit(int bucket_idx) {
    return bucket_idx >> GetLocalDepth(bucket_idx) & 1;
}


void HashTableDirectoryPage::PrintDirectory() {
    printf("======== DIRECTORY (global_depth_: %u) ==========================\n", global_depth_);
    fflush(stdout);
    printf("|    bucket_idx      |    block_num       |    local_depth     |\n");
    fflush(stdout);
    for (int idx = 0; idx < static_cast<int>(0x1 << global_depth_); idx++) {
        printf("|%10u          |%10u          |%10u          |\n", idx, bucket_block_nums_[idx], local_depths_[idx]);
        fflush(stdout);
    }
    printf("================ END DIRECTORY ==================================\n");
      fflush(stdout);
}





} // namespace SimpleDB

#endif