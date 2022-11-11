#ifndef HASH_TABLE_DIRECTORY_PAGE_CC
#define HASH_TABLE_DIRECTORY_PAGE_CC

#include "index/hash/hash_table_directory_page.h"

#include <unordered_map>

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


int HashTableDirectoryPage::GetBucketBlockNum(uint32_t bucket_idx) {
    SIMPLEDB_ASSERT(bucket_idx < 512, "overflow!");
    return bucket_block_nums_[bucket_idx];
}


void HashTableDirectoryPage::SetBucketBlockNum
(uint32_t bucket_idx, int bucket_block_num) {
    SIMPLEDB_ASSERT(bucket_idx < 512, "overflow!");
    bucket_block_nums_[bucket_idx] = bucket_block_num;
}


uint32_t HashTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) {
    // in split phase, assume that local_depth = k, we only use the 
    // last k bits under index binary to get the block number to 
    // which the key is to be inserted
    // @see extendible hash index to see more

    return bucket_idx ^(1 << GetLocalDepth(bucket_idx));
}


uint32_t HashTableDirectoryPage::GetGlobalDepthMask() {
    return (1 << (global_depth_ + 1)) - 1;
}


uint32_t HashTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) {
    return (1 << (GetLocalDepth(bucket_idx + 1))) - 1;
}


void HashTableDirectoryPage::SetGlobalDepth(uint32_t global_depth) {
    global_depth_ = global_depth;
}


uint32_t HashTableDirectoryPage::GetGlobalDepth() {
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
            return true;
        }
    }

    return false;
}


uint32_t HashTableDirectoryPage::Size() {
    return 1 << global_depth_;
}


uint32_t HashTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) {
    return local_depths_[bucket_idx];
}


void HashTableDirectoryPage::SetLocalDepth
(uint32_t bucket_idx, uint8_t local_depth) {
    local_depths_[bucket_idx] = local_depth;
}


void HashTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
    local_depths_[bucket_idx] ++;
}


void HashTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
    local_depths_[bucket_idx]--;
    SIMPLEDB_ASSERT(local_depths_[bucket_idx] >= 0, "can't less than 0");
}


uint32_t HashTableDirectoryPage::GetLocalHighBit(uint32_t bucket_idx) {
    return bucket_idx >> GetLocalDepth(bucket_idx) & 1;
}


void HashTableDirectoryPage::PrintDirectory() {
    printf("======== DIRECTORY (global_depth_: %u) ========\n", global_depth_);
    fflush(stdout);
    printf("| bucket_idx | block_num | local_depth |\n");
    fflush(stdout);
    for (uint32_t idx = 0; idx < static_cast<uint32_t>(0x1 << global_depth_); idx++) {
        printf("|      %u     |     %u     |     %u     |\n", idx, bucket_block_nums_[idx], local_depths_[idx]);
        fflush(stdout);
    }
    printf("================ END DIRECTORY ================\n");
      fflush(stdout);
}





} // namespace SimpleDB

#endif