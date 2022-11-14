#ifndef BPLUS_TREE_BUCKET_PAGE_CC
#define BPLUS_TREE_BUCKET_PAGE_CC

#include "index/btree/b_plus_tree_bucket_page.h"


namespace SimpleDB {


void BPlusTreeBucketPage::Init() {
    memset(&lsn_, 0, SIMPLEDB_BLOCK_SIZE);
    lsn_ = INVALID_LSN;
    page_type_ = PageType::BPLUS_TREE_BUCKET_PAGE;
    next_bucket_num_ = INVALID_BLOCK_NUM;
}


bool BPlusTreeBucketPage::GetValue(std::vector<RID> *result) {
    int max_size = BPLUS_TREE_BUCKET_MAX_SIZE;
    bool has_found = false;

    for (int i = 0;i < max_size; i++) {
        if (!IsOccupied(i)) {
            break;
        }

        if (!IsReadable(i)) {
            continue;
        }
    
        result->emplace_back(data_[i]);
    }

    return has_found;
}


bool BPlusTreeBucketPage::Insert(const RID &value) {
    int max_size = BPLUS_TREE_BUCKET_MAX_SIZE;
    int i;

    for (i = 0;i < max_size; i++) {
        if (IsReadable(i)) {
            continue;
        }
        
        SetReadable(i);
        SetOccupied(i);
        data_[i] = value;
        return true;
    }

    assert(i == max_size);
    return false;
}



int BPlusTreeBucketPage::GetSize() {
    return BPLUS_TREE_BUCKET_MAX_SIZE;
}


bool BPlusTreeBucketPage::Remove(const RID &value) {
    int max_size = BPLUS_TREE_BUCKET_MAX_SIZE;
    int i;

    for (i = 0;i < max_size; i++) {
        if (!IsOccupied(i)) {
            break;
        }
        
        if (!IsReadable(i)) {
            continue;
        }
        
        if (data_[i] == value) {
            RemoveAt(i);
            return true;
        }
    }


    return false;
}


bool BPlusTreeBucketPage::IsFull() {
    int max_size = BPLUS_TREE_BUCKET_MAX_SIZE;
    
    for (int i = 0;i < max_size; i++) {
        if (!IsOccupied(i)) {
            return false;
        }

        if (!IsReadable(i)) {
            return false;
        }
    }

    return true;
}


bool BPlusTreeBucketPage::IsEmpty() {
    int max_size = BPLUS_TREE_BUCKET_MAX_SIZE;
    
    for (int i = 0;i < max_size; i++) {
        if (IsReadable(i)) {
            return false;
        }
    }

    return true;
}


void BPlusTreeBucketPage::PrintBucket() {
    uint32_t size = 0;
    uint32_t taken = 0;
    uint32_t free = 0;
    int tuple_count = BPLUS_TREE_BUCKET_MAX_SIZE;

    for (int bucket_idx = 0; bucket_idx < tuple_count; bucket_idx++) {
        
        // printf("slot %d : IsReadable %d, IsOccupied %d\n", bucket_idx, IsReadable(bucket_idx), IsReadable(bucket_idx));
        // fflush(stdout);
        if (!IsOccupied(bucket_idx)) {
            break;
        }

        size++;

        if (IsReadable(bucket_idx)) {
            taken++;
        } else {
            free++;
        }
    }

    printf("Bucket Capacity: %d, Size: %u, Taken: %u, Free: %u\n", tuple_count, size, taken, free);
    fflush(stdout);
}


RID BPlusTreeBucketPage::RIDAt(uint32_t bucket_idx) const {
    assert(bucket_idx <= BPLUS_TREE_BUCKET_MAX_SIZE);
    return data_[bucket_idx];
}


void BPlusTreeBucketPage::RemoveAt(uint32_t bucket_idx) {
    assert(bucket_idx <= BPLUS_TREE_BUCKET_MAX_SIZE);
    SetUnReadable(bucket_idx);
}


// ----------------------------
// |      private method      |
// ----------------------------
bool BPlusTreeBucketPage::IsReadable(uint32_t bucket_idx) {
    int bit_map_index = bucket_idx / 8;
    int char_index = bucket_idx % 8;
    
    return readable_[bit_map_index] >> char_index & 1;
}


void BPlusTreeBucketPage::SetReadable(uint32_t bucket_idx) {
    int bit_map_index = bucket_idx / 8;
    int char_index = bucket_idx % 8;
    
    readable_[bit_map_index] |= (1 << char_index);
}


void BPlusTreeBucketPage::SetUnReadable(uint32_t bucket_idx) {
    int bit_map_index = bucket_idx / 8;
    int char_index = bucket_idx % 8;

    readable_[bit_map_index] & ((-1) ^ (1 << char_index));
}



bool BPlusTreeBucketPage::IsOccupied(uint32_t bucket_idx) {
    int bit_map_index = bucket_idx / 8;
    int char_index = bucket_idx % 8;

    return (occupied_[bit_map_index] >> char_index) & 1; 
}



void BPlusTreeBucketPage::SetOccupied(uint32_t bucket_idx) {
    int bit_map_index = bucket_idx / 8;
    int char_index = bucket_idx % 8;

    occupied_[bit_map_index] |= (1 << char_index);  
}

} // namespace SimpleDB

#endif