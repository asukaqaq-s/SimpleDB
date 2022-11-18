#ifndef BPLUS_TREE_BUCKET_PAGE_CC
#define BPLUS_TREE_BUCKET_PAGE_CC

#include "index/btree/b_plus_tree_bucket_page.h"


namespace SimpleDB {


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_BUCKET_PAGE_TYPE::Init(int block_num) {
    SetLsn(INVALID_LSN);
    SetPageType(PageType::BPLUS_TREE_BUCKET_PAGE);
    SetBlockNum(block_num);
    SetSize(0);
    SetMaxSize(BPLUS_TREE_BUCKET_MAX_SIZE);
    SetParentBlockNum(INVALID_BLOCK_NUM);
    SetNextBucketNum(INVALID_BLOCK_NUM);
}


INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_BUCKET_PAGE_TYPE::GetValue(std::vector<ValueType> *result) {
    int max_size = GetMaxSize();
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


INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_BUCKET_PAGE_TYPE::Insert(const ValueType &value) {
    int max_size = GetMaxSize();
    int i;
    
    for (i = 0;i < max_size; i++) {
        if (IsReadable(i)) {
            continue;
        }
        
        SetReadable(i);
        SetOccupied(i);
        data_[i] = value;
        IncreaseSize(1);
        return true;
    }

    assert(i == max_size);
    return false;
}



INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_BUCKET_PAGE_TYPE::Remove(const ValueType &value) {
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


INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_BUCKET_PAGE_TYPE::IsFull() {
    int max_size = GetMaxSize();
    
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


INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_BUCKET_PAGE_TYPE::IsEmpty() {
    int max_size = GetMaxSize();
    
    for (int i = 0;i < max_size; i++) {
        if (IsReadable(i)) {
            return false;
        }
    }

    return true;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_BUCKET_PAGE_TYPE::PrintBucket() {
    int size = 0;
    int taken = 0;
    int free = 0;
    int tuple_count = GetMaxSize();

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


INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_BUCKET_PAGE_TYPE::ValueTypeAt(int bucket_idx) const {
    assert(bucket_idx < GetMaxSize());
    return data_[bucket_idx];
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_BUCKET_PAGE_TYPE::RemoveAt(int bucket_idx) {
    assert(bucket_idx < GetMaxSize());
    SetUnReadable(bucket_idx);
    IncreaseSize(-1);
}


// ----------------------------
// |      private method      |
// ----------------------------
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_BUCKET_PAGE_TYPE::IsReadable(int bucket_idx) {
    int bit_map_index = bucket_idx / 8;
    int char_index = bucket_idx % 8;
    
    return readable_[bit_map_index] >> char_index & 1;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_BUCKET_PAGE_TYPE::SetReadable(int bucket_idx) {
    int bit_map_index = bucket_idx / 8;
    int char_index = bucket_idx % 8;
    
    readable_[bit_map_index] |= (1 << char_index);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_BUCKET_PAGE_TYPE::SetUnReadable(int bucket_idx) {
    int bit_map_index = bucket_idx / 8;
    int char_index = bucket_idx % 8;

    readable_[bit_map_index] &= ((-1) ^ (1 << char_index));
}


INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_BUCKET_PAGE_TYPE::IsOccupied(int bucket_idx) {
    int bit_map_index = bucket_idx / 8;
    int char_index = bucket_idx % 8;

    return (occupied_[bit_map_index] >> char_index) & 1; 
}



INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_BUCKET_PAGE_TYPE::SetOccupied(int bucket_idx) {
    int bit_map_index = bucket_idx / 8;
    int char_index = bucket_idx % 8;

    occupied_[bit_map_index] |= (1 << char_index);  
}



template class BPlusTreeBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeBucketPage<GenericKey<64>, RID, GenericComparator<64>>;



} // namespace SimpleDB

#endif