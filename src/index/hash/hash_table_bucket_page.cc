#ifndef HASH_TABLE_BUCKET_PAGE_CC
#define HASH_TABLE_BUCKET_PAGE_CC

#include "index/hash/hash_table_bucket_page.h"


namespace SimpleDB {


void HashTableBucketPage::InitHashBucketPage(int tuple_size, TypeID type) {
    // clear all data
    data_->ZeroPage();

    int block_size = data_->GetSize();
    int free_space = block_size - PAGE_HEADER_SIZE;
    int tuple_count = (4 * free_space / (4 * tuple_size + 1));
    int bit_map_size = (tuple_count - 1) / 8 + 1;
    int data_array_ptr = bit_map_size * 2 + PAGE_HEADER_SIZE;

    SetPageLsn(INVALID_LSN);
    SetPageType(PageType::HASH_BUCKET_PAGE);
    SetDataArrayPtr(data_array_ptr);
    SetTupleSize(tuple_size);
    SetTypeID(type);

}


bool HashTableBucketPage::GetValue(const Value &key, 
                                   std::vector<RID> *result) {
    bool has_get_value = false;
    int max_tuple_count = GetMaxTupleCount();

    for (int i = 0;i < max_tuple_count; i++) {
        
        // this position have been accessed
        if (!IsOccupied(i)) {
            break;
        }

        if (!IsReadable(i)) {
            continue;
        }

        if (KeyAt(i) == key) {
            has_get_value = true;
            result->emplace_back(ValueAt(i));
        }
    }

    return has_get_value;
}


bool HashTableBucketPage::Insert(const Value &key, const RID &value) {
    int max_tuple_count = GetMaxTupleCount();
    int key_size = GetTupleSize() - sizeof(RID);
    
    for (int i = 0;i < max_tuple_count; i ++) {

        if (IsReadable(i)) {
            
            if (key == KeyAt(i) && value == ValueAt(i)) {
                return false;
            }

            continue;
        }

        // insert k-v pair
        SetReadable(i);
        SetOccupied(i);
        int offset = GetDataArrayPtr() + i * GetTupleSize();
        data_->SetValue(offset, key); // key
        data_->SetInt(offset + key_size, value.GetBlockNum()); // value
        data_->SetInt(offset + key_size + sizeof(int), value.GetSlot());
        return true;
    }

    return false;
}


int HashTableBucketPage::GetSize() {
    return GetMaxTupleCount();
}



bool HashTableBucketPage::Remove(const Value &key, const RID &value) {
    int max_tuple_count = GetMaxTupleCount();
    bool has_removed = false;

    for (int i = 0;i < max_tuple_count; i++) {
        if (!IsOccupied(i)) {
            break;
        }
        
        if (!IsReadable(i)) {
            continue;
        }

        if (key == KeyAt(i) && value == ValueAt(i)) {
            RemoveAt(i);
            has_removed = true;
        }
    }    

    return has_removed;
}


Value HashTableBucketPage::KeyAt(uint32_t bucket_idx) const {
    int key_size = GetTupleSize() - sizeof(RID);
    assert(key_size > 0);
    int offset = GetDataArrayPtr() + bucket_idx * GetTupleSize();

    return data_->GetValue(offset, GetTypeID());
}


RID HashTableBucketPage::ValueAt(uint32_t bucket_idx) const {
    int key_size = GetTupleSize() - sizeof(RID);
    assert(key_size > 0);
    int offset = GetDataArrayPtr() + bucket_idx * GetTupleSize();
    int block_offset = offset + key_size;
    int slot_offset = block_offset + sizeof(int);

    return RID(data_->GetInt(block_offset), data_->GetInt(slot_offset));
}


void HashTableBucketPage::RemoveAt(uint32_t bucket_idx) {
    SetUnReadable(bucket_idx);
}



bool HashTableBucketPage::IsFull() {
    int max_tuple_count = GetMaxTupleCount();
    for (int i = 0;i < max_tuple_count; i++) {
        if (!IsOccupied(i)) {
            return false;
        }
        
        if (!IsReadable(i)) {
            return false;
        }
    }

    
    return true;
}


bool HashTableBucketPage::IsEmpty() {
    int max_tuple_count = GetMaxTupleCount();
    for (int i = 0;i < max_tuple_count; i++) {
        if (!IsOccupied(i)) {
            break;
        }

        if (IsReadable(i)) {
            return false;
        }
    }

    return true;

}




void HashTableBucketPage::PrintBucket() {
    uint32_t size = 0;
    uint32_t taken = 0;
    uint32_t free = 0;
    int tuple_count = GetMaxTupleCount();

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




// ----------------------------
// |      private method      |
// ----------------------------
bool HashTableBucketPage::IsReadable(uint32_t bucket_idx) {
    int bit_map_index = bucket_idx / 8;
    int char_index = bucket_idx % 8;
    int bit_map_offset = PAGE_HEADER_SIZE + bit_map_index;
    
    return (data_->GetByte(bit_map_offset) >> char_index) & 1;
}


void HashTableBucketPage::SetReadable(uint32_t bucket_idx) {
    int bit_map_index = bucket_idx / 8;
    int char_index = bucket_idx % 8;
    int bit_offset = PAGE_HEADER_SIZE + bit_map_index;
    auto c =  (data_->GetByte(bit_offset));
    
    c |= (1 << char_index);
    data_->SetByte(bit_offset, c);
}


void HashTableBucketPage::SetUnReadable(uint32_t bucket_idx) {
    int bit_map_index = bucket_idx / 8;
    int char_index = bucket_idx % 8;
    int bit_map_offset = PAGE_HEADER_SIZE + bit_map_index;
    auto c =  (data_->GetByte(bit_map_offset));
    char bit = (-1) ^ (1 << char_index);

    data_->SetByte(bit_map_offset, c & bit);
}



bool HashTableBucketPage::IsOccupied(uint32_t bucket_idx) {
    int bit_map_index = bucket_idx / 8;
    int char_index = bucket_idx % 8;
    int bit_map_offset = PAGE_HEADER_SIZE + bit_map_index + GetBitMapSize();


    return (data_->GetByte(bit_map_offset) >> char_index) & 1; 
}



void HashTableBucketPage::SetOccupied(uint32_t bucket_idx) {
    int bit_map_index = bucket_idx / 8;
    int char_index = bucket_idx % 8;
    int bit_offset = PAGE_HEADER_SIZE + bit_map_index + GetBitMapSize();
    auto c =  (data_->GetByte(bit_offset));


    c |= (1 << char_index);
    data_->SetByte(bit_offset, c);  
}


} // namespace SimpleDB


#endif