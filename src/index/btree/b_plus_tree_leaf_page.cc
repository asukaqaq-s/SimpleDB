#ifndef BPLUS_TREE_LEAF_PAGE_CC
#define BPLUS_TREE_LEAF_PAGE_CC

#include "index/btree/b_plus_tree_leaf_page.h"

#include <cstring>

namespace SimpleDB {

using KVPair = std::pair<Value, RID>;


void BPlusTreeLeafPage::Init(int block_num, int key_size, 
                             TypeID type, int parent_num) {
    SetLsn(INVALID_LSN);
    SetBlockNum(block_num);
    SetSize(0);
    SetTupleSize(key_size + sizeof(RID));
    SetMaxSize(SIMPLEDB_BLOCK_SIZE / GetTupleSize());
    SetParentBlockNum(parent_num);
    SetNextBlockNum(INVALID_BLOCK_NUM);
    SetTypeID(type);
}


Value BPlusTreeLeafPage::KeyAt(int index) const {
    assert(index < GetSize());
    int offset = LEAF_PAGE_HEADER_SIZE + index * GetTupleSize();
    return data_->GetValue(offset, GetTypeID());
}


RID BPlusTreeLeafPage::ValueAt(int index) const {
    assert(index < GetSize());
    int offset = LEAF_PAGE_HEADER_SIZE + (index + 1) * GetTupleSize() - sizeof(RID);
    int block = data_->GetInt(offset);
    int slot = data_->GetInt(offset + sizeof(int));

    return RID(block, slot);
}


int BPlusTreeLeafPage::KeyIndexGreaterEqual(const Value &key) const {
    int current_size = GetSize();
    int left = 0, right = current_size - 1;
    
    while (left < right) {
        int mid = (left + right) / 2;
        auto tmp_key = KeyAt(mid);

        if (tmp_key >= key) {
            right = mid;    
        }
        else {
            left = mid + 1;
        }
    }

    return left;
}


int BPlusTreeLeafPage::KeyIndexGreaterThan(const Value &key) const {
    int current_size = GetSize();
    int left = 0, right = current_size - 1;
    
    while (left < right) {
        int mid = (left + right) / 2;
        auto tmp_key = KeyAt(mid);

        if (tmp_key > key) {
            right = mid;    
        }
        else {
            left = mid + 1;
        }
    }

    return left;
}


KVPair BPlusTreeLeafPage::GetItem(int index) {
    return std::make_pair(KeyAt(index), ValueAt(index));    
}


void BPlusTreeLeafPage::SetItem(int index, const KVPair &kv) {
    int offset = LEAF_PAGE_HEADER_SIZE + index * GetTupleSize();
    int key_size = GetTupleSize() - sizeof(RID);

    data_->SetValue(offset, kv.first);
    data_->SetInt(offset + key_size, kv.second.GetBlockNum());
    data_->SetInt(offset + key_size + sizeof(int), kv.second.GetSlot());
}



int BPlusTreeLeafPage::Insert(const Value &key, const RID &value) {
    int current_size = GetSize();

    // should consider about this case fist
    if (current_size == 0) {
        SetItem(0, std::make_pair(key, value));
    }
    else if (current_size == GetMaxSize()) {
        SIMPLEDB_ASSERT(false, "we should deal this case in Btree class");
    }
    else {
        // find the last kv-pair which i.first <= key
        // We insert the key behind this position
        int left = 0, right = current_size - 1;
        while (left < right) {
            int mid = (left + right + 1) / 2;
            auto tmp_key = KeyAt(mid);

            if (tmp_key <= key) {
                left = mid;
            }
            else {
                right = mid - 1;
            }
        }


        // check if this page insert this key before, we only use 
        // this function to insert a key that does not exist in the B+ tree
        if (KeyAt(left) == key) {
            SIMPLEDB_ASSERT(false, "call wrong function");
        }


        // special case, key is smallest in the leaf.
        if (left == 0 && KeyAt(left) > key) {
            left--;
        }


        // move all pairs after this pair backwards
        int tuple_size = GetTupleSize();
        int be_moved_begin = LEAF_PAGE_HEADER_SIZE + (left + 1) * tuple_size;
        int be_moved_end = LEAF_PAGE_HEADER_SIZE + (current_size) *tuple_size;
        if (be_moved_begin < be_moved_end) {
            assert(current_size - 1 != left);
            char *page_begin = data_->GetRawDataPtr();
            std::memmove(page_begin + be_moved_begin + tuple_size, // dist
                         page_begin + be_moved_begin,              // src
                         be_moved_end - be_moved_begin);
        }

        
        // insert this pair into page
        SetItem(left + 1, std::make_pair(key, value));
    }

    SetSize(current_size + 1);
    return current_size + 1;
}



bool BPlusTreeLeafPage::GetValue(const Value &key, 
                                 std::vector<RID> *result, 
                                 int *overflow_bucket_num) const {
    
    // binary search to find the first record which >= key
    int index = KeyIndexGreaterEqual(key);

    // check if has this key
    if (key != KeyAt(index)) {
        return false;
    }

    // check if this key point to rid instead of a bucket page
    auto value = ValueAt(index);
    if (ValueIsRID(value)) {
        result->emplace_back(value);
    }
    else {
        if (overflow_bucket_num) {
            *overflow_bucket_num = value.GetBlockNum();
        }
    }

    return true;
}


bool BPlusTreeLeafPage::Remove(const Value &key, const RID &rid) {
    // binary search to find the first record which >= key
    int index = KeyIndexGreaterEqual(key);

    // check if has this key
    if (key != KeyAt(index)) {
        return false;
    }

    // Successfully found, move all pairs after this pair backwards
    int tuple_size = GetTupleSize();
    int current_size = GetSize();
    int dist = LEAF_PAGE_HEADER_SIZE + index * tuple_size;
    int be_moved_begin = LEAF_PAGE_HEADER_SIZE + (index + 1) * tuple_size;
    int be_moved_end = LEAF_PAGE_HEADER_SIZE + (current_size) *tuple_size;

    if (be_moved_begin != be_moved_end) {
        std::memmove(data_->GetRawDataPtr() + dist,
                     data_->GetRawDataPtr() + be_moved_begin,
                     be_moved_end - be_moved_begin);
    }

    SetSize(current_size - 1);
    return true;
}


void BPlusTreeLeafPage::MoveHalfTo(BPlusTreeLeafPage *recipient) {
    int half = GetHalfCurrSize();
    int tuple_size = GetTupleSize();
    char *src = data_->GetRawDataPtr() + LEAF_PAGE_HEADER_SIZE + 
                half * tuple_size;
    
    // in copynfrom we have removed these data
    recipient->CopyNFrom(src, GetSize() - half);
    SetSize(half);
}


void BPlusTreeLeafPage::MoveAllTo(BPlusTreeLeafPage *recipient) {
    int curr_size = GetSize();
    char *src = data_->GetRawDataPtr() + LEAF_PAGE_HEADER_SIZE;

    recipient->SetNextBlockNum(GetNextBlockNum());
    recipient->CopyNFrom(src, curr_size);
    SetSize(0);
}


void BPlusTreeLeafPage::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
    int curr_size = GetSize();
    assert(curr_size != 0);
    auto kv = GetItem(0);
    
    // remove first record
    int tuple_size = GetTupleSize();
    int current_size = GetSize();
    int be_moved_begin = LEAF_PAGE_HEADER_SIZE;
    int be_moved_end = LEAF_PAGE_HEADER_SIZE + current_size * tuple_size;
    if (be_moved_begin < be_moved_end) {
        char *page_begin = data_->GetRawDataPtr();
        std::memmove(page_begin + be_moved_begin,              // dist
                     page_begin + be_moved_begin + tuple_size, // src
                     be_moved_end - be_moved_begin);
    }

    recipient->CopyLastFrom(kv);
    SetSize(curr_size - 1);
}


void BPlusTreeLeafPage::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
    int curr_size = GetSize();
    assert(curr_size != 0);
    auto kv = GetItem(curr_size - 1);
    
    // i think don't need to move data because the data 
    // at this location will be overwritten
    recipient->CopyFirstFrom(kv);
    SetSize(curr_size - 1);
}



int BPlusTreeLeafPage::GetHalfCurrSize() {
    return (GetSize() + 1) / 2;
}


void BPlusTreeLeafPage::CopyNFrom(char *items, int size) {
    int tuple_size = GetTupleSize();
    int moved_size = size * tuple_size;
    int curr_size = GetSize();

    if (size != 0) {
        char *curr_end = data_->GetRawDataPtr() + LEAF_PAGE_HEADER_SIZE + 
                         curr_size * tuple_size;
        std::memmove(curr_end, items, moved_size);
    }

    SetSize(curr_size + size);
}


void BPlusTreeLeafPage::CopyLastFrom(const KVPair &item) {
    int curr_size = GetSize();
    assert(curr_size != GetMaxSize());

    SetItem(curr_size, item);
    SetSize(curr_size + 1);
}


void BPlusTreeLeafPage::CopyFirstFrom(const KVPair &item) {
    
    // move data first
    int tuple_size = GetTupleSize();
    int current_size = GetSize();
    int be_moved_begin = LEAF_PAGE_HEADER_SIZE;
    int be_moved_end = LEAF_PAGE_HEADER_SIZE + (current_size) *tuple_size;
    if (be_moved_begin < be_moved_end) {
        char *page_begin = data_->GetRawDataPtr();
        std::memmove(page_begin + be_moved_begin + tuple_size, // dist
                     page_begin + be_moved_begin,              // src
                     be_moved_end - be_moved_begin);
    }

    
    // set item
    SetItem(0, item);
    SetSize(current_size + 1);
}



bool BPlusTreeLeafPage::ValueIsRID(const RID &value) const {
    if (value.GetSlot() == POINT_TO_BUCKET_CHAIN) {
        return false;
    }
    return true;
}


bool BPlusTreeLeafPage::ValueIsBucket(const RID &value) const {
    if (value.GetSlot() == POINT_TO_BUCKET_CHAIN) {
        return true;
    }
    return false;
}


} // namespace SimpleDB


#endif