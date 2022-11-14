#ifndef BPLUS_TREE_DIRECTORY_PAGE_CC
#define BPLUS_TREE_DIRECTORY_PAGE_CC


#include "index/btree/b_plus_tree_directory_page.h"
#include "index/btree/b_plus_tree_leaf_page.h"


namespace SimpleDB {

using KVPair = std::pair<Value, int>;


void BPlusTreeDirectoryPage::Init(int key_size, TypeID type, int parent_num) {
    int tuple_size = key_size + sizeof(int);
    int free_space_size = data_->GetSize() - DIRECTORY_PAGE_HEADER_SIZE;
    int max_size = free_space_size / tuple_size;

    SetLsn(INVALID_LSN);
    SetPageType(PageType::BPLUS_TREE_DIRECTORY_PAGE);
    SetSize(0);
    SetMaxSize(max_size);
    SetTupleSize(tuple_size);
    SetTypeID(type);
    SetParentBlockNum(parent_num);
}



Value BPlusTreeDirectoryPage::KeyAt(int index) const {
    int tuple_size = GetTupleSize();
    int offset = DIRECTORY_PAGE_HEADER_SIZE + index * tuple_size;

    return data_->GetValue(offset, GetTypeID());
}


void BPlusTreeDirectoryPage::SetKeyAt(int index, const Value &key) {
    int tuple_size = GetTupleSize();
    int offset = DIRECTORY_PAGE_HEADER_SIZE + index * tuple_size;

    data_->SetValue(offset, key);
}



int BPlusTreeDirectoryPage::ValueIndex(const int &value) const {
    int size = GetSize();

    for (int i = 0;i < size; i++) {
        if (ValueAt(i) == value) {
            return i;
        }
    }

    return -1;
}


int BPlusTreeDirectoryPage::ValueAt(int index) const {
    int tuple_size = GetTupleSize();
    int offset = DIRECTORY_PAGE_HEADER_SIZE + (index + 1) * tuple_size - sizeof(int);

    return data_->GetInt(offset);
}


void BPlusTreeDirectoryPage::SetValueAt(int index, const int &num) {
    int tuple_size = GetTupleSize();
    int offset = DIRECTORY_PAGE_HEADER_SIZE + (index + 1) * tuple_size - sizeof(int);

    data_->SetInt(offset, num);
}



int BPlusTreeDirectoryPage::GetValue(const Value &key) const {
    
    // binary search for the first key which equal to key
    int left = 1, right = GetSize() - 1;
    while (left < right) {
        int mid = (left + right + 1) / 2;
        auto tmp_key = KeyAt(mid);

        if (tmp_key > key) {
            right = mid - 1;
        }
        else {
            left = mid;
        }
    }

    if (KeyAt(left) != key) {
        return -1;
    }

    return ValueAt(left);
}


void BPlusTreeDirectoryPage::PopulateNewRoot(const int &old_value, 
                                             const Value &new_key, 
                                             const int &new_value) {
    SetSize(GetSize() + 1);
    SetValueAt(0, old_value);
    InsertNodeAfter(old_value, new_key, new_value);
}


uint32_t BPlusTreeDirectoryPage::InsertNodeAfter(const int &old_value, 
                                                 const Value &new_key, 
                                                 const int &new_value) {
    int old_size = GetSize();
    int i;

    // search for a value which equal to old_value
    // and insert a node after it
    for (i = 0;i < old_size; i++) {
        if (ValueAt(i) == old_value) {
            
            // move data 
            int tuple_size = GetTupleSize();
            int be_moved_begin = DIRECTORY_PAGE_HEADER_SIZE + (i + 1) * tuple_size;
            int be_moved_end = DIRECTORY_PAGE_HEADER_SIZE + old_size * tuple_size;
            if (be_moved_begin != be_moved_end) {
                char *ptr = data_->GetRawDataPtr();
                std::memmove(ptr + be_moved_begin + tuple_size,
                             ptr + be_moved_begin,
                             be_moved_end - be_moved_begin);
            }

            // insert into it
            SetKeyAt(i, new_key);
            SetValueAt(i, new_value);
            break;
        }
    }

    if (i == old_size) {
        SIMPLEDB_ASSERT(false, "find failed");
    }

    SetSize(old_size + 1);
    return old_size + 1;
}




void BPlusTreeDirectoryPage::Remove(int index) {
    
    // move data
    int old_size = GetSize();
    int tuple_size = GetTupleSize();
    int be_moved_begin = DIRECTORY_PAGE_HEADER_SIZE + (index + 1) * tuple_size;
    int be_moved_end = DIRECTORY_PAGE_HEADER_SIZE + old_size * tuple_size;
    if (be_moved_begin != be_moved_end) {
        char *ptr = data_->GetRawDataPtr();
        std::memmove(ptr + be_moved_begin - tuple_size,
                     ptr + be_moved_begin,
                     be_moved_end - be_moved_begin);
    }

    SetSize(old_size - 1);
}



int BPlusTreeDirectoryPage::RemoveAndReturnOnlyChild() {
    SetSize(0);
    return ValueAt(0);
}


void BPlusTreeDirectoryPage::MoveHalfTo(BPlusTreeDirectoryPage *recipient, 
                                        BufferManager *bpm) {
    uint half = (GetSize() + 1) / 2;
    recipient->CopyNFrom(this, half, GetSize() - half, bpm);
    SetSize(half);
}


void BPlusTreeDirectoryPage::MoveAllTo(BPlusTreeDirectoryPage *recipient, 
                                       BufferManager *bpm) {
    int size = GetSize();
    recipient->CopyNFrom(this, 0, size, bpm);
    SetSize(0);
}


void BPlusTreeDirectoryPage::MoveFirstToEndOf(BPlusTreeDirectoryPage *recipient, 
                                              BufferManager *bpm) {
    auto kv = std::make_pair(KeyAt(0), ValueAt(0));
    recipient->CopyLastFrom(kv, bpm);
    Remove(0);
}

void BPlusTreeDirectoryPage::MoveLastToFrontOf(BPlusTreeDirectoryPage *recipient, 
                                               BufferManager *bpm) {
    int size = GetSize();
    auto kv = std::make_pair(KeyAt(size - 1), ValueAt(size - 1));
    recipient->CopyFirstFrom(kv, bpm);
    Remove(size - 1);
}

void BPlusTreeDirectoryPage::CopyNFrom(BPlusTreeDirectoryPage* sender, 
                                       int begin, int size, 
                                       BufferManager *bfm) {
    int curr_size = GetSize();
    std::string file_name = GetBlockID().FileName();
    int curr_block_num = GetBlockID().BlockNum();

    for (int i = 0;i < size; i++) {
        auto tmp_key = sender->KeyAt(i + begin);
        auto tmp_value = sender->ValueAt(i + begin);
        auto child = static_cast<BPlusTreeLeafPage*>
                     (bfm->PinBlock({file_name, tmp_value}));
        child->SetParentBlockNum(curr_block_num);
        bfm->UnpinBlock({file_name, tmp_value}, true);

        SetKeyAt(i + curr_size, tmp_key);
        SetValueAt(i + curr_size, tmp_value);
    }

    SetSize(curr_size + size);
}



void BPlusTreeDirectoryPage::CopyLastFrom(const KVPair &item, BufferManager *bfm) {
    int curr_size = GetSize();
    assert(curr_size != GetMaxSize());
    std::string file_name = GetBlockID().FileName();
    int curr_block_num = GetBlockID().BlockNum();

    // update parent num
    auto child = static_cast<BPlusTreeLeafPage*> 
                (bfm->PinBlock({file_name, item.second}));
    child->SetParentBlockNum(curr_block_num);
    bfm->UnpinBlock({file_name, item.second});


    SetSize(curr_size + 1);
    SetKeyAt(curr_size, item.first);
    SetValueAt(curr_size, item.second);
}



void BPlusTreeDirectoryPage::CopyFirstFrom(const KVPair &item, BufferManager *bfm) {
    // move data first
    int tuple_size = GetTupleSize();
    int current_size = GetSize();
    std::string file_name = GetBlockID().FileName();
    int curr_block_num = GetBlockID().BlockNum();
    
    // modify parent num
    auto child = static_cast<BPlusTreeLeafPage*> 
                (bfm->PinBlock({file_name, item.second}));
    child->SetParentBlockNum(curr_block_num);
    bfm->UnpinBlock({file_name, item.second});
    
    
    int be_moved_begin = DIRECTORY_PAGE_HEADER_SIZE;
    int be_moved_end = DIRECTORY_PAGE_HEADER_SIZE + (current_size) *tuple_size;
    if (be_moved_begin < be_moved_end) {
        char *page_begin = data_->GetRawDataPtr();
        std::memmove(page_begin + be_moved_begin + tuple_size, // dist
                     page_begin + be_moved_begin,              // src
                     be_moved_end - be_moved_begin);
    }
    
    // set item
    SetKeyAt(0, item.first);
    SetValueAt(0, item.second);
    SetSize(current_size + 1);
}





} // namespace SimpleDB


#endif