#ifndef B_PLUS_TREE_ITERATOR_CC
#define B_PLUS_TREE_ITERATOR_CC

#include "index/btree/b_plus_tree_iterator.h"
#include "index/btree/b_plus_tree.h"

namespace SimpleDB {


INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_ITERATOR_TYPE::BPlusTreeIterator
(int curr_slot, int curr_block_num, 
Buffer *buffer, BPlusTree<KeyType, ValueType, KeyComparator> *btree) 
    : curr_slot_(curr_slot), curr_block_num_(curr_block_num), 
      buffer_(buffer), btree_(btree) {
    
    // assume that we have received r-latch when begin function in btree
    if (!IsEnd()) {
        index_file_name_ = btree_->index_file_name_;
        leaf_page_ = reinterpret_cast<LeafPage*>(buffer_->contents()->GetRawDataPtr());
        buffer_manager_ = btree_->buffer_manager_;
        Get();
    }
    
}


INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_ITERATOR_TYPE::~BPlusTreeIterator() {
    if (!IsEnd()) {
        buffer_->RUnlock();
        buffer_manager_->UnpinBlock({index_file_name_, curr_block_num_}, false);
    }
}


INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_ITERATOR_TYPE::IsEnd() {
    return curr_block_num_ == INVALID_BLOCK_NUM;
}


INDEX_TEMPLATE_ARGUMENTS
const MappingType &BPLUSTREE_ITERATOR_TYPE::operator*() {
    return item_;
}



INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_ITERATOR_TYPE::operator==(const BPLUSTREE_ITERATOR_TYPE &itr) {
    return curr_block_num_ == itr.curr_block_num_ &&
           curr_slot_ == itr.curr_slot_;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_ITERATOR_TYPE::operator!=(const BPLUSTREE_ITERATOR_TYPE &itr) {
    return !((*this) == itr);
}



INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_ITERATOR_TYPE::Advance()  {
    
    
    // check if we have reached the end of this page
    int leaf_page_size = leaf_page_->GetSize();
    if (curr_slot_ != leaf_page_size - 1) {
        // if not, just increase the curr_slot
        curr_slot_ ++;
        Get();
        return;
    }

    
    // otherwise, move to the next block
    if (leaf_page_->GetNextBlockNum() != INVALID_BLOCK_NUM) {
        int next_block_num = leaf_page_->GetNextBlockNum();
        buffer_->RUnlock();
        buffer_manager_->UnpinBlock({index_file_name_, curr_block_num_}, false);
        
        
        // create new buffer and net leaf page
        buffer_ = buffer_manager_->PinBlock({index_file_name_, next_block_num});
        leaf_page_ = reinterpret_cast<LeafPage*>(buffer_->contents()->GetRawDataPtr());
        buffer_->RLock();
        curr_slot_ = 0;
        curr_block_num_ = next_block_num;
        
        Get();
    }
    else {
        int next_block_num = leaf_page_->GetNextBlockNum();
        buffer_->RUnlock();
        buffer_manager_->UnpinBlock({index_file_name_, curr_block_num_}, false);
        curr_slot_ = -1;
        curr_block_num_ = next_block_num;
    }
}


INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_ITERATOR_TYPE::GetValue(std::vector<RID> *result)  {
    Get();

    if (item_.second.GetSlot() != -1) {
        result->emplace_back(item_.second);
        return true;
    }

    // exist a bucket chain, read data from this chain
    int bucket_num = item_.second.GetBlockNum();
    while (bucket_num != INVALID_BLOCK_NUM) {
        Buffer *bucket_buffer = buffer_manager_->PinBlock({index_file_name_, bucket_num});
        auto bucket_page = reinterpret_cast<BucketPage*>(bucket_buffer->contents()->GetRawDataPtr());
        int next_bucket_num = INVALID_BLOCK_NUM;

        // read data
        bucket_buffer->RLock();
        bucket_page->GetValue(result);
        next_bucket_num = bucket_page->GetNextBucketNum();
        bucket_buffer->RUnlock();

        // unpin and move to the next bucket
        buffer_manager_->UnpinBlock({index_file_name_, bucket_num}, false);
        bucket_num = next_bucket_num;
    }

    return true;
}


INDEX_TEMPLATE_ARGUMENTS
KeyType BPLUSTREE_ITERATOR_TYPE::GetKey() const {
    return item_.first;
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_ITERATOR_TYPE::Get()  {
    assert(curr_slot_ != -1 && curr_block_num_ != INVALID_BLOCK_NUM);
    item_ = leaf_page_->GetItem(curr_slot_);
}



template class BPlusTreeIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace SimpleDB



#endif