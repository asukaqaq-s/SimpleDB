#ifndef TABLE_ITERATOR_CC
#define TABLE_ITERATOR_CC


#include "record/table_iterator.h"
#include "record/table_heap.h"

namespace SimpleDB {


TableIterator& TableIterator::operator=(const TableIterator &iter) { 
    if (&iter != this) {
        txn_ = iter.txn_;
        table_heap_ = iter.table_heap_;
        rid_ = iter.rid_;
        tuple_ = iter.tuple_;
        
        // unpin current block before pinblock
        Close();
        // we should pin this table page again
        if (rid_.GetBlockNum() != -1) {
            table_page_ = static_cast<TablePage*> (txn_->PinBlock(GetBlock()));
        }
    }
    return *this;
}


void TableIterator::GetTuple() {
    // gettuple directly
    txn_->LockShared(GetBlock());
    table_page_->RLock();

    bool res = table_page_->GetTuple(rid_, &tuple_);
    
    table_page_->RUnlock();
    txn_->UnLockWhenRUC(GetBlock());
    // catch the event
    if (!res) {
        SIMPLEDB_ASSERT(false, "Get Tuple error");
    }
}

const Tuple &TableIterator::operator*() {
    SIMPLEDB_ASSERT(rid_.GetBlockNum() >= 0 && rid_.GetSlot() >= 0, 
                    "Invalid Table Iterator");
    // maybe we can use the cache of tuple
    // if tuple.rid_ == this.rid_, we don't need to call this method
    // but it will bring some bugs which difficult to find 
    GetTuple();
    // still we may return with invalid tuple
    return tuple_;
}


Tuple *TableIterator::operator->() {
    
    SIMPLEDB_ASSERT(rid_.GetBlockNum() >= 0 && rid_.GetSlot() >= -1, 
                    "Invalid Table Iterator");
    GetTuple();
    return &tuple_;
}


// logic here is very similar to TableHeap::Begin()
TableIterator TableIterator::operator++() {
    // acquire resource but don't need to pin
    txn_->LockShared(GetBlock());
    table_page_->RLock();

    // find the next tuple's rid
    if (!table_page_->GetNextTupleRid(&rid_)) {
        while(rid_.GetSlot() == -1) {

            // release resource
            table_page_->RUnlock();
            // we should unpinblock here. but we can't replace the 
            // close function with unpinblock. it will unpin the block twice.
            Close(); 
            txn_->UnLockWhenRUC(GetBlock());

            // if not next block, we can't create new one in Next
            if (table_heap_->AtLastBlock(txn_, rid_.GetBlockNum())) {
                rid_ = RID(-1, 0); /* means move to end */
                return *this;
            }

            // move to the next block
            rid_ = RID(rid_.GetBlockNum() + 1, -1);
            
            // get resource again
            txn_->LockShared(GetBlock());
            table_page_ = static_cast<TablePage*>(txn_->PinBlock(GetBlock()));
            table_page_->RLock();

            // acquire next tuple again
            table_page_->GetNextTupleRid(&rid_);
        }
    }

    

    // release resource
    table_page_->RUnlock();
    txn_->UnLockWhenRUC(GetBlock());

    return *this;
}

TableIterator TableIterator::operator++(int) {
    // copy the old value.
    // i.e. rid, tuple
    TableIterator copy(*this);
    this->operator++();
    return copy;
}

const Tuple &TableIterator::Get() {
    return this->operator*();
}

RID TableIterator::GetRID() {
    return rid_;
}

bool TableIterator::IsEnd() { 
    return rid_ == RID(-1,0);
}


BlockId TableIterator::GetBlock() {
    return BlockId(table_heap_->file_name_, rid_.GetBlockNum());
}


void TableIterator::Close() {
    if (table_page_ != nullptr) {
        assert(rid_.GetBlockNum() != -1);
        txn_->UnpinBlock(GetBlock());
        table_page_ = nullptr;
    }
}


} // namespace SimpleDB

#endif