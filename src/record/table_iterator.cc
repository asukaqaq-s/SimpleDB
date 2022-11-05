#ifndef TABLE_ITERATOR_CC
#define TABLE_ITERATOR_CC


#include "record/table_iterator.h"
#include "record/table_heap.h"

namespace SimpleDB {


inline TableIterator& TableIterator::operator=(const TableIterator &iter) { 
    if (&iter != this) {
        txn_ = iter.txn_;
        table_heap_ = iter.table_heap_;
        rid_ = iter.rid_;
        tuple_ = iter.tuple_;
    }
    return *this;
}


void TableIterator::GetTuple() {
    bool res = table_heap_->GetTuple(txn_, rid_, &tuple_);
    // log the event
    if (!res) {
        SIMPLEDB_ASSERT(false, "Get Tuple error");
    }
}

const Tuple &TableIterator::operator*() {
    SIMPLEDB_ASSERT(rid_.GetBlockNum() >= 0 && rid_.GetSlot() >= -1, 
                    "Invalid Table Iterator");
    // maybe we can use the cache of tuple
    // if tuple.rid_ == this.rid_, we don't need to call this method
    // but it will bring some bugs
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

    // get resource
    txn_->LockShared(GetBlock());
    auto *bfm = txn_->GetBufferManager();
    auto *table_page = static_cast<TablePage*>(bfm->PinBlock(GetBlock()));
    table_page->RLock();

    // find the next tuple's rid
    if (!table_page->GetNextTupleRid(&rid_)) {
        while(rid_.GetSlot() == -1) {

            // release resource
            table_page->RUnlock();
            bfm->UnpinBlock(GetBlock());
            if (txn_->GetIsolationLevel() == IsoLationLevel::READ_COMMITED &&
                txn_->IsSharedLock(GetBlock())) {
                // if this txn's isolation level is rc, means we need to acquire
                // s-block but don't need to grant it until txn terminated
                txn_->UnLock(GetBlock());
            }

            // if not next block, we can't create new one in Next
            if (table_heap_->AtLastBlock(txn_, rid_.GetBlockNum())) {
                *this = table_heap_->End();
                return *this;
            }

            // move to the next block and update infor
            rid_ = RID(rid_.GetBlockNum() + 1, -1);
            
            // get resource again
            txn_->LockShared(GetBlock());
            table_page = static_cast<TablePage*>(bfm->PinBlock(GetBlock()));
            table_page->RLock();

            // acquire next tuple again
            table_page->GetNextTupleRid(&rid_);
        }
    }

    

    // release resource
    table_page->RUnlock();
    bfm->UnpinBlock(GetBlock());
    if (txn_->GetIsolationLevel() == IsoLationLevel::READ_COMMITED &&
        txn_->IsSharedLock(GetBlock())) {
        txn_->UnLock(GetBlock());
    }

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

} // namespace SimpleDB

#endif