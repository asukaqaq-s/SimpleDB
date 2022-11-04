#ifndef TABLE_ITERATOR_CC
#define TABLE_ITERATOR_CC


#include "record/table_iterator.h"
#include "record/table_heap.h"

namespace SimpleDB {

void TableIterator::GetTuple() {
    bool res = table_heap_->GetTuple(txn_, rid_, &tuple_);
    // log the event
    if (!res) {
        SIMPLEDB_ASSERT(false, "Get Tuple error");
    }
}

const Tuple &TableIterator::operator*() {
    SIMPLEDB_ASSERT(rid_.GetBlockNum() >= 0, "Invalid Table Iterator");
    if (tuple_.GetRID() != rid_) {
        GetTuple();
    }
    // still we may return with invalid tuple
    return tuple_;
}


Tuple *TableIterator::operator->() {
    SIMPLEDB_ASSERT(rid_.GetBlockNum() >= 0, "Invalid Table Iterator");
    if (tuple_.GetRID() != rid_) {
        GetTuple();
    }
    return &tuple_;
}


// logic here is very similar to TableHeap::Begin()
TableIterator TableIterator::operator++() {

    txn_->LockShared(lock_mgr_, GetBlock());
    auto *bfm = table_heap_->buffer_pool_manager_;
    auto *table_page = static_cast<TablePage*>(bfm->PinBlock(GetBlock()));
    table_page->RLock();

    // find the next tuple's rid
    if (!table_page->GetNextTupleRid(&rid_)) {
        while(rid_.GetSlot() == -1) {

            // if this txn's isolation level is rc, means we need to acquire
            // s-block but don't need to grant it until txn terminated
            if (txn_->GetIsolationLevel() == IsoLationLevel::READ_COMMITED) {
                lock_mgr_->UnLock(txn_, GetBlock());
            }

            // remember that unlock and unpin before return
            table_page->RUnlock();
            bfm->UnpinBlock(GetBlock());

            // not next block, we can't create new one in Next
            if (table_heap_->AtLastBlock(txn_, rid_.GetBlockNum())) {
                *this = table_heap_->End();
                return *this;
            }

            // move to the next block and update infor
            rid_ = RID(rid_.GetBlockNum() + 1, -1);
            
            txn_->LockShared(lock_mgr_, GetBlock());
            table_page = static_cast<TablePage*>(bfm->PinBlock(GetBlock()));

            // acquire next tuple again
            table_page->RLock();
            table_page->GetNextTupleRid(&rid_);
        }
    }

    if (txn_->GetIsolationLevel() == IsoLationLevel::READ_COMMITED) {
        lock_mgr_->UnLock(txn_, GetBlock());
    }

    table_page->RUnlock();
    bfm->UnpinBlock(GetBlock());

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

void TableIterator::Advance() {
    this->operator++();
}

bool TableIterator::IsEnd() {
    return rid_ == RID(-1,0);
}

} // namespace SimpleDB

#endif