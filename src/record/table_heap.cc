#ifndef TABLE_SCAN_CC
#define TABLE_SCAN_CC

#include "record/table_heap.h"
#include "concurrency/transaction_manager.h"

#include <iostream>

namespace SimpleDB {
    
TableHeap::TableHeap(Transaction *txn, std::string table_name, FileManager *file_mgr,
                     RecoveryManager *rm, BufferManager *bfm, LockManager *lock)
    : file_name_(table_name + ".table"), file_manager_(file_mgr), recovery_manager_(rm), 
      buffer_pool_manager_(bfm), lock_manager_(lock) {
    
    if (GetFileSize(txn) == 0) {
        Append(txn);
    }
}

void TableHeap::Insert(Transaction *txn, const Tuple &tuple, RID *rid) {

    RID curr_rid(0, -1);
    Buffer *buffer = buffer_pool_manager_->PinBlock(GetBlock(curr_rid));
    auto table_page = static_cast<TablePage*> (buffer);


    // acquire s-lock in this page first.
    // note that acuqire lock should before readerwrite latch
    if (!txn->LockShared(lock_manager_, GetBlock(curr_rid))) {
        SIMPLEDB_ASSERT(false, "acquire s-lock error");
    }


    // why we need this function?
    // for insertion in tablepage, It is very possible that this page cannot be inserted
    // then x-lock for these page requests which cannot be inserted will bring a performance 
    // penalty, so we just acquire s-lock in these pages and upgrade s-lock necessary. 
    auto *lock_manager = lock_manager_;
    std::function<bool(const BlockId &)> upgrade = [lock_manager, txn](const BlockId &block) {
        if (txn->IsSharedLock(block)) {
            return lock_manager->LockUpgrade(txn, block);
        }
        SIMPLEDB_ASSERT(txn->IsExclusiveLock(block), "logic error");
        return true;
    };
    

    // because different txns can read/write buffer 
    // at the same time, so we should use this latch to protect content
    table_page->WLock();

    
    while (!table_page->Insert(&curr_rid, tuple, upgrade, txn, recovery_manager_)) {
        
        // if we insert fail, try to move to the next block
        table_page->WUnlock();
        buffer_pool_manager_->UnpinBlock(GetBlock(curr_rid));
        
        // should remember that read committed txn should release
        // s-lock after read operation finish
        if (txn->GetIsolationLevel() == IsoLationLevel::READ_COMMITED &&
            txn->IsSharedLock(GetBlock(curr_rid))) {
            SIMPLEDB_ASSERT(lock_manager_->UnLock(txn, GetBlock(curr_rid)), "unlock error");
        }

        if (AtLastBlock(txn, curr_rid.GetBlockNum())) {
            MoveToNewBlock(txn, curr_rid);

            // since we have granted x-lock in this new block
            // in "MoveToNewBlock", don't need to acquire any lock again
            buffer = buffer_pool_manager_->PinBlock(GetBlock(curr_rid));
            table_page = static_cast<TablePage*> (buffer);
            
            table_page->WLock();
        }
        else {
            MoveToBlock(curr_rid, curr_rid.GetBlockNum() + 1);
            
            // we only acquire s-lock first
            // and upgrade it in the "insert" method of tablepage
            if (txn->LockShared(lock_manager_, GetBlock(curr_rid))) {
                SIMPLEDB_ASSERT(false, "acquire s-lock error");    
            }

            buffer = buffer_pool_manager_->PinBlock(GetBlock(curr_rid));
            table_page = static_cast<TablePage*> (buffer);

            table_page->WLock();
        }
    }

    

    table_page->WUnlock();
    buffer_pool_manager_->UnpinBlock(GetBlock(curr_rid));

    if (txn->GetIsolationLevel() == IsoLationLevel::READ_COMMITED &&
        txn->IsSharedLock(GetBlock(curr_rid))) {
        SIMPLEDB_ASSERT(lock_manager_->UnLock(txn, GetBlock(curr_rid)), "unlock error");
    }

    if (rid != nullptr) {
        *rid = curr_rid;
    }
}

bool TableHeap::GetTuple(Transaction *txn, const RID &rid, Tuple *tuple) {
    
    txn->LockShared(lock_manager_, GetBlock(rid));
    Buffer *buffer = buffer_pool_manager_->PinBlock(GetBlock(rid));
    auto table_page = static_cast<TablePage*> (buffer);
    bool res = true;

    table_page->RLock();
    
    if (!table_page->GetTuple(rid, tuple)) {
        res = false;
    }


    table_page->RUnlock();
    buffer_pool_manager_->UnpinBlock(GetBlock(rid));

    if (txn->GetIsolationLevel() == IsoLationLevel::READ_COMMITED) {
        SIMPLEDB_ASSERT(lock_manager_->UnLock(txn, GetBlock(rid)), "unlock error");
    }
    
    return res;
}

bool TableHeap::InsertWithRid(Transaction *txn, 
                              const Tuple &tuple, const RID &rid) {
    
    txn->LockExclusive(lock_manager_, GetBlock(rid));
    Buffer *buffer = buffer_pool_manager_->PinBlock(GetBlock(rid));
    auto *table_page = static_cast<TablePage*> (buffer);
    bool res = true;

    table_page->WLock();
    

    if (!table_page->InsertWithRID(rid, tuple, txn, recovery_manager_)) {
        res = false;
    }
    

    // why we must unpin here instead of more early?
    table_page->WUnlock();
    buffer_pool_manager_->UnpinBlock(GetBlock(rid));

    return res;    
}

void TableHeap::Delete(Transaction *txn, const RID& rid) {
    
    txn->LockExclusive(lock_manager_, GetBlock(rid));
    Tuple tuple;
    Buffer *buffer = buffer_pool_manager_->PinBlock(GetBlock(rid));
    auto *table_page = static_cast<TablePage*> (buffer);
    bool res = true;

    table_page->WLock();

    // since we have received s-lock in this block when Next method
    // other txns can't delete this tuple
    if (!table_page->Delete(rid, &tuple, txn, recovery_manager_)) {
        res = false;
        SIMPLEDB_ASSERT(false, "concurrency error");
    }


    table_page->WUnlock();
    buffer_pool_manager_->UnpinBlock(GetBlock(rid));
}


bool TableHeap::Update(Transaction *txn, const RID& rid, const Tuple &new_tuple) {
    
    txn->LockExclusive(lock_manager_, GetBlock(rid));
    Tuple old_tuple;

    Buffer *buffer = buffer_pool_manager_->PinBlock(GetBlock(rid));
    auto *table_page = static_cast<TablePage*> (buffer);
    bool res = true;

    table_page->WLock();

    if (!table_page->Update(rid, &old_tuple, new_tuple, txn, recovery_manager_)) {
        // update may be fail, if update fail, we should abort this txn
        res = false;
    }


    table_page->WUnlock();
    buffer_pool_manager_->UnpinBlock(GetBlock(rid));
    return res;
}


// //
// // priavte auxiliary methods
// //
bool TableHeap::AtLastBlock(Transaction *txn, int block_number) {
    if (GetFileSize(txn) == block_number - 1) {
        return true;
    }
    return false;
}


void TableHeap::MoveToBlock(RID &rid, int block_number) {
    rid.SetBlockNum(block_number);
    rid.SetSlot(-1);
}

void TableHeap::MoveToNewBlock(Transaction *txn, RID &rid) {
    BlockId block = Append(txn);
    auto *table_page = static_cast<TablePage*>(buffer_pool_manager_->PinBlock(block));
    table_page->WLock();

    table_page->InitPage(txn, recovery_manager_);

    table_page->WUnlock();
    rid.SetBlockNum(block.BlockNum());
    rid.SetSlot(-1);
}


int TableHeap::GetFileSize(Transaction *txn) {
    BlockId dummy_block(file_name_, -1);
    
    // only serizable level need to acquire s-lock on the file
    if (txn->GetIsolationLevel() == IsoLationLevel::SERIALIZABLE) {
        assert(txn->LockShared(lock_manager_, dummy_block));
    }

    return file_manager_->GetFileBlockNum(file_name_);
}


BlockId TableHeap::Append(Transaction *txn) {
    BlockId dummy_block(file_name_, -1);
    assert(txn->LockExclusive(lock_manager_, dummy_block));

    return file_manager_->Append(file_name_);
}


TableIterator TableHeap::Begin(Transaction *txn) {
    RID rid(0,-1);
    txn->LockShared(lock_manager_, GetBlock(rid));
    auto *table_page = static_cast<TablePage*> (buffer_pool_manager_->PinBlock(GetBlock(rid)));
    table_page->RLock();


    if (!table_page->GetFirstTupleRid(rid)) {
        while (rid.GetSlot() == -1) {

            table_page->RUnlock();

            // if this txn's isolation level is rc, means we need to acquire
            // s-block but don't need to grant it until txn terminated
            if (txn->GetIsolationLevel() == IsoLationLevel::READ_COMMITED) {
                lock_manager_->UnLock(txn, GetBlock(rid));
            }

            // remember unpin before return
            buffer_pool_manager_->UnpinBlock(GetBlock(rid));

            // not have next block, we can't create new one in query
            if (AtLastBlock(txn, rid.GetBlockNum())) {
                return End();
            }

            // move to next block and update infor
            MoveToBlock(rid, rid.GetBlockNum() + 1);
            
            // acquire lock again
            txn->LockShared(lock_manager_, GetBlock(rid));
            table_page = static_cast<TablePage*> (buffer_pool_manager_->PinBlock(GetBlock(rid)));

            // acquire next tuple again
            table_page->RLock();
            table_page->GetNextTupleRid(&rid);
        }
    }

    return TableIterator(txn, lock_manager_, this, rid);
}


TableIterator TableHeap::End() {
    return TableIterator(nullptr, lock_manager_, this, RID(-1,0));
}

} // namespace SimpleDB

#endif