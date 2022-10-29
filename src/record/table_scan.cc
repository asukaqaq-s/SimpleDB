#ifndef TABLE_SCAN_CC
#define TABLE_SCAN_CC

#include "record/table_scan.h"

#include <iostream>

namespace SimpleDB {
    
TableScan::TableScan(Transaction *txn, std::string table_name, Layout layout)
    : txn_(txn), file_name_(table_name + ".table"), layout_(layout) {
    
    if (txn_->GetFileSize(file_name_) == 0) {
        MoveToNewBlock();
        
    } else {
        MoveToBlock(0);
    }
}

void TableScan::Begin() {
    MoveToBlock(0);
}

bool TableScan::Next() {

    txn_->AcquireLock(GetBlock(), LockMode::SHARED);
    Buffer *buffer = txn_->GetBuffer(GetBlock());
    auto table_page = TablePage(buffer->contents(), GetBlock());
    
    // because of different txns can read/write 
    // at the same time, so we should use this latch to protect content
    buffer->RLock();

    // find the next tuple's rid
    if (!table_page.GetNextTupleRid(&rid_)) {
        while(rid_.GetSlot() == -1) {

            // if this txn's isolation level is rc, means we need to acquire
            // s-block but don't need to grant it until txn terminated
            if (txn_->GetIsolationLevel() == IsoLationLevel::READ_COMMITED) {
                txn_->ReleaseLock(BlockId(file_name_, rid_.GetBlockNum()));
            }

            // remember that unlock and unpin before return
            buffer->RUnlock();
            txn_->Unpin(GetBlock());

            // not the next block
            if (AtLastBlock()) {
                return false;
            }

            // move to the next block and update infor
            MoveToBlock(rid_.GetBlockNum() + 1);
            txn_->AcquireLock(GetBlock(), LockMode::SHARED);
            buffer = txn_->GetBuffer(GetBlock());
            table_page = TablePage(buffer->contents(), GetBlock());

            // acquire next tuple again
            buffer->RLock();
            table_page.GetNextTupleRid(&rid_);
        }
    }

    if (txn_->GetIsolationLevel() == IsoLationLevel::READ_COMMITED) {
        txn_->ReleaseLock(BlockId(file_name_, rid_.GetBlockNum()));
    }

    buffer->RUnlock();
    txn_->Unpin(GetBlock());
    return true;
}


bool TableScan::HasField(const std::string &field_name) {
    return layout_.GetSchema().HasField(field_name);
}


void TableScan::Insert(const Tuple &tuple, RID *rid) {

    txn_->AcquireLock(GetBlock(), LockMode::EXCLUSIVE);
    Buffer *buffer = txn_->GetBuffer(GetBlock());
    auto table_page = TablePage(buffer->contents(), GetBlock());
    // because of different txns can read 
    // at the same time, so we should use this latch
    buffer->WLock();
    
    while (!table_page.Insert(&rid_, tuple)) {
        // if we insert fail, try to move to the next block
        buffer->WUnlock();
        if (AtLastBlock()) {
            
            // create a new block
            MoveToNewBlock();

            // since we have granted x-lock in this new block
            // don't need to acquire any lock again
            buffer = txn_->GetBuffer(GetBlock());
            table_page = TablePage(buffer->contents(), GetBlock());

            buffer->WLock();
        }
        else {
            MoveToBlock(rid_.GetBlockNum() + 1);
            
            // should we acquire x-lock in this block?
            // even if we can't insert it?
            // i think it's a stupid way....
            // may be we can acquire lock in tablepage instead of tablescan
            txn_->AcquireLock(GetBlock(), LockMode::EXCLUSIVE);
            buffer = txn_->GetBuffer(GetBlock());
            table_page = TablePage(buffer->contents(), GetBlock());

            buffer->WLock();
        }
    }
    
    // if insert successfully, we should write a log to disk
    {
        auto rm = txn_->GetRecoveryMgr();
        lsn_t lsn = rm->InsertLogRec(txn_, file_name_, rid_, tuple, false);
        table_page.SetPageLsn(lsn);
        buffer->SetModified(txn_->GetTxnID(), lsn);
    }

    buffer->WUnlock();
    txn_->Unpin(GetBlock());

    if (rid != nullptr) {
        *rid = rid_;
    }
}

bool TableScan::GetTuple(Tuple *tuple) {

    txn_->AcquireLock(GetBlock(), LockMode::SHARED);
    Buffer *buffer = txn_->GetBuffer(GetBlock());
    auto table_page = TablePage(buffer->contents(), GetBlock());
    bool res = true;

    buffer->RLock();
    
    if (!table_page.GetTuple(rid_, tuple)) {
        res = false;
    }

    // if this txn's isolation level is rc, means we need to acquire
    // s-block but don't need to grant it until txn terminated
    if (txn_->GetIsolationLevel() == IsoLationLevel::READ_COMMITED) {
        txn_->ReleaseLock(BlockId(file_name_, rid_.GetBlockNum()));
    }

    buffer->RUnlock();
    txn_->Unpin(GetBlock());
    
    return res;
}

bool TableScan::InsertWithRid(const Tuple &tuple, const RID &rid) {
    
    // update rid
    MoveToRid(rid);
    
    // acquire lock before get buffer
    txn_->AcquireLock(GetBlock(), LockMode::EXCLUSIVE);
    Buffer *buffer = txn_->GetBuffer(GetBlock());
    auto table_page = TablePage(buffer->contents(), GetBlock());
    bool res = true;

    buffer->WLock();
    

    if (!table_page.InsertWithRID(rid_, tuple)) {
        res = false;
    }
    

    // if insert successfully, we should write a log to disk
    if (res) {
        auto rm = txn_->GetRecoveryMgr();
        lsn_t lsn = rm->InsertLogRec(txn_, file_name_, rid_, tuple, false);
        table_page.SetPageLsn(lsn);
        buffer->SetModified(txn_->GetTxnID(), lsn);
    }

    // why we must unpin here instead of more early?
    buffer->WUnlock();
    txn_->Unpin(GetBlock());
    return res;    
}

void TableScan::Delete() {
    Tuple tuple;

    txn_->AcquireLock(GetBlock(), LockMode::EXCLUSIVE);
    Buffer *buffer = txn_->GetBuffer(GetBlock());
    auto table_page = TablePage(buffer->contents(), GetBlock());
    bool res = true;

    buffer->WLock();

    // since we have received s-lock in this block when Next method
    // other txns can't delete this tuple
    if (!table_page.Delete(rid_, &tuple)) {
        res = false;
        SIMPLEDB_ASSERT(false, "concurrency error");
    }


    // write a DELETE log to disk
    if (res) {
        auto rm = txn_->GetRecoveryMgr();
        lsn_t lsn = rm->DeleteLogRec(txn_, file_name_, rid_, tuple, false);
        table_page.SetPageLsn(lsn);
        buffer->SetModified(txn_->GetTxnID(), lsn);
    }

    buffer->WUnlock();
    txn_->Unpin(GetBlock());
}


bool TableScan::Update(const Tuple &new_tuple) {
    Tuple old_tuple;

    txn_->AcquireLock(GetBlock(), LockMode::EXCLUSIVE);
    Buffer *buffer = txn_->GetBuffer(GetBlock());
    auto table_page = TablePage(buffer->contents(), GetBlock());
    bool res = true;

    buffer->WLock();

    if (!table_page.Update(rid_, &old_tuple, new_tuple)) {
        // update may be fail, if update fail
        // we should abort this txn
        res = false;
        SIMPLEDB_ASSERT(false, "concurrency error");
    }

    
    if (res) {
        auto rm = txn_->GetRecoveryMgr();
        lsn_t lsn = rm->UpdateLogRec(txn_, file_name_, rid_, 
                                     old_tuple, new_tuple, false);
        table_page.SetPageLsn(lsn);
        buffer->SetModified(txn_->GetTxnID(), lsn);
    }

    buffer->WUnlock();
    txn_->Unpin(GetBlock());
    return res;
}


// in these "moveto..." method, i just want to modify relative infor
// and don't want to pin or unpin

void TableScan::MoveToRid(const RID &rid) {
    rid_ = rid;
}


// //
// // priavte auxiliary methods
// //

void TableScan::MoveToBlock(int block_number) {
    rid_.SetBlockNum(block_number);
    rid_.SetSlot(-1);
}

void TableScan::MoveToNewBlock() {
    
    BlockId block = txn_->Append(file_name_);
    rid_.SetBlockNum(block.BlockNum());
    rid_.SetSlot(-1);

    // init page
    txn_->AcquireLock(block, LockMode::EXCLUSIVE);
    Buffer *buffer = txn_->GetBuffer(block);
    auto table_page = TablePage(buffer->contents(), block);

    buffer->WLock();
    table_page.InitPage();

    // write a initpage log to disk
    {
        auto rm = txn_->GetRecoveryMgr();
        lsn_t lsn = rm->InitPageLogRec(txn_, file_name_, block.BlockNum(), false);
        table_page.SetPageLsn(lsn);
        buffer->SetModified(txn_->GetTxnID(), lsn);
    }

    buffer->WUnlock();
    txn_->Unpin(block);
    // write log
}

bool TableScan::AtLastBlock() {
    return rid_.GetBlockNum() == 
           txn_->GetFileSize(file_name_) - 1;
}

} // namespace SimpleDB

#endif