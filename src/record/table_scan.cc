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

void TableScan::FirstTuple() {
    MoveToBlock(0);
}

bool TableScan::Next() {
    // move to the next tuple    
    txn_->Pin(GetBlock());
    Buffer *buffer = txn_->GetBuffer(file_name_, rid_, LockMode::SHARED);
    auto table_page = TablePage(txn_, buffer->contents(), GetBlock());
    // because of different txns can read 
    // at the same time, so we should use this latch
    buffer->RLock();

    // find the next tuple's rid
    if (!table_page.GetNextTupleRid(&rid_)) {
        while(rid_.GetSlot() == -1) {

            // remember that unlock and unpin before return
            buffer->RUnlock();
            txn_->Unpin(GetBlock());

            if (AtLastBlock()) {
                return false;
            }

            MoveToBlock(rid_.GetBlockNum() + 1);
            txn_->Pin(GetBlock());
            buffer = txn_->GetBuffer(file_name_, rid_, LockMode::SHARED);
            table_page = TablePage(txn_, buffer->contents(), GetBlock());

            // acquire next tuple again
            buffer->RLock();
            table_page.GetNextTupleRid(&rid_);
        }
    }

    buffer->RUnlock();
    txn_->Unpin(GetBlock());
    return true;
}


bool TableScan::HasField(const std::string &field_name) {
    return layout_.GetSchema().HasField(field_name);
}



void TableScan::NextInsert(const Tuple &tuple) {
    // a optimization is we use nextinsert and insert two methods to
    // finish a insert operations, because in some blocks which can't 
    // inserted, we don't need to x-lock, just acquire s-lock in these
    // blocks and acuqire x-lock in aim-block

    // move to the next empty tuple    
    txn_->Pin(GetBlock());
    Buffer *buffer = txn_->GetBuffer(file_name_, rid_, LockMode::SHARED);
    auto table_page = TablePage(txn_, buffer->contents(), GetBlock());
    // because of different txns can read 
    // at the same time, so we should use this latch
    buffer->RLock();
    
    // find the next tuple's rid
    if (!table_page.GetNextEmptyRid(&rid_, tuple)) {
        while(rid_.GetSlot() == -1) {
            buffer->RUnlock();
            txn_->Unpin(GetBlock());

            // this is different from method 'Next'
            // if can't find, we shall append a new block
            if (AtLastBlock()) {
                MoveToNewBlock();
            }
            else {
                MoveToBlock(rid_.GetBlockNum() + 1);
            }
            
            txn_->Pin(GetBlock());
            buffer = txn_->GetBuffer(file_name_, rid_, LockMode::SHARED);
            table_page = TablePage(txn_, buffer->contents(), GetBlock());
            buffer->RLock();

            // acquire next tuple again
            table_page.GetNextEmptyRid(&rid_, tuple);
        }
    }
    
    buffer->RUnlock();
    txn_->Unpin(GetBlock());
}

bool TableScan::GetTuple(Tuple *tuple) {
    txn_->Pin(GetBlock());
    Buffer *buffer = txn_->GetBuffer(file_name_, rid_, LockMode::SHARED);
    auto table_page = TablePage(txn_, buffer->contents(), GetBlock());
    bool res = true;

    buffer->RLock();
    
    if (!table_page.GetTuple(rid_, tuple)) {
        res = false;
    }

    buffer->RUnlock();
    txn_->Unpin(GetBlock());
    
    return res;
}

bool TableScan::Insert(const Tuple &tuple) {
    // insert the tuple to this block
    txn_->Pin(GetBlock());
    Buffer *buffer = txn_->GetBuffer(file_name_, rid_, LockMode::EXCLUSIVE);
    auto table_page = TablePage(txn_, buffer->contents(), GetBlock());
    bool res = true;

    buffer->WLock();
    
    // since we have acquired s-lock in this lock before
    // then not any txns can modify this block
    if (!table_page.InsertWithRID(rid_, tuple)) {
        res = false;
        SIMPLEDB_ASSERT(false, "concurrency error");
    }
    

    // if insert successfully, we should write a log to disk
    if (res) {
        auto rm = txn_->GetRecoveryMgr();
        lsn_t lsn = rm->InsertLogRec(txn_, file_name_, rid_, tuple, false);
        table_page.SetPageLsn(lsn);
        buffer->SetModified(txn_->GetTxnID(), lsn);
    }

    buffer->WUnlock();
    // why we must unpin here instead of more early?
    txn_->Unpin(GetBlock());
    return res;    
}

void TableScan::Delete() {
    txn_->Pin(GetBlock());
    Tuple tuple;
    Buffer *buffer = txn_->GetBuffer(file_name_, rid_, LockMode::EXCLUSIVE);
    auto table_page = TablePage(txn_, buffer->contents(), GetBlock());
    bool res = true;

    buffer->WLock();

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
    txn_->Pin(GetBlock());
    Tuple old_tuple;
    Buffer *buffer = txn_->GetBuffer(file_name_, rid_, LockMode::EXCLUSIVE);
    auto table_page = TablePage(txn_, buffer->contents(), GetBlock());
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
    txn_->Pin(block);
    Buffer *buffer = txn_->GetBuffer(file_name_, rid_, LockMode::EXCLUSIVE);
    auto table_page = TablePage(txn_, buffer->contents(), block);
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