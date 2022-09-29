#ifndef TRANSACTION_CC
#define TRANSACTION_CC

#include "concurrency/transaction.h"

namespace SimpleDB {

int Transaction::Next_Txn_ID = 0;
std::mutex Transaction::latch_;

Transaction::Transaction(FileManager *fm, LogManager *lm, BufferManager *bm) :
file_manager_(fm), buffer_manager_(bm) {
    txn_id = NextTxnID();
    recovery_manager_ = std::make_unique<RecoveryManager>
                        (this, txn_id, lm, buffer_manager_);
    buffers_ = std::make_unique<BufferList>(buffer_manager_);
}

void Transaction::Commit() {
    recovery_manager_->Commit();
    std::cout << "Transaction: " << std::to_string(txn_id) 
              << " committed" << std::endl;
    
    buffers_->UnpinAll();
}

void Transaction::RollBack() {
    recovery_manager_->RollBack();
    std::cout << "Transaction: " << std::to_string(txn_id) 
                << " rolled back" << std::endl;
    
    buffers_->UnpinAll();
}

void Transaction::Recovery() { 
    recovery_manager_->Recover();
}

void Transaction::Pin(const BlockId &block) {
    buffers_->Pin(block);
}

void Transaction::Unpin(const BlockId &block) {
    buffers_->Unpin(block);
}

int Transaction::GetInt(const BlockId &block, int offset) {
    // concurMgr.sLock(blk);
    Buffer *buffer = buffers_->GetBuffer(block);
    return buffer->contents()->GetInt(offset);
}

std::string Transaction::GetString(const BlockId &block, int offset) {
    // concurMgr.sLock(blk);
    Buffer *buffer = buffers_->GetBuffer(block);
    return buffer->contents()->GetString(offset);
}

void Transaction::SetInt
(const BlockId &block, int offset, int new_value,bool is_log) {
    // concurMgr.xLock(blk);
    Buffer *buffer = buffers_->GetBuffer(block);
    lsn_t lsn = INVALID_LSN;
    // first, write log
    if (is_log) {
        lsn = recovery_manager_->SetIntLogRec(buffer, offset, new_value);
    }
    // second, modify the specified block
    Page* page = buffer->contents();
    page->SetInt(offset, new_value);
    buffer->SetModified(txn_id, lsn);
}

void Transaction::SetString
(const BlockId &block, int offset, std::string new_value, bool is_log) {
    // concurMgr.xLock(blk);
    Buffer *buffer = buffers_->GetBuffer(block);
    lsn_t lsn = INVALID_LSN;
    // first, write log
    if (is_log) {
        lsn = recovery_manager_->SetStringLogRec(buffer, offset, new_value);
    }
    // second, modify the specified block
    Page *page = buffer->contents();
    page->SetString(offset, new_value);
    buffer->SetModified(txn_id, lsn);
}

} // namespace SimpleDB

#endif
