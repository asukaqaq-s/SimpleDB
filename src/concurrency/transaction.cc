#ifndef TRANSACTION_CC
#define TRANSACTION_CC

#include "concurrency/transaction.h"


namespace SimpleDB {

TransactionMap Transaction::txn_map;


txn_id_t Transaction::NextTxnID() { 
    int res = txn_map.GetNextTransactionID();
    return res;
}

TransactionMap::TransactionMap() {}
    
TransactionMap::~TransactionMap() {}

void TransactionMap::InsertTransaction(Transaction *txn) {
    std::lock_guard<std::mutex> latch(latch_);
    txn_map_[txn->GetTxnID()] = txn;
}

void TransactionMap::RemoveTransaction(Transaction *txn) {
    std::lock_guard<std::mutex> latch(latch_);
    txn_map_.erase(txn->GetTxnID());
}

bool TransactionMap::IsTransactionAlive(txn_id_t txn_id) {
    std::lock_guard<std::mutex> latch(latch_);
    return txn_map_.find(txn_id) != txn_map_.end();            
}

Transaction* TransactionMap::GetTransaction(txn_id_t txn_id) {
    std::lock_guard<std::mutex> latch(latch_);
    if (IsTransactionAlive(txn_id))
        return txn_map_[txn_id];
    else return NULL;
}

txn_id_t TransactionMap::GetNextTransactionID() {
    std::lock_guard<std::mutex> latch(latch_);
    int res = ++next_txn_id_;
    return res;
}


Transaction::Transaction(FileManager *fm, LogManager *lm, BufferManager *bm) :
file_manager_(fm), buffer_manager_(bm) {
    txn_id_ = NextTxnID();
    // update transaction map
    txn_map.InsertTransaction(this);

    concurrency_manager_ = std::make_unique<ConcurrencyManager>
                        (this);
    recovery_manager_ = std::make_unique<RecoveryManager>
                        (this, txn_id_, lm, buffer_manager_);
    buffers_ = std::make_unique<BufferList>(buffer_manager_);
}

Transaction::~Transaction() {
    txn_map.RemoveTransaction(this);
}

void Transaction::Commit() {
    state_ = TransactionState::COMMITTED;
    recovery_manager_->Commit();
    std::cout << "Transaction: " << std::to_string(txn_id_) 
              << " committed" << std::endl;
    concurrency_manager_->Release(); // strong 2pl
    buffers_->UnpinAll();
}

void Transaction::RollBack() {
    state_ = TransactionState::ABORTED;
    recovery_manager_->RollBack();
    std::cout << "Transaction: " << std::to_string(txn_id_) 
                << " rolled back" << std::endl;
    concurrency_manager_->Release(); // strong 2pl
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
    // we should acquire lock before pinning a buffer
    concurrency_manager_->LockShared(block);
    Buffer *buffer = buffers_->GetBuffer(block);
    // mutex 
    buffer->RLock();
    auto res = buffer->contents()->GetInt(offset);
    buffer->RUnlock();
    return res;
}

std::string Transaction::GetString(const BlockId &block, int offset) {
    concurrency_manager_->LockShared(block);
    Buffer *buffer = buffers_->GetBuffer(block);
    // mutex 
    buffer->RLock();
    auto res = buffer->contents()->GetString(offset);
    buffer->RUnlock();
    return res;
}

void Transaction::SetInt
(const BlockId &block, int offset, int new_value,bool is_log) {
    concurrency_manager_->LockExclusive(block);
    Buffer *buffer = buffers_->GetBuffer(block);
    lsn_t lsn = INVALID_LSN;

    // we shuold receive writer lock before writing a log
    buffer->WLock();

    // first, write log
    if (is_log) {
        lsn = recovery_manager_->SetIntLogRec(buffer, offset, new_value);
    }
    // second, modify the specified block
    Page* page = buffer->contents();
    page->SetInt(offset, new_value);
    buffer->SetModified(txn_id_, lsn);

    buffer->WUnlock();
}

void Transaction::SetString
(const BlockId &block, int offset, std::string new_value, bool is_log) {
    concurrency_manager_->LockExclusive(block);
    Buffer *buffer = buffers_->GetBuffer(block);
    lsn_t lsn = INVALID_LSN;

    // we shuold receive writer lock before writing a log
    buffer->WLock();

    // first, write log
    if (is_log) {
        lsn = recovery_manager_->SetStringLogRec(buffer, offset, new_value);
    }
    // second, modify the specified block
    Page *page = buffer->contents();
    page->SetString(offset, new_value);
    buffer->SetModified(txn_id_, lsn);

    buffer->WUnlock();
}

int Transaction::GetFileSize(std::string file_name) {
    BlockId dummy_block(file_name, END_OF_FILE);

    concurrency_manager_->LockShared(dummy_block);
    return file_manager_->Length(file_name);
}

BlockId Transaction::Append(std::string file_name) {
    BlockId dummy_block(file_name, END_OF_FILE);
    
    concurrency_manager_->LockExclusive(dummy_block);
    return file_manager_->Append(file_name);
}

int Transaction::BlockSize() {
    return file_manager_->BlockSize();
}

int Transaction::AvialableBuffers() {
    return buffer_manager_->available();
}


Transaction* Transaction::TransactionLookUp(txn_id_t txn_id) {
    return txn_map.GetTransaction(txn_id);
}

} // namespace SimpleDB

#endif
