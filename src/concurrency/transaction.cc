#ifndef TRANSACTION_CC
#define TRANSACTION_CC

#include "concurrency/transaction.h"

namespace SimpleDB {

TransactionMap Transaction::txn_map;

Transaction::Transaction(FileManager *fm,
                         BufferManager *bm, RecoveryManager *rm) 
    : file_manager_(fm), recovery_manager_(rm) {
    txn_id_ = NextTxnID();
    // update transaction map
    txn_map.InsertTransaction(this);

    concurrency_manager_ = std::make_unique<ConcurrencyManager>(this);
    buffers_ = std::make_unique<BufferList>(bm);

    // txn-begin log
    rm->Begin(this);
}

Transaction::Transaction(FileManager *fm, 
                         BufferManager *bm, 
                         RecoveryManager *rm, 
                         txn_id_t id) 
    : file_manager_(fm), recovery_manager_(rm) {
    txn_id_ = id;
    // update transaction map
    txn_map.InsertTransaction(this);

    concurrency_manager_ = std::make_unique<ConcurrencyManager>(this);
    buffers_ = std::make_unique<BufferList>(bm);

    // txn-begin log
    rm->Begin(this);
}



Transaction::~Transaction() {
    txn_map.RemoveTransaction(this);
}

void Transaction::Commit() {
    // maybe this txn will be setted abort by lock_manager
    // we should avoid this problem
    SIMPLEDB_ASSERT(!IsAborted(), "logic error");

    state_ = TransactionState::COMMITTED;
    recovery_manager_->Commit(this);
    std::cout << "Transaction: " << txn_id_ 
              << " committed" << std::endl;
    concurrency_manager_->Release(); // strict 2pl
    buffers_->UnpinAll();
}

void Transaction::Abort() {
    state_ = TransactionState::ABORTED;
    recovery_manager_->Abort(this);
    std::cout << "Transaction: " << txn_id_ 
              << " rolled back" << std::endl;
    concurrency_manager_->Release(); // strict 2pl
    buffers_->UnpinAll();
}

void Transaction::Recovery() { 
    recovery_manager_->Recover(this);
}


void Transaction::Unpin(const BlockId &block) {
    buffers_->Unpin(block);
}

Buffer* Transaction::GetBuffer(const BlockId& block) {
    buffers_->Pin(block);
    Buffer *buffer = buffers_->GetBuffer(block);
    
    return buffer;    
}


void Transaction::AcquireLock(const BlockId &block, LockMode lock_mode) {
    
    if (lock_mode == LockMode::SHARED) {
        concurrency_manager_->LockShared(block);
    }
    else if (lock_mode == LockMode::EXCLUSIVE) {
        concurrency_manager_->LockExclusive(block);
    }
    else {
        assert(false);
    }
}


void Transaction::ReleaseLock(const BlockId &block) {
    SIMPLEDB_ASSERT(isolation_level_ == IsoLationLevel::READ_UNCOMMITED, 
                   "we only call this method when isolation level is read_uncommited");

    concurrency_manager_->ReleaseOne(block);
}

int Transaction::GetFileSize(const std::string &file_name) {

    if (isolation_level_ == IsoLationLevel::SERIALIZABLE) {
        // only for serializable level, we can use file-level-lock to
        // solve phantom-read.other levels, we don't need to acuqire 
        // the s-lock of dummy_block   
        BlockId dummy_lock(file_name, END_OF_FILE);
    }

    return file_manager_->Length(file_name);
}


void Transaction::SetFileSize(const std::string &file_name, int size) {

    BlockId dummy_block(file_name, END_OF_FILE);
    concurrency_manager_->LockExclusive(dummy_block);
    file_manager_->SetFileSize(file_name, size);
}


BlockId Transaction::Append(const std::string &file_name) {
    
    BlockId dummy_block(file_name, END_OF_FILE);
    concurrency_manager_->LockExclusive(dummy_block);
    return file_manager_->Append(file_name);
}

int Transaction::BlockSize() {
    return file_manager_->BlockSize();
}

int Transaction::AvialableBuffers() {
    return buffers_->BufferPoolAvailSize();
}


Transaction* Transaction::TransactionLookUp(txn_id_t txn_id) {
    return txn_map.GetTransaction(txn_id);
}

} // namespace SimpleDB

#endif