#ifndef TRANSACTION_CC
#define TRANSACTION_CC

#include "concurrency/transaction.h"

namespace SimpleDB {

TransactionMap Transaction::txn_map;

Transaction::Transaction(FileManager *fm,
                         BufferManager *bm, RecoveryManager *rm) 
    : file_manager_(fm), buffer_manager_ (bm), recovery_manager_(rm) {
    txn_id_ = NextTxnID();
    // update transaction map
    txn_map.InsertTransaction(this);

    concurrency_manager_ = std::make_unique<ConcurrencyManager>(this);
    buffers_ = std::make_unique<BufferList>(buffer_manager_);

    // txn-begin log
    rm->Begin(this);
}

Transaction::~Transaction() {
    txn_map.RemoveTransaction(this);
}

void Transaction::Commit() {
    state_ = TransactionState::COMMITTED;
    recovery_manager_->Commit(this);
    std::cout << "Transaction: " << txn_id_ 
              << " committed" << std::endl;
    concurrency_manager_->Release(); // strong 2pl
    buffers_->UnpinAll();
}

void Transaction::Abort() {
    state_ = TransactionState::ABORTED;
    recovery_manager_->Abort(this);
    std::cout << "Transaction: " << txn_id_ 
              << " rolled back" << std::endl;
    concurrency_manager_->Release(); // strong 2pl
    buffers_->UnpinAll();
}

void Transaction::Recovery() { 
    recovery_manager_->Recover(this);
}

void Transaction::Pin(const BlockId &block) {
    buffers_->Pin(block);
}


void Transaction::Unpin(const BlockId &block) {
    buffers_->Unpin(block);
}

Buffer* Transaction::GetBuffer(const std::string &file_name,
                               const RID &rid,
                               LockMode lock_mode) {
    BlockId block = BlockId(file_name, rid.GetBlockNum());
    if (lock_mode == LockMode::SHARED) {
        concurrency_manager_->LockShared(block);
    }
    else if (lock_mode == LockMode::EXCLUSIVE) {
        concurrency_manager_->LockExclusive(block);
    }
    else {
        SIMPLEDB_ASSERT(false, "");
    }

    Buffer *buffer = buffers_->GetBuffer(block);

    return buffer;    
}

int Transaction::GetFileSize(std::string file_name) {
    BlockId dummy_block(file_name, END_OF_FILE);
    concurrency_manager_->LockShared(dummy_block);

    return file_manager_->Length(file_name);
}


void Transaction::SetFileSize(std::string file_name, int size) {
    BlockId dummy_block(file_name, END_OF_FILE);
    concurrency_manager_->LockExclusive(dummy_block);

    file_manager_->SetFileSize(file_name, size);
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