#ifndef TRANSACTION_MANAGER_CC
#define TRANSACTION_MANAGER_CC

#include "concurrency/transaction_manager.h"
#include "metadata/table_manager.h"

namespace SimpleDB {

std::unique_ptr<Transaction> TransactionManager::Begin
(IsoLationLevel level) {
    
    // get txn_id
    txn_id_t txn_id = txn_map_->GetNextTransactionID();
    
    // create a new txn
    auto txn = std::make_unique<Transaction>(file_manager_, buffer_manager_, lock_mgr_.get(), txn_id, level);
    
    // insert into txn_map
    txn_map_->InsertTransaction(txn.get());

    // write a begin log to logmanager
    recovery_mgr_->Begin(txn.get());


    return txn;
}


void TransactionManager::Commit(Transaction *txn) {
    SIMPLEDB_ASSERT(txn->GetTxnState() == TransactionState::RUNNING, 
                    "a aborted txn can't commit");
    txn->SetCommitted();

    // write a commit log to logmanager
    recovery_mgr_->Commit(txn);

    // release all locks
    ReleaseAllLocks(txn);

    // free the txn content
    txn_map_->RemoveTransaction(txn);
}


void TransactionManager::Abort(Transaction *txn) {
    txn->SetAborted();

    // write a commit log to logmanager
    // the recoverymanager undoes all operations on this transaction
    recovery_mgr_->Abort(txn);

    // release all locks
    // note that we can't release lock before recoverymanager
    // because we use these locks to undo operations
    ReleaseAllLocks(txn);

    // free the txn content
    txn_map_->RemoveTransaction(txn);
}



void TransactionManager::ReleaseAllLocks(Transaction *txn) {
    // start release lock
    txn->SetLockStage(LockStage::SHRINKING);

    for (auto &t:*txn->GetLockSet()) {
        lock_mgr_->UnLock(txn, t.first);
    }
    SIMPLEDB_ASSERT(txn->GetLockSet()->size() == 0, "");
}



} // namespace SimpleDB


#endif