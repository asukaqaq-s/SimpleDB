#ifndef CONCURRENCY_MANAGER_CC
#define CONCURRENCY_MANAGER_CC

#include "concurrency/transaction.h"
#include "concurrency/concurrency_manager.h"

namespace SimpleDB {

LockManager ConcurrencyManager::lock_manager;

bool ConcurrencyManager::HasSLock(const BlockId &block) {
    return (lock_set_->find(block) != lock_set_->end()) &&
            lock_set_->at(block) == LockMode::SHARED;
}

bool ConcurrencyManager::HasXLock(const BlockId &block) {
    return (lock_set_->find(block) != lock_set_->end()) &&
            lock_set_->at(block) == LockMode::EXCLUSIVE;
}


void ConcurrencyManager::LockShared(const BlockId &block) {
    if (txn_->GetIsolationLevel() == IsoLationLevel::READ_UNCOMMITED) {
        // this isolation level don't need to acquire slock
        return;
    }

    if (!HasSLock(block) && !HasXLock(block)) {
        lock_manager.LockShared(txn_, block);
        (*lock_set_)[block] = LockMode::SHARED;
    }
}

void ConcurrencyManager::LockExclusive(const BlockId &block) {
    if (!HasXLock(block)) {
        
        if(HasSLock(block)) {
            // if this txn has shared lock, we just need to upgrade it 
            lock_manager.LockUpgrade(txn_, block);   
        } else {
            // if this txn has not lock, we need to acquire xlock directly
            lock_manager.LockExclusive(txn_, block);
        }

        (*lock_set_)[block] = LockMode::EXCLUSIVE;
    }
}


void ConcurrencyManager::ReleaseOne(const BlockId &block) {

    if (HasXLock(block)) {
        // we can't release a x-block in growing phase 
        // even if this txn's isolation level is read committed
        return;
    }

    if (HasSLock(block)) {
        lock_manager.UnLock(txn_, block);
    } else {
        SIMPLEDB_ASSERT(false, "");
    }
}

/**
* @brief because we use strong 2pl concurrency strategy
* so we should release all lock when commit or abort
* this function will be called when transaction commit or abort.
*/
void ConcurrencyManager::Release() {
    txn_->SetLockStage(LockStage::SHRINKING);

    for (auto t:*lock_set_) {
        lock_manager.UnLock(txn_, t.first);
    }

    assert(lock_set_->empty());
}

} // namespace SimpleDB


#endif