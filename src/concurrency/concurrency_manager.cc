#ifndef CONCURRENCY_MANAGER_CC
#define CONCURRENCY_MANAGER_CC

#include "concurrency/transaction.h"
#include "concurrency/concurrency_manager.h"

namespace SimpleDB {

LockManager ConcurrencyManager::lock_manager;

bool ConcurrencyManager::HasSLock(const BlockId &block) {
    bool res = held_locks_.find(block) != held_locks_.end();
    return res;
}

bool ConcurrencyManager::HasXLock(const BlockId &block) {
    bool res = held_locks_.find(block) != held_locks_.end();
    if(res) {
        return held_locks_[block] == LockMode::EXCLUSIVE;
    } else 
        return false;
}


void ConcurrencyManager::LockShared(const BlockId &block) {
    if (!HasSLock(block)) {
        lock_manager.LockShared(txn_, block);
        held_locks_[block] = LockMode::SHARED;
    }
}

void ConcurrencyManager::LockExclusive(const BlockId &block) {
    if (!HasXLock(block)) {
        LockShared(block);
        lock_manager.LockExclusive(txn_, block);
        held_locks_[block] = LockMode::EXCLUSIVE;
    }
}
    
/**
* @brief because we use strong 2pl concurrency strategy
* so we should release all lock when commit or abort
* this function will be called when transaction commit or abort.
*/
void ConcurrencyManager::Release() {
    txn_->SetLockStage(LockStage::SHRINKING);
    for (auto t:held_locks_) {
        lock_manager.UnLock(txn_, t.first);
    }
    held_locks_.clear();
}

} // namespace SimpleDB


#endif