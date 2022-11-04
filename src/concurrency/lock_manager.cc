#ifndef LOCK_MANAGER_CC
#define LOCK_MANAGER_CC

#include "concurrency/lock_manager.h"
#include "config/config.h"
#include "config/exception.h"
#include "config/macro.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"


namespace SimpleDB {

// note that, because SimpleDB use Rigorous 2PL concurrency strategy
// so it will not happen cascading aborts.
// we don't need to care about it, just need to deal with deadlock.

bool LockManager::LockShared(Transaction *txn, const BlockId &block) {
    std::unique_lock<std::mutex> latch(latch_);

    
    // check if growing phase
    SIMPLEDB_ASSERT(txn->GetLockStage() == LockStage::GROWING, 
                    "acquire lock when shrink");
    
    // check if isolation level correct
    SIMPLEDB_ASSERT(txn->GetIsolationLevel() != IsoLationLevel::READ_UNCOMMITED,
                    "isolation level read_uncommitted don't need acquire lock");

    // if lock_request_queue is not exist, we should create one
    if (lock_table_.find(block) == lock_table_.end()) {
        lock_table_.emplace(std::piecewise_construct, 
                            std::forward_as_tuple(block), 
                            std::forward_as_tuple());
    }

    // traverse the lock_request_queue, this request should not exist
    auto lock_request_queue = &lock_table_[block];
    auto request_queue = &lock_request_queue->request_queue_;
    auto it = lock_request_queue->request_queue_.begin();
    for (;it != request_queue->end(); it ++) {
        if (it->txn_id_ == txn->GetTxnID()) {
            break;
        }
    }

    SIMPLEDB_ASSERT(it == request_queue->end(), "request not exist");
    
    // insert request into queue
    request_queue->emplace_back(LockRequest(txn->GetTxnID(), LockMode::SHARED));
    
    
    // find new_inserted request object, it must is in 
    // the end of queue, because we have a latch.
    // note that, we can not use std::rbegin in here
    // because it is unthread-safe, maybe happen error.
    it = std::prev(lock_request_queue->request_queue_.end());
    
    
    // if have other writers, we can't received s-lock
    // wait until writers release lock or kill this writer(WOUND_WAIT)
    if (lock_request_queue->writing_) {
        DeadLockPrevent(txn, lock_request_queue, block);
        
        // according to WOUND_WAIT protocol
        // wait until abort or not have writers
        lock_request_queue->wait_cv_.wait(latch, 
            [txn, lock_request_queue, it]() {
                return txn->IsAborted() ||
                       !lock_request_queue->writing_;
            });
    }

    // if this txn is aborted, this request which not 
    // granted lock should be moved out the queue
    if (txn->IsAborted()) {
        request_queue->erase(it);
        throw TransactionAbortException(txn->GetTxnID(), "acquire s-lock error");
    }


    // current, we have received this lock successfully.
    // update some necessary informations
    it->granted_ = true;
    lock_request_queue->shared_count_ ++;
    assert(it->lock_mode_ == LockMode::SHARED);
    assert(lock_request_queue->writing_ == false);
    (*txn->GetLockSet())[block] = LockMode::SHARED;
    
    return true;
}


bool LockManager::LockExclusive(Transaction *txn, const BlockId &block) {
    std::unique_lock<std::mutex> latch(latch_);

    
     // check if growing phase
    SIMPLEDB_ASSERT(txn->GetLockStage() == LockStage::GROWING, 
                    "acquire lock when shrink");
    
    // check if isolation level correct
    SIMPLEDB_ASSERT(txn->GetIsolationLevel() != IsoLationLevel::READ_UNCOMMITED,
                    "isolation level read_uncommitted don't need acquire lock");

    // if lock_request_queue is not exist, we should create one
    if (lock_table_.find(block) == lock_table_.end()) {
        lock_table_.emplace(std::piecewise_construct, 
                            std::forward_as_tuple(block), 
                            std::forward_as_tuple());
    }

    // traverse the lock_request_queue, this request should not exist
    auto lock_request_queue = &lock_table_[block];
    auto request_queue = &lock_request_queue->request_queue_;
    auto it = lock_request_queue->request_queue_.begin();
    for (;it != request_queue->end(); it ++) {
        if (it->txn_id_ == txn->GetTxnID()) {
            break;
        }
    }


    // why this txn request must not exist?
    // because we think transaction is a single-thread in simpledb
    // txn acquire x-lock means it doesn't have s-lock before
    // and if not receive s-lock, the txn will be blocked until receive it
    SIMPLEDB_ASSERT(it == request_queue->end(), "request not exist");


    request_queue->emplace_back(LockRequest(txn->GetTxnID(), LockMode::EXCLUSIVE));

    // can't use rbegin
    it = std::prev(lock_request_queue->request_queue_.end());

    if (lock_request_queue->writing_ ||
        lock_request_queue->shared_count_ > 0) {
        
        DeadLockPrevent(txn, lock_request_queue, block);
        lock_request_queue->wait_cv_.wait(latch, 
            [txn, lock_request_queue, it]() {
                return  txn->IsAborted() ||
                       (lock_request_queue->shared_count_ == 0 &&
                        lock_request_queue->writing_ &&
                        it->granted_ == true);
            });
    }


    // remove the wait_request of the aborted txn
    if (txn->IsAborted()) {
        request_queue->erase(it);
        throw TransactionAbortException(txn->GetTxnID(), "acquire x-lock error");
    }


    // current, we have received x-lock successfully
    // this txn may not be blocked, so we should update writting_ and granted
    it->granted_ = true;
    lock_request_queue->writing_ = true;
    assert(lock_request_queue->shared_count_ == 0);
    assert(it->txn_id_ == txn->GetTxnID());
    assert(it->lock_mode_ == LockMode::EXCLUSIVE);
    (*txn->GetLockSet())[block] = LockMode::EXCLUSIVE;

    return true;
}


bool LockManager::LockUpgrade(Transaction *txn, const BlockId &block) {
    std::unique_lock<std::mutex> latch(latch_);

    // check if growing phase
    SIMPLEDB_ASSERT(txn->GetLockStage() == LockStage::GROWING, 
                    "acquire lock when shrink");
    
    // check if isolation level correct
    SIMPLEDB_ASSERT(txn->GetIsolationLevel() != IsoLationLevel::READ_UNCOMMITED,
                    "isolation level read_uncommitted don't need acquire lock");
    

    // this lock_queue should exist
    SIMPLEDB_ASSERT(lock_table_.find(block) != lock_table_.end(), "logic error");
    auto lock_request_queue = &lock_table_[block];
    auto request_queue = &lock_request_queue->request_queue_;
    auto it = lock_request_queue->request_queue_.begin();
    for (;it != request_queue->end(); it ++) {
        if (it->txn_id_ == txn->GetTxnID()) {
            break;
        }
    }


    SIMPLEDB_ASSERT(it != request_queue->end(), "request must exist");
    SIMPLEDB_ASSERT(it->lock_mode_ == LockMode::SHARED, "can't upgrade x-lock");
    SIMPLEDB_ASSERT(it->txn_id_ == txn->GetTxnID(), "txn_id not match");
    SIMPLEDB_ASSERT(it->granted_ == true, "txn  should grant this lock");
    SIMPLEDB_ASSERT(lock_request_queue->writing_ == false &&
                    lock_request_queue->shared_count_ > 0, "logic error");
    
    // update infor
    it->granted_ = false;
    it->lock_mode_ = LockMode::EXCLUSIVE;
    lock_request_queue->shared_count_ --;
    

    // if have other other readers
    // it is not possible to have a writer holding a x-lock
    if (lock_request_queue->shared_count_ > 0) {
        DeadLockPrevent(txn, lock_request_queue, block);
        lock_request_queue->wait_cv_.wait(latch, 
            [txn, lock_request_queue, it]() {
                return txn->IsAborted() ||
                       (it->granted_ &&
                        lock_request_queue->writing_ &&
                        lock_request_queue->shared_count_ == 0);
            });  
    }

    

    // remove the wait_request of the aborted txn
    if (txn->IsAborted()) {
        request_queue->erase(it);
        throw TransactionAbortException(txn->GetTxnID(), "acquire x-lock error");
    }


    // current, we have received x-lock successfully
    // this txn may not be blocked, so we should update writting_ and granted
    it->granted_ = true;
    lock_request_queue->writing_ = true;
    assert(lock_request_queue->shared_count_ == 0);
    assert(it->txn_id_ == txn->GetTxnID());
    assert(it->lock_mode_ == LockMode::EXCLUSIVE);
    txn->GetLockSet()->emplace(block, LockMode::EXCLUSIVE);
    (*txn->GetLockSet())[block] = LockMode::EXCLUSIVE;

    return true;
}

bool LockManager::UnLock(Transaction *txn, const BlockId &block) {
    std::unique_lock<std::mutex> latch(latch_);

    // check if shrinking phase
    if (txn->GetIsolationLevel() != IsoLationLevel::READ_COMMITED) {
        SIMPLEDB_ASSERT(txn->GetLockStage() == LockStage::SHRINKING, 
                        "acquire lock when shrink");
    }
    
    // check if isolation level correct
    SIMPLEDB_ASSERT(txn->GetIsolationLevel() != IsoLationLevel::READ_UNCOMMITED,
                    "isolation level read_uncommitted don't need acquire lock");  

    // this lock_queue should exist
    SIMPLEDB_ASSERT(lock_table_.find(block) != lock_table_.end(), "logic error");
    auto lock_request_queue = &lock_table_[block];

    // just call UnLockImp method to update infor
    UnLockImp(txn, lock_request_queue, block);

    return true;
}


bool LockManager::UnLockImp(Transaction *txn, 
                            LockRequestQueue* lock_request_queue, 
                            const BlockId &block) {


    auto request_queue = &lock_request_queue->request_queue_;
    auto it = lock_request_queue->request_queue_.begin();
    for (;it != request_queue->end(); it ++) {
        if (it->txn_id_ == txn->GetTxnID()) {
            break;
        }
    }

    SIMPLEDB_ASSERT(it != request_queue->end(), "request must exist");


    SIMPLEDB_ASSERT(it->granted_, "is not granting this lock");
    
    bool should_notify = false;
    

    if (it->lock_mode_ == LockMode::EXCLUSIVE) {
        assert(lock_request_queue->shared_count_ == 0);
        it->granted_ = false;
        lock_request_queue->writing_ = false;
        should_notify = true;
    }
    else {
        assert(lock_request_queue->writing_ == false);
        it->granted_ = false;
        lock_request_queue->shared_count_ --;
    
        if (lock_request_queue->shared_count_ == 0) {
            should_notify = true;
        }
    }
    

    // remove this request
    lock_request_queue->request_queue_.erase(it);
    
    // In WOUND_WAIT, we can only manually allocate x-locks by ourselves 
    // and cannot automatically let transactions compete for x-locks to 
    // avoid deadlock.we always allocate lock to request which not grant locks  
    // but for s-lock, can automatically let txns compete for s-locks
    if (should_notify && request_queue->size()) {
        auto it_tmp = request_queue->begin();
        SIMPLEDB_ASSERT(it_tmp->granted_ == false, "");

        if (it_tmp->lock_mode_ == LockMode::EXCLUSIVE) {   
            it_tmp->granted_ = true;
            lock_request_queue->writing_ = true;
        }
    }

    // should the txn which should_abort is true should notify other txns?
    // It stands to reason that the lock should be passed to the caller of 
    // deadlockprevent
    if (should_notify) {
        lock_request_queue->wait_cv_.notify_all();
    }

    
    // remove this lock in the lock_set of txn
    txn->GetLockSet()->erase(block); 

    return true;
}

void LockManager::DeadLockPrevent(Transaction *txn, 
                                  LockRequestQueue* lock_request_queue, 
                                  const BlockId &block) {
    
    // find the lockmode of txn
    LockMode lock_mode;
    auto it_origin = lock_request_queue->request_queue_.begin();
    for (; it_origin != lock_request_queue->request_queue_.end(); it_origin++) {
        if (it_origin->txn_id_ == txn->GetTxnID()) {
            lock_mode = it_origin->lock_mode_;
            break;
        }
    }

    assert(it_origin != lock_request_queue->request_queue_.end());

    
    // check if we should notify txn
    // if a txn is aborted, this value is set
    bool should_notify = false;

    // the older the txn_id, the younger the txn's age 
    // if lock_mode is executive, we should abort any requests which txn_id
    // is over than txn->txn_id_.
    // if lock_mode is shared, we just need to abort any x-lock quests which
    // txn_id is over than txn_txn_id_
    auto it = lock_request_queue->request_queue_.begin();
    for (;it != lock_request_queue->request_queue_.end();) {
        bool should_abort = false;

        if (it->txn_id_ > txn->GetTxnID()) {        
            if (lock_mode == LockMode::SHARED && 
                it->lock_mode_ == LockMode::EXCLUSIVE) {
                should_abort = true;
            }
            if (lock_mode == LockMode::EXCLUSIVE) {
                should_abort = true;
            }
        }

        auto old_it = it;
        it ++;
        
        if (should_abort) {
            txn->GetTxnManager()->TransactionLookUp(old_it->txn_id_)->SetAborted();
            should_notify = true;
        }

        if (should_abort && old_it->granted_) {
            UnLockImp(txn->GetTxnManager()->TransactionLookUp(old_it->txn_id_), lock_request_queue, block);
        }
        
    }

    // allocate lock to the first free-txn
    if (!lock_request_queue->writing_ &&
        !lock_request_queue->shared_count_ &&
        lock_request_queue->request_queue_.size()) {
        auto it_tmp = lock_request_queue->request_queue_.begin();
        assert(it_tmp->granted_ == false);

        if (it_tmp->lock_mode_ == LockMode::EXCLUSIVE) {
            it_tmp->granted_ = true;
            lock_request_queue->writing_ = true;
        }
    }

    // awake up the aborted txns
    if (should_notify) {
        lock_request_queue->wait_cv_.notify_all();
    }
    
}

} // namespace SimpleDB


#endif