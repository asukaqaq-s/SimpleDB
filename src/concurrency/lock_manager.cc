#ifndef LOCK_MANAGER_CC
#define LOCK_MANAGER_CC

#include "concurrency/lock_manager.h"
#include "config/macro.h"
#include "config/type.h"

#include "concurrency/transaction.h"

#include <iostream>

namespace SimpleDB {

// note that, because SimpleDB use Rigorous 2PL concurrency strategy
// so it will not happen cascading aborts.
// we don't need to care about it, just need to deal with deadlock.
// it will  three case:
// 1. acuqire

bool LockManager::LockShared(Transaction *txn, const BlockId &block) {
    std::unique_lock<std::mutex> latch(latch_);

    SIMPLEDB_ASSERT(txn->stage_ != LockStage::SHRINKING,
                    "Acquire lock when shrink phase");
    
    // read uncommitted doesn't need shared lock
    if (txn->isolation_level_ == IsoLationLevel::READ_UNCOMMITED) {
        SIMPLEDB_ASSERT(false, 
            "trying to acquire shared lock on read uncommitted isolation level");
    }


    // if can not find the block, 
    // we should construct a new request queue into it
    if (lock_table_.find(block) == lock_table_.end()) {
        lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(block), std::forward_as_tuple());
    }
    
    SIMPLEDB_ASSERT(lock_table_.find(block) != lock_table_.end(),
                    "Fail to create a new request queue in lock_table");

    // push the new txn in lock_reuqest_queue
    // save this txn until unlock    
    auto *lock_request_queue = &lock_table_[block];
    lock_request_queue->request_queue_.emplace_back(
        LockRequest(txn->txn_id_, LockMode::SHARED));

    // note that, we can not use std::rbegin in here
    // because it is unthread-safe, maybe happen error.
    auto it = std::prev(lock_request_queue->request_queue_.end());
    
    // if a txn is writing, it must wait until unlock
    auto start = std::chrono::high_resolution_clock::now();
    while (lock_request_queue->writing_ &&
           !WaitTooLong(start)) {
        lock_request_queue->wait_cv_.wait_for(latch, std::chrono::milliseconds(max_time_));
    }

    // if can not acquire a shared lock
    if (lock_request_queue->writing_) {
        throw std::runtime_error("Wait too long when acquire shared-lock");
    }
    // According to different deadlock deal strategy
    // will rollback this transaction
    // return false if this transaction rollback
    // if (txn->state_ == TransactionState::ABORTED) {
    // lock_request_queue->request_queue_->erase(it);
    //     return false;
    // }

    // update necessary meta data
    lock_request_queue->shared_count_ ++;
    it->granted_ = true;
    
    // std::cout << "TX " << std::to_string(txn->GetTxnID()) << " request slock in block" << block.BlockNum() << std::endl;
    // PrintLockTable(block);

    // return true if this transaction acquire the lock
    return true;
}


bool LockManager::LockExclusive(Transaction *txn, const BlockId &block) {
    std::unique_lock<std::mutex> latch(latch_);

    SIMPLEDB_ASSERT(txn->stage_ != LockStage::SHRINKING,
                    "Acquire lock when shrink phase");
    
    // if can not find the block, 
    // we should construct a new request queue into it
    if (lock_table_.find(block) == lock_table_.end()) {
        lock_table_.emplace(std::piecewise_construct, 
        std::forward_as_tuple(block), std::forward_as_tuple());
    }
    
    SIMPLEDB_ASSERT(lock_table_.find(block) != lock_table_.end(),
                    "Fail to create a new request queue in lock_table");
    
    // push the new txn in lock_reuqest_queue
    // save this txn until unlock
    auto *lock_request_queue = &lock_table_[block];
    lock_request_queue->request_queue_.emplace_back(
            txn->txn_id_, LockMode::EXCLUSIVE);
    
    // the iterator points to the new request
    // because we use latch to protect lock manager
    // so, this will not happen to logical error
    auto it = std::prev(lock_request_queue->request_queue_.end());
    
    // If there are others transactions has xlock or slock
    // this transactions will wait until acquire xlock
    auto start = std::chrono::high_resolution_clock::now();
    while ((lock_request_queue->writing_ ||
           lock_request_queue->shared_count_ > 0) &&
           !WaitTooLong(start)) {
        lock_request_queue->wait_cv_.wait_for(latch, std::chrono::milliseconds(max_time_));
    }
    
    // if can not acquire a exclusive lock
    if (lock_request_queue->writing_ || 
        lock_request_queue->shared_count_ > 0) {
        // current, we just do nothing.TODO
        throw std::runtime_error(
            "Wait too long when acquire exclusive-lock"
        );
    }
    
    // we should abort this transaction 
    // if (txn->state_ == TransactionState::ABORTED) {
    //    lock_request_queue->request_queue_->erase(it);
    //    return false;
    // }
    
    // if we acquire the x lock, means no transactions has shared lock
    lock_request_queue->shared_count_ = 0;
    lock_request_queue->writing_ = true;
    it->granted_ = true;
    
    // std::cout << "TX " << std::to_string(txn->GetTxnID()) << " request xlock in block" << block.BlockNum() << std::endl;
    // PrintLockTable(block);
    
    return true;
}



// lock update should 
// 1. not writing in this block
// 2. not other transactions share this block
bool LockManager::LockUpgrade(Transaction *txn, const BlockId &block) {
    std::unique_lock<std::mutex> latch(latch_);
    
    SIMPLEDB_ASSERT(txn->stage_ != LockStage::SHRINKING, 
                    "update lock when shrinking phase");
    
    SIMPLEDB_ASSERT(lock_table_.find(block) != lock_table_.end(),
                    "update lock when not acquire shared lock");

    auto *lock_request_queue = &lock_table_[block];
    
    // go through the queue to find this transaction
    auto it = lock_request_queue->request_queue_.begin();
    for (;it != lock_request_queue->request_queue_.end();it++) {
        if (it->txn_id_ == txn->GetTxnID()) {
            break;
        }
    }
    
    // simple test whether have logical error
    if (it == lock_request_queue->request_queue_.end()) {
        SIMPLEDB_ASSERT(false, "logical error");
    }
    if ((it->granted_ = false) ||
        (it->lock_mode_ != LockMode::SHARED)) {
        SIMPLEDB_ASSERT(false, "logical error");      
    }
    
    // update data before wait for 
    it->lock_mode_ = LockMode::EXCLUSIVE;
    it->granted_ = false;
    lock_request_queue->shared_count_ --;

    auto start = std::chrono::high_resolution_clock::now();
    while ((lock_request_queue->writing_ ||
           lock_request_queue->shared_count_ > 0) &&
           !WaitTooLong(start)) {
        lock_request_queue->wait_cv_.wait_for(latch, wait_max_time_);
    }
    
    if (lock_request_queue->writing_ ||
        lock_request_queue->shared_count_ > 0) {
        throw std::runtime_error(
        "Wait too long when update exclusive-lock");
    }

    it->granted_ = true;
    lock_request_queue->writing_ = true;
    lock_request_queue->shared_count_ = 0;

    // std::cout << "TX " << std::to_string(txn->GetTxnID()) << " request update in block" << block.BlockNum() << std::endl;
    // PrintLockTable(block);

    return true;
}


bool LockManager::UnLock(Transaction *txn, const BlockId &block) {
    std::unique_lock<std::mutex> latch(latch_);
    
    SIMPLEDB_ASSERT(txn->stage_ != LockStage::GROWING,
                    "Unlock when Growing phase");

    auto lock_request_queue = &lock_table_[block];
    auto it = lock_request_queue->request_queue_.begin();
    for (;it != lock_request_queue->request_queue_.end();it ++) {
        if(it->txn_id_ == txn->txn_id_) {
            break;
        }
    }

    if (it == lock_request_queue->request_queue_.end()) {
        SIMPLEDB_ASSERT(false, "release a non-exist lock");
    }
    
    SIMPLEDB_ASSERT(it->granted_, 
        "the transaction does't have lock before unlock");
    
    bool should_notify = false;
    // if current lock is exlusive lock
    // or any shared lock about this lock all released
    // we should notify the waiting transactions
    if (it->lock_mode_ == LockMode::EXCLUSIVE) {
        SIMPLEDB_ASSERT(
            lock_request_queue->shared_count_ == 0, 
            "unlock error");
        
        lock_request_queue->writing_ = false;
        should_notify = true;
    } else {
        SIMPLEDB_ASSERT(
            !lock_request_queue->writing_,
            "unlock error");

        lock_request_queue->shared_count_ --;
        if (lock_request_queue->shared_count_ == 0) {
            should_notify = true;
        }

    }
    
    // std::cout << "should_notify = " << should_notify << " "
    //           << "lockmode = " << static_cast<int>(it->lock_mode_) << "  "
    //           << "shared_count = " << lock_request_queue->shared_count_ << " "
    //           << "writring = " << lock_request_queue->writing_ << " " 
    //           << "block_num = " << block.BlockNum() << std::endl;
    
    // std::cout << "TX " << std::to_string(txn->GetTxnID()) << " release block" << block.BlockNum() << std::endl;
    // PrintLockTable(block);

    lock_request_queue->request_queue_.erase(it);
    // notifly all transactions in wait list
    // rescheduled by themselves
    if(should_notify) {
        lock_request_queue->wait_cv_.notify_all();
    }
    
    return true;
}


bool LockManager::WaitTooLong(std::chrono::time_point<std::chrono::
high_resolution_clock> start) {
    auto end = std::chrono::high_resolution_clock::now(); 
    double elapsed = std::chrono::duration_cast<std::chrono::milliseconds>
        (end - start).count();
    
    return elapsed > max_time_;
}

// TODO:
// TODO:
// ....
bool LockManager::DeadLockDeal(Transaction *txn, const BlockId &block) {

    SIMPLEDB_ASSERT(lock_table_.find(block) != lock_table_.end(),
                    "deadlockdeal: request block which not lock");
    
    auto *lock_request_queue = &lock_table_[block];
    
    // Traverse the request queue to find the 
    // transaction that hold the lock
    auto it = lock_request_queue->request_queue_.begin();
    for (;it != lock_request_queue->request_queue_.end();it ++) {
        if (it->granted_ == true) {
            break;        
        }
    }
    
    // this case should not happen
    SIMPLEDB_ASSERT(it == lock_request_queue->request_queue_.end(),
                    "No transaction hold the block");

    // Transaction *hold_lock_txn = Transaction::TransactionLookUp(it->txn_id_);

    // according to global transaction map, we can use txn_id to lockup
    // a pointer that points to its transaction
    // following, we can view txn_id as timestamp, because it is monotonic increment
    if (protocol_ == DeadLockResolveRrotocol::DO_NOTHING) {
        // do nothing, just test
        std::cout << "Test dead lock deal" << std::endl;
    } else if (protocol_ == DeadLockResolveRrotocol::WAIT_DIE) {
        // inpreemptive
        // if hold_lock_txn.txn_id > this.txn_id, this txn wait.
        // if hold_lock_txn.txn_id < this.txn_id, this txn rollback and restart

    } else if (protocol_ == DeadLockResolveRrotocol::WOUND_WAIT) {
        // preemptive
        // if hold_lock_txn.txn_id < this.txn_id, this txn wait
        // if hold_lock_txn.txn_id > this.txn_id, hold_lock_txn rollback and restart

    }
    return false;
}

} // namespace SimpleDB

#endif