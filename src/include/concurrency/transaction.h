#ifndef TRANSACTION_H
#define TRANSACTION_H

#include "recovery/recovery_manager.h"
#include "buffer/buffer_manager.h"
#include "concurrency/lock_manager.h"

#include <forward_list>
#include <functional>
#include <string>
#include <memory>
#include <iostream>
#include <mutex>

namespace SimpleDB {

class TransactionManager;


/**
* @brief Provide transaction management for clients, 
* which contains the context required for transaction execution
* in transaction object, we can't do actually operations and just
* obtain a buffer object or obtain a global-lock in the specified block
*/
class Transaction {

using TxnEndAction = std::function<void()>;

    friend class LockManager;

public:

    Transaction(TransactionManager *txn_mgr, txn_id_t txn_id, 
                IsoLationLevel level = IsoLationLevel::SERIALIZABLE) {
        lock_set_ = std::make_unique<std::map<BlockId, LockMode>> ();                
    }
    
    ~Transaction() = default;

    std::map<BlockId, LockMode>* GetLockSet() { return lock_set_.get(); }

    bool IsExclusiveLock(BlockId block) const { 
        return lock_set_->find(block) != lock_set_->end() && 
               lock_set_->at(block) == LockMode::EXCLUSIVE;
    }

    bool IsSharedLock(BlockId block) const { 
        return lock_set_->find(block) != lock_set_->end() && 
               lock_set_->at(block) == LockMode::SHARED;
    }

    
    bool LockShared(LockManager* lock_mgr, BlockId block) {
        
        if (isolation_level_ == IsoLationLevel::READ_UNCOMMITED) {
            return true;
        }

        if (IsExclusiveLock(block) || IsSharedLock(block)) {
            return true;
        }
        return lock_mgr->LockShared(this, block);
    }

    bool LockExclusive(LockManager* lock_mgr, BlockId block) {
        if (IsSharedLock(block)) {
            return lock_mgr->LockUpgrade(this, block);
        }
        
        if (IsExclusiveLock(block)) {
            return true;
        }

        return lock_mgr->LockExclusive(this, block);
    }


public:

    inline void SetTxnState(TransactionState txn_state) { state_ = txn_state;}

    inline TransactionState GetTxnState() { return state_; }

    inline void SetLockStage(LockStage lks) { stage_ = lks;}

    inline LockStage GetLockStage() { return stage_; }

    inline txn_id_t GetTxnID() { return txn_id_; }

    inline void SetCommitted() { state_ = TransactionState::COMMITTED; }
    
    inline bool IsCommitted() { return state_ == TransactionState::COMMITTED; }

    inline void SetAborted() { state_ = TransactionState::ABORTED; }
    
    inline bool IsAborted() { return state_ == TransactionState::ABORTED; }

    // Isolation level only for read operations, not for
    // write operations. So, unlike deadlock deal protocols
    // we can tolerate with different txns have different isolation levels
    // in SimpleDB, the default isolation level is Serializable
    inline void SetIsolationLevel(IsoLationLevel level) { isolation_level_ = level; }

    inline IsoLationLevel GetIsolationLevel() const { return isolation_level_; }

    inline TransactionManager* GetTxnManager() const { return txn_mgr_; }

private:

    // global transaction manager to complete real op
    TransactionManager *txn_mgr_;

    // the lock set of this transaction granted
    std::unique_ptr<std::map<BlockId, LockMode>> lock_set_;

    // to identify transaction
    txn_id_t txn_id_;

    // 2pl lock stage
    LockStage stage_{LockStage::GROWING};
    
    // isolation level
    IsoLationLevel isolation_level_{IsoLationLevel::SERIALIZABLE};    
    
    // transactionstate
    TransactionState state_{TransactionState::RUNNING};

};

} // namespace SimpleDB

#endif
