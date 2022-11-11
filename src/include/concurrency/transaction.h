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

    Transaction(FileManager *file_manager,
                BufferManager *buffer_manager,
                LockManager *lock_manager,
                txn_id_t txn_id, 
                IsoLationLevel level = IsoLationLevel::SERIALIZABLE) 
        : file_manager_(file_manager), buffer_manager_(buffer_manager), 
          lock_manager_(lock_manager), txn_id_(txn_id) {
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

    
    bool LockShared(BlockId block) {
        if (isolation_level_ == IsoLationLevel::READ_UNCOMMITED) {
            return true;
        }

        if (IsExclusiveLock(block) || IsSharedLock(block)) {
            return true;
        }
        return lock_manager_->LockShared(this, block);
    }

    bool LockExclusive(BlockId block) {
        if (IsSharedLock(block)) {
            return lock_manager_->LockUpgrade(this, block);
        }
        
        if (IsExclusiveLock(block)) {
            return true;
        }

        return lock_manager_->LockExclusive(this, block);
    }

    bool UnLockWhenRUC(BlockId block) {
        if (isolation_level_ == IsoLationLevel::READ_COMMITED 
               && IsSharedLock(block)) {
            return lock_manager_->UnLock(this, block);     
        }
        return false;
    }

    Buffer *PinBlock(const BlockId &block) const {
        return buffer_manager_->PinBlock(block);
    }

    void UnpinBlock(const BlockId &block) const {
        assert(buffer_manager_->UnpinBlock(block));
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

    inline BufferManager* GetBufferManager() const { return buffer_manager_; }

    inline FileManager* GetFileManager() const { return file_manager_; }


private:

//---------------------------------------------
// global manager which execute a txn need
//---------------------------------------------
    
    // global file manager
    FileManager *file_manager_;

    // global buffer manager
    BufferManager *buffer_manager_;

    // global lock manager
    LockManager *lock_manager_;
    
    // we don't need recovery manager in txn
    // because abort and commit a txn will implemented by txn manager
    
//----------------
// information
//----------------

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
