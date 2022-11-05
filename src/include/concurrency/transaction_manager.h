#ifndef TRANSACTION_MANAGER_H
#define TRANSACTION_MANAGER_H

#include "concurrency/transaction.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_map.h"


namespace SimpleDB {

class TableInfo;

/**
* @brief
* transaction manager is responsible to perform the concurrency control protocols 
* when executor is trying to execute transactional operations.
*/
class TransactionManager {

public:

    TransactionManager(std::unique_ptr<LockManager> &&lock_manager, RecoveryManager *rm,
                       FileManager *file_manager, BufferManager *buffer_manager) 
    : lock_mgr_(std::move(lock_manager)), recovery_mgr_(rm), file_manager_(file_manager),
      buffer_manager_(buffer_manager) {
        txn_map_ = std::make_unique<TransactionMap>();
    }

    /**
    * @brief create a new txn and return a transaction object
    * 
    * @param level default level is SERIALIZABLE which can't happen phantom read
    */
    std::unique_ptr<Transaction> Begin(IsoLationLevel level = IsoLationLevel::SERIALIZABLE);


     /**
     * @brief 
     * Commit a transaction and flush commit log immediately
     * @param txn 
     */
    void Commit(Transaction *txn);


    /**
     * @brief 
     * Abort a transaction but not need to flush log immediately
     * @param txn 
     */
    void Abort(Transaction *txn);

    

public:

    inline Transaction* TransactionLookUp(txn_id_t txn_id) { return txn_map_->GetTransaction(txn_id); }

    inline RecoveryManager* GetRecoveryMgr() const { return recovery_mgr_; }

    /**
    * @brief get a lockmanager, this method will only be used to recoverymanager
    */
    inline LockManager* GetLockManager() const { return lock_mgr_.get(); }

private:

    // helper functions
    void ReleaseAllLocks(Transaction *txn);


private:
    
    // lock manager which every txns share it
    std::unique_ptr<LockManager> lock_mgr_;

    // shared recovery manager
    RecoveryManager *recovery_mgr_;

    // transaction map which every txns share it
    std::unique_ptr<TransactionMap> txn_map_;
    
    // global file manager to create a txn need
    FileManager *file_manager_;
    
    // global buffer manager to create a txn need
    BufferManager *buffer_manager_;
};

} // namespace SimpleDB


#endif
