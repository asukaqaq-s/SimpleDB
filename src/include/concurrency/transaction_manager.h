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

    TransactionManager(FileManager *fm, RecoveryManager *rm, BufferManager *bfm) 
    : file_mgr_(fm), recovery_mgr_(rm), buf_mgr_(bfm) {
        lock_mgr_ = std::make_unique<LockManager> ();
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

    // shared file manager
    FileManager *file_mgr_;

    // shared recovery manager 
    RecoveryManager *recovery_mgr_;

    // shared buffer manager
    BufferManager *buf_mgr_;

    // lock manager which every txns share it
    std::unique_ptr<LockManager> lock_mgr_;

    // transaction map which every txns share it
    std::unique_ptr<TransactionMap> txn_map_;
    
};

} // namespace SimpleDB


#endif
