#ifndef CONCURRENCY_MANAGER_H
#define CONCURRENCY_MANAGER_H

#include "concurrency/lock_manager.h"

#include <map>
#include <set>

namespace SimpleDB {

/**
 * @brief
 * The concurrency manager for the transaction. Each transaction has 
 * its own concurrency manager. The concurrency manager keeps track 
 * of flush All which locks the transaction currently has, and interacts
 * with the global lock table as needed. 
 */
class ConcurrencyManager {

    friend class Transaction;

public:
    
    /**
    * @param txn every transactions has their own concurrency manager
    * txn means this manager belong to which transaction
    */
    ConcurrencyManager(Transaction *txn) :txn_(txn) {
        lock_set_ = std::make_unique<std::map<BlockId, LockMode>> ();
    }
    
    ~ConcurrencyManager() = default;

    /**
    * @brief check if has a s-lock in this block
    */
    inline bool HasSLock(const BlockId &block);

    /**
    * @brief check if has a x-lock in this block
    */
    inline bool HasXLock(const BlockId &block);

    /**
    * @brief acquire a s-lock in this block
    * if we have s-lock or x-lock, will not enter lock_manager
    */
    void LockShared(const BlockId &block);

    /**
    * @brief acquire a x-lock in this block
    * if we have s-lock, will upgrade it to x-lock
    * if we have x-lock, will not enter lock_manager
    */
    void LockExclusive(const BlockId &block);   

    /**
    * @brief just release the specified s-lock
    * this method is just be called the txn which 
    * isolation level is read-committed
    */
    void ReleaseOne(const BlockId &block);

    /**
    * @brief because we use strong 2pl concurrency strategy
    * so we should release all lock when commit or abort
    * this function will be called when transaction commit or abort.
    */
    void Release();

private:

    // it corresponding to which transaction
    Transaction *txn_;

    // the global lock manager
    static LockManager lock_manager;

    // hold locks
    std::unique_ptr<std::map<BlockId, LockMode>> lock_set_;
    
};

} // namespace SimpleDB

#endif