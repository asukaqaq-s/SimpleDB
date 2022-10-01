#ifndef CONCURRENCY_MANAGER_H
#define CONCURRENCY_MANAGER_H

#include "concurrency/lock_manager.h"

#include <map>

namespace SimpleDB {

/**
 * @brief
 * The concurrency manager for the transaction.
 * Each transaction has its own concurrency manager. 
 * The concurrency manager keeps track of flush All which locks the
 * transaction currently has, and interacts with the
 * global lock table as needed. 
 */
class ConcurrencyManager {

public:
    
    /**
    * @param txn every transactions has their own concurrency manager
    * txn means this manager belong to which transaction
    */
    ConcurrencyManager(Transaction *txn) :txn_(txn) {}
    
    ~ConcurrencyManager() = default;

    bool HasSLock(const BlockId &block);

    bool HasXLock(const BlockId &block);


    void LockShared(const BlockId &block);

    void LockExclusive(const BlockId &block);   


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
    // the shard-block i hold
    std::map<BlockId, LockMode> held_locks_;
};

} // namespace SimpleDB

#endif