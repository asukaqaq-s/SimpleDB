#ifndef LOCK_MANAGER_H
#define LOCK_MANAGER_H

#include "config/type.h"
#include "config/config.h"
#include "file/block_id.h"


#include <condition_variable>
#include <list>
#include <memory>
#include <unordered_map>
#include <iostream>
#include <map>

namespace SimpleDB {

class Transaction;

enum class LockMode {
    SHARED = 1, // for read purpose
    EXCLUSIVE, // for write purpose
};

enum class LockStage {
    GROWING = 0,
    SHRINKING,
};

enum class TransactionState {
    INVALID = 0,
    RUNNING = 1,
    COMMITTED = 2,
    ABORTED = 3,
};

enum class IsoLationLevel {
    READ_UNCOMMITED = 0,
    READ_COMMITED,
    REPETABLE_READ,
    SERIALIZABLE,
};

/**
* @brief SimpleDB has a global lock manager which each transactions
* acquire shared lock(s lock) and executive lock(x lock).
* Every transactions must acquiring the corresponding lock before read and write block.
* If a transaction requests a lock that causes a conflict with an
* existing lock, then that transaction is placed on a wait list.
* There is only one wait list for all blocks.
* when the last lock on a block is unlocked, then all transactions are
* removed from the wait list and resheduled.
* If one of those transactions discovers that the lock it is waiting for
* is still locked, it will place itself back on the wait list.
*/
class LockManager {

// some helper substruct or subclass
    class LockRequest {
    public:
        LockRequest(txn_id_t txn_id, Transaction *txn, LockMode lock_mode) :
            txn_id_(txn_id), txn_(txn), lock_mode_(lock_mode) {}
        
        txn_id_t txn_id_;

        // why we need a txn ptr?
        // because we don't have transaction map in txn class
        // use txn ptr can help us to set the state of txn.
        Transaction* txn_{nullptr};

        LockMode lock_mode_;
        
        bool granted_{false};
    };
    
    class LockRequestQueue {
    public:

        // request list or request queue
        std::list<LockRequest> request_queue_;
        // for notifying blocked transactions on this block
        std::condition_variable wait_cv_;
        // whther lock is held in execlusive mode
        bool writing_{false};
        // count for txn that hold shared lock
        int shared_count_{0};
    };

public:

    LockManager() = default;

    ~LockManager() = default;

    /**
    * @brief acquire a s-lock
    */
    bool LockShared(Transaction *txn, const BlockId &block);

    /**
    * @brief acquire a x-lock
    */
    bool LockExclusive(Transaction *txn, const BlockId &block);
    
    /**
    * @brief upgrade s-lock to x-lock
    */
    bool LockUpgrade(Transaction *txn, const BlockId &block);

    /**
    * @brief release a lock
    */
    bool UnLock(Transaction *txn, const BlockId &block);


    void PrintLockTable();


private:

    /**
    * @brief this method is be used to erase a 
    */
    bool UnLockImp(Transaction *txn, 
                   LockRequestQueue* queue, 
                   const BlockId &block);

    
    /**
    * @brief we should deal deadlock correspond to different protocol
    * 
    * @param txn the transaction for requesting block
    * @param block requested block
    */
    void DeadLockPrevent(Transaction *txn, LockRequestQueue* request_queue, const BlockId &block);

private:
    
    // for protecting lock_table_
    std::mutex latch_;
    // lock table for lock requests
    std::map<BlockId, LockRequestQueue> lock_table_;
    // dead lock deal protocol
    DeadLockResolveProtocol protocol_{GLOBAL_DEAD_LOCK_DEAL_PROTOCOL};
    
    std::chrono::milliseconds wait_max_time_{10000};
    
    int max_time_{10000};
    
};

} // namespace SimpleDB

#endif
