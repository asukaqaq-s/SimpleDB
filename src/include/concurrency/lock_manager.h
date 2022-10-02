#ifndef LOCK_MANAGER_H
#define LOCK_MANAGER_H

#include "config/type.h"
#include "file/block_id.h"

#include <condition_variable>
#include <list>
#include <memory>
#include <unordered_map>
#include <map>

namespace SimpleDB {

class Transaction;

enum class LockMode {
    SHARED = 1, // for read purpose
    EXCLUSIVE, // for write purpose
};

enum class DeadLockResolveRrotocol {
    INVALID = 0,
    DO_NOTHING,
    DL_DETECT,
    WAIT_DIE,
    WOUND_WAIT,
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
* Every transactions should read and write block after acquiring the corresponding lock.
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
        LockRequest(txn_id_t txn_id, LockMode lock_mode) :
            txn_id_(txn_id), lock_mode_(lock_mode) {}
        
        txn_id_t txn_id_;
        
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

    bool LockShared(Transaction *txn, const BlockId &block);

    bool LockExclusive(Transaction *txn, const BlockId &block);
    
    bool LockUpgrade(Transaction *txn, const BlockId &block);

    bool UnLock(Transaction *txn, const BlockId &block);

private:
    
    /**
    * @brief we should deal deadlock correspond to different protocol
    * 
    * @param txn the transaction for requesting block
    * @param block requested block
    */
    bool DeadLockDeal(Transaction *txn, const BlockId &block);

    bool WaitTooLong(
    std::chrono::time_point<std::chrono::high_resolution_clock> start);

private:
    
    // for protecting lock_table_
    std::mutex latch_;
    // lock table for lock requests
    std::map<BlockId, LockRequestQueue> lock_table_;
    // dead lock deal protocol
    DeadLockResolveRrotocol protocol_{DeadLockResolveRrotocol::INVALID};
    
    std::chrono::milliseconds wait_max_time_{10000};
    
    int max_time_{10000};
    
};

} // namespace SimpleDB

#endif
