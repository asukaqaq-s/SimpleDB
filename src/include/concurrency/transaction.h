#ifndef TRANSACTION_H
#define TRANSACTION_H

#include "recovery/recovery_manager.h"
#include "concurrency/concurrency_manager.h"
#include "concurrency/buffer_list.h"
#include "buffer/buffer_manager.h"

#include <string>
#include <memory>
#include <iostream>
#include <mutex>

namespace SimpleDB {

class Transaction;

class TransactionMap {

public:

    TransactionMap();
    
    ~TransactionMap();
    
    void InsertTransaction(Transaction *txn);

    void RemoveTransaction(Transaction *txn);

    bool IsTransactionAlive(txn_id_t txn_id);

    Transaction* GetTransaction(txn_id_t txn_id);

    txn_id_t GetNextTransactionID();
    
private:

    std::unordered_map<txn_id_t, Transaction*> txn_map_;

    std::mutex latch_;

    int next_txn_id_{0};
};


/**
* @brief Provide transaction management for clients, 
* which contains the context required for transaction execution
* in transaction object, we can't do actually operations and just
* obtain a buffer object or obtain a global-lock in the specified block
*/
class Transaction {

    friend class LockManager;

public:

    Transaction(FileManager *fm, BufferManager *bm, RecoveryManager *rm);

    Transaction(FileManager *fm, BufferManager *bm, RecoveryManager *rm, txn_id_t id);
    
    ~Transaction();

    /**
    * @brief commit the current transaction
    * flush the commit log records immediately,
    * release all locks, and unpin any pinned buffers
    */
    void Commit();
    
    /**
    * @brief rollback the current transaction
    * Undo any modified values and flush a rollback record to the log
    * release all locks, and unpin any pinned buffers.
    */
    void Abort();
    
    /**
    * @brief go through the log, redo any log after checkpoints
    * rolling back all uncommitted transactions, finally
    * write a quiescent checkpoint record to the log.
    * 
    * This method is called during system startup,
    * before user transactions begin.
    */
    void Recovery();

    /**
    * @brief Unpin the specified block.
    * 
    * @param block a reference to the disk block
    */
    void Unpin(const BlockId &block);

    /**
    * @brief return a buffer which exists in buffer_pool
    * 
    * @param block
    * @return the buffer 
    */
    Buffer* GetBuffer(const BlockId& block);

    /**
    * @brief acquire the specified lock of block
    * 
    * @param lock_mode 
    */
    void AcquireLock(const BlockId &block, LockMode lock_mode);


    /**
    * @brief release s-lock of block
    * 
    * @param block
    */
    void ReleaseLock(const BlockId &block);


    /**
    * @brief Get file size and obtain the s-lock of file
    *
    * @param file_name
    * @return file's size
    */
    int GetFileSize(const std::string &file_name);

    /**
    * @brief Set file size and obtain the x-lock of file
    * we usually use this method to reduce the size of file
    * this method usually be used when undo phase
    */
    void SetFileSize(const std::string &file_name, int size);

    /**
    * @brief Set file size and obtain the x-lock of file
    * we usually use this method to increase the size of file
    */
    BlockId Append(const std::string &file_name);

    /**
    * @brief return the size of block
    */
    int BlockSize();
    
    /**
    * @brief return how many avialable buffers in bufferpool
    */
    int AvialableBuffers();

    static Transaction* TransactionLookUp(txn_id_t txn_id);
    
    RecoveryManager* GetRecoveryMgr() { return recovery_manager_; }

public:

    inline void SetTxnState(TransactionState txn_state) { state_ = txn_state;}

    inline TransactionState GetTxnState() { return state_; }

    inline void SetLockStage(LockStage lks) { stage_ = lks;}

    inline LockStage GetLockStage() { return stage_; }

    inline txn_id_t GetTxnID() { return txn_id_; }

    inline void SetAborted() { state_ = TransactionState::ABORTED; }
    
    inline bool IsAborted() { return state_ == TransactionState::ABORTED; }

    // Isolation level only for read operations, not for
    // write operations. So, unlike deadlock deal protocols
    // we can tolerate with different txns have different isolation levels
    // in SimpleDB, the default isolation level is Serializable
    inline void SetIsolationLevel(IsoLationLevel level) { isolation_level_ = level; }

    inline IsoLationLevel GetIsolationLevel() { return isolation_level_; }

    std::map<BlockId, LockMode>* GetLockSet() { 
        return concurrency_manager_->lock_set_.get(); 
    }
    


private:

    // which be used to append file or get file size
    // to solve Phantom Read
    static constexpr int END_OF_FILE = -1;

    static txn_id_t NextTxnID();

private:
    // global transaction map
    // cuncur_manager use it to get the corresponding transaction
    static TransactionMap txn_map;
    
    // unique concurrency manager
    std::unique_ptr<ConcurrencyManager> concurrency_manager_;
    // buffser list for remembering all pinned buffer
    std::unique_ptr<BufferList> buffers_;

    // shard filemanager
    FileManager *file_manager_;

    // shared recovery manager
    // this is different from SimpleDB-origin
    RecoveryManager* recovery_manager_;
    

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
