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
* @brief * Provide transaction management for clients, 
* which contains the context required for transaction execution
*/
class Transaction {

    friend class LockManager;

public:

    Transaction(FileManager *fm, BufferManager *bm, RecoveryManager *rm);
    
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
    * @brief Pin the specified block
    * and not require the lock of this block
    * 
    * @param block a reference to the disk block
    */
    void Pin(const BlockId &block);
    
    /**
    * @brief Unpin the specified block.
    * 
    * @param block a reference to the disk block
    */
    void Unpin(const BlockId &block);

    /**
    * @brief read the tuple from the specified block
    * 
    * @param file_name
    * @param rid 
    * @return the tuple stored at that rid
    */
    Buffer* GetBuffer(const std::string &file_name,
                      const RID &rid, 
                      LockMode lock_mode);

    /**
    * @brief txn object does not modify page,
    * in this method we just acquire lock, write
    * log and get buffer
    */

    int GetFileSize(std::string file_name);

    /**
    * @brief this method usually be used when reduce file size
    */
    void SetFileSize(std::string file_name, int size);

    BlockId Append(std::string file_name);

    int BlockSize();
    
    int AvialableBuffers();

    static Transaction* TransactionLookUp(txn_id_t txn_id);
    
    RecoveryManager* GetRecoveryMgr() { return recovery_manager_; }

public: // todo

    void SetTxnState(TransactionState txn_state) {state_ = txn_state;}

    void SetLockStage(LockStage lks) {stage_ = lks;}

    txn_id_t GetTxnID() { return txn_id_;}

private:

    static txn_id_t NextTxnID();

private:
    // global transaction map
    // cuncur_manager use it to get the corresponding transaction
    static TransactionMap txn_map;
    
    // which be used to append file or get file size
    // to solve Phantom Read
    const int END_OF_FILE = -1;
    
    // unique concurrency manager
    std::unique_ptr<ConcurrencyManager> concurrency_manager_;
    // buffser list for remembering all pinned buffer
    std::unique_ptr<BufferList> buffers_;


    
    // shard filemanager
    FileManager *file_manager_;
    // shared buffermanager
    BufferManager *buffer_manager_;

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
