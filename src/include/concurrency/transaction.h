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

    Transaction(FileManager *fm, LogManager *lm, BufferManager *bm);
    
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
    void RollBack();
    
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
    * The transaction manages the buffer for the client.
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
    * @brief return the integer value stored at the specified
    * offset of the specified block.
    * 
    * @param block a reference to a disk block
    * @param offset the byte offset within the block
    * @return the integer stored at that offset
    */
    int GetInt(const BlockId &block, int offset);
    
    
    void SetInt(const BlockId &block, int offset, int new_value,bool is_log);

    void SetString(const BlockId &block, int offset, std::string new_value, bool is_log);
    
    std::string GetString(const BlockId &block, int offset);
    
    int GetFileSize(std::string file_name);

    BlockId Append(std::string file_name);

    int BlockSize();
    
    int AvialableBuffers();
    

public: // todo

    void SetTxnState(TransactionState txn_state) {state_ = txn_state;}

    void SetLockStage(LockStage lks) {stage_ = lks;}

    txn_id_t GetTxnID() { return txn_id_;}

private:

    static txn_id_t NextTxnID();

private:
    
    static TransactionMap txn_map;
    // which be used to append file or get file size
    const int END_OF_FILE = -1;
    // unique concurrency manager
    std::unique_ptr<ConcurrencyManager> concurrency_manager_;
    // unique recovery manager
    std::unique_ptr<RecoveryManager> recovery_manager_;
    // shard filemanager
    FileManager *file_manager_;
    // shared buffermanager
    BufferManager *buffer_manager_;
    // to identify transaction
    txn_id_t txn_id_;
    // buffser list for remembering all pinned buffer
    std::unique_ptr<BufferList> buffers_;
    // 2pl lock stage
    LockStage stage_{LockStage::GROWING};
    // isolation level
    IsoLationLevel isolation_level_{IsoLationLevel::READ_COMMITED};    
    // transactionstate
    TransactionState state_{TransactionState::RUNNING};

};

} // namespace SimpleDB

#endif
