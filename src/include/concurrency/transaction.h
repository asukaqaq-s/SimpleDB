#ifndef TRANSACTION_H
#define TRANSACTION_H

#include "recovery/recovery_manager.h"
#include "concurrency/buffer_list.h"
#include "buffer/buffer_manager.h"

#include <string>
#include <memory>
#include <iostream>
#include <mutex>

namespace SimpleDB {

// class RecoveryManager;

/**
* @brief * Provide transaction management for clients, 
* which contains the context required for transaction execution
*/
class Transaction {

public:

    Transaction(FileManager *fm, LogManager *lm, BufferManager *bm);
    
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
    
private:
    
    static txn_id_t Next_Txn_ID;
    
    static std::mutex latch_;

    const int END_OF_FILE = -1;
    
    std::unique_ptr<RecoveryManager> recovery_manager_;

    BufferManager *buffer_manager_;

    FileManager *file_manager_;
    
    txn_id_t txn_id;

    std::unique_ptr<BufferList> buffers_;

    

    static txn_id_t NextTxnID() { 
        std::lock_guard<std::mutex> lock(latch_);
        int x = ++Next_Txn_ID; 
        return x;
    }

};

} // namespace SimpleDB

#endif
