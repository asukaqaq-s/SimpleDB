#ifndef RECOVERY_MANAGER_H
#define RECOVERY_MANAGER_H


#include "buffer/buffer_manager.h"
#include "log/log_manager.h"

#include <iostream>
#include <unordered_map>


namespace SimpleDB {

class Transaction;
/**
 * @brief The recovery manager.  
 * Each transaction has its own recovery manager.
 * 
 * current, due to use undo-redo strategy, we don't flush the buffer of being used immediatly.
 * TODO: non-quiescent checkpoint
 * TODO: clr log
 * TODO: TXN-END log 
 * TODO: will store checkpoint offset in meta-data manager.
 */
class RecoveryManager {

public:


    RecoveryManager(Transaction *txn, txn_id_t txn_id, LogManager *lm, BufferManager *bm);
    
    /**
    * @brief if a transaction commit, the log should be flushed out immediately
    * the buffer of transaction just unpin and flush to disk when it is replaced out
    */
    void Commit();

    /**
    * @brief if a transaction rollback, the log should be flushed out immediately
    * current, we just undo any operations and don't write the clr log to disk
    */
    void RollBack();

    /**
    * @brief recover uncompleted transaction from the log
    * and then write a quiescent checkpoint record to the log and flush it.
    * 
    * Usually be called when db is restarted
    * There are three phases in recover function:
    * 1. scan(analyse): find the checkpoint position and dirty pages
    *                   current, we just need to find the quiescent checkpoint
    * 2. redo: redo any log record before crash.
    * 3. undo: undo any log record of uncommited transaction.
    */
    void Recover();
    
    /**
    * @brief Write a setint record to the log and return its lsn.
    * 
    * @param buffer the buffer conatining the page
    * @param offset the offset of the value in the page
    * @param new_value the value to be written
    * @param is_clr Whether it is clr or not
    *
    * @return the lsn of the log
    */
    lsn_t SetIntLogRec(Buffer *buffer, int offset, int new_value);
    
    /**
    * @brief Write a setstring record to the log and return its lsn.
    * 
    * @param buffer the buffer conatining the page
    * @param offset the offset of the value in the page
    * @param new_value the value to be written
    * @param is_clr Whether it is clr or not
    *
    * @return the lsn of the log
    */
    lsn_t SetStringLogRec(Buffer *buffer, int offset, std::string new_value);
    

private: // helper function
    
    /**
    * @brief rollback current transaction
    * undo any operations but not write the clr log to disk
    */
    void DoRollBack();

    /**
    * @brief analyze phase  
    * find the location of the checkpoint 
    */
    int DoRecoverScan();

    /**
    * @brief redo phase
    * 
    */
    void DoRecoverRedo(std::unordered_map<txn_id_t, lsn_t>* active_txn, int);
    
    /**
    * @brief undo phase
    */
    void DoRecoverUndo(std::unordered_map<txn_id_t, lsn_t>* active_txn);


// these three functions are for manipulation lsn_map_    

    inline void InsertLsnMap(lsn_t lsn, int offset) {
        lsn_map_[lsn] = offset;
    }

    inline void RemoveLsnMap(lsn_t lsn) {
        lsn_map_.erase(lsn);
    }

    inline int GetLsnMap(lsn_t lsn) {
        return lsn_map_.at(lsn);
    }

private:
    
    // shared logmanager
    LogManager *log_manager_;
    // shared buffermanager
    BufferManager *buffer_manager_;
    // belong to which transaction
    Transaction *txn_;
    // the corresponding txn_id in concurrency manager
    txn_id_t txn_id_;
    // the lsn of the lastest log for this transaction
    lsn_t last_lsn_{INVALID_LSN};
    // the lsn of the earliest log for this transaction
    lsn_t earlist_lsn_{INVALID_LSN};
    // keep track of the offset of a log record
    // map lsn ---> (block_number, offset)
    // during the rollback, it just store the logs of one transaction
    // And during recovery, it store huge logs 
    std::unordered_map<lsn_t, int> lsn_map_;
};

} // namespace SimpleDB

#endif