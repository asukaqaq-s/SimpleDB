#ifndef RECOVERY_MANAGER_H
#define RECOVERY_MANAGER_H

#include "log/log_manager.h"
#include "buffer/buffer_manager.h"


#include <iostream>
#include <unordered_map>
#include <map>


namespace SimpleDB {

class Transaction;
/**
 * @brief The recovery manager.  
 * 
 * current, due to use undo-redo strategy, we don't flush the buffer of being used immediatly.
 * TODO: non-quiescent checkpoint
 * TODO: clr log
 * TODO: TXN-END log 
 * TODO: will store checkpoint offset in meta-data manager.
 */
class RecoveryManager {

public:


    RecoveryManager(LogManager *lm);

    /**
    * @brief transaction begin
    */
    void Begin(Transaction *txn);
    
    /**
    * @brief if a transaction commit, the log should be flushed out immediately
    * the buffer of transaction just unpin and flush to disk when it is replaced out
    * now, we should flush 2 log_records when commit phase:
    * 1. flush commit log record
    * 2. txn-end log record written immediately after commit log record
    */
    void Commit(Transaction *txn);

    /**
    * @brief if a transaction rollback, the log should be flushed out immediately
    * now, we should flush 2 log_records when rollback phase and rollback phase 
    * has 3 sub-phases 
    * 1. flush abort log record
    * 2. undo phase
    * 3. flush txn-end log record
    */
    void Abort(Transaction *txn);

    void CheckPoint();

    /**
    * @brief corresponding to insert tuple operations
    * write log record before insert op
    */
    lsn_t InsertLogRec(Transaction *txn,
                       const std::string &file_name,
                       const RID &rid,
                       const Tuple &tuple,
                       bool is_clr);

    /**
    * @brief corresponding to delete tuple operations
    * write log record before delete op
    */
    lsn_t DeleteLogRec(Transaction *txn,
                      const std::string &file_name,
                      const RID &rid,
                      const Tuple &tuple,
                      bool is_clr);    

    /**
    * @brief corresponding to update tuple operations
    * write log record before update op
    */
    lsn_t UpdateLogRec(Transaction *txn,
                      const std::string &file_name,
                      const RID &rid,
                      const Tuple &old_tuple,
                      const Tuple &new_tuple,
                      bool is_clr);

    lsn_t InitPageLogRec(Transaction *txn,
                         const std::string file_name,
                         int block_numer,
                         bool is_clr);
    

    void FlushBlock(BlockId block, lsn_t lsn);


    /**
    * @brief recover uncompleted transaction from the log
    * and then write a  checkpoint record to the log and flush it.
    * 
    * Usually be called when db is restarted
    * There are three phases in recover function:
    * 1. scan(analyse): find the checkpoint position and dirty pages
    *                   current, we just need to find the quiescent checkpoint
    * 2. redo: redo any log record before crash.
    * 3. undo: undo any log record of uncommited transaction.
    */
    void Recover(Transaction *txn);
    



private: // helper function


    /**
    * @brief rollback current transaction
    * undo any operations and write the clr log to disk
    */
    void DoRollBack(Transaction* txn);

    /**
    * @brief analyze phase  
    * find the location of the checkpoint 
    */
    void DoAnalyze();

    /**
    * @brief redo phase
    * 
    */
    bool DoRedo(Transaction *txn);
    
    /**
    * @brief undo phase
    */
    void DoUndo(Transaction *txn);

    void TxnEnd(txn_id_t txn_id);

    void RedoLog(LogRecord *log);

    void UndoLog(LogRecord *log, lsn_t undo_lsn);


private: 

    inline lsn_t GetLastLsn(txn_id_t txn_id) {
        std::lock_guard<std::mutex> latch(latch_);
        // if we not find it in txtable
        if (tx_table_.find(txn_id) == tx_table_.end()) {
            return INVALID_LSN;
        }
        return tx_table_[txn_id].last_lsn_;
    }

    inline void SetLastLsn(txn_id_t txn_id, lsn_t lsn) {
        std::lock_guard<std::mutex> latch(latch_);
        if (tx_table_.find(txn_id) == tx_table_.end()) {
            tx_table_[txn_id] = TxTableEntry(lsn, TxStatus::U);
        }
        else {
            tx_table_[txn_id].last_lsn_ = lsn;
        }
    }

    inline TxTableEntry GetTxTableEntry(txn_id_t txn_id) {
        std::lock_guard<std::mutex> latch(latch_);
        // if we not find it in txtable
        if (tx_table_.find(txn_id) == tx_table_.end()) {
            return TxTableEntry();
        }
        return tx_table_[txn_id];
    }

    inline void SetTxTableEntry(txn_id_t txn_id, TxTableEntry tte) {
        std::lock_guard<std::mutex> latch(latch_);
        tx_table_[txn_id] = tte;
    }

    inline void RemoveTableEntry(txn_id_t txn_id) {
        std::lock_guard<std::mutex> latch(latch_);
        tx_table_.erase(txn_id);
    }

// 

    inline lsn_t GetEarlistLsn(const BlockId &block) {
        std::lock_guard<std::mutex> latch(latch_);
        if (dp_table_.find(block) == dp_table_.end()) {
            return INVALID_LSN;
        }
        return dp_table_[block];
    }

    inline void SetEarlistLsn(const BlockId &block, lsn_t lsn) {
        std::lock_guard<std::mutex> latch(latch_);
        if (dp_table_.find(block) == dp_table_.end()) {
            dp_table_[block] = lsn;
        }
    }

    inline void RemoveEarlistLsn(const BlockId &block) {
        std::lock_guard<std::mutex> latch(latch_);
        dp_table_.erase(block);
    }


// these three functions are for manipulation lsn_map_    

    inline void InsertLsnMap(lsn_t lsn, int offset) {
        std::lock_guard<std::mutex> latch(latch_);
        lsn_map_[lsn] = offset;
    }

    inline void RemoveLsnMap(lsn_t lsn) {
        std::lock_guard<std::mutex> latch(latch_);
        lsn_map_.erase(lsn);
    }

    inline int GetLsnMap(lsn_t lsn) {
        std::lock_guard<std::mutex> latch(latch_);
        SIMPLEDB_ASSERT(lsn_map_.find(lsn) != lsn_map_.end(),
                        "log not exist");
        return lsn_map_.at(lsn);
    }

private:
    
    // shared filemanager
    FileManager *file_manager_;
    // shared logmanager
    LogManager *log_manager_;
    // shared buffermanager
    BufferManager *buffer_manager_;

    // these map we have introduced in checkpointend_record
    // recovery manager should maintain the infor which
    // map txn_id --> {the last_lsn of this txn, txn_status }
    std::map<txn_id_t, TxTableEntry> tx_table_;
    // map block --> {the earlist_lsn of this page}
    std::map<BlockId, lsn_t> dp_table_;
    

    // keep track of the offset of a log record
    // map lsn ---> (block_number, offset)
    // during the rollback, it just store the logs of one transaction
    // And during recovery, it store huge logs 
    std::unordered_map<lsn_t, int> lsn_map_;

    std::mutex latch_;
};

} // namespace SimpleDB

#endif