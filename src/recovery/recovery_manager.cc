#ifndef RECOVERY_MANAGER_CC
#define RECOVERY_MANAGER_CC

#include "recovery/recovery_manager.h"
#include "config/macro.h"

#include <map>
#include <set>
#include <iostream>

namespace SimpleDB {

RecoveryManager::RecoveryManager(Transaction *txn, txn_id_t txn_id, LogManager *lm, BufferManager *bm) :
    txn_(txn), txn_id_(txn_id), log_manager_(lm), buffer_manager_(bm) {
    int log_offset;
    // transaction begin
    auto begin_record = BeginRecord(txn_id_);
    last_lsn_ = log_manager_->AppendLogWithOffset(begin_record, &log_offset);
    earlist_lsn_ = last_lsn_;
    // update lsn_map_
    InsertLsnMap(last_lsn_, log_offset);
}

// not need to flush buffer immediately
// but need to flush the commit log record immediately
void RecoveryManager::Commit() {
    auto commit_record = CommitRecord(txn_id_);
    commit_record.SetPrevLSN(last_lsn_);
    last_lsn_ = log_manager_->AppendLogRecord(commit_record);
    // flush the lsn immediately
    log_manager_->Flush(last_lsn_);
    // no need to update lsn_map_
}

// not need to flush buffer immediately
// but need to flush the rollback log record immediatly
void RecoveryManager::RollBack() {
    int log_offset;
    DoRollBack();
    auto rollback_record = RollbackRecord(txn_id_);
    rollback_record.SetPrevLSN(last_lsn_);
    last_lsn_ = log_manager_->AppendLogWithOffset(rollback_record, &log_offset);
    // flush the lsn immediately
    log_manager_->Flush(last_lsn_);
    // update lsn_map_
    InsertLsnMap(last_lsn_, log_offset);
}

// 
lsn_t RecoveryManager::SetIntLogRec(Buffer *buffer, 
    int offset, int new_value) {
    int log_offset;
    int old_value = buffer->contents()->GetInt(offset);
    BlockId block = buffer->GetBlockID();
    
    auto setint_record = SetIntRecord(txn_id_, block, offset, old_value, new_value);
    setint_record.SetPrevLSN(last_lsn_);
    last_lsn_ = log_manager_->AppendLogWithOffset(setint_record, &log_offset);
    // update lsn_map_
    InsertLsnMap(last_lsn_, log_offset);
    return last_lsn_;
}

lsn_t RecoveryManager::SetStringLogRec(Buffer *buffer,
    int offset, std::string new_value) {
    int log_offset;
    std::string old_value = buffer->contents()->GetString(offset);
    BlockId block = buffer->GetBlockID();

    auto setstring_record = SetStringRecord(txn_id_, block, offset, old_value, new_value);
    setstring_record.SetPrevLSN(last_lsn_);
    last_lsn_ = log_manager_->AppendLogWithOffset(setstring_record, &log_offset);
    // update lsn_map_
    InsertLsnMap(last_lsn_, log_offset); 
    return last_lsn_;
}

void RecoveryManager::DoRollBack() {
    lsn_t lsn_ptr = last_lsn_;
    auto log_iter = log_manager_->Iterator();

    while(true) { /* while will quit when it encounters begin */
        
        // get the offset of the current log
        int offset = GetLsnMap(lsn_ptr);
        // move to the specified record
        auto byte_array = log_iter.MoveToRecord(offset);
        auto log_record = LogRecord::DeserializeFrom(byte_array);
        auto log_record_type = log_record->GetRecordType();
        // should not happen 
        SIMPLEDB_ASSERT(log_record_type != LogRecordType::CHECKPOINT ||
                        log_record_type != LogRecordType::INVALID || 
                        log_record_type != LogRecordType::COMMIT, "txn rollback type error");

        if(log_record_type == LogRecordType::BEGIN) {
            return;
        } 
        // undo
        log_record->Undo(txn_);
        // move to next record
        lsn_ptr = log_record->GetPrevLSN();
    }
}

void RecoveryManager::Recover() {
    // keep track of what txn we need to undo,txn -> last lsn
    std::unique_ptr<std::unordered_map<txn_id_t, lsn_t>> active_txn
                = std::make_unique<std::unordered_map<txn_id_t, lsn_t>>();
    int checkpoint_pos = DoRecoverScan();
    // redo any log after checkpoint 
    DoRecoverRedo(active_txn.get(), checkpoint_pos);
    // undo any uncommited transactions before crashing
    DoRecoverUndo(active_txn.get());
    // checkpoint means that all logs and corresponding operations 
    // that precede it should update to disk
    buffer_manager_->FlushAll();
    auto checkpoint_record = CheckpointRecord();
    // flush log
    lsn_t lsn = log_manager_->AppendLogRecord(checkpoint_record);
    log_manager_->Flush(lsn);
    // not need to update lsn_map_
}


int RecoveryManager::DoRecoverScan() {
    // analyze phase: find the checkpoint
    int checkpoint_pos = 0;
    auto log_iter = log_manager_->Iterator();
    int max_lsn = INVALID_LSN;

    // a stupid way to find the checkpoint
    while(log_iter.HasNextRecord()) {
        auto byte_array = log_iter.CurrentRecord();
        auto log_record = LogRecord::DeserializeFrom(byte_array);
        auto log_record_type = log_record->GetRecordType();
        lsn_t log_record_lsn = log_record->GetLsn();

        max_lsn = log_record_lsn;
        // update the lastest checkpoint position
        if(log_record_type == LogRecordType::CHECKPOINT) {
            checkpoint_pos = log_iter.GetLogOffset();
        }
        log_iter.NextRecord();
    }
    // update to lastest lsn
    log_manager_->SetLastestLsn(max_lsn); 
    return checkpoint_pos;
}


void RecoveryManager::DoRecoverRedo
(std::unordered_map<txn_id_t, lsn_t>* active_txn, int pos) {
    // pos is start positon
    auto log_iter = log_manager_->Iterator(pos);
    
    while(log_iter.HasNextRecord()) {
        // we should update lsn_map_ in redo phase
        // to ensure that can rollback txn in undo phase
        int log_record_offset = log_iter.GetLogOffset();
        auto byte_array = log_iter.CurrentRecord();
        auto log_record = LogRecord::DeserializeFrom(byte_array);
        auto log_record_type = log_record->GetRecordType();
        txn_id_t log_record_txn = log_record->GetTxnID();
        lsn_t log_record_lsn = log_record->GetLsn();
        // update lsn_map_, should update any record, even commit, rollback and begin
        InsertLsnMap(log_record_lsn, log_record_offset);
        
        SIMPLEDB_ASSERT(log_record_type != LogRecordType::INVALID, 
                            "recover redo error");
        
        if(log_record_type == LogRecordType::COMMIT ||
           log_record_type == LogRecordType::ROLLBACK ||
           log_record_type == LogRecordType::BEGIN || 
           log_record_type == LogRecordType::CHECKPOINT
           ) {
            active_txn->erase(log_record_txn);
        } else { // SETINT or SETSTRING, we should redo it
            // update active_txn_
            (*active_txn)[log_record_txn] = log_record_lsn;
            log_record->Redo(txn_);
        }
        log_iter.NextRecord();
    } 
}

void RecoveryManager::DoRecoverUndo
(std::unordered_map<txn_id_t, lsn_t>* active_txn) {
    // undo list
    std::set<lsn_t> next_lsn;
    auto log_iter = log_manager_->Iterator();
    // insert all unfinished txns to undo list
    for (auto t: *active_txn) {
        next_lsn.insert(t.second);
    }
    
    while (!next_lsn.empty()) {
        auto lsn = *next_lsn.rbegin();
        int offset = GetLsnMap(lsn);
        
        auto byte_array = log_iter.MoveToRecord(offset);
        auto log_record = LogRecord::DeserializeFrom(byte_array);
        // undo log
        log_record->Undo(txn_);
        // erase lsn and insert the previous lsn
        next_lsn.erase(lsn);
        if(log_record->GetPrevLSN() != INVALID_LSN) {
            next_lsn.insert(log_record->GetPrevLSN());
        }
    }
    // after undo phase is done, write abort record
    for( auto t:*active_txn) {
        auto txn_id = t.first;
        auto abort_log_record = RollbackRecord(txn_id);
        log_manager_->AppendLogRecord(abort_log_record);
    }
}

} // namespace SimpleDB

#endif