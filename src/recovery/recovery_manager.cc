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


void RecoveryManager::Commit() {
    int offset = 0;
    buffer_manager_->FlushAll(txn_id_);
    auto commit_record = CommitRecord(txn_id_);
    commit_record.SetPrevLSN(last_lsn_);
    last_lsn_ = log_manager_->AppendLogWithOffset(commit_record, &offset);
    // flush the lsn immediately
    log_manager_->Flush(last_lsn_);
    // no need to update lsn_map_
}

void RecoveryManager::RollBack() {
    int log_offset;
    DoRollBack();
    buffer_manager_->FlushAll(txn_id_);
    auto rollback_record = RollbackRecord(txn_id_);
    rollback_record.SetPrevLSN(last_lsn_);
    last_lsn_ = log_manager_->AppendLogWithOffset(rollback_record, &log_offset);
    // flush the lsn immediately
    log_manager_->Flush(last_lsn_);
    // update lsn_map_
    InsertLsnMap(last_lsn_, log_offset);
}


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
        int offset = lsn_map_[lsn_ptr];
        // move to the specified record
        auto byte_array = log_iter.MoveToRecord(offset);
        auto log_record = LogRecord::DeserializeFrom(byte_array);
        auto log_record_type = log_record->GetRecordType();
        // should not happen 
        SIMPLEDB_ASSERT(log_record_type != LogRecordType::CHECKPOINT ||
                        log_record_type != LogRecordType::INVALID || 
                        log_record_type != LogRecordType::COMMIT, "txn rollback type error");

        if(log_record->GetRecordType() == LogRecordType::BEGIN) {
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
    // find the checkpoint
    int checkpoint_pos = sizeof(int);
    auto log_iter = log_manager_->Iterator();
    int max_lsn = INVALID_LSN;

    // a stupid way to find the checkpoint
    while(log_iter.HasNextRecord()) {
        auto byte_array = log_iter.NextRecord();
        auto log_record = LogRecord::DeserializeFrom(byte_array);
        auto log_record_type = log_record->GetRecordType();
        lsn_t log_record_lsn = log_record->GetLsn();

        max_lsn = log_record_lsn;
        if(log_record_type == LogRecordType::CHECKPOINT) {
            checkpoint_pos = log_iter.GetLogOffset();
        }
    }

    log_manager_->SetLastestLsn(max_lsn); // update to lastest
    return checkpoint_pos;
}


void RecoveryManager::DoRecoverRedo
(std::unordered_map<txn_id_t, lsn_t>* active_txn, int pos) {
    // pos is start positon
    auto log_iter = log_manager_->Iterator();
    
    while(log_iter.HasNextRecord()) {
        // Since nextrecord is called, logiterator at the location of the next record
        // so, we should save the log_record_offset before calling nextrecord
        int old_log_record_offset = log_iter.GetLogOffset();
        auto byte_array = log_iter.NextRecord();
        auto log_record = LogRecord::DeserializeFrom(byte_array);
        auto log_record_type = log_record->GetRecordType();
        txn_id_t log_record_txn = log_record->GetTxnID();
        lsn_t log_record_lsn = log_record->GetLsn();
        // update lsn_map_, should update any record, even commit, rollback and begine
        InsertLsnMap(log_record_lsn, old_log_record_offset);
        
        SIMPLEDB_ASSERT(log_record_type != LogRecordType::CHECKPOINT ||
                        log_record_type != LogRecordType::INVALID, "recover redo error");

        if(log_record_type == LogRecordType::COMMIT ||
           log_record_type == LogRecordType::ROLLBACK ||
           log_record_type == LogRecordType::BEGIN) {
            active_txn->erase(log_record_txn);
            continue;
        }
        
        // update active_txn_
        (*active_txn)[log_record_txn] = log_record_lsn;
        log_record->Redo(txn_);
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
        int offset = lsn_map_[lsn];
        
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