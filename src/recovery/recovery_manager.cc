#ifndef RECOVERY_MANAGER_CC
#define RECOVERY_MANAGER_CC

#include "recovery/recovery_manager.h"
#include "config/macro.h"
#include "concurrency/transaction.h"

#include <map>
#include <set>
#include <iostream>
#include <queue>

namespace SimpleDB
{

RecoveryManager::RecoveryManager(LogManager *lm) : log_manager_(lm) {}

void RecoveryManager::Begin(Transaction *txn) {
    
    int log_offset;
    txn_id_t txn_id = txn->GetTxnID();
    lsn_t last_lsn;

    // transaction begin
    auto begin_record = BeginRecord(txn_id);
    last_lsn = log_manager_->AppendLogWithOffset(begin_record, &log_offset);

    // update tx_table
    SetTxTableEntry(txn_id, TxTableEntry(last_lsn, TxStatus::U));
    // update lsn_map_
    InsertLsnMap(last_lsn, log_offset);
}

void RecoveryManager::Commit(Transaction *txn) {
    
    // not need to flush buffer immediately
    // but need to flush the commit log record immediately
    txn_id_t txn_id = txn->GetTxnID();
    lsn_t last_lsn = GetLastLsn(txn_id);

    // 1. transaction commit
    auto commit_record = CommitRecord(txn_id);
    commit_record.SetPrevLSN(last_lsn);
    last_lsn = log_manager_->AppendLogRecord(commit_record);

    // flush the lsn immediately
    // no need to update lsn_map_ and txn_map
    log_manager_->Flush(last_lsn);

    // 2. write the txn-end log after commit log immediately
    TxnEnd(txn_id);

    // should we flush the txn-end immediately ?
    // i think this is unnecessary
    // log_manager_->Flush(last_lsn_);
}

void RecoveryManager::Abort(Transaction *txn) {
    
    // not need to flush buffer immediately
    // and not need to flush log immediately
    int offset;
    txn_id_t txn_id = txn->GetTxnID();
    lsn_t last_lsn = GetLastLsn(txn_id);

    // 1. transaction abort
    auto abort_record = AbortRecord(txn_id);
    abort_record.SetPrevLSN(last_lsn);
    // abort is not a clr log
    // so we don't need to set undonext lsn

    last_lsn = log_manager_->AppendLogWithOffset(abort_record, &offset);

    // update lsn_map and txn_map
    // why we need to do it in abort phase
    // because we should produce clr log and undo every ops
    // note that, abort is not the last lsn of this txn
    SetLastLsn(txn_id, last_lsn);
    InsertLsnMap(last_lsn, offset);

    // 2. undo this txn
    // 3. write txn-end log to logmanager
    DoRollBack(txn);
}


void RecoveryManager::CheckPoint() {
    int offset;
    auto chkpt_begin = ChkptBeginRecord();
    lsn_t prev_lsn = log_manager_->AppendLogRecord(chkpt_begin);
    auto chkpt_end = ChkptEndRecord(tx_table_, dp_table_);
    
    chkpt_end.SetPrevLSN(prev_lsn);
    prev_lsn = log_manager_->AppendLogWithOffset(chkpt_end, &offset);

    // flush to disk
    log_manager_->SetMasterLsnOffset(offset);
    log_manager_->Flush(prev_lsn);
}


lsn_t RecoveryManager::InsertLogRec(Transaction *txn,
                                    const std::string &file_name,
                                    const RID &rid,
                                    const Tuple &tuple,
                                    bool is_clr) {
    
    int offset;
    txn_id_t txn_id = txn->GetTxnID();
    lsn_t last_lsn = GetLastLsn(txn_id);
    BlockId block(file_name, rid.GetBlockNum());

    auto insert_record = InsertRecord(txn_id, file_name, rid, tuple);
    insert_record.SetPrevLSN(last_lsn);
    insert_record.SetCLR(is_clr);
    last_lsn = log_manager_->AppendLogWithOffset(insert_record, &offset);

    // update lsn_map and txn_map
    // this is not clr log
    SetLastLsn(txn_id, last_lsn);
    InsertLsnMap(last_lsn, offset);

    SIMPLEDB_ASSERT(GetTxTableEntry(txn_id).status_ == TxStatus::U,
                    "current it should not commit");

    // moreover, update dp_table is necessary
    SetEarlistLsn(block, last_lsn);
    return last_lsn;
}

lsn_t RecoveryManager::DeleteLogRec(Transaction *txn,
                                    const std::string &file_name,
                                    const RID &rid,
                                    const Tuple &tuple,
                                    bool is_clr) {
    
    int offset;
    txn_id_t txn_id = txn->GetTxnID();
    lsn_t last_lsn = GetLastLsn(txn_id);
    BlockId block(file_name, rid.GetBlockNum());

    auto delete_record = DeleteRecord(txn_id, file_name, rid, tuple);
    delete_record.SetPrevLSN(last_lsn);
    delete_record.SetCLR(is_clr);
    last_lsn = log_manager_->AppendLogWithOffset(delete_record, &offset);

    // update lsn_map and txn_map
    // this is not clr log
    SetLastLsn(txn_id, last_lsn);
    InsertLsnMap(last_lsn, offset);

    SIMPLEDB_ASSERT(GetTxTableEntry(txn_id).status_ == TxStatus::U,
                    "current it should not commit");

    // moreover, update dp_table is necessary
    SetEarlistLsn(block, last_lsn);
    return last_lsn;
}

lsn_t RecoveryManager::UpdateLogRec(Transaction *txn,
                                    const std::string &file_name,
                                    const RID &rid,
                                    const Tuple &old_tuple,
                                    const Tuple &new_tuple,
                                    bool is_clr) {
    
    // update is same to insert and delete
    int offset;
    txn_id_t txn_id = txn->GetTxnID();
    lsn_t last_lsn = GetLastLsn(txn_id);
    BlockId block(file_name, rid.GetBlockNum());

    auto update_record = UpdateRecord(txn_id, file_name, rid, old_tuple, new_tuple);
    update_record.SetPrevLSN(last_lsn);
    update_record.SetCLR(is_clr);
    last_lsn = log_manager_->AppendLogWithOffset(update_record, &offset);

    // update lsn_map and txn_map
    // this is not clr log
    SetLastLsn(txn_id, last_lsn);
    InsertLsnMap(last_lsn, offset);

    SIMPLEDB_ASSERT(GetTxTableEntry(txn_id).status_ == TxStatus::U,
                    "current it should not commit");

    // moreover, update dp_table is necessary
    SetEarlistLsn(block, last_lsn);
    return last_lsn;
}


lsn_t RecoveryManager::InitPageLogRec(Transaction *txn,
                                      const std::string file_name,
                                      int block_numer,
                                      bool is_clr) {
    
    int offset;
    txn_id_t txn_id = txn->GetTxnID();
    lsn_t last_lsn = GetLastLsn(txn_id);
    BlockId block(file_name, block_numer);

    auto init_record = InitPageRecord(txn_id, file_name, block_numer);
    init_record.SetPrevLSN(last_lsn);
    init_record.SetCLR(is_clr);
    last_lsn = log_manager_->AppendLogWithOffset(init_record, &offset);

    // update lsn_map and txn_map
    // this is not clr log
    SetLastLsn(txn_id, last_lsn);
    InsertLsnMap(last_lsn, offset);

    SIMPLEDB_ASSERT(GetTxTableEntry(txn_id).status_ == TxStatus::U,
                    "current it should not commit");

    // moreover, update dp_table is necessary
    SetEarlistLsn(block, last_lsn);
    return last_lsn;                                       
}

void RecoveryManager::FlushBlock(BlockId block, lsn_t lsn) {
    log_manager_->Flush(lsn);
    
    // SIMPLEDB_ASSERT(lsn >= GetEarlistLsn(block), "logic error");

    
    // bacause we have written the block to disk, the block isn't dirty
    // so we should erase it from dp_table_
    RemoveEarlistLsn(block);
}


void RecoveryManager::DoRollBack(Transaction *txn) {
    // this method has two tasks:
    // 1. undo any ops and write clr log to disk
    // 2. write txn-end log to disk and remove it from txn-map

    // Sort from largest to smallest, statify the rule of undo
    std::priority_queue<int> toUndo;
    txn_id_t txn_id = txn->GetTxnID();
    auto log_iter = log_manager_->Iterator();

    if (GetLastLsn(txn_id) != INVALID_LSN)
    {
        toUndo.push(GetLastLsn(txn_id));
    }

    // if this is the last log to undo, we should write a txn-end to disk
    // because we just undo one txn, so not write this log immediately
    while (toUndo.size())
    {   
        
        int offset;
        lsn_t lsn = toUndo.top();
        toUndo.pop();

        // next lsn is not exist
        if (lsn == INVALID_LSN)
        {
            break;
        }

        offset = GetLsnMap(lsn);
        log_iter.MoveToRecord(offset);

        // get log record object
        auto log_record_vec = log_iter.CurrentRecord();
        auto log_record = LogRecord::DeserializeFrom(log_record_vec);
        LogRecordType type = log_record->GetRecordType();
        lsn_t prev_lsn = log_record->GetPrevLSN();
        lsn_t last_lsn = GetLastLsn(txn_id); 
        lsn_t curr_lsn; // clr log's lsn
        BlockId block;  // only update-relative log will need it

        SIMPLEDB_ASSERT(type != LogRecordType::TXNEND &&
                        type != LogRecordType::COMMIT &&
                        type != LogRecordType::CHECKPOINTBEGIN &&
                        type != LogRecordType::CHECKPOINTEND,
                        "log type not match");

        if (log_record->IsCLR()) 
        {
            SIMPLEDB_ASSERT(false, "rollback a txn can't encounter clr");
        }


        if (type == LogRecordType::BEGIN) 
        {
            // BEGIN always is the first log of txn
            SIMPLEDB_ASSERT(prev_lsn == INVALID_LSN, "begin should not have prevlsn");
            break;
        }

        SIMPLEDB_ASSERT(prev_lsn != INVALID_LSN,
                        "not begin record, it should have prevlsn");

        if (type == LogRecordType::ABORT)
        {
            // There is at least one "begin" log before the "abort" log
            // so we just need to push it
            toUndo.push(prev_lsn);
            continue;
        }


        switch (type)
        {
        case LogRecordType::INSERT:
        {
            InsertRecord *log = dynamic_cast<InsertRecord *>(log_record.get());
            std::string file_name = log->GetFileName();
            Tuple tuple = log->GetTuple();
            RID rid = log->GetRID();

            // 1. write clr log to disk, set the information of clr log
            auto clr_record = std::make_shared<DeleteRecord>(txn_id, file_name, rid, tuple);
            clr_record->SetCLR(true);
            clr_record->SetPrevLSN(last_lsn);
            clr_record->SetUndoNext(prev_lsn);
            curr_lsn = log_manager_->AppendLogWithOffset(*clr_record, &offset);

            // 2. undo
            log->Undo(txn, curr_lsn);

            // 3. update block
            block = BlockId(file_name, rid.GetBlockNum());
            break;
        }

        case LogRecordType::DELETE:
        {
            DeleteRecord *log = dynamic_cast<DeleteRecord *>(log_record.get());
            std::string file_name = log->GetFileName();
            Tuple tuple = log->GetTuple();
            RID rid = log->GetRID();

            // 1. write clr to disk
            auto clr_record = std::make_shared<InsertRecord>(txn_id, file_name, rid, tuple);
            clr_record->SetCLR(true);
            clr_record->SetPrevLSN(last_lsn);
            clr_record->SetUndoNext(prev_lsn);
            curr_lsn = log_manager_->AppendLogWithOffset(*clr_record, &offset);

            // 2. undo
            log->Undo(txn, curr_lsn);

            // 3. update block
            block = BlockId(file_name, rid.GetBlockNum());
            break;
        }

        case LogRecordType::UPDATE:
        {
            UpdateRecord *log = dynamic_cast<UpdateRecord *>(log_record.get());
            std::string file_name = log->GetFileName();
            Tuple old_tuple = log->GetOldTuple();
            Tuple new_tuple = log->GetNewTuple();
            RID rid = log->GetRID();

            // 1. write clr to disk
            auto clr_record = std::make_shared<UpdateRecord>(txn_id, file_name, rid, new_tuple, old_tuple);
            clr_record->SetCLR(true);
            clr_record->SetPrevLSN(last_lsn);
            clr_record->SetUndoNext(prev_lsn);
            curr_lsn = log_manager_->AppendLogWithOffset(*clr_record, &offset);

            // 2. undo
            log->Undo(txn, curr_lsn);

            // 3. update block
            block = BlockId(file_name, rid.GetBlockNum());
            break;
        }

        case LogRecordType::INITPAGE:
        {   
            InitPageRecord *log = dynamic_cast<InitPageRecord *>(log_record.get());
            BlockId block = log->GetBlockID();

            // 1. write a clr log to disk
            auto clr_record = std::make_shared<InitPageRecord>(txn_id, block.FileName(), block.BlockNum());
            clr_record->SetCLR(true);
            clr_record->SetPrevLSN(last_lsn);
            clr_record->SetUndoNext(prev_lsn);
            curr_lsn = log_manager_->AppendLogWithOffset(*clr_record, &offset);

            // 2. undo
            log->Undo(txn, curr_lsn);
            
            // 3. update block

            break;
        }

        default:
            SIMPLEDB_ASSERT(false, "should not happen");
            break;
        }

        // remember push prev lsn
        toUndo.push(prev_lsn);
        // update lsn_map, txn_table and dp_table
        InsertLsnMap(curr_lsn, offset);
        SetLastLsn(txn_id, curr_lsn);
        SetEarlistLsn(block, curr_lsn);
    }

    // write txn-end log to this txn
    TxnEnd(txn_id);
}

void RecoveryManager::Recover(Transaction *txn) {
    
    DoAnalyze();
    if (DoRedo(txn) == false) {
        return;
    }
    DoUndo(txn);

    // it 's a good time to write a checkpoint
    CheckPoint();
}

void RecoveryManager::DoAnalyze() {
    // run analyze phase in ARIES:
    // 1. get the lastest checkpoint log record
    // 2. get txn_table and dp_table from checkpoint end log
    // 3. scan backward, update txn_table and dp_table
    //    (1) update txn_table 's last_lsn and txstatus(C,U)
    //    (2) update dp_table 's earliest_lsn
    
    // 1. get the lastest checkpoint log record
    int chkpt_offset = log_manager_->GetMasterLsnOffset();
    auto log_iter = log_manager_->Iterator();
   
    // 2. get txn_table and dp_table
    if (chkpt_offset == -1) {
        log_iter.MoveToRecord(0);

        // checkpoint not exist, so txn_table and dp_table is empty
        tx_table_.clear();
        dp_table_.clear();
    }
    else {
        log_iter.MoveToRecord(chkpt_offset);
        
        // checkpoint exist, we can get tx_table and dp_table from log
        auto log_record_vec = log_iter.CurrentRecord();
        auto log_record = std::dynamic_pointer_cast<ChkptEndRecord>
                          (LogRecord::DeserializeFrom(log_record_vec));
        tx_table_ = log_record->GetTxnTable();
        dp_table_ = log_record->GetDPTable();
    }


    // 3. scan backward, update txn_table and dp_table
    while (log_iter.HasNextRecord()) {
        auto log_record_vec = log_iter.CurrentRecord();
        auto log_record = LogRecord::DeserializeFrom(log_record_vec);
        auto log_record_type = log_record->GetRecordType();
        txn_id_t log_txn_id = log_record->GetTxnID();
        lsn_t log_lsn = log_record->GetLsn();
        
        // 3.1 update txn_table
        if (log_record_type == LogRecordType::COMMIT) {
            // modify this txn to commit status
            SetTxTableEntry(log_txn_id, TxTableEntry(log_lsn, TxStatus::C));
        }
        else {
            // modify this txn to undo status
            SetTxTableEntry(log_txn_id, TxTableEntry(log_lsn, TxStatus::U));
        }

        // update tx_table by log after checkpoint
        SetLastLsn(log_txn_id, log_lsn);


        // if this txn had terminated before crash, we should erase it.
        // we don't need to undo it and just need to redo it.
        if (log_record_type == LogRecordType::TXNEND) {
            RemoveTableEntry(log_txn_id);
        }
        
        BlockId block;
        
        
        // 3.2 update dp_table
        // only consider about update、insert、delete and initpage log
        // note that clr log is included by it.
        if (log_record_type == LogRecordType::UPDATE) {
            auto new_log_record = std::dynamic_pointer_cast<UpdateRecord>(log_record);
            block = BlockId(new_log_record->GetFileName(), 
                            new_log_record->GetRID().GetBlockNum()); 
        }
        else if (log_record_type == LogRecordType::INSERT) {
            auto new_log_record = std::dynamic_pointer_cast<InsertRecord>(log_record);
            block = BlockId(new_log_record->GetFileName(), 
                            new_log_record->GetRID().GetBlockNum()); 
        }
        else if (log_record_type == LogRecordType::DELETE) {
            auto new_log_record = std::dynamic_pointer_cast<DeleteRecord>(log_record);
            block = BlockId(new_log_record->GetFileName(), 
                            new_log_record->GetRID().GetBlockNum()); 
        }
        else if (log_record_type == LogRecordType::INITPAGE) { // init page
            auto new_log_record = std::dynamic_pointer_cast<InitPageRecord>(log_record);
            block = BlockId(new_log_record->GetBlockID()); 
        }


        if (block.FileName().size()) {
            SetEarlistLsn(block, log_lsn);
        }

        log_iter.NextRecord();
    }
}

bool RecoveryManager::DoRedo(Transaction *txn) {
    // run redo phase in ARIES:
    // 1. get the minimum lsn of dirty pages's earliest lsn
    // 2. move to this lsn 
    // 3. start redo, we have 2-type logs and different deal schemes
    //    (1) for initpage, insert, delete, update, we should redo and update pagelsn
    //        note that, just redo the log which the corresponding block exist in dp-table
    //        and the corresponding lsn > earliest lsn
    //    (2) for other types, such as txn-end, commit, abort and so on, just pass
    // 4. write txn-end log to committed txn and note that we have
    //    remove end-txn(which had written txn-end log) in analyze phase. 

    if (dp_table_.empty()) {
        // not need to redo
        return true;
    }


    lsn_t min_lsn = INVALID_LSN;

    /* 1. get the minimal lsn */
    for (auto t:dp_table_) {
        if (min_lsn == INVALID_LSN)
            min_lsn = t.second;
        min_lsn = std::min(min_lsn, t.second);
    }

    SIMPLEDB_ASSERT(min_lsn > INVALID_LSN, "logic error");

    /* 2. move to this lsn */
    auto log_iter = log_manager_->Iterator();
    
    // i think this is a stupid way to find lsn
    // but lsn_map don't president in disk so that
    // we can't get the offset of min_lsn
    while (log_iter.HasNextRecord()) {
        auto log_record_vec = log_iter.CurrentRecord();
        auto log_record = LogRecord::DeserializeFrom(log_record_vec);
        lsn_t log_lsn = log_record->GetLsn();
        
        if (log_lsn == min_lsn) {
            break;
        }   

        InsertLsnMap(log_lsn, log_iter.GetLogOffset());
        log_iter.NextRecord();   
    }

    /* 3. start redo */
    // note that in this phase, we just redo and don't need to undo
    // so that we don't need to write log in this phase
    // for commited txn, un-finish txn, end-txn, abort-txn, we all
    // need to redo
    while (log_iter.HasNextRecord()) {
        auto log_record_vec = log_iter.CurrentRecord();
        auto log_record = LogRecord::DeserializeFrom(log_record_vec);
        auto log_record_type = log_record->GetRecordType();
        lsn_t log_lsn = log_record->GetLsn();
        
        BlockId block;
        if (log_record_type == LogRecordType::UPDATE) {
            auto new_log_record = std::dynamic_pointer_cast<UpdateRecord>(log_record);
            block = BlockId(new_log_record->GetFileName(), 
                            new_log_record->GetRID().GetBlockNum()); 
        }
        else if (log_record_type == LogRecordType::INSERT) {
            auto new_log_record = std::dynamic_pointer_cast<InsertRecord>(log_record);
            block = BlockId(new_log_record->GetFileName(), 
                            new_log_record->GetRID().GetBlockNum()); 
        }
        else if (log_record_type == LogRecordType::DELETE) {
            auto new_log_record = std::dynamic_pointer_cast<DeleteRecord>(log_record);
            block = BlockId(new_log_record->GetFileName(), 
                            new_log_record->GetRID().GetBlockNum()); 
        }
        else if (log_record_type == LogRecordType::INITPAGE){ // init page
            auto new_log_record = std::dynamic_pointer_cast<InitPageRecord>(log_record);
            block = BlockId(new_log_record->GetBlockID()); 
        }

        // even clr log also need to redo
        if (block.FileName().size() && 
            GetEarlistLsn(block) <= log_lsn // this op not update to disk
           ) {
            // why this is <= but not < in here ? it's interesting 
            // and in redo method, if log_lsn <= page_lsn,  also don't need to update

            if (log_record_type == LogRecordType::INITPAGE &&
                log_record->IsCLR()) {
                log_record->Undo(txn, INVALID_LSN);
            } else {
                log_record->Redo(txn);
            } 
        }

        // for other log-type, we don't need to do anything

        InsertLsnMap(log_lsn, log_iter.GetLogOffset());
        log_iter.NextRecord();
    }

    /* 4. write an end for every commited Tx */

    for (auto t:tx_table_) {
        if (t.second.status_ == TxStatus::C) {
            txn_id_t txn_id = t.first;

            TxnEnd(txn_id);
        }
    }

    return true;
}


void RecoveryManager::DoUndo(Transaction *txn) {
    // run undo phase in ARIES:
    // undo phase is similar to dorollback method
    // but we choose to write txn-end log as early as possible
    // 1. update undo_list by tx_able
    // 2. undo start with the largest LSN and move forward
    //    maintain undo_list, dp_table, tx_table and lsn_map 
    //    have been updated in previous phases 
    // 3. write txn-end logs for ervery txn in tx_table

    std::priority_queue<lsn_t> toUndo;
    auto log_iter = log_manager_->Iterator();

    /* 1. update undo_list */
    for (auto t:tx_table_) {
        if (t.second.last_lsn_ == INVALID) {
            SIMPLEDB_ASSERT(false, "");
        }
        toUndo.push(t.second.last_lsn_);
    }

    while (toUndo.size()) {
        int offset;
        lsn_t lsn = toUndo.top();
        toUndo.pop();

         // next lsn is not exist
        if (lsn == INVALID_LSN) {
            break;
        }

        offset = GetLsnMap(lsn);
        log_iter.MoveToRecord(offset);

        auto log_record_vec = log_iter.CurrentRecord();
        auto log_record = LogRecord::DeserializeFrom(log_record_vec);
        auto log_record_type = log_record->GetRecordType();
        txn_id_t txn_id = log_record->GetTxnID();
        lsn_t prev_lsn = log_record->GetPrevLSN();
        lsn_t last_lsn = GetLastLsn(txn_id); 
        lsn_t curr_lsn; // clr log's lsn

       
        SIMPLEDB_ASSERT(log_record->GetLsn() == lsn, "log error");

        if (log_record->IsCLR()) {
            // When we first encounter the CLR log, usually that log is 
            // the last log for this transaction and its undo_next points 
            // to the next log to undo we just need to push undo_next lsn 
            // into work_queue instead of prevlsn

            // if this is clr log, we have updated it when redo phase
            // so don't need to update here
            lsn_t undo_next = log_record->GetUndoNext();

            SIMPLEDB_ASSERT(undo_next != INVALID_LSN, "the last undo log is begin log");
            toUndo.push(undo_next);
            continue;
        }
        else { // non-clr
            // for begin, commit, txn-end log 
            if (log_record_type == LogRecordType::COMMIT) {
                SIMPLEDB_ASSERT(false, "have remove it in redo phase");
            } else if (log_record_type == LogRecordType::BEGIN) {
                TxnEnd(txn_id);
                continue;
            } else if (log_record_type == LogRecordType::TXNEND) {
                SIMPLEDB_ASSERT(false, "have remove");
            }

            // for abort log
            if (log_record_type == LogRecordType::ABORT) {

                SIMPLEDB_ASSERT(prev_lsn != INVALID_LSN, "");
                toUndo.push(prev_lsn);
                continue;
            }

            BlockId block; // dirty page

            // for update, insert, delete, initpage log
            switch (log_record_type)
            {
            case LogRecordType::INSERT:
            {                  
                InsertRecord *log = dynamic_cast<InsertRecord *>(log_record.get());
                std::string file_name = log->GetFileName();
                Tuple tuple = log->GetTuple();
                RID rid = log->GetRID();

                // 1. write clr log to disk, set the information of clr log
                auto clr_record = std::make_shared<DeleteRecord>(txn_id, file_name, rid, tuple);
                clr_record->SetCLR(true);
                clr_record->SetPrevLSN(last_lsn);
                clr_record->SetUndoNext(prev_lsn);
                curr_lsn = log_manager_->AppendLogRecord(*clr_record);

                // 2. undo
                log->Undo(txn, curr_lsn);

                // 3. update block
                block = BlockId(file_name, rid.GetBlockNum());
                break;
            }


            case LogRecordType::DELETE:
            {    
                DeleteRecord *log = dynamic_cast<DeleteRecord *>(log_record.get());
                std::string file_name = log->GetFileName();
                Tuple tuple = log->GetTuple();
                RID rid = log->GetRID();

                // 1. write clr log to disk, set the information of clr log
                auto clr_record = std::make_shared<InsertRecord>(txn_id, file_name, rid, tuple);
                clr_record->SetCLR(true);
                clr_record->SetPrevLSN(last_lsn);
                clr_record->SetUndoNext(prev_lsn);
                curr_lsn = log_manager_->AppendLogRecord(*clr_record);

                // 2. undo
                log->Undo(txn, curr_lsn);

                // 3. update block
                block = BlockId(file_name, rid.GetBlockNum());
                break;
            
            }

            case LogRecordType::UPDATE:
            {   
                UpdateRecord *log = dynamic_cast<UpdateRecord *>(log_record.get());
                std::string file_name = log->GetFileName();
                Tuple old_tuple = log->GetOldTuple();
                Tuple new_tuple = log->GetNewTuple();
                RID rid = log->GetRID();

                // 1. write clr log to disk, set the information of clr log
                auto clr_record = std::make_shared<UpdateRecord>(txn_id, file_name, rid, new_tuple, old_tuple);
                clr_record->SetCLR(true);
                clr_record->SetPrevLSN(last_lsn);
                clr_record->SetUndoNext(prev_lsn);
                curr_lsn = log_manager_->AppendLogRecord(*clr_record);

                // 2. undo
                log->Undo(txn, curr_lsn);

                // 3. update block
                block = BlockId(file_name, rid.GetBlockNum());
                break;
            }

            case LogRecordType::INITPAGE:
            {
                InitPageRecord *log = dynamic_cast<InitPageRecord *>(log_record.get());
                BlockId block = log->GetBlockID();

                // 1. write a clr log to disk
                auto clr_record = std::make_shared<InitPageRecord>(txn_id, block.FileName(), block.BlockNum());
                clr_record->SetCLR(true);
                clr_record->SetPrevLSN(last_lsn);
                clr_record->SetUndoNext(prev_lsn);
                curr_lsn = log_manager_->AppendLogWithOffset(*clr_record, &offset);

                // 2. undo
                log->Undo(txn, curr_lsn);
                
                // 3. update block

                break;
            }

            default:
            {    
                break;
            }

            }
            
            toUndo.push(prev_lsn);
            SetLastLsn(txn_id, curr_lsn);
            SetEarlistLsn(block, curr_lsn);
        } 

    } // end while  

   
    SIMPLEDB_ASSERT(tx_table_.empty(), "any txn before crash should be removed");
}



void RecoveryManager::TxnEnd(txn_id_t txn_id) {
    auto txn_end_record = TxnEndRecord(txn_id);
    txn_end_record.SetPrevLSN(GetLastLsn(txn_id));
    log_manager_->AppendLogRecord(txn_end_record);

    RemoveTableEntry(txn_id);
}



} // namespace SimpleDB


#endif