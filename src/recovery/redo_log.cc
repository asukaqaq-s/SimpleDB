#ifndef REDO_LOG_CC
#define REDO_LOG_CC

#include "recovery/recovery_manager.h"
#include "concurrency/transaction_manager.h"
#include "record/table_page.h"

namespace SimpleDB {

// for crash recovery, a lock request is not required because 
// no new transactions enter until recovery is complete

void RecoveryManager::RedoLog(LogRecord *log) {
    auto type = log->GetRecordType();

    switch (type)
    {
    case LogRecordType::BEGIN:
    case LogRecordType::ABORT:
    case LogRecordType::TXNEND:
    case LogRecordType::COMMIT:
    case LogRecordType::CHECKPOINTBEGIN:
    case LogRecordType::CHECKPOINTEND:

        SIMPLEDB_ASSERT(false, "can't redo these logs");
        break;

    case LogRecordType::INITPAGE:
    {
        auto log_record = static_cast<InitPageRecord*>(log);
        auto block = log_record->GetBlockID();

        if (block.BlockNum() != file_manager_->GetFileBlockNum(block.FileName())) {
            // if this file's size < block_number, logic error
            // if this file's size == block_number, only this case we should redo
            // if this file's size > block_number, this op has been done
            SIMPLEDB_ASSERT(block.BlockNum() > file_manager_->GetFileBlockNum(block.FileName()),
                            "redo initpage record content error");
            return;
        }


        SIMPLEDB_ASSERT(block == file_manager_->Append(block.FileName()), "should equal");
        Buffer *buffer = buffer_manager_->PinBlock(block);
        auto *table_page = static_cast<TablePage*>(buffer);
        
        table_page->WLock();
        // check if need to redo
        if (table_page->GetPageLsn() < log_record->GetLsn()) {
            table_page->InitPage(); // don't need to log
            table_page->SetPageLsn(log_record->GetLsn()); // but we still need to update PageLsn
        }
        table_page->WUnlock();
        buffer_manager_->UnpinBlock(block);

        break;
    }

    case LogRecordType::INSERT:
    {
        auto log_record = static_cast<InsertRecord*>(log);
        auto block = BlockId(log_record->GetFileName(), log_record->GetRID().GetBlockNum());
        auto rid = log_record->GetRID();

        Buffer *buffer = buffer_manager_->PinBlock(block);
        auto *table_page = static_cast<TablePage*> (buffer);

        table_page->WLock();
        // check if need to redo
        if (table_page->GetPageLsn() < log_record->GetLsn()) {
            table_page->InsertWithRID(rid, log_record->GetTuple());
            table_page->SetPageLsn(log_record->GetLsn());
        }
        table_page->WUnlock();
        buffer_manager_->UnpinBlock(block);
        
        break;
    }

    case LogRecordType::DELETE:
    {
        auto log_record = static_cast<DeleteRecord*>(log);
        auto block = BlockId(log_record->GetFileName(), log_record->GetRID().GetBlockNum());
        auto rid = log_record->GetRID();

        Buffer *buffer = buffer_manager_->PinBlock(block);
        auto *table_page = static_cast<TablePage*> (buffer);

        table_page->WLock();
        // check if need to redo
        if (table_page->GetPageLsn() < log_record->GetLsn()) {
            Tuple old_tuple;
            table_page->Delete(rid, &old_tuple);
            table_page->SetPageLsn(log_record->GetLsn());
            assert(old_tuple == log_record->GetTuple());
        }
        table_page->WUnlock();
        buffer_manager_->UnpinBlock(block);

        break;
    }
    
    case LogRecordType::UPDATE:
    {
        auto log_record = static_cast<UpdateRecord*>(log);
        auto block = BlockId(log_record->GetFileName(), log_record->GetRID().GetBlockNum());
        auto rid = log_record->GetRID();
        auto old_tuple = log_record->GetOldTuple();
        auto new_tuple = log_record->GetNewTuple();

        Buffer *buffer = buffer_manager_->PinBlock(block);
        auto *table_page = static_cast<TablePage*> (buffer);

        table_page->WLock();
        // check if need to redo
        if (table_page->GetPageLsn() < log_record->GetLsn()) {
            Tuple tuple;
            table_page->Update(rid, &tuple, new_tuple);
            table_page->SetPageLsn(log_record->GetLsn());
            assert(old_tuple == tuple);
        }
        table_page->WUnlock();
        buffer_manager_->UnpinBlock(block);

        break;
    }

    default:
        SIMPLEDB_ASSERT(false, "can't reach");
        break;
    }
}

} // namespace SimpleDB


#endif