#ifndef UNDO_LOG_CC
#define UNDO_LOG_CC

#include "recovery/recovery_manager.h"
#include "concurrency/transaction_manager.h"
#include "record/table_page.h"

namespace SimpleDB {


// for crash recovery, a lock request is not required because 
// no new transactions enter until recovery is complete

void RecoveryManager::UndoLog(Transaction *txn, LogRecord *log, lsn_t undo_lsn) {
    auto type = log->GetRecordType();
    auto file_manager = txn->GetFileManager();


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


        SIMPLEDB_ASSERT(block.BlockNum() + 1 == file_manager->GetFileBlockNum(block.FileName()),
                        "logic error");

        // since we append a block and init it when initpage
        // undo this operations is reduce a block of file. 
        file_manager->SetFileSize(block.FileName(), block.BlockNum());

        break;
    }

    case LogRecordType::INSERT:
    {
        auto log_record = static_cast<InsertRecord*>(log);
        auto block = BlockId(log_record->GetFileName(), log_record->GetRID().GetBlockNum());
        auto rid = log_record->GetRID();
        Tuple tuple;
    
        // acquire resource
        Buffer *buffer = txn->PinBlock(block); 
        auto *table_page = static_cast<TablePage*> (buffer);
        table_page->WLock();
        
        // if pagelsn < lsn, this op has been undo, i think it's a logic error
        assert(table_page->GetPageLsn() >= log_record->GetLsn());
        
        // undo
        bool res = table_page->Delete(rid, &tuple);
        if (res == false || tuple != log_record->GetTuple()) {
            SIMPLEDB_ASSERT(false, "logic error");
        }
        table_page->SetPageLsn(undo_lsn);
        
        // release resource
        table_page->WUnlock();
        txn->UnpinBlock(block);
        break;
    }

    case LogRecordType::DELETE:
    {
        auto log_record = static_cast<DeleteRecord*>(log);
        auto block = BlockId(log_record->GetFileName(), log_record->GetRID().GetBlockNum());
        auto rid = log_record->GetRID();
        Tuple tuple = log_record->GetTuple();
        
        Buffer *buffer = txn->PinBlock(block);
        auto *table_page = static_cast<TablePage*> (buffer);

        table_page->WLock();
        
        // if pagelsn < lsn, i think it's a logic error
        assert(table_page->GetPageLsn() >= log_record->GetLsn());
        
        // undo
        bool res = table_page->InsertWithRID(rid, tuple);
        SIMPLEDB_ASSERT(res == true, "logic error");
        table_page->SetPageLsn(undo_lsn);

        table_page->WUnlock();
        txn->UnpinBlock(block);
        
        break;
    }
    
    case LogRecordType::UPDATE:
    {
        auto log_record = static_cast<UpdateRecord*>(log);
        auto block = BlockId(log_record->GetFileName(), log_record->GetRID().GetBlockNum());
        auto rid = log_record->GetRID();
        auto old_tuple = log_record->GetOldTuple();
        auto new_tuple = log_record->GetNewTuple();

        // acquire resource
        Buffer *buffer = txn->PinBlock(block);
        auto *table_page = static_cast<TablePage*> (buffer);
        table_page->WLock();
        
        // if pagelsn < lsn, i think it's a logic error
        assert(table_page->GetPageLsn() >= log_record->GetLsn());
        
        // undo
        Tuple tuple;
        bool res = table_page->Update(rid, &tuple, old_tuple);
        SIMPLEDB_ASSERT(res == true && new_tuple == tuple, "logic error");
        table_page->SetPageLsn(undo_lsn);

        // release resource
        table_page->WUnlock();
        txn->UnpinBlock(block);

        break;
    }

    default:
        SIMPLEDB_ASSERT(false, "can't reach");
        break;
    }

}



} // namespace SimpleDB


#endif