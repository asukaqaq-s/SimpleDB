#ifndef LOG_RECORD_CC
#define LOG_RECORD_CC

#include "recovery/log_record.h"
#include "file/page.h"

namespace SimpleDB {

Page LogRecord::GetHeaderPage(int record_size) {
    int lsn_pos = 0;
    int txn_pos = sizeof(lsn_t);
    int prev_lsn_pos = txn_pos + sizeof(txn_id_t);
    int logtype_pos = prev_lsn_pos + sizeof(lsn_t);
    int clr_pos = logtype_pos + sizeof(LogRecordType);
    int undo_next_pos = clr_pos + sizeof(int);

    std::shared_ptr<std::vector<char>> array = 
            std::make_shared<std::vector<char>> (record_size);
    Page p(array);
    
    p.SetInt(lsn_pos, lsn_);
    p.SetInt(txn_pos, txn_id_);
    p.SetInt(prev_lsn_pos, prev_lsn_);
    p.SetInt(logtype_pos, static_cast<int>(type_));
    p.SetInt(clr_pos, is_clr_);
    p.SetInt(undo_next_pos, undo_next_);
    return p;
}


void LogRecord::GetHeaderInfor(Page *p) {
    int lsn_pos = 0;
    int txn_pos = sizeof(lsn_t);
    int prev_lsn_pos = txn_pos + sizeof(txn_id_t);
    int logtype_pos = prev_lsn_pos + sizeof(lsn_t);
    int clr_pos = logtype_pos + sizeof(LogRecordType);
    int undo_next_pos = clr_pos + sizeof(int);

    lsn_ = p->GetInt(lsn_pos);
    txn_id_ = p->GetInt(txn_pos);
    prev_lsn_ = p->GetInt(prev_lsn_pos);
    type_ = static_cast<LogRecordType>(p->GetInt(logtype_pos));
    is_clr_ = p->GetInt(clr_pos);
    undo_next_ = p->GetInt(undo_next_pos);
}

std::shared_ptr<LogRecord> LogRecord::DeserializeFrom
(const std::vector<char> &byte_array) {
    auto vector_ptr = std::make_shared<std::vector<char>> (byte_array);      
    Page page(vector_ptr);
    
    LogRecordType type = static_cast<LogRecordType> 
                            (page.GetInt(0 + 2 * sizeof(lsn_t) + sizeof(txn_id_t)));
                            
    switch(type) {
    case LogRecordType::INSERT:
        return std::make_shared<InsertRecord>(&page);
        break;
    case LogRecordType::DELETE:
        return std::make_shared<DeleteRecord>(&page);
        break;
    case LogRecordType::UPDATE:
        return std::make_shared<UpdateRecord>(&page);
        break;
    
    case LogRecordType::INITPAGE:
        return std::make_shared<InitPageRecord>(&page);
        break;
    
    case LogRecordType::CHECKPOINTBEGIN:
        return std::make_shared<ChkptBeginRecord>(&page);
        break;
    
    case LogRecordType::CHECKPOINTEND:
        return std::make_shared<ChkptEndRecord>(&page);
        break;
    
    case LogRecordType::BEGIN:
        return std::make_shared<BeginRecord>(&page);
        break;

    case LogRecordType::COMMIT:
        return std::make_shared<CommitRecord>(&page);
        break;
    case LogRecordType::ABORT:
        return std::make_shared<AbortRecord>(&page);
        break;

    case LogRecordType::TXNEND:
        return std::make_shared<TxnEndRecord>(&page);
        break;

    default:
        return nullptr;
    }
    
    return nullptr;
}



std::shared_ptr<std::vector<char>> ChkptBeginRecord::Serializeto() {
    auto page = GetHeaderPage(HEADER_SIZE);

    return page.content();
}


//////////////////////////////////////////// Begin Transaction record ///////////////////////////////////////////////////

std::shared_ptr<std::vector<char>> BeginRecord::Serializeto() {
    auto page = GetHeaderPage(HEADER_SIZE);

    // should not undo
    SIMPLEDB_ASSERT(undo_next_ == INVALID_LSN, "");

    return page.content();
} 

std::shared_ptr<std::vector<char>> CommitRecord::Serializeto() {
    auto page = GetHeaderPage(HEADER_SIZE);
    
    // should have prev record
    SIMPLEDB_ASSERT(prev_lsn_ != INVALID_LSN, "");
    // should not undo
    SIMPLEDB_ASSERT(undo_next_ == INVALID_LSN, "");

    return page.content();
}


std::shared_ptr<std::vector<char>> AbortRecord::Serializeto() {
    auto page = GetHeaderPage(HEADER_SIZE);

    // should have prev record
    SIMPLEDB_ASSERT(prev_lsn_ != INVALID_LSN, "");
    // should not record
    SIMPLEDB_ASSERT(undo_next_ == INVALID_LSN, "");

    return page.content();
}


std::shared_ptr<std::vector<char>> TxnEndRecord::Serializeto() {
    auto page = GetHeaderPage(HEADER_SIZE);

    // should have prev record
    //SIMPLEDB_ASSERT(prev_lsn_ != INVALID_LSN, "");

    return page.content();
}


//////////////////////////////////////////// End Transaction record ///////////////////////////////////////////////////


} // namespace SimpleDB

#endif