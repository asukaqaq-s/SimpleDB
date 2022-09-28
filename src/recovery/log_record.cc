#ifndef LOG_RECORD_CC
#define LOG_RECORD_CC

#include "recovery/log_record.h"

namespace SimpleDB {

std::shared_ptr<LogRecord> LogRecord::DeserializeFrom
    (const std::vector<char> &byte_array) {
    auto vector_ptr = std::make_shared<std::vector<char>> (byte_array);      
    Page page(vector_ptr);
    
    LogRecordType type = static_cast<LogRecordType> 
                            (page.GetInt(0 + 2 * sizeof(lsn_t) + sizeof(txn_id_t)));
    
    switch(type) {
    case LogRecordType::SETINT:
        return std::make_shared<SetIntRecord>(&page);
        break;
    case LogRecordType::SETSTRING:
        return std::make_shared<SetStringRecord>(&page);
        break;
    case LogRecordType::COMMIT:
        return std::make_shared<CommitRecord>(&page);
        break;
    case LogRecordType::ROLLBACK:
        return std::make_shared<RollbackRecord>(&page);
        break;
    case LogRecordType::BEGIN:
        return std::make_shared<BeginRecord>(&page);
        break;
    case LogRecordType::CHECKPOINT:
        return std::make_shared<CheckpointRecord>(&page);
        break;
    default:
        return nullptr;
    }
    
    return nullptr;
}

Page LogRecord::GetHeaderPage
(txn_id_t txn_id, LogRecordType type, int is_clr, int record_size) {
    int lsn_pos = 0;
    int txn_pos = sizeof(lsn_t);
    int prev_lsn_pos = txn_pos + sizeof(txn_id_t);
    int logtype_pos = prev_lsn_pos + sizeof(lsn_t);
    int clr_pos = logtype_pos + sizeof(LogRecordType);

    std::shared_ptr<std::vector<char>> array = 
            std::make_shared<std::vector<char>> (record_size);
    Page p(array);
    
    p.SetInt(0, INVALID_LSN);
    p.SetInt(txn_pos, txn_id);
    p.SetInt(prev_lsn_pos, INVALID_LSN);
    p.SetInt(logtype_pos, static_cast<int>(type));
    p.SetInt(clr_pos, is_clr);
    return p;
}

void LogRecord::GetHeaderInfor(Page *p) {
    int lsn_pos = 0;
    int txn_pos = sizeof(lsn_t);
    int prev_lsn_pos = txn_pos + sizeof(txn_id_t);
    int logtype_pos = prev_lsn_pos + sizeof(lsn_t);
    int clr_pos = logtype_pos + sizeof(LogRecordType);

    lsn_ = p->GetInt(lsn_pos);
    txn_id_ = p->GetInt(txn_pos);
    prev_lsn_ = p->GetInt(prev_lsn_pos);
    type_ = static_cast<LogRecordType>(p->GetInt(logtype_pos));
    is_clr_ = p->GetInt(clr_pos);
}

std::shared_ptr<std::vector<char>> CommitRecord::Serializeto() {
    auto page = GetHeaderPage(txn_id_, type_, is_clr_, HEADER_SIZE);
    int lsn_pos = 0;
    int prev_lsn_pos = sizeof(lsn_t) + sizeof(txn_id_t);
    
    page.SetInt(lsn_pos, lsn_);
    page.SetInt(prev_lsn_pos, prev_lsn_);

    return page.content();
}

std::shared_ptr<std::vector<char>> CheckpointRecord::Serializeto() {
    auto page = GetHeaderPage(txn_id_, type_, is_clr_, HEADER_SIZE);
    int lsn_pos = 0;
    int prev_lsn_pos = sizeof(lsn_t) + sizeof(txn_id_t);
    
    page.SetInt(lsn_pos, lsn_);
    page.SetInt(prev_lsn_pos, prev_lsn_);

    return page.content();
}

std::shared_ptr<std::vector<char>> BeginRecord::Serializeto() {
    auto page = GetHeaderPage(txn_id_, type_, is_clr_, HEADER_SIZE);
    int lsn_pos = 0;
    int prev_lsn_pos = sizeof(lsn_t) + sizeof(txn_id_t);
    
    page.SetInt(lsn_pos, lsn_);
    page.SetInt(prev_lsn_pos, prev_lsn_);

    return page.content();
} 


std::shared_ptr<std::vector<char>> RollbackRecord::Serializeto() {
    auto page = GetHeaderPage(txn_id_, type_, is_clr_, HEADER_SIZE);
    int lsn_pos = 0;
    int prev_lsn_pos = sizeof(lsn_t) + sizeof(txn_id_t);
    
    page.SetInt(lsn_pos, lsn_);
    page.SetInt(prev_lsn_pos, prev_lsn_);

    return page.content();
}


} // namespace SimpleDB

#endif