#ifndef INIT_PAGE_RECORD_CC
#define INIT_PAGE_RECORD_CC

#include "recovery/log_record.h"
#include "concurrency/transaction.h"
#include "record/table_page.h"

namespace SimpleDB {

/**
* For InitPage 
* -----------------------------------------------------
* | Header | FileName.size() | FileName | BlockNumber |
* -----------------------------------------------------
*/

InitPageRecord::InitPageRecord(Page *p) {
    GetHeaderInfor(p);
    SIMPLEDB_ASSERT(type_ == LogRecordType::INITPAGE, "");
    
    int file_pos = HEADER_SIZE;
    int blk_num_pos;
    
    file_name_ = p->GetString(file_pos);
    blk_num_pos = file_pos + Page::MaxLength(file_name_.size());
    block_number_ = p->GetInt(blk_num_pos);

    record_size_ = blk_num_pos + sizeof(int);   
}

InitPageRecord::InitPageRecord(txn_id_t txn, 
                               const std::string &file_name, 
                               int block_number) :
    file_name_(file_name), block_number_(block_number) {
    txn_id_ = txn;
    type_ = LogRecordType::INITPAGE;
    
    record_size_ = HEADER_SIZE + Page::MaxLength(file_name_.size()) +
                   sizeof(int);
}

void InitPageRecord::Redo(Transaction *txn) {
    BlockId block(file_name_, block_number_);

    if (txn->GetFileSize(file_name_) != block_number_) {
        // if this file's size < block_number, logic error
        // if this file's size > block_number, this op has been done
        SIMPLEDB_ASSERT(txn->GetFileSize(file_name_) > block_number_, "logic error");
        return;
    }

    SIMPLEDB_ASSERT(txn->Append(file_name_) == block, "logic error");
    txn->Pin(block);
    
    // since we have obtained x-lock in end-of-file
    // should we get x-lock in this new-block ?
    // or because isolation level rr, we just get s-lock here?
    // maybe the first case is right
    Buffer *buffer = txn->GetBuffer(file_name_, RID(block_number_, -1), LockMode::EXCLUSIVE);
    auto table_page = TablePage(txn, buffer->contents(), block);
    buffer->WLock();

    table_page.InitPage();

    table_page.SetPageLsn(lsn_);
    buffer->SetModified(txn_id_, lsn_);
    buffer->WUnlock();
    txn->Unpin(block);
}

void InitPageRecord::Undo(Transaction *txn, lsn_t lsn) {
    BlockId block(file_name_, block_number_);

    SIMPLEDB_ASSERT(txn->GetFileSize(file_name_) == block_number_ + 1, "logic error");
    
    // since we append a block and init it when initpage
    // undo this operations is reduce a block of file. 
    txn->SetFileSize(file_name_, block_number_);
}

std::string InitPageRecord::ToString() {
    std::string str = GetHeaderToString();
    str += "file_name = " + file_name_ + " "
        +  "block_number " + std::to_string(block_number_);
    return str;
}

std::shared_ptr<std::vector<char>> InitPageRecord::Serializeto() {
    auto p = GetHeaderPage(record_size_);
    
    int file_pos = HEADER_SIZE;
    int blk_num_pos = file_pos + Page::MaxLength(file_name_.size());
    
    p.SetString(file_pos, file_name_);
    p.SetInt(blk_num_pos, block_number_);

    return p.content();
}



} // namespace SimpleDB

#endif