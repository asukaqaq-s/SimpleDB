#ifndef SETSTRING_RECORD_CC
#define SETSTRING_RECORD_CC

#include "recovery/log_record.h"

#include <memory>
#include <iostream>

namespace SimpleDB {

/*
* Header
* --------------------------------------------
* | LSN | txn_ID | prevLSN | LogType | IsCLR |
* --------------------------------------------
* SetInt-LOG
* -------------------------------------------------------------------------------------------------------------------------
* | Header | FileName.size() | FileName | BlockNum | offset | Old String Size | Old String | New String Size | New String |
* -------------------------------------------------------------------------------------------------------------------------
*/

SetStringRecord::SetStringRecord(Page *p) : LogRecord() {
    int file_name_pos = HEADER_SIZE;
    int file_name_size = p->GetInt(file_name_pos);
    int block_number_pos = HEADER_SIZE + Page::MaxLength(file_name_size);
    int offset_pos = block_number_pos + sizeof(int);
    int old_string_pos = offset_pos + sizeof(int);
    int old_string_size = p->GetInt(old_string_pos);
    int new_string_pos = old_string_pos + Page::MaxLength(old_string_size);

    GetHeaderInfor(p); 

    std::string file_name = p->GetString(file_name_pos);
    int block_number = p->GetInt(block_number_pos);

    block_ = BlockId(file_name, block_number);
    offset_ = p->GetInt(offset_pos);
    old_value_ = p->GetString(old_string_pos);
    new_value_ = p->GetString(new_string_pos);
    record_size_ = new_string_pos + Page::MaxLength(new_value_.size());
}

SetStringRecord::SetStringRecord(txn_id_t txn_id,  BlockId block, 
    int offset, std::string old_string, std::string new_string) :
LogRecord(),  block_(block), offset_(offset), old_value_(old_string), new_value_(new_string) {
    txn_id_ = txn_id;
    type_ = LogRecordType::SETSTRING;
    
    record_size_ = HEADER_SIZE + Page::MaxLength(block_.FileName().size()) // file_name 
                               + sizeof(int)  // block_number
                               + sizeof(int)  // offset
                               + Page::MaxLength(old_value_.size()) // old_value
                               + Page::MaxLength(new_value_.size()); // new_value
}

void SetStringRecord::Redo(Transaction *txn) {
    txn->Pin(block_);
    txn->SetString(block_, offset_, new_value_, false); /* redo not need to write to log */
    txn->Unpin(block_);
}
    
void SetStringRecord::Undo(Transaction *txn) {
    txn->Pin(block_);
    txn->SetString(block_, offset_, old_value_, true); /* undo should write to log */
    txn->Unpin(block_);
}

std::string SetStringRecord::ToString() {
    std::string str = GetHeaderToString();
    str = str + block_.to_string() + " "
              + "offset: " + std::to_string(offset_) + " "
              + "old_value: " + old_value_ + " "
              + "new_value: " + new_value_;
    return str;
}

std::shared_ptr<std::vector<char>> SetStringRecord::Serializeto() {
    auto page = GetHeaderPage(txn_id_, type_, is_clr_, record_size_);
    int prev_lsn_pos = sizeof(lsn_t) + sizeof(txn_id_t);
    int file_name_pos = HEADER_SIZE;
    int block_number_pos = HEADER_SIZE + Page::MaxLength(block_.FileName().size());
    int offset_pos = block_number_pos + sizeof(int);
    int old_value_pos = offset_pos + sizeof(int);
    int new_value_pos = old_value_pos + Page::MaxLength(old_value_.size());

    page.SetInt(0, lsn_);
    page.SetInt(prev_lsn_pos, prev_lsn_);
    page.SetString(file_name_pos, block_.FileName());
    page.SetInt(block_number_pos, block_.BlockNum());
    page.SetInt(offset_pos, offset_);
    page.SetString(old_value_pos, old_value_);
    page.SetString(new_value_pos, new_value_);

    return page.content();
}

} // namespace SimpleDB

#endif