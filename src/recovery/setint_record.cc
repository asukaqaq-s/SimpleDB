#ifndef SETINT_RECORD_CC
#define SETINT_RECORD_CC

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
* --------------------------------------------------------------------------
* | Header | FileName.size() | FileName | BlockNum | Old value | New value |
* --------------------------------------------------------------------------
*/

SetIntRecord::SetIntRecord(txn_id_t txn_id, BlockId block, 
int offset, int old_value, int new_value) : LogRecord(), block_(block),  offset_(offset), old_value_(old_value), 
new_value_(new_value){ 

    txn_id_ = txn_id;
    type_ = LogRecordType::SETINT;

    int block_number_pos = HEADER_SIZE + Page::MaxLength(block_.FileName().size());
    int offset_pos = block_number_pos + sizeof(int);
    int old_value_pos = offset_pos + sizeof(int);
    int new_value_pos = old_value_pos + sizeof(int);

    record_size_ = new_value_pos + sizeof(int);
}

SetIntRecord::SetIntRecord(Page *p) : LogRecord() {
    int file_name_size = p->GetInt(HEADER_SIZE);
    int block_number_pos = HEADER_SIZE + Page::MaxLength(file_name_size);
    int offset_pos = block_number_pos + sizeof(int);
    int old_value_pos = offset_pos + sizeof(int);
    int new_value_pos = old_value_pos + sizeof(int);
    
    // get header infomation
    GetHeaderInfor(p);
    // get blockid 
    std::string file_name(p->GetString(HEADER_SIZE));
    std::cout << file_name << std::endl;
    int block_number = p->GetInt(block_number_pos);
    block_ = BlockId(file_name, block_number);
    // get offset
    offset_ = p->GetInt(offset_pos);
    // get old_value
    old_value_ = p->GetInt(old_value_pos);
    // get new_value
    new_value_ = p->GetInt(new_value_pos);

    record_size_ = new_value_pos + sizeof(int);
}



void SetIntRecord::Redo(Transaction *txn) {
    txn->Pin(block_);
    txn->SetInt(block_, offset_, new_value_, false); /* redo don't write to log */
    txn->Unpin(block_);
}

void SetIntRecord::Undo(Transaction *txn) {
    txn->Pin(block_);
    txn->SetInt(block_, offset_, old_value_, true); /* undo should write to log */
    txn->Unpin(block_);
}

std::string SetIntRecord::ToString() {
    std::string str = GetHeaderToString();
    
    str = str + block_.to_string() + " "
              + "offset: " + std::to_string(offset_) + " "
              + "new_value: " + std::to_string(new_value_) + " "
              + "old_value: " + std::to_string(old_value_);
    
    return str;
}
 
std::shared_ptr<std::vector<char>> SetIntRecord::Serializeto() {

    auto page = GetHeaderPage(txn_id_, type_, false, record_size_);
    int file_name_pos = HEADER_SIZE;
    int file_name_size = block_.FileName().size();
    int block_num_pos = HEADER_SIZE + Page::MaxLength(file_name_size);
    int offset_pos = block_num_pos + sizeof(int);
    int old_value_pos = offset_pos + sizeof(int);
    int new_value_pos = old_value_pos + sizeof(int);
    
    page.SetString(file_name_pos, block_.FileName());
    page.SetInt(block_num_pos, block_.BlockNum());
    page.SetInt(offset_pos, offset_);
    page.SetInt(old_value_pos, old_value_);
    page.SetInt(new_value_pos, new_value_);
    
    return page.content();
}


} // namespace SimpleDB

#endif