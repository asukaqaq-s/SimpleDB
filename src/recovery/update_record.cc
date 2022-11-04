#ifndef UPDATE_RECORD_CC
#define UPDATE_RECORD_CC

#include "recovery/log_record.h"
#include "record/table_page.h"
#include "concurrency/transaction.h"

namespace SimpleDB {

/**
* For Update
* -----------------------------------------------------------------------------------------------------------------
* | Header | FileName.size() | FileName | RID | Old Tuple size | Old Tuple Data | New Tuple size | New Tuple Data |
* -----------------------------------------------------------------------------------------------------------------
*/

UpdateRecord::UpdateRecord(Page *p) {
    GetHeaderInfor(p);
    SIMPLEDB_ASSERT(type_ == LogRecordType::UPDATE, "");

    int addr = HEADER_SIZE;
    int file_pos = addr;
    int rid_pos;
    int old_tuple_pos;
    int new_tuple_pos;
    int rid_x, rid_y;
    std::vector<char> data;

    // get file_name
    file_name_ = p->GetString(file_pos);
    // get rid
    rid_pos = file_pos + Page::MaxLength(file_name_.size());
    rid_x = p->GetInt(rid_pos);
    rid_y = p->GetInt(rid_pos + sizeof(int));
    rid_ = RID(rid_x, rid_y);
    // get old tuple
    old_tuple_pos = rid_pos + 2 * sizeof(int);
    data = p->GetBytes(old_tuple_pos);
    old_tuple_ = Tuple(data);
    // get new tuple
    new_tuple_pos = old_tuple_pos + Page::MaxLength(old_tuple_.GetSize());
    data = p->GetBytes(new_tuple_pos);
    new_tuple_ = Tuple(data);

    record_size_ = new_tuple_pos + Page::MaxLength(new_tuple_.GetSize());
}

UpdateRecord::UpdateRecord(txn_id_t txn, 
                           const std::string &file_name, 
                           const RID &rid,
                           const Tuple &old_tuple,
                           const Tuple &new_tuple) 
    : LogRecord(), file_name_(file_name), 
      rid_(rid), old_tuple_(old_tuple), new_tuple_(new_tuple) {
    txn_id_ = txn;
    type_ = LogRecordType::UPDATE;

    record_size_ = HEADER_SIZE + Page::MaxLength(file_name_.size()) + 
                   2 * sizeof(int) + 
                   Page::MaxLength(old_tuple_.GetSize()) +
                   Page::MaxLength(new_tuple_.GetSize());
}


std::string UpdateRecord::ToString() {
    std::string str = GetHeaderToString();
    str += "file_name = " + file_name_ 
        +  rid_.ToString() + " "
        + "old_tuple = " + old_tuple_.ToString() + " "
        + "new_tuple = " + new_tuple_.ToString();
    return str;
}

std::shared_ptr<std::vector<char>> UpdateRecord::Serializeto() {
    auto p = GetHeaderPage(record_size_);
    int addr = HEADER_SIZE;
    int file_pos = addr;
    int rid_pos;
    int old_tuple_pos;
    int new_tuple_pos;
    std::vector<char> data;

    // set file_name
    p.SetString(file_pos, file_name_);
    // set rid
    rid_pos = file_pos + Page::MaxLength(file_name_.size());
    p.SetInt(rid_pos, rid_.GetBlockNum());
    p.SetInt(rid_pos + sizeof(int), rid_.GetSlot());
    // set old tuple
    old_tuple_pos = rid_pos + 2 * sizeof(int);
    p.SetBytes(old_tuple_pos, *old_tuple_.GetData());
    // set new tuple
    new_tuple_pos = old_tuple_pos + Page::MaxLength(old_tuple_.GetSize());
    p.SetBytes(new_tuple_pos, *new_tuple_.GetData());

    return p.content();
}


} // namespace SimpleDB

#endif