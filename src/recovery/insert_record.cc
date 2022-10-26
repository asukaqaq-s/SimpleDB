#ifndef INSERT_RECORD_CC
#define INSERT_RECORD_CC

#include "recovery/log_record.h"
#include "record/table_page.h"

namespace SimpleDB {

/**
*  For Insert
* -----------------------------------------------------------------------
* | Header | FileName.size() | FileName | RID | Tuple size | Tuple Data |
* -----------------------------------------------------------------------
*/

InsertRecord::InsertRecord(Page *p) {
    GetHeaderInfor(p);
    SIMPLEDB_ASSERT(type_ == LogRecordType::INSERT, "");

    int addr = HEADER_SIZE;
    int file_pos = addr;
    int rid_pos;
    int tuple_pos;
    int rid_x, rid_y;
    std::vector<char> data;

    // get file_name
    file_name_ = p->GetString(file_pos);
    // get rid
    rid_pos = file_pos + Page::MaxLength(file_name_.size());
    rid_x = p->GetInt(rid_pos);
    rid_y = p->GetInt(rid_pos + sizeof(int));
    rid_ = RID(rid_x, rid_y);
    // get tuple
    tuple_pos = rid_pos + 2 * sizeof(int);
    data = p->GetBytes(tuple_pos);
    tuple_ = Tuple(data);

    record_size_ = tuple_pos + Page::MaxLength(tuple_.GetSize());
}

InsertRecord::InsertRecord(txn_id_t txn, 
                           const std::string &file_name, 
                           const RID &rid,
                           const Tuple &tuple)
    : LogRecord(), file_name_(file_name), 
      rid_(rid), tuple_(tuple) {
    txn_id_ = txn;
    type_ = LogRecordType::INSERT;

    record_size_ = HEADER_SIZE + Page::MaxLength(file_name_.size()) + 
                   2 * sizeof(int) + 
                   Page::MaxLength(tuple_.GetSize());
}


void InsertRecord::Redo(Transaction *txn) {
    // redo this operation but not logs
    BlockId blk(file_name_, rid_.GetBlockNum());
    txn->Pin(blk);
    Buffer *buffer = txn->GetBuffer(file_name_, rid_, LockMode::EXCLUSIVE);
    auto table_page = TablePage(txn, buffer->contents(), blk);

    buffer->WLock();
    
    // if this log has redone, we can't redo again
    // for avoid logic error which happen in logical-physical scheme.
    if (table_page.GetPageLsn() >= lsn_) {
        buffer->WUnlock();
        txn->Unpin(blk);
        return;
    }

    bool res = table_page.InsertWithRID(rid_, tuple_);
    
    if (res == false) {
        SIMPLEDB_ASSERT(false, "logic error");
    }

    table_page.SetPageLsn(lsn_);
    buffer->SetModified(txn_id_, lsn_);
    // redo can not write log

    buffer->WUnlock();
    txn->Unpin(blk);
}

void InsertRecord::Undo(Transaction *txn, lsn_t lsn) {
    // do this operation but not logs
    // log will be written by recovery_manager
    BlockId blk(file_name_, rid_.GetBlockNum());
    Tuple old_tuple;

    // we must pin block before getbuffer
    txn->Pin(blk);

    Buffer *buffer = txn->GetBuffer(file_name_, rid_, LockMode::EXCLUSIVE);
    auto table_page = TablePage(txn, buffer->contents(), blk);
    
    buffer->WLock();

    // if table_page's last lsn >= lsn
    // the corresponding log should be flushed to disk
    // and this undo method can't be called.
    SIMPLEDB_ASSERT(table_page.GetPageLsn() < lsn, "logic error");

    table_page.Delete(rid_, &old_tuple);
    
    // i think old tuple is equal to tuple_ 
    SIMPLEDB_ASSERT(old_tuple == tuple_, "logic error");
    
    table_page.SetPageLsn(lsn);
    buffer->SetModified(txn_id_, lsn);

    buffer->WUnlock();
    txn->Unpin(blk);
}

std::string InsertRecord::ToString() {
    std::string str = GetHeaderToString();
    str += "file_name = " + file_name_ 
        +  rid_.ToString() + " "
        + "tuple = " + tuple_.ToString();
    return str;
}

std::shared_ptr<std::vector<char>> InsertRecord::Serializeto() {
    auto p = GetHeaderPage(record_size_);
    int addr = HEADER_SIZE;
    int file_pos = addr;
    int rid_pos;
    int tuple_pos;
    std::vector<char> data;

    // set file_name
    p.SetString(file_pos, file_name_);
    // set rid
    rid_pos = file_pos + Page::MaxLength(file_name_.size());
    p.SetInt(rid_pos, rid_.GetBlockNum());
    p.SetInt(rid_pos + sizeof(int), rid_.GetSlot());
    // set tuple
    tuple_pos = rid_pos + 2 * sizeof(int);
    p.SetBytes(tuple_pos, *tuple_.GetData());

    
    return p.content();
}


} // namespace SimpleDB

#endif