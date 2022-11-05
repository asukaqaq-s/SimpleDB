#ifndef TABLE_PAGE_CC
#define TABLE_PAGE_CC

#include "record/table_page.h" 

namespace SimpleDB {

void TablePage::InitPage(Transaction *txn, RecoveryManager *rm) {

    // write a InitPage log record to logmanager
    if (rm != nullptr) {
        lsn_t lsn = rm->InitPageLogRec(txn, block_.FileName(), block_.BlockNum(), false);
        SetPageLsn(lsn);
    }

    SetFreeSpacePtr(data_->GetSize());
    SetTupleCount(0);
}

bool TablePage::GetTuple(const RID &rid, Tuple *tuple) {
    SIMPLEDB_ASSERT(rid.GetBlockNum() == block_.BlockNum(), 
                    "block number don't match");
    SIMPLEDB_ASSERT(rid.GetSlot() >= 0, "slot error");

    int tuple_count = GetTupleCount();
    int slot_number = rid.GetSlot();


    if (slot_number >= tuple_count) {
        return false;
    }

    int tuple_offset = GetTupleOffset(slot_number);
    
    // the tuple is not exist
    if (IsDeleted(tuple_offset)) {
        return false;
    }


    *tuple = Tuple(data_->GetBytes(tuple_offset));
    tuple->SetRID(rid);
    return true;
}

bool TablePage::Insert(RID *rid, const Tuple &tuple, 
                       const std::function<bool(const BlockId &)> &upgrade, 
                       Transaction *txn, RecoveryManager *rm) {
    SIMPLEDB_ASSERT(tuple.GetSize() > 0, "can not insert a empty tuple");
    
    int free_space_size = GetFreeSpaceRemaining();
    int need_size = Page::MaxLength(tuple.GetSize());

    // check if we can insert this tuple
    if (free_space_size < need_size) {
        return false;
    }

    // find a empty tuple, inserting any empty tuple is the same
    // we don't care about it 
    int tuple_count = GetTupleCount();
    int i;
    for (i = 0;i < tuple_count; i ++) {
        if (IsDeleted(GetTupleOffset(i))) {
            break;
        }
    }
    
    // Consider a situation where there are no empty tuples
    // check if can insert it again
    if (i == tuple_count && 
        free_space_size < need_size + SLOT_SIZE) {
        return false;
    }

    // if we can insert successfully, upgrade s-lock to x-lock 
    if (upgrade != nullptr && !upgrade(block_)) {
        SIMPLEDB_ASSERT(false, "upgrade lock error");
        return false;
    }

    // create a new slot
    if (i == tuple_count) {
        SetTupleCount(tuple_count + 1);
    }

    SetFreeSpacePtr(GetFreeSpacePtr() - need_size);
    SetTupleOffset(i, GetFreeSpacePtr());
    data_->SetBytes(GetTupleOffset(i), *tuple.GetData());

    // set rid
    if (rid != nullptr) {
        *rid = RID(block_.BlockNum(), i);
    }

    if (rm != nullptr) {
        lsn_t lsn = rm->InsertLogRec(txn, block_.FileName(), *rid, tuple, false);
        SetPageLsn(lsn);
    }

    return true;
}

bool TablePage::InsertWithRID(const RID &rid, const Tuple &tuple,
                              Transaction *txn, RecoveryManager *rm) {
    SIMPLEDB_ASSERT(rid.GetBlockNum() == block_.BlockNum(), 
                    "block number don't match");
    SIMPLEDB_ASSERT(rid.GetSlot() >= 0, "wrong slot");
    SIMPLEDB_ASSERT(tuple.GetSize() > 0, "can not insert a empty tuple");

    int tuple_count = GetTupleCount();
    int slot_number = rid.GetSlot();

    if (slot_number < tuple_count && 
        !IsDeleted(GetTupleOffset(slot_number))) {
        // the record is not empty
        return false;
    }

    // if slot_number == tuple_count, we can insert a 
    // tuple to the end of the tuple_array
    // but if slot_number > tuple_count, we can't do it
    SIMPLEDB_ASSERT(slot_number <= tuple_count, 
                    "we can't insert tuple");

    int free_space_size = GetFreeSpaceRemaining();
    int need_size = Page::MaxLength(tuple.GetSize());

    SIMPLEDB_ASSERT(free_space_size >= 0, "data overlap");

    if (slot_number == tuple_count) {
        // we need add a slot-entry
        need_size += SLOT_SIZE;
    }

    if (free_space_size < need_size) {
        return false; 
    }

    // in tuple-orient page scheme, we just insert a tuple
    // to the end of tuple array, there is no need to
    // according to the order of the slot array
    if (slot_number == tuple_count) {
        SetTupleCount(tuple_count + 1);
    }

    // we can't use need_size to replace Page::Maxlength(...)
    // because slot_size is not need to add.
    int new_free_space_ptr = GetFreeSpacePtr() - Page::MaxLength(tuple.GetSize());
    data_->SetBytes(new_free_space_ptr, *tuple.GetData());
    
    SetFreeSpacePtr(new_free_space_ptr);
    SetTupleOffset(slot_number, new_free_space_ptr);


    if (rm != nullptr) {
        lsn_t lsn = rm->InsertLogRec(txn, block_.FileName(), rid, tuple, false);
        SetPageLsn(lsn);
    }

    return true;
}

bool TablePage::Delete(const RID &rid, Tuple *tuple,
                       Transaction *txn, RecoveryManager *rm) {

    SIMPLEDB_ASSERT(rid.GetBlockNum() == block_.BlockNum(), 
                    "block number don't match");
    SIMPLEDB_ASSERT(rid.GetSlot() >= 0, "wrong slot");

    int tuple_count = GetTupleCount();
    int slot_number = rid.GetSlot();

    if (IsDeleted(GetTupleOffset(slot_number))) {
        // the record has been delete
        return true;
    }

    int tuple_offset = GetTupleOffset(slot_number);
    *tuple = Tuple(data_->GetBytes(tuple_offset));
    tuple->SetRID(rid);
    int tuple_size = Page::MaxLength(tuple->GetSize());
    int old_free_space_ptr = GetFreeSpacePtr();
    int new_free_space_ptr = old_free_space_ptr + tuple_size;
    int move_total_size = tuple_offset - old_free_space_ptr;

    // for deleting a tuple, we should move the 
    // tuple before the tuple_pos_begin backwards
    memmove(data_->GetRawDataPtr() + new_free_space_ptr, // dist
            data_->GetRawDataPtr() + old_free_space_ptr, // src
            move_total_size);


    SetTupleOffset(slot_number, 0);
    SetFreeSpacePtr(new_free_space_ptr);
    // reset tuple count
    if (slot_number == tuple_count - 1) {
        SetTupleCount(tuple_count - 1);
        tuple_count --;
    }

    // update tuple offset
    for (int i = 0;i < tuple_count; i ++) {
        int cur_tuple_offset = GetTupleOffset(i);
        if (!IsDeleted(cur_tuple_offset) && 
            cur_tuple_offset < tuple_offset) {
            SetTupleOffset(i, cur_tuple_offset + tuple_size);
        }
    }

    if (rm != nullptr) {
        lsn_t lsn = rm->DeleteLogRec(txn, block_.FileName(), rid, *tuple, false);
        SetPageLsn(lsn);
    }

    return true;
}

bool TablePage::Update(const RID &rid, 
                       Tuple *old_tuple, 
                       const Tuple &new_tuple,
                       Transaction *txn, RecoveryManager *rm) {
    SIMPLEDB_ASSERT(rid.GetBlockNum() == block_.BlockNum(), 
                    "block number don't match");
    SIMPLEDB_ASSERT(rid.GetSlot() >= 0, "wrong slot");
    SIMPLEDB_ASSERT(new_tuple.GetSize() > 0, "can not update a empty tuple");
    
    int tuple_count = GetTupleCount();
    int slot_number = rid.GetSlot();
    int tuple_offset = GetTupleOffset(slot_number);

    if (slot_number >= tuple_count) {
        // the slot isn't exist, we can't update it
        return false;
    }

    SIMPLEDB_ASSERT(tuple_offset > 0, "can not update a non-exist tuple");

    *old_tuple = data_->GetBytes(tuple_offset);
    old_tuple->SetRID(rid);
    
    int old_tuple_size = Page::MaxLength(old_tuple->GetSize());
    int new_tuple_size = Page::MaxLength(new_tuple.GetSize());
    int change_size = new_tuple_size - old_tuple_size;

    // check whether we have enough space to update
    if (GetFreeSpaceRemaining() < change_size) {
        return false;
    }
    
    int old_free_space_ptr = GetFreeSpacePtr();
    int new_free_space_ptr = GetFreeSpacePtr() - change_size;

    // for updating a tuple, we should move the 
    // tuple before the tuple_offset backwards
    memmove(data_->GetRawDataPtr() + new_free_space_ptr,
            data_->GetRawDataPtr() + old_free_space_ptr,
            tuple_offset - old_free_space_ptr);
    
    SetTupleOffset(slot_number, tuple_offset - change_size);
    SetFreeSpacePtr(new_free_space_ptr);
    
    // update new tuple offset
    tuple_offset = tuple_offset - change_size;
    // update new tuple data
    data_->SetBytes(tuple_offset, *new_tuple.GetData());


    // update the offset of slot which before the tuple_offset
    for (int i = 0;i < tuple_count; i++) {
        int cur_tuple_offset = GetTupleOffset(i);
        if (!IsDeleted(cur_tuple_offset) &&
            cur_tuple_offset < tuple_offset) {
            SetTupleOffset(i, cur_tuple_offset - change_size);
        }
    }

    if (rm != nullptr) {
        lsn_t lsn = rm->UpdateLogRec(txn, block_.FileName(), rid, *old_tuple, new_tuple, false);
        SetPageLsn(lsn);
    }

    return true;
}

bool TablePage::GetFirstTupleRid(RID &next_rid) {
    int tuple_count = GetTupleCount();
    
    for (int i = 0;i < tuple_count; i ++) {
        int cur_tuple_offset = GetTupleOffset(i);
        if (!IsDeleted(cur_tuple_offset)) {
            next_rid = RID(block_.BlockNum(), i);
            return true;
        }
    }
    next_rid.SetSlot(-1);
    return false;
}

bool TablePage::GetNextTupleRid(RID *rid) {
    int tuple_count = GetTupleCount();

    for (int i = rid->GetSlot() + 1;i < tuple_count;i ++) {
        int cur_tuple_offset = GetTupleOffset(i);
        if (!IsDeleted(cur_tuple_offset)) {
            *rid = RID(block_.BlockNum(), i);
            return true;
        }
    }

    rid->SetSlot(-1);
    return false;
}

bool TablePage::GetNextEmptyRid(RID *rid, const Tuple &tuple) {
    int tuple_count = GetTupleCount();
    int need_size = Page::MaxLength(tuple.GetSize());
    // for inserting a new tuple, we should
    // ensure that have enough space to do it
    if (GetFreeSpaceRemaining() < need_size) {
        rid->SetSlot(-1);
        return false;
    }

    // if we can find a discarded tuple
    for (int i = rid->GetSlot() + 1;i < tuple_count;i ++) {
        int cur_tuple_offset = GetTupleOffset(i);
        if (IsDeleted(cur_tuple_offset)) {
            *rid = RID(block_.BlockNum(), i);
            return true;
        }
    }
    
    // if we can insert a new tuple in this page
    need_size += SLOT_SIZE;
    
    if (GetFreeSpaceRemaining() < need_size) {
        rid->SetSlot(-1);
        return false;
    }
    
    *rid = RID(block_.BlockNum(), tuple_count);
    return true;
}

std::string TablePage::ToString(const Schema &schema) {
    std::stringstream str;
    str << "free space ptr = " << GetFreeSpacePtr() << " "
                  << "free space remain = " << GetFreeSpaceRemaining() << " "
                  << "tuple count = " << GetTupleCount() << std::endl;
        
    int tuple_cnt = GetTupleCount();
    for (int i = 0;i < tuple_cnt;i ++) {
        if (IsDeleted(GetTupleOffset(i))) {
            str << "not exist   " << i << std::endl;
        }
        else {
            Tuple tuple;
            GetTuple(RID(block_.BlockNum(), i), &tuple);
            str << "exist   " << i << " "
                << "tuple offset = " << GetTupleOffset(i) << " " 
                << "tuple size = " << GetTupleSize(i) << " "
                << "Get Tuple = " <<  tuple.ToString(schema)<< std::endl;
        }
    }
    
    return str.str();
}


std::string TablePage::ToString() {
    std::stringstream str;
    str << "block = " << block_.to_string() << " "
        << "free space ptr = " << GetFreeSpacePtr() << " "
        << "free space remain = " << GetFreeSpaceRemaining() << " "
        << "tuple count = " << GetTupleCount() << std::endl;
        
    int tuple_cnt = GetTupleCount();
    for (int i = 0;i < tuple_cnt;i ++) {
        if (IsDeleted(GetTupleOffset(i))) {
            str << "not exist   " << i << std::endl;
        }
        else {
            Tuple tuple;
            GetTuple(RID(block_.BlockNum(), i), &tuple);
            str << "exist   " << i << " "
                << "tuple offset = " << GetTupleOffset(i) << " " 
                << "tuple size = " << GetTupleSize(i) << std::endl;
        }
    }
    
    return str.str();
}


} // namespace SimpleDB

#endif