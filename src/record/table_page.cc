#ifndef TABLE_PAGE_CC
#define TABLE_PAGE_CC

#include "record/table_page.h"
#include "file/page.h"

#include <string>
#include <cstring>


namespace SimpleDB {

TablePage::TablePage(Transaction *txn, BlockId block, Layout layout) :
    txn_(txn), block_(block), layout_(layout) {
    txn->Pin(block);
}

// just set tuple count and do nothing
void TablePage::InitPage() {
    SetFreeSpacePtr(txn_->BlockSize());
    SetTupleCount(0);
}

void TablePage::SetTupleOffset(int slot,int val) {
    SIMPLEDB_ASSERT(slot < GetTupleCount(), "");
    
    int slot_offset = SLOT_ARRAY_OFFSET + slot * SLOT_SIZE;
    txn_->SetInt(block_, slot_offset, val, true);
}

int TablePage::GetTupleOffset(int slot) {
    SIMPLEDB_ASSERT(slot < GetTupleCount(), "");
    
    int slot_offset = SLOT_ARRAY_OFFSET + slot * SLOT_SIZE;
    int res = txn_->GetInt(block_, slot_offset); 
    // SIMPLEDB_ASSERT(res != 0, "Get Tuple Offset When Offset = 0");
    
    return res;
}

bool TablePage::IsValidTuple(int slot) {
    int tuple_count = GetTupleCount();
    return slot < tuple_count;
}

int TablePage::GetInt(int slot, std::string field_name) {
    int field_pos = GetTupleOffset(slot) + layout_.GetOffset(field_name);
    return txn_->GetInt(block_, field_pos);
}

std::string TablePage::GetString(int slot, std::string field_name) {
    int field_pos = GetTupleOffset(slot) + layout_.GetOffset(field_name);
    return txn_->GetString(block_, field_pos);
}

void TablePage::SetInt(int slot, std::string field_name, int val) {
    int field_pos = GetTupleOffset(slot) + layout_.GetOffset(field_name);
    SIMPLEDB_ASSERT(field_pos != 0, "Field Position is zero");
    txn_->SetInt(block_, field_pos, val, true);
}

void TablePage::SetString(int slot, std::string field_name, std::string val) {
    int field_pos = GetTupleOffset(slot) + layout_.GetOffset(field_name);
    
    SIMPLEDB_ASSERT(field_pos != 0, "Field Position is zero");
    txn_->SetString(block_, field_pos, val, true);
}


void TablePage::MarkDeleteTuple(int slot) {
    // we just mark the delete flag in this tuple
    
    SIMPLEDB_ASSERT(slot < GetTupleCount(), "delete a tuple which not exist");
    SIMPLEDB_ASSERT(IsDeleted(GetTupleOffset(slot)) == false,
                    "delete a deleted tuple");
    
    int tuple_offset = GetTupleOffset(slot);
    tuple_offset = SetDeleteFlag(tuple_offset);
    
    SetTupleOffset(slot, tuple_offset);
}

int TablePage::SearchTuple(int slot, TupleStatus flag) {
    slot ++;
    while (IsValidTuple(slot)) {
        
        int tuple_offset = GetTupleOffset(slot);
        // if tuple_offset != 0, flag != 0
        // or tuple_offset == 0, flag == 0
        if (flag ^ IsDeleted(tuple_offset)) {
            return slot;
        }
        slot ++;
    }
    return -1;
}

bool TablePage::CanInsert(int tuple_size) {
    int tuple_count = GetTupleCount();
    int slot_array_end = SLOT_ARRAY_OFFSET + tuple_count * SLOT_SIZE;
    int tuple_array_end = GetFreeSpacePtr();
    
    SIMPLEDB_ASSERT(slot_array_end <= tuple_array_end, 
        "The slot array and tuple array overlap");
    // a tuple will be stored like this
    // | tuple size | tuple data | 
    return tuple_array_end - slot_array_end >= Page::MaxLength(tuple_size) + SLOT_SIZE;
}

int TablePage::NextTuple(int slot) {
    return SearchTuple(slot, TupleStatus::USED);
}

int TablePage::NextEmptyTuple(int slot) {
    int slot_number = SearchTuple(slot, TupleStatus::EMPTY);
    int tuple_offset;
    int tuple_size = layout_.GetTupleSize();

    // if we can find a discarded tuple
    if (slot_number >= 0) {
        tuple_offset = GetTupleOffset(slot);  
        tuple_offset = UnsetDeleteFlag(tuple_offset);
        SetTupleOffset(slot_number, tuple_offset);        
    }

    // if we can insert a new tuple in this page
    if (slot_number < 0 && CanInsert(tuple_size)) {
        slot_number = GetTupleCount();
        tuple_offset = GetFreeSpacePtr() - Page::MaxLength(tuple_size);
        SetFreeSpacePtr(tuple_offset);
        SetTupleCount(slot_number + 1);
        SetTupleOffset(slot_number, tuple_offset);
    }

    return slot_number;
}



} // namespace SimpleDB

#endif