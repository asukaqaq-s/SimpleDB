#ifndef TABLE_SCAN_CC
#define TABLE_SCAN_CC

#include "record/table_scan.h"

#include <iostream>

namespace SimpleDB {
    
TableScan::TableScan(Transaction *txn, std::string table_name, Layout layout)
    : txn_(txn), file_name_(table_name + ".table"), layout_(layout) {
    
    if (txn_->GetFileSize(file_name_) == 0) {
        BlockId blk = txn_->Append(file_name_);
        table_page_ = std::make_unique<TablePage> (txn_, blk, layout_);
        table_page_->InitPage();
        current_slot_ = -1;
    } else {
        BlockId blk(file_name_, 0);
        table_page_ = std::make_unique<TablePage> (txn_, blk, layout_);
        current_slot_ = -1;
    }
}

void TableScan::FirstTuple() {
    MoveToBlock(0);
}

bool TableScan::Next() {
    current_slot_ = table_page_->NextTuple(current_slot_);
    while (current_slot_ < 0) {
        if (AtLastBlock()) // at last tuple
            return false;

        MoveToBlock(table_page_->GetBlock().BlockNum() + 1);
        current_slot_ = table_page_->NextTuple(current_slot_);
    }

    return true;
}

int TableScan::GetInt(const std::string &field_name) {
    return table_page_->GetInt(current_slot_, field_name);
}

std::string TableScan::GetString(const std::string &field_name) {
    return table_page_->GetString(current_slot_, field_name);
}

Constant TableScan::GetVal(const std::string &field_name) {
    if (layout_.GetSchema().GetType(field_name) == FieldType::INTEGER) {
        return Constant(GetInt(field_name));
    } else {
        return Constant(GetString(field_name));
    }
}

void TableScan::SetInt(const std::string &field_name, int value) {
    table_page_->SetInt(current_slot_, field_name, value);
}

void TableScan::SetString(const std::string &field_name, 
                          const std::string &value) {
    table_page_->SetString(current_slot_, field_name, value);
}

void TableScan::SetVal(const std::string &field_name, const Constant &val) {
    if (layout_.GetSchema().GetType(field_name) == FieldType::INTEGER) {
        SetInt(field_name, val.AsInt());
    } else {
        SetString(field_name, val.AsString());
    }
}

bool TableScan::HasField(const std::string &field_name) {

    return layout_.GetSchema().HasField(field_name);
}

void TableScan::Close() {
    if (table_page_ != nullptr) 
        txn_->Unpin(table_page_->GetBlock());
}

void TableScan::Insert() {
    current_slot_ = table_page_->NextEmptyTuple(current_slot_);

    while (current_slot_ < 0) {
        if (AtLastBlock()) {
            MoveToNewBlock();
        } else {
            MoveToBlock(table_page_->GetBlock().BlockNum() + 1);  
        }
        current_slot_ = table_page_->NextEmptyTuple(current_slot_);
    }
}

void TableScan::Remove() {
    table_page_->MarkDeleteTuple(current_slot_);
}

void TableScan::MoveToRid(const RID &rid) {
    Close(); // will not use previous block
    BlockId block(file_name_, rid.GetBlockNum());
    table_page_ = std::make_unique<TablePage> (txn_, block, layout_);
    current_slot_ = rid.GetSlot();
}

RID TableScan::GetRid() {
    return RID(table_page_->GetBlock().BlockNum(), current_slot_);
}

//
// priavte auxiliary methods
//

void TableScan::MoveToBlock(int block_number) {
    Close();
    BlockId block(file_name_, block_number);
    table_page_ = std::make_unique<TablePage> (txn_, block, layout_);
    current_slot_ = -1;
}

void TableScan::MoveToNewBlock() {
    Close();
    BlockId block = txn_->Append(file_name_);
    table_page_ = std::make_unique<TablePage> (txn_, block, layout_);
    table_page_->InitPage();
    current_slot_ = -1;
}

bool TableScan::AtLastBlock() {
    return table_page_->GetBlock().BlockNum() == 
           txn_->GetFileSize(file_name_) - 1;
}

} // namespace SimpleDB

#endif