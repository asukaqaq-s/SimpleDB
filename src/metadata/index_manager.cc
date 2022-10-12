#ifndef INDEX_MANAGER_CC
#define INDEX_MANAGER_CC

#include "metadata/index_manager.h"

namespace SimpleDB {

IndexInfo::IndexInfo(std::string index_name, std::string field_name, 
              Schema table_schema, Transaction *txn, StatInfo si) : 
              index_name_(index_name), field_name_(field_name), txn_(txn),
              table_schema_(table_schema), info_(si) {
    // we should use stasinfo to construct the schema for
    // the index record so that estimate the size of the index file
    index_layout_ = CreateIdxLayout();
}

int IndexInfo::GetAccessBlocks() {
    // Estimate the number of block accesses required to
    // find all index records having a particular search key.
    // this method uses the table's metadata to estimate the size 
    // of the index file and the number of index records per block.
    
    // int record_per_block = txn_->BlockSize() / index_layout_.GetTupleSize();
    // int block_nums = info_.GetOutputTuples() / record_per_block;
    
    // passes this information to estimate search cost
    return 0;
    // return HashIndex.GetSearchCost(block_nums, record_per_block);
}

int IndexInfo::GetOutputTuples() {
    // estimate the number of records having a search key
    // this is a stupid way to just take an average value
    // current, total_tuple_nums / search_key_nums = average value

    return info_.GetOutputTuples() /
           info_.GetDistinctVals(field_name_);
}

int IndexInfo::GetDistinctVals(std::string field_name) {
    // estimate the distinct values for a specified field
    // if this specified field is equal to the field which indexing
    // we can simply estimate distinct values is one
    
    if (field_name == field_name_) {
        return 1;
    } else {
        return info_.GetDistinctVals(field_name);
    }
}

Layout IndexInfo::CreateIdxLayout() {
    // a index tuple consists of three columns
    // --------------------------------------
    // | block number | slot number | value |
    // --------------------------------------
    Schema index_schema;

    index_schema.AddIntField("block");
    index_schema.AddIntField("id");
    
    if (table_schema_.GetType(field_name_) == 
        FieldType::INTEGER) {
        index_schema.AddIntField("value");
    } else {
        int field_length = table_schema_.GetLength(field_name_);
        index_schema.AddStringField("value", field_length);
    }

    return Layout(index_schema);
}

IndexManager::IndexManager(bool IsNew, TableManager *table_mgr,
                           StatManager *stat_mgr, Transaction *txn) 
    : table_mgr_(table_mgr), stat_mgr_(stat_mgr) {
    // if this is a new database
    // we should create a Index table which stores in disk
    if (IsNew) {
        Schema index_table_schema;
        // 
        index_table_schema.AddStringField("index_name", 
                                          MAX_TABLE_NAME_LENGTH);
        index_table_schema.AddStringField("table_name", 
                                          MAX_TABLE_NAME_LENGTH);
        index_table_schema.AddStringField("field_name", 
                                          MAX_TABLE_NAME_LENGTH);

        table_mgr_->CreateTable(INDEX_CATCH, index_table_schema, txn);
    }

    idx_table_layout_ = table_mgr_->GetLayout(INDEX_CATCH, txn);
}

void IndexManager::CreateIndex(std::string index_name, 
                               std::string table_name, 
                               std::string field_name, 
                               Transaction *txn) {
    TableScan index_table_scan(txn, INDEX_CATCH, idx_table_layout_);

    index_table_scan.Insert();
    
    index_table_scan.SetString("index_name", index_name);
    index_table_scan.SetString("table_name", table_name);
    index_table_scan.SetString("field_name", field_name);

    index_table_scan.Close();
}

std::map<std::string, IndexInfo> IndexManager::GetIndexInfo
                                 (std::string table_name, 
                                  Transaction *txn) {
    TableScan index_table_scan(txn, INDEX_CATCH, idx_table_layout_);
    std::map<std::string, IndexInfo> res;
    

    while (index_table_scan.Next()) {
        if (index_table_scan.GetString("table_name")
            == table_name) {
            auto index_name = index_table_scan.GetString("index_name");
            auto field_name = index_table_scan.GetString("field_name");
            Layout table_layout = table_mgr_->GetLayout(table_name, txn);
            StatInfo table_info = stat_mgr_->GetStatInfo(table_name, table_layout, txn);


            res[field_name] = IndexInfo(index_name, 
                                        field_name, 
                                        table_layout.GetSchema(),
                                        txn,
                                        table_info);
        }
    }

    index_table_scan.Close();
    return res;
}


} // namespace SimpleDB

#endif