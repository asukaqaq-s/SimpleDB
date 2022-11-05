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
    index_schema_ = CreateIdxSchema();
}

int IndexInfo::GetAccessBlocks() {
    // Estimate the number of block accesses re quired to
    // find all index records having a particular searchkey.
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
    // tuple count / distinct_value count
    // we just need to search 

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



Schema IndexInfo::CreateIdxSchema() {
    // a index tuple consists of three columns
    // --------------------------------------
    // | block number | slot number | value |
    // --------------------------------------
    Schema index_schema;

    index_schema.AddColumn({"block", TypeID::INTEGER});
    index_schema.AddColumn({"slot", TypeID::INTEGER});
    

    index_schema.AddSameColumn(field_name_, table_schema_);
    
    auto type1 = table_schema_.GetColumn(field_name_).GetType();
    auto type2 = index_schema.GetColumn(field_name_).GetType();
    SIMPLEDB_ASSERT(type1 == type2, "add field error");

    return index_schema;
}



IndexManager::IndexManager(bool IsNew, TableManager *table_mgr,
                           StatManager *stat_mgr, Transaction *txn) 
    : table_mgr_(table_mgr), stat_mgr_(stat_mgr) {
    
    // if this is a new database
    // we should create a Index table which stores in disk
    if (IsNew) {
        Schema index_table_schema;
         
        index_table_schema.AddColumn(Column(INDEX_INDEX_NAME_FIELD,
                                            TypeID::VARCHAR, 
                                            MAX_TABLE_NAME_LENGTH));
        index_table_schema.AddColumn(Column(INDEX_TABLE_NAME_FIELD, 
                                            TypeID::VARCHAR,
                                            MAX_TABLE_NAME_LENGTH));
        index_table_schema.AddColumn(Column(INDEX_FIELD_NAME_FIELD,
                                            TypeID::VARCHAR, 
                                            MAX_TABLE_NAME_LENGTH));

        table_mgr_->CreateTable(INDEX_CATCH, index_table_schema, txn);
    }

    index_table_info_ = table_mgr_->GetTable(INDEX_CATCH, txn);

}

void IndexManager::CreateIndex(std::string index_name, 
                               std::string table_name, 
                               std::string field_name, 
                               Transaction *txn) {
    auto *table_heap = index_table_info_->table_heap_.get();

    std::vector<Value> field_list {
        Value(index_name, TypeID::VARCHAR), 
        Value(table_name, TypeID::VARCHAR),
        Value(field_name, TypeID::VARCHAR)
    };
    Tuple inserted_tuple(field_list, index_table_info_->schema_);    
    RID rid;

    table_heap->Insert(txn, inserted_tuple, &rid);
}

std::map<std::string, IndexInfo> IndexManager::GetIndexInfo
                                 (std::string table_name, 
                                  Transaction *txn) {
    auto *table_heap = index_table_info_->table_heap_.get();
    auto table_iterator = table_heap->Begin(txn);
    Schema idx_table_schema = index_table_info_->schema_;

    std::map<std::string, IndexInfo> res;
    

    while (!table_iterator.IsEnd()) {
        Tuple tmp_tuple = table_iterator.Get();
        table_iterator++;
        

        if (tmp_tuple.GetValue(INDEX_TABLE_NAME_FIELD, 
                               idx_table_schema).AsString() == table_name) {
            
            auto index_name = tmp_tuple.GetValue(INDEX_INDEX_NAME_FIELD, 
                                                 idx_table_schema).AsString();
            auto field_name = tmp_tuple.GetValue(INDEX_FIELD_NAME_FIELD, 
                                                 idx_table_schema).AsString();


            Schema table_schema = table_mgr_->GetTable(table_name, txn)->schema_;
            StatInfo table_info = stat_mgr_->GetStatInfo(table_name, table_schema, txn);


            res[field_name] = IndexInfo(index_name, 
                                        field_name, 
                                        table_schema,
                                        txn,
                                        table_info);
        }
    }

    return res;
}


} // namespace SimpleDB

#endif