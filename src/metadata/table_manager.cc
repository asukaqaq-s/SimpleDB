#ifndef TABLE_MANAGER_CC
#define TABLE_MANAGER_CC

#include "metadata/table_manager.h"



namespace SimpleDB {

TableManager::TableManager(bool IsNew, Transaction *txn, FileManager *fm, 
                           RecoveryManager *rm, BufferManager *bfm) : 
        fm_(fm), rm_(rm), bfm_(bfm) {


    // init tcat's schema
    tcat_schema_.AddColumn(Column(TCAT_TABLE_NAME_FIELD, 
                           TypeID::VARCHAR, MAX_TABLE_NAME_LENGTH));
    tcat_schema_.AddColumn(Column(TCAT_SCHEMA_SIZE_FIELD, TypeID::INTEGER));

    

    // init fcat's schema
    std::vector<Column> vec 
    {
        Column(FCAT_TABLE_NAME_FIELD, TypeID::VARCHAR, MAX_TABLE_NAME_LENGTH),
        Column(FCAT_FIELD_NAME_FIELD, TypeID::VARCHAR, MAX_TABLE_NAME_LENGTH),
        Column(FCAT_FIELD_TYPE_FIELD, TypeID::INTEGER),
        Column(FCAT_FIELD_LENGTH_FIELD, TypeID::INTEGER),
        Column(FCAT_FIELD_OFFSET_FIELD, TypeID::INTEGER)
    };
    fcat_schema_.AddAllColumns(vec);
    
    
    // add this two table's tableinfo to table_infos_
    table_infos_[TABLE_CATCH] = std::make_unique<TableInfo> (tcat_schema_, TABLE_CATCH, 
                                std::make_unique<TableHeap> (txn, TABLE_CATCH, fm_, rm_, bfm_));
    table_infos_[FIELD_CATCH] = std::make_unique<TableInfo> (fcat_schema_, FIELD_CATCH, 
                                std::make_unique<TableHeap> (txn, FIELD_CATCH, fm_, rm_, bfm_));
    
    
    // if this database is new, create fcat and tcat table
    if (IsNew) {
        CreateTable(TABLE_CATCH, tcat_schema_, txn);
        CreateTable(FIELD_CATCH, fcat_schema_, txn);
    }
}


void TableManager::CreateTable(const std::string &table_name, 
                               const Schema &schema, 
                               Transaction *txn) {
    std::unique_lock<std::mutex> latch(latch_);
    
    if (table_infos_.find(table_name) != table_infos_.end() &&
        table_name != TABLE_CATCH &&
        table_name != FIELD_CATCH) {
        SIMPLEDB_ASSERT(false, "create table multipy");
    }


    // create a tableinfo which stored in memory
    auto new_table = std::make_unique<TableInfo> (schema, table_name, 
                     std::make_unique<TableHeap> (txn, table_name, fm_, rm_, bfm_));
    table_infos_[table_name] = std::move(new_table);


    // get the tableinfo of tcat and fcat
    auto *tcat_info = GetTableInfo(TABLE_CATCH);
    auto *fcat_info = GetTableInfo(FIELD_CATCH);
    latch.unlock();

    
    
    // generate a tuple and insert it into table
    // in this phase, we don't need grant latch
    RID rid;
    std::vector<Value> tcat_list { Value(table_name, TypeID::VARCHAR), Value(schema.GetLength())};
    Tuple tcat_tuple(tcat_list, tcat_schema_);
    tcat_info->table_heap_->Insert(txn, tcat_tuple, &rid); 

    
    // for every columns, generate a tuple and insert it into fcat_table
    for (auto &column : schema.GetColumns()) {
        
        // generate a fieldinfo tuple
        std::vector<Value> fcat_list 
        {
            Value(table_name, TypeID::VARCHAR),       // table_name field
            Value(column.GetName(), TypeID::VARCHAR), // column_name field
            Value(static_cast<int>(column.GetType())),      // column_type field
            Value(column.GetLength()),    // column_length field
            Value(column.GetOffset())     // column_offset field
        };

        // insert this tuple into fcat table
        Tuple fcat_tuple(fcat_list, fcat_schema_);
        fcat_info->table_heap_->Insert(txn, fcat_tuple, nullptr);
    }
}

TableInfo* TableManager::GetTable(const std::string &table_name, 
                                  Transaction *txn) {
    std::unique_lock<std::mutex> latch(latch_);
    
    if (GetTableInfo(table_name) != nullptr) {
        return GetTableInfo(table_name);
    }

    auto *tcat_info = GetTableInfo(TABLE_CATCH);
    auto *fcat_info = GetTableInfo(FIELD_CATCH);
    latch.unlock();

    // in begin method, we will locate to the first tuple
    auto tcat_iterator = tcat_info->table_heap_->Begin(txn);
    auto fcat_iterator = fcat_info->table_heap_->Begin(txn);

    // Retrieve the size of schema's size
    int schema_size = 0;
    while (!tcat_iterator.IsEnd()) {
        Tuple tmp_tuple = tcat_iterator.Get();
        tcat_iterator++;

        if (tmp_tuple.GetValue(TCAT_TABLE_NAME_FIELD, tcat_schema_).AsString() == table_name) {
            schema_size = tmp_tuple.GetValue(TCAT_SCHEMA_SIZE_FIELD, tcat_schema_).AsInt();
            
            // only one tuple has the information of this table
            break;
        }
    }
    
    if (schema_size == 0) {
        // this table is not exist
        return nullptr;
    }

    // Retrieve the offset of columns
    std::vector<Column> vec;
    
    while (!fcat_iterator.IsEnd()) {
        Tuple tmp_tuple = fcat_iterator.Get();
        tcat_iterator++;

        if (tmp_tuple.GetValue(FCAT_TABLE_NAME_FIELD, fcat_schema_).AsString() == table_name) {
            
            std::string column_name = tmp_tuple.GetValue(FCAT_FIELD_NAME_FIELD, fcat_schema_).AsString();
            int column_typeid = tmp_tuple.GetValue(FCAT_FIELD_TYPE_FIELD, fcat_schema_).AsInt();
            int column_length = tmp_tuple.GetValue(FCAT_FIELD_LENGTH_FIELD, fcat_schema_).AsInt();
            int column_offset = tmp_tuple.GetValue(FCAT_FIELD_OFFSET_FIELD, fcat_schema_).AsInt();
            
            vec.emplace_back(Column(column_name, static_cast<TypeID> (column_typeid), 
                                    column_length, column_offset));
        }
    }

    Schema schema(schema_size, vec);
    
    latch.lock();
    auto table_info = std::make_unique<TableInfo>(schema, table_name,
                      std::make_unique<TableHeap>(txn, table_name, fm_, rm_, bfm_));
    table_infos_[table_name] = std::move(table_info);
    return table_infos_[table_name].get();
}


TableInfo* TableManager::GetTableInfo(const std::string &table_name) {
    if (table_infos_.find(table_name) != table_infos_.end()) {
        return table_infos_[table_name].get();
    }
    return nullptr;
}

} // namespace SimpleDB

#endif
