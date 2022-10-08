#ifndef TABLE_MANAGER_CC
#define TABLE_MANAGER_CC

#include "metadata/table_manager.h"

namespace SimpleDB {

TableManager::TableManager(bool IsNew, Transaction *txn) {
    Schema table_catch_schema;
    Schema field_catch_schema;

    // create table_catch schema
    table_catch_schema.AddStringField("table_name", 
                                       MAX_TABLE_NAME_LENGTH);
    table_catch_schema.AddIntField("slot_size");

    // create tcat_layout
    tcat_layout_ = Layout(table_catch_schema);

    // create field_catch schema
    field_catch_schema.AddStringField("table_name",
                                       MAX_TABLE_NAME_LENGTH);
    field_catch_schema.AddStringField("field_name",
                                       MAX_TABLE_NAME_LENGTH);
    field_catch_schema.AddIntField("field_type");
    field_catch_schema.AddIntField("field_length");
    field_catch_schema.AddIntField("field_offset");

    // create fcat_layout
    fcat_layout_ = Layout(field_catch_schema);

    // If the database we are creating now is a new database
    // we should create table_catch table and field_catch_table
    // this param "IsNew" will be setted by file_manager
    if (IsNew) {
        CreateTable(TABLE_CATCH, table_catch_schema, txn);
        CreateTable(FIELD_CATCH, field_catch_schema, txn);
    }
    // If we add metadata from these two tables to the catalog, 
    // we can call GetLayout function to return relevant information
}

void TableManager::CreateTable(const std::string &table_name, 
                               const Schema &schema, 
                               Transaction *txn) {
    // Uses a table scan to insert records into the catalog.
    // It inserts one record into table_catch for the table 
    // and one record into field_catch for each field of the table.
    TableScan tcat_table_scan(txn, TABLE_CATCH, tcat_layout_);
    TableScan fcat_table_scan(txn, FIELD_CATCH, fcat_layout_);
    Layout new_layout(schema);

    // insert into tcat table
    // just for the table
    tcat_table_scan.NextFreeTuple();
    tcat_table_scan.SetString("table_name", table_name);
    tcat_table_scan.SetInt("slot_size", new_layout.GetTupleSize());
    tcat_table_scan.Close();
    
    // insert into fcat table
    // each field of the table     
    for (auto field_name : schema.GetFields()) {
        fcat_table_scan.NextFreeTuple();
        fcat_table_scan.SetString("table_name", table_name);
        fcat_table_scan.SetString("field_name", field_name);
        fcat_table_scan.SetInt("field_type", schema.GetType(field_name));
        fcat_table_scan.SetInt("field_length", schema.GetLength(field_name));
        fcat_table_scan.SetInt("field_offset", new_layout.GetOffset(field_name));
    }
    // remember that close table_scan every time
    fcat_table_scan.Close();
}

Layout TableManager::GetLayout(const std::string &table_name, 
                               Transaction *txn) {
    // Uses a table scan to query records from the catalog.
    // In table_catch table, we retrieve a tuple size for this table.
    // In field_catch table, we retrieve each field infomations for this table.
    // finally, we can construt a layout and return it.
    TableScan tcat_table_scan(txn, TABLE_CATCH, tcat_layout_);
    TableScan fcat_table_scan(txn, FIELD_CATCH, fcat_layout_);
    int tuple_size;
    Schema new_schema;
    std::map<std::string, int> map_offset;

    // retrieve a tuple size
    while (tcat_table_scan.NextTuple()) {
        if (tcat_table_scan.GetString("table_name")
            == table_name) {
            tuple_size = tcat_table_scan.GetInt("slot_size");
            break;        
        }
    }
    tcat_table_scan.Close();
        
    // retrieve each field infomations
    while (fcat_table_scan.NextTuple()) {
        if (fcat_table_scan.GetString("table_name")
            == table_name) {
            
            // read field infomation
            std::string field_name = fcat_table_scan.GetString("field_name");
            int field_offset = fcat_table_scan.GetInt("field_offset");
            int field_type = fcat_table_scan.GetInt("field_type");
            int field_length = fcat_table_scan.GetInt("field_length");

            // update schema
            new_schema.AddField(field_name, 
                                static_cast<FieldType> (field_type), 
                                field_length);

            // update map_offset
            map_offset[field_name] = field_offset;
        }
    }
    fcat_table_scan.Close();

    return Layout(new_schema, map_offset, tuple_size);
}

} // namespace SimpleDB

#endif