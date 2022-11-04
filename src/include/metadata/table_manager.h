#ifndef TABLE_MANAGER_H
#define TABLE_MANAGER_H

#include "record/table_heap.h"
#include "config/config.h"

#include <memory>

namespace SimpleDB {

class IndexInfo;
class IndexManager;

/**
* @brief the table's metadata, this class is used to
* get a table's information to modify it.
*/
class TableInfo {

    TableInfo(const Schema &schema, const std::string &table_name,
              std::unique_ptr<TableHeap> &&table)
              : schema_(schema), table_name_(table_name), table_heap_(std::move(table)) {
        // init index info...
    }


public:

    // table schema
    Schema schema_;

    // this table's name, since each table is stored 
    // as a separate file, we can scan this table by using table_name_  
    std::string table_name_;
    
    // a pointer points to table heap
    std::unique_ptr<TableHeap> table_heap_;

    // cache indexinfo
    // index name ---> index info
    std::map<std::string, IndexInfo> indexs_;

};




/**
* @brief 
* we will create two new tables to store table metadata.
* 
* A table named tcat(table catch) which has two columns
* --------------------------
* | table_name | layout_size |
* --------------------------
* the table will store each table and their layout's size
*  
* A table named fcat(field catch) which has five columns
* ----------------------------------------------------------------------
* | table_name | field_name | field_type | field_length | field_offset |
* ----------------------------------------------------------------------
* the table will store each table and their each field infomations.
* we can restore a old layout and schema according to this information.
* 
* we can build the schema of a exist table by these tables.
*/
class TableManager {

    friend class StatManager;

    const std::string TCAT_TABLE_NAME_FIELD = "table_name";
    const std::string TCAT_SCHEMA_SIZE_FIELD = "schema_size";
    const std::string FCAT_TABLE_NAME_FIELD = "table_name";
    const std::string FCAT_FIELD_NAME_FIELD = "field_name";
    const std::string FCAT_FIELD_TYPE_FIELD = "field_type";
    const std::string FCAT_FIELD_LENGTH_FIELD = "field_length";
    const std::string FCAT_FIELD_OFFSET_FIELD = "field_offset";


public:

    /**
    * @brief Create a catalog manager for the database system.
    * If the database is new, the two catalog tables are created.
    * 
    * @param IsNew has the value true if the database is new
    * @param tx the startup transaction
    */
    TableManager(bool IsNew, Transaction *txn, FileManager *fm, 
                 RecoveryManager *rm, BufferManager *bfm, LockManager *lock_mgr);

    /**
    * @brief Create a new table having the specified name and schema
    * 
    * @param table_name the name of the new table
    * @param schema the table's schema
    * @param txn the transaction creating the table
    */
    void CreateTable(const std::string &table_name, 
                    const Schema &schema, 
                    Transaction *tx);

    /**
    * @brief Retrieve the layout of the specified table
    * from the catalog.
    * 
    * @param table_name the name of the table
    * @param txn the transaction
    * @return the table's stored metadata
    */
    TableInfo* GetTable(const std::string &table_name, Transaction *txn);
    

private:  

    /**
    * @brief create the tableinfo object of fcat and tcat table
    */
    TableInfo* GetTableInfo(const std::string &table_name);

    // table catch 
    Schema tcat_schema_;
    
    // field catch
    Schema fcat_schema_;

    // cache tableinfo to avoid multiply access
    std::unordered_map<std::string, std::unique_ptr<TableInfo>> table_infos_;

    // shared filemanager to create table heap
    FileManager *fm_;

    // shared recovery managaer to create table heap
    RecoveryManager *rm_;

    // shared bufferpool manager to create table heap
    BufferManager *bfm_;
    
    // shared lockmanager to createe table heap
    LockManager *lock_mgr_;


    // because we use unordered_map, so a latch is useful
    std::mutex latch_;
};

} // namespace SimpleDB

#endif