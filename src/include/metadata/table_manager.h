#ifndef TABLE_MANAGER_H
#define TABLE_MANAGER_H

#include "record/table_scan.h"
#include "config/config.h"

namespace SimpleDB {

/**
* @brief we use two tables to store the metadata of each table.
* we will create two new tables to store table metadata.
* 
* A table named tcat(table catch) which has two columns
* --------------------------
* | table_name | tuple_size |
* --------------------------
* the table will store each table and their tuple_size.
* currently, we just support fixed-length tuple.
*  
* A table named fcat(field catch) which has five columns
* ----------------------------------------------------------------------
* | table_name | field_name | field_type | field_length | field_offset |
* ----------------------------------------------------------------------
* the table will store each table and their each field infomations.
* we can restore a old layout and schema according to this information.
*
*/
class TableManager {

public:

    /**
    * @brief Create a catalog manager for the database system.
    * If the database is new, the two catalog tables are created.
    * 
    * @param IsNew has the value true if the database is new
    * @param tx the startup transaction
    */
    TableManager(bool IsNew, Transaction *txn);

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
    Layout GetLayout(const std::string &table_name, Transaction *txn);
    

private:  

    // table catch 
    Layout tcat_layout_;
    
    // field catch
    Layout fcat_layout_;
};

} // namespace SimpleDB

#endif