#ifndef INDEX_MANAGER_H
#define INDEX_MANAGER_H

#include "metadata/table_manager.h"
#include "metadata/stat_manager.h"


namespace SimpleDB {

/**
* @brief the information about an index.
* This information is used by the query planner 
* in order to estimate the costs of using the index, 
* and to obtain the layout of the index records. Its 
* methods are essentially the same as those of plan
*/
class IndexInfo {
    
public:

    IndexInfo() {}

    IndexInfo(const IndexInfo& obj) : index_name_(obj.index_name_),
                                      field_name_(obj.field_name_),
                                      txn_(obj.txn_),
                                      table_schema_(obj.table_schema_),
                                      index_layout_(obj.index_layout_),
                                      info_(obj.info_) {}

    IndexInfo &operator=(const IndexInfo& obj) {
        if (this != &obj) {
            index_name_ = obj.index_name_;
            field_name_ = obj.field_name_;
            txn_ = obj.txn_;
            table_schema_ = obj.table_schema_;
            index_layout_ = obj.index_layout_;
            info_ = obj.info_;   
        }
        return *this;
    }


    /**
    * @brief create an IndexInfo object for the specified index.
    * 
    * @param index_name
    * @param field_name
    * @param table_schema
    * @param txn the calling transaction
    * @param si the statistics for the table.
    */
    IndexInfo(std::string index_name, std::string field_name, 
              Schema table_schema,
              Transaction *txn,
              StatInfo si);
    
    // Index Open();

    /** 
    * @brief Estimate the number of block accesses
    * @return the number of block accesses required to travese the index.
    */
    int GetAccessBlocks();

    /**
    * @brief Estimate the number of records having a serach key.
    * @return the estimated number of records having a search key
    */
    int GetOutputTuples();

    /**
    * @brief return the distinct values for a specified field
    * in the underlying table, or 1 for the indexed field.
    * @param field_name the specified field
    */
    int GetDistinctVals(std::string field_name);


private:

    /**
    * @brief return the layout of the index records.
    * @return the layout of the index records
    */
    Layout CreateIdxLayout();

    

private:

    std::string index_name_;
    
    std::string field_name_;
    
    Transaction *txn_;

    Schema table_schema_;
    
    Layout index_layout_;
    // the statinfo of the specified table
    StatInfo info_;
};

/**
* @brief the index manager has similar functionality
* to the table manager.
* NOTE THAT, index info is Persistenly stores in disk
* Index_table has three columns
* ----------------------------------------
* | index_name | table_name | field_name |
* ----------------------------------------
*/
class IndexManager {

public:
    
    /**
    * @brief create the index manager
    * this constructor is called during system startup
    * @param IsNew
    * @param table_mgr
    * @param stat_mgr
    * @param txn the start up transaction
    */
    IndexManager(bool IsNew, TableManager *table_mgr,
                 StatManager *stat_mgr, Transaction*txn);

    /**
    * @brief create an index of the specified type for the specified field
    * A RID is assigned to this index, and its infomation is stored
    * in the index_catch table
    */
    void CreateIndex(std::string index_name, std::string table_name, 
                     std::string field_name, Transaction *txn);

    /**
    * @brief return a map containing the index info for all indexes
    * on the specified table.
    * @param table_name the name of the table
    * @param txn the calling transaction
    * @return a map of indexinfo objects
    */
    std::map<std::string, IndexInfo> GetIndexInfo(std::string table_name, 
                                                  Transaction *txn);

private:

    Layout idx_table_layout_;

    TableManager *table_mgr_;

    StatManager *stat_mgr_;
};

} // namespace SimpleDB


#endif