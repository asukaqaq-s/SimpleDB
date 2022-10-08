#ifndef STAT_MANAGER_H
#define STAT_MANAGER_H

#include "metadata/table_manager.h"

#include <mutex>

namespace SimpleDB {

/**
* @brief A statinfo object holds three pieces of
* statistical information about a table:
*   the number of blocks
*   the number of records
*   the number of distinct values of each field
*/
class StatInfo {
    
public:


    StatInfo() = default;

    StatInfo(int block_nums, int tuple_nums)
        : block_nums_(block_nums), tuple_nums_(tuple_nums) {}

    /**
    * @brief return the estimated number of blocks in the table.
    */
    int GetAccessBlocks() { return block_nums_; }

    /**
    * @brief return the estimated number of tuples in the table.
    */
    int GetOutputTuples() { return tuple_nums_; }

    /**
    * @brief just set one field's info
    */
    void SetDistinctVals(const std::string &field_name, int val) {
        distinct_val_[field_name] = val;
    }

    /**
    * @brief set all field's info
    */
    void SetDistinctValAll(const std::map<std::string, int> &mp) {
        distinct_val_ = mp;
    }

    /**
    * @brief return the estimated number of distinct values
    * for the specified field
    */
    int GetDistinctVals(std::string field_name) {
        // SIMPLEDB_ASSERT(distinct_val_.find(field_name) != 
        //                 distinct_val_.end(),
        //                 "the field is not found");
        // return distinct_val_.at(field_name);
        return 1 + (tuple_nums_ / 3);
    }

private:
    
    int block_nums_;
    
    int tuple_nums_;
    
    // map field_name to distinct_values
    std::map<std::string, int> distinct_val_;

};

/**
* @brief the statistics manager is responsible for
* keeping statistical information about each table.
* The manager does not store this information in the database.
* Instead, it calculates this information on system startup,
* and periodically refreshes it.
*
* @todo: I think this is a stupid way which let current txn
* recalculate info and acquire info.
* Can we make a background transaction or a transaction
* which ioslation level is "READ UNCOMMITED ?" 
*/
class StatManager {

public:

    /**
    * @brief create the statistics manager
    * The initial statistics are calculated by 
    * traversing the entire database
    * @param txn the startup transaction
    */
    StatManager(TableManager *table_mgr, Transaction *txn);

    /**
    * @brief Return the statistical information about the specified table.
    * @param table_name the name of the table
    * @param layout the table's layout
    * @param txn the calling transaction
    * @return the statistical information about the table
    */
    StatInfo GetStatInfo(const std::string &table_name,
                         const Layout &layout, 
                         Transaction* txn);

private:

    /**
    * @brief iterates through the metadata table
    * and find every table for recalculating its statinfo.
    * @param txn a transaction which require statinfo 
    */
    void RefreshStatistics(Transaction *txn);

    /**
    * @brief Use tablescan to iterates the specified table
    * and calulates its statinfo
    * @return the table's statinfo
    */
    StatInfo CalculateStat(const std::string &table_name,
                           Layout layout,
                           Transaction *txn);

private:
    
    TableManager *table_mgr_;

    std::map<std::string, StatInfo> map_stat_;
    
    // This value is increased each time when statinfo is accessed
    // If the value exceeds the threshold, clear it and refersh
    // the statmanager for updating informations.
    int num_calls_{0};

    // because we should maintain the info of map_stat_ 
    // and num_calls, so mutex is necessary.
    std::recursive_mutex latch_;
};

} // namespace SimpleDB

#endif