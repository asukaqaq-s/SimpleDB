#ifndef STAT_MANAGER_CC
#define STAT_MANAGER_CC

#include "metadata/stat_manager.h"

namespace SimpleDB {

StatManager::StatManager(TableManager *table_mgr, 
        Transaction *txn) : table_mgr_(table_mgr) {
    RefreshStatistics(txn);
}

StatInfo StatManager::GetStatInfo(const std::string &table_name,
                                  const Layout &layout, 
                                  Transaction* txn) {
    std::unique_lock<std::recursive_mutex> latch(latch_);
    num_calls_ ++;

    // we should refersh the stat info.
    // because the current transaction is asked to do this, 
    // it is reasonable to read statinfo after refershing.
    if (num_calls_ > MAX_REFERSH_THRESHOLDS) {
        RefreshStatistics(txn);
    }                          

    // SIMPLEDB_ASSERT(map_stat_.find(table_name) != map_stat_.end(),
    //                 "this table is not exist");

    // if we can not find it, should we need to refresh it?
    if (map_stat_.find(table_name) == map_stat_.end()) {
        RefreshStatistics(txn);
    }


    return map_stat_[table_name];          
}

void StatManager::RefreshStatistics(Transaction *txn) {
    std::unique_lock<std::recursive_mutex> latch(latch_);
    Layout layout = table_mgr_->GetLayout(TABLE_CATCH, txn);
    TableScan tcat_table_scan(txn, TABLE_CATCH, layout);
    
    // Since during this period, We may drop some tables 
    map_stat_.clear();
    // clear the call times
    num_calls_ = 0;
    
    while (tcat_table_scan.NextTuple()) {
        auto table_name = tcat_table_scan.GetString("table_name");
        auto layout = table_mgr_->GetLayout(table_name, txn);
        auto statinfo = CalculateStat(table_name, layout, txn);
        
        map_stat_[table_name] = statinfo;
    }
    tcat_table_scan.Close();
}


StatInfo StatManager::CalculateStat(const std::string &table_name,
                                    Layout layout,
                                    Transaction *txn) {
    // iterates this table and calculates three statinfo
    // 1. the number of blocks
    // 2. the number of records
    // 3. the number of distinct values
    int block_nums = 0;
    int tuple_nums = 0;
    std::map<std::string, int> distinict_val;
    TableScan table_scan(txn, table_name, layout);

    while (table_scan.NextTuple()) {
        block_nums = table_scan.GetRid().GetBlockNum() + 1;
        tuple_nums ++;
        // todo
        // this is a simple way to calculate distinct values
        // for (auto field_name : layout.GetSchema().GetFields()) {
        //     
        // }
    }

    table_scan.Close();

    auto statinfo = StatInfo(block_nums, tuple_nums);
    statinfo.SetDistinctValAll(distinict_val);
    return statinfo;
}


} // namespace SimpleDB

#endif