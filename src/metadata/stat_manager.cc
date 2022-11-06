#ifndef STAT_MANAGER_CC
#define STAT_MANAGER_CC

#include "metadata/stat_manager.h"
#include "type/value.h"

#include <map>
#include <set>

namespace SimpleDB {

StatManager::StatManager(TableManager *table_mgr, 
        Transaction *txn) : table_mgr_(table_mgr) {
    RefreshStatistics(txn);
}

StatInfo StatManager::GetStatInfo(const std::string &table_name,
                                  Transaction* txn) {

    std::unique_lock<std::recursive_mutex> latch(latch_);
    num_calls_ ++;


    // we should refersh the stat info.
    // because the current transaction is asked to do this, 
    // it is reasonable to read statinfo after refershing.
    if (num_calls_ > MAX_REFERSH_THRESHOLDS) {
        RefreshStatistics(txn);
    }                          


    // if we can not find it, should we need to refresh it?
    if (map_stat_.find(table_name) == map_stat_.end()) {
        RefreshStatistics(txn);
    }


    return map_stat_[table_name];          
}

void StatManager::RefreshStatistics(Transaction *txn) {
    std::unique_lock<std::recursive_mutex> latch(latch_);
    auto table_info = table_mgr_->GetTable(TABLE_CATCH, txn);
    Schema schema = table_info->schema_;
    auto table_iterator = table_info->table_heap_->Begin(txn);
    
    // because during this period, We may drop some tables 
    map_stat_.clear();
    // clear the call times
    num_calls_ = 0;
    
    while (!table_iterator.IsEnd()) {
        Tuple tcat_tuple = table_iterator.Get();
        table_iterator++;

        auto table_name = tcat_tuple.GetValue(table_mgr_->TCAT_TABLE_NAME_FIELD, 
                                              table_mgr_->tcat_schema_).AsString();
        auto schema = table_mgr_->GetTable(table_name, txn)->schema_;
        auto statinfo = CalculateStat(table_name, schema, txn);
        
        map_stat_[table_name] = statinfo;
    }
}


StatInfo StatManager::CalculateStat(const std::string &table_name,
                                    Schema schema,
                                    Transaction *txn) {
    // iterates this table and calculates three statinfo
    // 1. the number of blocks
    // 2. the number of records
    // 3. the number of distinct values
    int block_nums = 0;
    int tuple_nums = 0;
    auto table_info = table_mgr_->GetTable(table_name, txn);
    auto table_iterator = table_info->table_heap_->Begin(txn);

    // a stupid way to caculate the number of distinct values
    // map column_name to a value-set
    std::map<std::string, std::set<Value>> cal_distinict_val;

    while (!table_iterator.IsEnd()) {
        block_nums = table_iterator.GetRID().GetBlockNum() + 1;
        tuple_nums ++;
        
        Tuple tmp_tuple = table_iterator.Get();
        table_iterator++;

        for (auto &column : schema.GetColumns()) {
            auto *constant_set = &cal_distinict_val[column.GetName()];
            auto tmp_constant = tmp_tuple.GetValue(column.GetName(), schema);
            constant_set->insert(tmp_constant);
        }
    }

    std::map<std::string, int> distinict_val;
    for (const auto &t:cal_distinict_val) {
        distinict_val[t.first] = t.second.size();
    }


    auto statinfo = StatInfo(block_nums, tuple_nums);
    statinfo.SetDistinctValAll(distinict_val);
    return statinfo;
}


} // namespace SimpleDB

#endif