#ifndef ANALYZER_H
#define ANALYZER_H

#include "metadata/metadata_manager.h"


namespace SimpleDB {


/**
* @brief this class help parser check statement legality.
*/
class Analyzer {


public:


    Analyzer(Transaction *txn, MetadataManager *mdm) 
        : txn_(txn), mdm_(mdm) {}
    
    
    bool IsTableExist(const std::string &table) {
        TableInfo *res = nullptr;
        if (tables_.find(table) != tables_.end()) {
            res = tables_[table];
        }
        else {
            res = mdm_->GetTable(table, txn_);
            tables_[table] = res;
        }

        return res != nullptr;
    }

    bool IsColumnExist(const std::string &table, const std::string &column) {
        
        if (!IsTableExist(table)) {
            return false;
        }

        TableInfo* info = tables_[table];
        assert(info);
        return info->schema_.HasColumn(column);
    }

    TypeID GetColumnType(const std::string &table, const std::string &column) {
        if (!IsTableExist(table)) {
            assert(false);
        }
        if (!IsColumnExist(table, column)) {
            assert(false);
        }
        
        TableInfo* info = tables_[table];
        assert(info);
        return info->schema_.GetColumn(column).GetType();
    }

    Schema* GetSchema(const std::string &table) {
        if (!IsTableExist(table)) {
            assert(false);
        }

        TableInfo* info = tables_[table];
        assert(info);
        return &info->schema_;
    }


private:

    Transaction *txn_;

    MetadataManager *mdm_;

    // map table_name --> tableinfo 
    // we can cache tableinfos by this way for avoid concurrency penalty
    std::map<std::string, TableInfo*> tables_;

};

} // namespace SimpleDB


#endif