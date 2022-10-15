#ifndef TABLE_PLAN_H
#define TABLE_PLAN_H

#include "plan/plan.h"
#include "record/layout.h"
#include "metadata/metadata_manager.h"
#include "concurrency/transaction.h"

namespace SimpleDB {

/**
* @brief the plan class corresponding to a table
*/
class TablePlan : public Plan {

public:

    /**
    * @brief Creates a leaf node in the query tree 
    * corresponding to the specified table.
    */
    TablePlan(Transaction *txn, const std::string &table_name,
              MetadataManager *md) :
              table_name_(table_name), txn_(txn) {
        layout_ = md->GetLayout(table_name_, txn_);
        info_ = md->GetStatInfo(table_name, layout_, txn_);          
    }

    /**
    * @brief creates a tablescan object for this query
    */
    std::shared_ptr<Scan> Open() override {
        return std::make_shared<TableScan> (txn_, table_name_, layout_);
    }

    /**
    * @brief estimates the number of block accesses for
    * the table, which is obtainable from the statistics
    * manager.
    */
    int GetAccessBlocks() override {
        return info_.GetAccessBlocks();
    }

    /**
    * @brief Estimates the number of records in the table,
    * which is obtainable from the statistics manager.
    */
    int GetOutputTuples() override {
        return info_.GetOutputTuples();
    }

    /**
    * @brief Estimates the number of distinct field values 
    * in the table, which is obtainable from the statistics 
    * manager.
    */
    int GetDistinctVals(const std::string &field_name) override {
        return info_.GetDistinctVals(field_name);
    }

    /**
    * Determines the schema of the table,
    * which is obtainable from the catalog manager.
    * @see simpledb.plan.Plan#schema()
    */
    Schema GetSchema() override {
      return layout_.GetSchema();
    }


private:

    std::string table_name_;
    
    // we need txn and layout to create a tablescan object
    Transaction *txn_;

    Layout layout_;

    StatInfo info_;

};

} // namespace SimpleDB


#endif

