#ifndef SEQ_SCAN_EXECUTOR_H
#define SEQ_SCAN_EXECUTOR_H

#include "execution/executors/abstract_executor.h"
#include "plans/seq_scan_plan.h"

namespace SimpleDB {

/**
 * @brief 
 * Execute a sequential scan over a table
 */
class SeqScanExecutor : public AbstractExecutor {
public:
    SeqScanExecutor(ExecutionContext *context, AbstractPlan *node)
        : AbstractExecutor(context, node) {
        SIMPLEDB_ASSERT(node->GetType() == PlanType::SeqScanPlan, "Invalid plan type");
    }

    void Init() override;

    bool Next(Tuple *tuple) override;


private:

    // stored the pointer to table metadata to avoid additional indirection
    TableInfo *table_info_;
    // iterator used to scan table
    TableIterator iterator_;
    // cache the table schema
    Schema *table_schema_;
    // cache txn
    Transaction *txn_;    

};



} // namespace SimpleDB

#endif