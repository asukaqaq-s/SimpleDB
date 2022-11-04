#ifndef DELETE_EXECUTOR_H
#define DELETE_EXECUTOR_H

#include "execution/executors/abstract_executor.h"
#include "concurrency/transaction.h"
#include "record/tuple.h"

namespace SimpleDB {

class DeleteExecutor : public AbstractExecutor {

public:


    /**
    * @brief Construct a new Delete Executor object
    * 
    * @param context execution context
    * @param node corresponding plan node
    * @param child child executor, always seq_scan executor
    */
    DeleteExecutor(ExecutionContext *content, AbstractPlan *node, std::unique_ptr<AbstractExecutor> &&child)
        : AbstractExecutor(content, node),
          child_(std::move(child)) {}

    void Init() override;

    bool Next(Tuple *tuple) override;

private:

    // child executor
    std::unique_ptr<AbstractExecutor> child_;

    // stored the pointer to table metadata to avoid additional indirection
    TableInfo *table_info_;
    
    // cache the table schema
    Schema *table_schema_;
    
    // caching all the indexes that we need to insert
    // std::vector<IndexInfo *> indexes_;

    // cache txn to avoid indirection
    Transaction *txn_;
    
};


} // namespace SimpleDB



#endif