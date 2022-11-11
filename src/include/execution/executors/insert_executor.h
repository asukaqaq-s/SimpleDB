#ifndef INSERT_EXECUTOR_H
#define INSERT_EXECUTOR_H

#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"

#include <memory>

namespace SimpleDB {

/**
 * @brief 
 * InsertExecutor. Check InsertPlan for more details
 */
class InsertExecutor : public AbstractExecutor {
public:

    InsertExecutor(ExecutionContext *context, AbstractPlan *node, std::unique_ptr<AbstractExecutor> &&child)
        : AbstractExecutor(context, node),
          child_(std::move(child)) {}

    void Init() override;

    bool Next(Tuple *tuple) override;

private:
    // helper function


    // stored the pointer to table metadata to avoid additional indirection
    TableInfo *table_info_;
    
    // cache the table schema
    Schema *table_schema_;
    
    // child executor, could be null when we are inserting raw values
    std::unique_ptr<AbstractExecutor> child_;

    // cache txn
    Transaction* txn_;

};

} // namespace SimpleDB


#endif