#ifndef UPDATE_EXECUTOR_H
#define UPDATE_EXECUTOR_H


#include "execution/executors/abstract_executor.h"
#include "plans/update_plan.h"

#include <memory>

namespace SimpleDB {

/**
* @brief 
* UpdateExecutor, check UpdatePlan for more details.
*/
class UpdateExecutor : public AbstractExecutor {


public:
    
    
    /**
    * @brief Construct a new Update Executor object.
    * 
    * @param context execution context
    * @param node plan node corresponding to current executor
    * @param child child executor
    */
    UpdateExecutor(ExecutionContext *context, AbstractPlan *node, std::unique_ptr<AbstractExecutor> &&child)
        : AbstractExecutor(context, node),
          child_(std::move(child)) {}
        

    void Init() override;


    bool Next(Tuple *tuple) override;

private:

    // helper function to generate new tuple
    Tuple GenerateUpdatedTuple(const Tuple &tuple);

    // child executor
    std::unique_ptr<AbstractExecutor> child_;
    
    // stored the pointer to table metadata to avoid additional indirection
    TableInfo *table_info_;
    
    // cache the table schema
    Schema *table_schema_;

    // cache txn
    Transaction *txn_;

};



} // namespace SimpleDB


#endif