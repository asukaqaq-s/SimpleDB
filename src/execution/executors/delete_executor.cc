#ifndef DELETE_EXECUTOR_CC
#define DELETE_EXECUTOR_CC

#include "execution/executors/delete_executor.h"
#include "execution/plans/delete_plan.h"


namespace SimpleDB {

// a simple SQL statement that implements deletion format
// DELETE FROM table_name WHERE (predicate)
// in our implementation, we can generate the executor tree as following
// 
//                   DELETE EXECUTOR
//                          |
//                          |
//                   SEQSCAN EXECUTOR(only one table)
// 
void DeleteExecutor::Init() {
    
    // init seqscan executor
    child_->Init();

    // initialize metadata that we will use while performing updation
    auto node = GetPlanNode<DeletePlan>();
    txn_ = content_->GetTransactionContext();
    table_info_ = content_->GetMetadataMgr()->GetTable(node.table_name_, txn_);
    table_schema_ = &table_info_->schema_;
    
}

bool DeleteExecutor::Next(Tuple *tuple) {
    Tuple tmp;

    if (child_->Next(&tmp)) {
        table_info_->table_heap_->Delete(txn_, tmp.GetRID());
        return true;
    }
    return false;
}

} // namespace SimpleDB


#endif